from elasticsearch import Connection
import pycurl
from io import BytesIO
from elasticsearch.compat import urlencode
import re
import sys
from enum import IntEnum
import time
from logging import Logger
import queue


def get_header_function(headers):
    def header_function(header_line):
        # HTTP standard specifies that headers are encoded in iso-8859-1.
        header_line = header_line.decode('iso-8859-1')

        # Header lines include the first status line (HTTP/1.x ...).
        # We are going to ignore all lines that don't have a colon in them.
        # This will botch headers that are split on multiple lines...
        if ':' not in header_line:
            return

        # Break the header line into header name and value.
        name, value = header_line.split(':', 1)

        # Remove whitespace that may be present.
        # Header lines include the trailing newline, and there may be whitespace
        # around the colon.
        name = name.strip()
        value = value.strip()

        # Header names are case insensitive.
        # Lowercase name here.
        name = name.lower()

        # Now we can actually record the header name and value.
        headers[name] = value
    return header_function

content_type_re = re.compile("(?P<content_type>.*?); (?:charset=(?P<charset>.*))")

def decode_body(handler, headers, body):
    encoding = 'UTF-8'
    content_type = None
    if 'content-type' in headers:
        content_type_header = headers['content-type']
    else:
        try:
            content_type_header = handler.get_info(pycurl.CONTENT_TYPE)
        except AttributeError:
            content_type_header = None
    if content_type_header is not None:
        result = content_type_re.match(content_type_header)
        if result is not None:
            encoding = result.group('charset')
            content_type = result.group('content_type')

    body = body.getvalue().decode(encoding)
    return (content_type, body)


class CurlDebugType(IntEnum):
    TEXT = 1
    HEADER = 2
    DATA = 4
    SSL = 8


def get_curl_debug(debug_filter, logger):
    def _curl_debug(debug_type, data):
        """
        This is the implementation of the cURL debug callback.
        :param debug_type:  what kind of data to debug.
        :param data: the data to debug.
        """

        prefix = {pycurl.INFOTYPE_TEXT: '   ' if CurlDebugType.TEXT & debug_filter else False,
                  pycurl.INFOTYPE_HEADER_IN: '<  ' if CurlDebugType.HEADER & debug_filter else False,
                  pycurl.INFOTYPE_HEADER_OUT: '>  ' if CurlDebugType.HEADER & debug_filter else False,
                  pycurl.INFOTYPE_DATA_IN: '<< ' if CurlDebugType.DATA & debug_filter else False,
                  pycurl.INFOTYPE_DATA_OUT: '>> ' if CurlDebugType.DATA & debug_filter else False,
                  pycurl.INFOTYPE_SSL_DATA_IN: '<S ' if CurlDebugType.SSL & debug_filter else False,
                  pycurl.INFOTYPE_SSL_DATA_OUT: '>S ' if CurlDebugType.SSL & debug_filter else False
                  }[debug_type]
        if prefix is False:
            return

        # Some versions of PycURL provide the debug data as strings, and
        # some as arrays of bytes, so we need to check the type of the
        # provided data and convert it to strings before trying to
        # manipulate it with the "replace", "strip" and "split" methods:
        if not debug_type == pycurl.INFOTYPE_SSL_DATA_IN and not pycurl.INFOTYPE_SSL_DATA_OUT:
            text = data.decode('utf-8') if type(data) == bytes else data
        else:
            text = data.decode('unicode_escape') if type(data) == bytes else data

        # Split the debug data into lines and send a debug message for
        # each line:
        lines = [x for x in text.replace('\r\n', '\n').split('\n') if len(x) > 0]
        for line in lines:
            if isinstance(logger, Logger):
                logger.debug("%s%s" % (prefix, line))
            else:
                print("%s%s" % (prefix, line), file=logger)
    return _curl_debug


class PyCyrlMuliHander(object):

    def __init__(self, max_query=10):
        self.multi = pycurl.CurlMulti()
        self.share = pycurl.CurlShare()

        self.share.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_COOKIE)
        self.share.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_COOKIE)
        self.share.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_DNS)
        self.share.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_SSL_SESSION)

        self.handles = set()
        self.waiting_handles = queue.Queue(1000)

        self.max_query = max_query

    def query(self, handle, callback, headers, buffer):
        handle.cb = callback
        def manage_callback():
            status = handle.getinfo(pycurl.RESPONSE_CODE)

            handle.close()

            (content_type, body) = decode_body(handle, headers, buffer)
            callback(status, headers, body)

        handle.cb = manage_callback

        if self.handles < self.max_query:
            self.handles.add(handle)
            self.multi.add_handle(handle)
        else:
            self.waiting_handles.put(handle)

    def close(self):
        self.multi.close()
        self.share.close()

    def _perform_loop(self):
        # I don't understand this code, but pycurl says it works this way
        # so I keep it
        while True:
            self._try_load_queries()
            ret, num_handles = self.multi.perform()
            if ret != pycurl.E_CALL_MULTI_PERFORM: break
        return ret, num_handles

    def _try_load_queries(self):
        """
        Take as much as possible for the waiting handles queue, but don't send
        too much of them
        :return: None
        """
        while len(self.handles) < self.max_query and self.waiting_handles.qsize() > 0:
            try:
                handler = self.waiting_handles.get(block=False)
                # needed to keep reference count
                self.handles.add(handler)
                self.multi.add_handle(handler)
            except queue.Empty:
                break

    def perform(self, timeout=1.0):
        """
        Loop on waiting handles to process them until they are no more waiting one and all send are finished
        :param timeout: the timeout for the loop
        :return: Nothing
        """
        while True:
            ret, num_handles = self._perform_loop()
            if self.multi.select(timeout) != -1:
                # some handles to process
                (waiting, succed, failed) = self.multi.info_read()
                for handle in succed:
                    self.handles.remove(handle)
                    status = handle.getinfo(pycurl.RESPONSE_CODE)
                    if status >= 200 and status < 300:
                        handle.cb()
                    elif status >= 300:
                        content_type, decoded = decode_body(handle, handle.headers, handle.buffer)
                        if content_type == 'application/json' and 'error' in decoded:
                            message = decoded['error']
                        else:
                            message = decoded
                        print("http failed %s: %d\n    %s" % (handle.getinfo(pycurl.EFFECTIVE_URL), status, message), file=sys.stderr)
                for handle, code, message in failed:
                    self.handles.remove(handle)
                    print("pycurl failed %s: %d" % (handle.getinfo(pycurl.EFFECTIVE_URL), code), file=sys.stderr)
            # Nothing is waiting any more or is processed, finished
            if num_handles == 0 and len(self.handles) == 0 and self.waiting_handles.qsize() == 0:
                break

multi_handle = PyCyrlMuliHander()

class PyCyrlConnection(Connection):
    """
     Default connection class using the `urllib3` library and the http protocol.

     :arg host: hostname of the node (default: localhost)
     :arg port: port to use (integer, default: 9200)
     :arg url_prefix: optional url prefix for elasticsearch
     :arg timeout: default timeout in seconds (float, default: 10)
     :arg http_auth: optional http auth information as either ':' separated
         string or a tuple
     :arg kerberos: can use kerberos ticket for authentication
     :arg user_agent: user_agent to use, needed because CAS is picky about that
     :arg use_ssl: use ssl for the connection if `True`
     :arg verify_certs: whether to verify SSL certificates
     :arg ca_certs: optional path to CA bundle.
     :arg debug: activate curl debuging
     :arg debug_filter: sum of curl filters, for debuging
     :arg logger: debug output, can be a file or a logging.Logger
     """
    #self, host = 'localhost', port = 9200, use_ssl = False, url_prefix = '', timeout = 10, scheme = 'http',
    #verify_certs = True, ca_certs = None,
    #
    #debug_filter = CurlDebugType.HEADER + CurlDebugType.DATA, debug = False, logger = sys.stderr,
    #** kwargs


    def __init__(self,
                 http_auth=None, kerberos=False, user_agent="eslib",
                 use_ssl=False, verify_certs=False, ca_certs=None,
                 debug=False, debug_filter=CurlDebugType.HEADER + CurlDebugType.DATA, logger=sys.stderr,
                 **kwargs):
        super(PyCyrlConnection, self).__init__(use_ssl=use_ssl, **kwargs)

        if use_ssl:
            self.use_ssl = True
            self.verify_certs = verify_certs
            self.ca_certs = ca_certs
        else:
            self.use_ssl = False
        self.kerberos = kerberos
        self.user_agent = user_agent
        if debug:
            self.debug = True
            self.debug_filter = get_curl_debug(debug_filter, logger)
        else:
            self.debug = False

    def _get_curl_handler(self):
        handle = pycurl.Curl()

        settings = {
            pycurl.USERAGENT: self.user_agent,
            pycurl.ACCEPT_ENCODING: "",
            #pycurl.NOSIGNAL: True,
            # We manage ourself our buffer, Help from Nagle is not needed
            pycurl.TCP_NODELAY: 1,
            # ES connections are long, keep them alive to be firewall-friendly
            pycurl.TCP_KEEPALIVE: 1,
            # Don't keep persistent data
            pycurl.COOKIEFILE: '/dev/null',
            pycurl.COOKIEJAR: '/dev/null',
            pycurl.SHARE: multi_handle.share,

            # Follow redirect but not too much, it's needed for CAS
            pycurl.FOLLOWLOCATION: True,
            pycurl.MAXREDIRS: 5,

            # Needed for CAS login
            pycurl.UNRESTRICTED_AUTH: True,
            pycurl.POSTREDIR: pycurl.REDIR_POST_ALL,
        }
        if self.use_ssl:
            # Strict TLS check
            if self.verify_certs:
                settings.update({
                    pycurl.SSL_VERIFYPEER: 1,
                    pycurl.SSL_VERIFYHOST: 2,
                })
            if self.ca_certs is not None:
                settings[pycurl.CAINFO] = self.ca_certs

        if self.kerberos:
            settings.update({
                pycurl.HTTPAUTH: pycurl.HTTPAUTH_NEGOTIATE,
                pycurl.USERPWD: ':'
            })

        # Debug setup
        if self.debug:
            settings.update({
                pycurl.VERBOSE: self.debug,
                pycurl.DEBUGFUNCTION: self.debug_filter
            })

        for key, value in settings.items():
            try:
                handle.setopt(key, value)
            except TypeError as e:
                print(e, key, value)

        # Prepare headers:
        header_lines = [
            'Accept: application/json',
            'Connection: keep-alive',
            # ES is not found of Expect
            'Expect:',
        ]
        handle.setopt(pycurl.HTTPHEADER, header_lines)

        return handle

    def perform_request(self, method, url, params=None, body=None, timeout=None, headers={}, ignore=(), callback=None):
        url = self.url_prefix + url
        if params is not None:
            url = '%s?%s' % (url, urlencode(params))
        full_url = self.host + url
        curl_handle = self._get_curl_handler()
        curl_handle.setopt(pycurl.URL, full_url)

        if method == 'HEAD':
            curl_handle.setopt(pycurl.NOBODY, True)

        # Prepare the headers callback
        headers = {}
        curl_handle.setopt(pycurl.HEADERFUNCTION, get_header_function(headers))

        # Prepare the body buffer
        buffer = BytesIO()
        curl_handle.setopt(pycurl.WRITEDATA, buffer)

        # The possible body of a request
        if body is not None:
            curl_handle.setopt(pycurl.POSTFIELDS, body)
        # Set after pycurl.POSTFIELDS to ensure that the request is the wanted one
        curl_handle.setopt(pycurl.CUSTOMREQUEST, method)

        if callback is not None:
            curl_handle.connection = self
            multi_handle.query(curl_handle, callback, buffer, headers)
        else:
            start = time.time()
            curl_handle.perform()
            duration = time.time() - start

            status = curl_handle.getinfo(pycurl.RESPONSE_CODE)
            curl_handle.close()

            (content_type, body) = decode_body(curl_handle, headers, buffer)

            if not (200 <= status < 300) and status not in ignore:
                self.log_request_fail(method, url, buffer.getvalue(), duration, status, body)
                self._raise_error(status, body, content_type)

            self.log_request_success(method, full_url, url, buffer.getvalue(), status,
                                     body, duration)

            return status, headers, body

    def close(self):
        pass

