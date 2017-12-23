import pycurl
from io import BytesIO
from enum import IntEnum
import json
import re
import queue
import sys
import collections

content_type_re = re.compile("(?P<content_type>.*?); (?:charset=(?P<charset>.*))")

class CurlDebugType(IntEnum):
    TEXT = 1
    HEADER = 2
    DATA = 4
    SSL = 8


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
    if content_type == 'application/json':
        decoded = json.loads(body)
    else:
        decoded = body
    return (content_type, decoded)


class ElasticConnection(object):

    def __init__(self, base="http://localhost:9200", debug_filter=CurlDebugType.HEADER + CurlDebugType.DATA, insecure=False, verbose=False, ca_file=None, kerberos=None):
        self._curl = pycurl.CurlMulti()

        # Activate share settings
        self._share = pycurl.CurlShare()
        self._share.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_COOKIE)
        self._share.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_DNS)
        self._share.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_SSL_SESSION)

        self._base = base
        self.debug_filter = debug_filter
        self.handles = set()
        self.waiting_handles = queue.Queue(1000)

        self.insecure = insecure
        self.verbose = verbose
        self.ca_file = ca_file
        self.kerberos = kerberos

    def get_curl(self):
        new_curl = pycurl.Curl()

        settings = {
            # Don't handle signals
            pycurl.NOSIGNAL: True,
            # Don't keep persistent data
            pycurl.COOKIEFILE: '/dev/null',
            pycurl.COOKIEJAR: '/dev/null',
            pycurl.SHARE: self._share,

            # Follow redirect but not too much, it's needed for CAS
            pycurl.FOLLOWLOCATION: True,
            pycurl.MAXREDIRS: 5,

            # Strict TLS check
            pycurl.SSL_VERIFYPEER: not self.insecure,
            pycurl.SSL_VERIFYHOST: 2,
            pycurl.CAINFO: self.ca_file,

            # Debug setup
            pycurl.VERBOSE: self.verbose,
            pycurl.DEBUGFUNCTION: self._curl_debug,

            # Needed for SPNEGO authentication
            pycurl.HTTPAUTH: pycurl.HTTPAUTH_NEGOTIATE,
            pycurl.USERPWD: ':',

            # Needed for CAS login
            pycurl.UNRESTRICTED_AUTH: True,
            pycurl.POSTREDIR: pycurl.REDIR_POST_ALL,
            pycurl.USERAGENT: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/603.2.5 (KHTML, like Gecko) Version/10.1.1 Safari/603.2.5 pycurl'

        }
        if self.ca_file is not None:
            settings['pycurl.CAINFO'] = self.ca_file

        if self.kerberos:
            settings.update({
                pycurl.HTTPAUTH: pycurl.HTTPAUTH_NEGOTIATE,
                pycurl.USERPWD: ':'
            })

        for key, value in settings.items():
            try:
                new_curl.setopt(key, value)
            except TypeError:
                print(key, value)

        # Prepare headers:
        header_lines = [
            'Accept: application/json',
            'Expect:',
        ]
        new_curl.setopt(pycurl.HTTPHEADER, header_lines)

        return new_curl

    def doquery(self, indexes=None, action=None, request_body=None, verbose=None, finish_cb=None, request=None):
        if verbose is None:
            verbose = self.verbose

        handler = self.get_curl()
        if isinstance(indexes, collections.Iterable) and not isinstance(indexes, str):
            indexes = ','.join(indexes)
        url = '/'.join(filter(lambda x: x is not None, [self._base,  indexes, action if action is not None else '']))
        handler.setopt(pycurl.URL, url)
        handler.setopt(pycurl.VERBOSE, verbose)

        # A buffer to store the content
        buffer = BytesIO()
        handler.setopt(pycurl.WRITEDATA, buffer)
        handler.buffer = buffer

        # The possible body of a request
        if request_body is not None:
            handler.setopt(pycurl.POSTFIELDS, request_body)

        # Set after pycurl.POSTFIELDS to ensure that the request is the wanted one
        if request is not None:
            handler.setopt(pycurl.CUSTOMREQUEST, request)

        # A callback closure to store headers in the provided hash
        headers = {}
        def header_handler(header_line):
            return self.header_function(headers, header_line)
        handler.setopt(pycurl.HEADERFUNCTION, header_handler)
        handler.headers = headers

        # If a callback is given, store it in a closure function
        if finish_cb is not None:
            handler.cb = lambda: finish_cb(handler, headers, buffer)
        else:
            handler.cb = None

        self.waiting_handles.put(handler)

    def _curl_debug(self, debug_type, data):
        """
        This is the implementation of the cURL debug callback.
        :param debug_type:  what kind of data to debug.
        :param data: the data to debug.
        """

        prefix = {pycurl.INFOTYPE_TEXT: '   ' if CurlDebugType.TEXT & self.debug_filter else False,
                  pycurl.INFOTYPE_HEADER_IN: '<  ' if CurlDebugType.HEADER & self.debug_filter else False,
                  pycurl.INFOTYPE_HEADER_OUT: '>  ' if CurlDebugType.HEADER & self.debug_filter else False,
                  pycurl.INFOTYPE_DATA_IN: '<< ' if CurlDebugType.DATA & self.debug_filter else False,
                  pycurl.INFOTYPE_DATA_OUT: '>> ' if CurlDebugType.DATA & self.debug_filter else False,
                  pycurl.INFOTYPE_SSL_DATA_IN: '<S ' if CurlDebugType.SSL & self.debug_filter else False,
                  pycurl.INFOTYPE_SSL_DATA_OUT: '>S ' if CurlDebugType.SSL & self.debug_filter else False
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
            print("%s%s" % (prefix, line), file=sys.stderr)

    def close(self):
        self._curl.close()
        self._share.close()

    def header_function(self, headers, header_line):
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

    def _perform_loop(self):
        # I don't understand this code, but pycurl says it works this way
        # so I keep it
        while True:
            self._try_load_queries()
            ret, num_handles = self._curl.perform()
            if ret != pycurl.E_CALL_MULTI_PERFORM: break
        return ret, num_handles

    def _try_load_queries(self):
        """
        Take as much as possible for the waiting handles queue, but don't send
        too much of them
        :return: None
        """
        while len(self.handles) < 30 and self.waiting_handles.qsize() > 0:
            try:
                handler = self.waiting_handles.get(block=False)
                # needed to keep reference count
                self.handles.add(handler)
                self._curl.add_handle(handler)
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
            if self._curl.select(timeout) != -1:
                # some handles to process
                (waiting, succed, failed) = self._curl.info_read()
                for handle in succed:
                    self.handles.remove(handle)
                    status = handle.getinfo(pycurl.RESPONSE_CODE)
                    if status >= 200 and status < 300 and handle.cb is not None:
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
            self._try_load_queries()
            # Nothing is waiting any more or is processed, finished
            if num_handles == 0 and len(self.handles) == 0 and self.waiting_handles.qsize() == 0:
                break
