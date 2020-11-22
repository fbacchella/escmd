from configparser import ConfigParser
from elasticsearch import Elasticsearch
from eslib.asynctransport import AsyncTransport
from eslib.exceptions import resolve_exception
from elasticsearch.exceptions import TransportError
from asyncio import new_event_loop, ensure_future, wait, FIRST_COMPLETED
import copy
import urllib.parse

class ConfigurationError(Exception):
    def __init__(self, value):
        super(ConfigurationError, self).__init__(value)
        self.error_message = value

    def __str__(self):
        return self.value.__str__


class Context(object):
    # The settings that store boolean values
    boolean_options = {'api': frozenset(['debug', 'kerberos', 'sniff']), 'logging': {}, 'kerberos': {}, 'ssl': frozenset(['verify_certs']), 'pycurl': {}}

    # The settings that store integer values
    integer_options = {'api': frozenset(['timeout', 'max_active']), 'logging': {}, 'kerberos': {}, 'ssl': {}, 'pycurl': {}}

    # mapping from command line options to configuration options:
    arg_options = {'debug': ['api', 'debug'],
                   'username': ['api', 'username'],
                   'passwordfile': ['api', 'passwordfile'],
                   'kerberos': ['api', 'kerberos'],
                   'url': ['api', 'url'],
                   'transport_class': ['api', 'transport_class'],
                   'connection_class': ['api', 'connection_class'],
                   'sniff': ['api', 'sniff'],
                   'timeout': ['api', 'timeout']}

    # default values for connection
    default_settings = {
        'api': {
            'url': None,
            'sniff': True,
            'username': None,
            'password': None,
            'passwordfile': None,
            'kerberos': False,
            'debug': False,
            'log': None,
            'user_agent': 'eslib/pycurl',
            'max_active': 10,
            'timeout': 10,
            'transport_class': AsyncTransport,
            'connection_class': None,
        },
        'logging': {
            'filters': 'header,data,text',
        },
        'kerberos': {
            'ccache': None,
            'keytab': None,
            'principal': None,
        },
        'ssl': {
            'ca_certs_directory': None,
            'ca_certs_file': None,
            'verify_certs': True,
            'cert_file': None,
            'cert_type': None,
            'key_file': None,
            'key_type': None,
            'key_password': None,
        },
        'pycurl': {
            'libcurl_path': None,
            'pycurl_path': None
        }
    }

    def __init__(self, config_file=None, **kwargs):
        super(Context, self).__init__()
        self.connected = False

        # Check consistency of authentication setup
        explicit_user = 'password' in kwargs or 'passwordfile' in kwargs or 'username' in kwargs
        explicit_kerberos = 'kerberos' in kwargs and kwargs.get('kerberos')
        if explicit_user and explicit_kerberos:
            raise ConfigurationError('both kerberos and login/password authentication requested')

        if 'url' in kwargs and 'sniff' not in kwargs:
            kwargs['sniff'] = False

        config = ConfigParser()
        if config_file is not None:
                try:
                    with open(config_file, mode='rt', encoding='utf-8') as f:
                        config.read_file(f)
                except OSError as e:
                    raise ConfigurationError("Can't read configuration file '" + config_file + "': " + str(e))

            #print(config.read(config_file, encoding='utf-8'))

        # Prepare the configuration with default settings
        self.current_config = copy.deepcopy(Context.default_settings)

        # Read the configuration
        for section in config.sections():
            for k,v in config.items(section):
                if k in Context.boolean_options[section]:
                    self.current_config[section][k] = config.getboolean(section, k)
                elif k in Context.integer_options[section]:
                    self.current_config[section][k] = config.getint(section, k)
                else:
                    self.current_config[section][k] = v
        # extract values from explicit arguments or else from config file
        for arg_name, arg_destination in Context.arg_options.items():
            if arg_name in kwargs:
                self.current_config[arg_destination[0]][arg_destination[1]] = kwargs.pop(arg_name)

        passwordfilename = self.current_config['api'].pop('passwordfile')
        if passwordfilename is not None:
            with open(passwordfilename,'r') as passwordfile:
                self.current_config['api']['password'] = passwordfile.read()

        if explicit_user:
            self.current_config['api']['kerberos'] = False
            if 'ssl' in self.current_config:
                self.current_config['ssl']['cert_file'] = None
                self.current_config['ssl']['key_file'] = None
        elif explicit_kerberos:
            self.current_config['api']['username'] = None
            self.current_config['api']['password'] = None
            if 'ssl' in self.current_config:
                self.current_config['ssl']['cert_file'] = None
                self.current_config['ssl']['key_file'] = None

        if self.current_config['api']['kerberos'] and self.current_config.get('kerberos', {}).get('keytab', None) is not None:
            import gssapi
            import os

            ccache = self.current_config['kerberos']['ccache']
            keytab = self.current_config['kerberos']['keytab']
            kname = self.current_config['kerberos']['principal']
            if kname is not None:
                kname = gssapi.Name(kname)

            gssapi.creds.Credentials(name=kname, usage='initiate', store={'ccache': ccache, 'client_keytab': keytab})
            os.environ['KRB5CCNAME'] = ccache

        if self.current_config['api']['url'] == None:
            raise ConfigurationError('incomplete configuration, Elastic url not found')
        if self.current_config['api']['username'] is None and self.current_config['api']['kerberos'] is None:
            raise ConfigurationError('not enough authentication informations')

        if self.current_config['api']['connection_class'] == None:
            self.check_pycurl(**self.current_config['pycurl'])

            from eslib.pycurlconnection import CurlDebugType, PyCurlConnection, http_versions, version_info
            self.current_config['api']['connection_class'] = PyCurlConnection

        if self.current_config['api']['connection_class'] == PyCurlConnection:
            if self.current_config['logging']['filters'] is not None and self.current_config['api']['debug']:
                self.filter = 0
                filters = [x.strip() for x in self.current_config['logging']['filters'].split(',')]
                for f in filters:
                    self.filter |= CurlDebugType[f.upper()]

            if self.current_config['api']['kerberos'] and 'SPNEGO' not in version_info.features:
                raise ConfigurationError('Kerberos authentication requested, but SPNEGO is not available')

        connect_url = urllib.parse.urlparse(self.current_config['api']['url'])
        if connect_url[0] != 'https':
            self.current_config.pop('ssl')
        else:
            # Default file format for x509 identity is PEM
            if self.current_config['ssl'].get('cert_file', None) is not None and self.current_config['ssl'].get('cert_type', None) is None:
                self.current_config['ssl']['cert_type'] = 'PEM'
            if self.current_config['ssl'].get('key_file', None) is not None and self.current_config['ssl'].get('key_type', None) is None:
                self.current_config['ssl']['key_type'] = 'PEM'

    def check_pycurl(self, libcurl_path=None, pycurl_path=None):
        # Some distributions provides really old curl and pycurl
        # So a custom definition can be provided
        if libcurl_path is not None:
            from ctypes import cdll
            cdll.LoadLibrary(libcurl_path)

        if pycurl_path is not None:
            import importlib.util
            import sys
            pycurlspec = importlib.util.spec_from_file_location('pycurl', pycurl_path)
            sys.modules[pycurlspec.name] = pycurlspec.loader.load_module()

    def connect(self, parent=None):
        if parent is not None:
            self.multi_handle = parent.multi_handle
            self.curl_perform_task = parent.curl_perform_task
            self.loop = parent.loop
        else:
            from eslib.pycurlconnection import PyCurlMultiHander

            self.loop = new_event_loop()
            self.multi_handle = PyCurlMultiHander(self.current_config['api']['max_active'], loop=self.loop)
            self.curl_perform_task = None

        cnxprops={'multi_handle': self.multi_handle,
                  'timeout': self.current_config['api']['timeout']}
        if self.current_config['api']['debug']:
            cnxprops.update({
                'debug': self.current_config['api']['debug'],
                'debug_filter': self.filter
            })

        if self.current_config['api']['sniff']:
            cnxprops.update({
                'sniff_on_start': True,
                'sniff_on_connection_fail': True,
                'sniffer_timeout': 60
            })
        else:
            cnxprops.update({
                'sniff_on_start': False
            })

        if self.current_config['api']['user_agent'] is not None:
            cnxprops.update({
                'user_agent': self.current_config['api']['user_agent'],
            })

        if self.current_config['api']['username'] is not None and self.current_config['api']['password'] is not None:
            http_auth = (self.current_config['api']['username'], self.current_config['api']['password'])
        else:
            http_auth = None

        if 'ssl' in self.current_config:
            ssl_opts = self.current_config['ssl']
            use_ssl = True
            verify_certs = self.current_config['ssl'].pop('verify_certs')
        else:
            ssl_opts = None
            use_ssl = False
            verify_certs = False

        self.escnx = Elasticsearch(self.current_config['api']['url'],
                                   transport_class=self.current_config['api']['transport_class'],
                                   connection_class=self.current_config['api']['connection_class'],
                                   use_ssl=use_ssl, verify_certs=verify_certs, ssl_opts=ssl_opts,
                                   kerberos=self.current_config['api']['kerberos'],
                                   http_auth=http_auth, loop=self.loop,
                                   **cnxprops)
        if self.curl_perform_task is None:
            self.curl_perform_task = ensure_future(self.multi_handle.perform(), loop=self.loop)
        if parent is None:
            return self.perform_query(self.escnx.ping())
        else:
            return True

    def perform_query(self, query):
        def looper():
            done, pending = yield from wait((query, self.curl_perform_task), loop=self.loop, return_when=FIRST_COMPLETED)
            return done, pending

        done, pending = self.loop.run_until_complete(looper())
        # done contain either a result/exception from run_phrase or an exception from multi_handle.perform()
        # In both case, the first result is sufficient
        for i in done:
            try:
                running = i.result()
            except TransportError as e:
                raise resolve_exception(e)
            # If running is None, run_phrase excited with sys.exit, because of argparse
            if running is not None:
                return running

    def disconnect(self):
        if self.loop is not None:
            self.multi_handle.running = False
            self.loop.run_until_complete(self.curl_perform_task)
            self.loop.stop()
            self.loop.close()
            self.loop = None
        self.escnx = None
        self.connected = False
