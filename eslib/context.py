from configparser import ConfigParser
from elasticsearch import Elasticsearch
from eslib.pycurlconnection import PyCyrlConnection, CurlDebugType, PyCyrlMuliHander
from eslib.asynctransport import AsyncTransport
from asyncio import get_event_loop, new_event_loop, ensure_future, wait, FIRST_COMPLETED


class ConfigurationError(Exception):
    def __init__(self, value):
        super(Exception, self).__init__(value)
        self.error_message = value

    def __str__(self):
        return self.value.__str__


class Context(object):
    # The api settings that store boolean values
    api_booleans = frozenset(['debug', 'insecure', 'kerberos', 'sniff'])

    # default values for connection
    api_connect_settings = {
        'url': None,
        'sniff': True,
        'username': None,
        'password': None,
        'passwordfile': None,
        'ca_file': None,
        'verify_certs': True,
        'kerberos': False,
        'debug': False,
        'log': None,
        'user_agent': None,
        'max_active': 10
    }

    # default values for logging
    logging_settings = {
        'filters': 'header,data,text',
    }

    def __init__(self, config_file=None, **kwargs):
        super(Context, self).__init__()
        self.connected = False
        self.loop = None

        explicit_user = 'password' in kwargs or 'passwordfile' in kwargs or 'username' in kwargs
        explicit_kerberos = 'kerberos' in kwargs and kwargs.get('kerberos')
        if explicit_user and explicit_kerberos:
            raise ConfigurationError('both kerberos and login/password authentication requested')

        if 'url' in kwargs and 'sniff' not in kwargs:
            kwargs['sniff'] = False

        config = ConfigParser()
        if config_file is not None:
            config.read(config_file, encoding='utf-8')

        config_api = {}
        config_logging = {}
        config_kerberos = {}

        if len(config.sections()) != 0:
            for (k, v) in config.items("api"):
                if k in Context.api_booleans:
                    config_api[k] = config.getboolean('api', k)
                else:
                    config_api[k] = v

            if config.has_section('logging'):
                config_logging = {k: v for k, v in config.items('logging')}

            if config.has_section('kerberos'):
                config_kerberos = {k: v for k,v in config.items('kerberos')}

        # extract values from explicit arguments or else from config file
        for attr_name in Context.api_connect_settings.keys():
            if attr_name in kwargs:
                self.api_connect_settings[attr_name] = kwargs.pop(attr_name)
                # given in the command line
            elif attr_name in config_api:
                # given in the config file
                self.api_connect_settings[attr_name] = config_api[attr_name]

        # extract values from explicit arguments or else from config file
        for attr_name in Context.logging_settings.keys():
            if attr_name in config_logging:
                # given in the config file
                self.logging_settings[attr_name] = config_logging[attr_name]

        if 'passwordfile' in self.api_connect_settings and self.api_connect_settings['passwordfile'] is not None:
            passwordfilename = self.api_connect_settings.pop('passwordfile')
            with open(passwordfilename,'r') as passwordfilename:
                self.api_connect_settings['password'] = passwordfilename.read()

        if explicit_user:
            self.api_connect_settings['kerberos'] = False
        elif explicit_kerberos:
            self.api_connect_settings['username'] = None
            self.api_connect_settings['password'] = None

        if self.api_connect_settings['kerberos'] and config_kerberos.get('keytab', None) is not None:
            import gssapi
            import os

            ccache = config_kerberos.get('ccache', None)
            keytab = config_kerberos.get('keytab', None)
            kname = config_kerberos.get('principal', None)
            if kname is not None:
                kname = gssapi.Name(kname)

            gssapi.creds.Credentials(name=kname, usage='initiate', store={'ccache': ccache, 'client_keytab': keytab})
            os.environ['KRB5CCNAME'] = ccache
            self.api_connect_settings['kerberos'] = True

        if self.api_connect_settings['url'] == None:
            raise ConfigurationError('incomplete configuration, Elastic url not found')
        if self.api_connect_settings['username'] is None and self.api_connect_settings['kerberos'] is None:
            raise ConfigurationError('not enought authentication informations')

        if self.logging_settings.get('filters', None) is not None and self.api_connect_settings['debug']:
            self.filter = 0
            filters = [x.strip() for x in self.logging_settings['filters'].split(',')]
            for f in filters:
                self.filter |= CurlDebugType[f.upper()]

    def connect(self):
        self.loop = new_event_loop()
        self.loop.set_debug(True)
        self.multi_handle = PyCyrlMuliHander(self.api_connect_settings['max_active'], loop=self.loop)
        cnxprops={'multi_handle': self.multi_handle}
        if self.api_connect_settings['debug']:
            cnxprops.update({
                'debug': self.api_connect_settings['debug'],
                'debug_filter': self.filter
            })
        if self.api_connect_settings['sniff']:
            cnxprops.update({
                'sniff_on_start': True,
                'sniff_on_connection_fail': True,
                'sniffer_timeout': 60
            })
        else:
            cnxprops.update({
                'sniff_on_start': False
            })

        if self.api_connect_settings['user_agent'] is not None:
            cnxprops.update({
                'user_agent': self.api_connect_settings['user_agent'],
            })

        if self.api_connect_settings['username'] is not None and self.api_connect_settings['password'] is not None:
            http_auth = (self.api_connect_settings['username'], self.api_connect_settings['password'])
        else:
            http_auth = None
        self.escnx = Elasticsearch(self.api_connect_settings['url'],
                                   transport_class=AsyncTransport,
                                   connection_class=PyCyrlConnection,
                                   verify_certs=self.api_connect_settings['verify_certs'],
                                   ca_certs=self.api_connect_settings['ca_file'],
                                   kerberos=self.api_connect_settings['kerberos'],
                                   http_auth=http_auth, loop=self.loop,
                                   **cnxprops)
        self.curl_perform_task = ensure_future(self.multi_handle.perform(), loop=self.loop)
        return self.perform_query(self.escnx.ping())

    def perform_query(self, query):
        def looper():
            done, pending = yield from wait((query, self.curl_perform_task), loop=self.loop, return_when=FIRST_COMPLETED)
            return done, pending

        done, pending = self.loop.run_until_complete(looper())
        # done contain either a result/exception from run_phrase or an exception from multi_handle.perform()
        # In both case, the first result is sufficient
        for i in done:
            running = i.result()
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
