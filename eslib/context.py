from configparser import ConfigParser
from elasticsearch import Elasticsearch
from eslib.pycurlconnection import PyCyrlConnection, CurlDebugType, PyCyrlMuliHander
from eslib.asynctransport import AsyncTransport
from asyncio import get_event_loop, new_event_loop, ensure_future, wait, FIRST_COMPLETED

import copy

class ConfigurationError(Exception):
    def __init__(self, value):
        super(Exception, self).__init__(value)
        self.error_message = value

    def __str__(self):
        return self.value.__str__


class Context(object):
    # The settings that store boolean values
    boolean_options = {'api': frozenset(['debug', 'kerberos', 'sniff']), 'logging': {}, 'kerberos': {}, 'ssl': frozenset(['verify_certs'])}

    # mapping from command line options to configuration options:
    arg_options = {'debug': ['api', 'debug'],
                   'username': ['api', 'username'],
                   'passwordfile': ['api', 'passwordfile'],
                   'kerberos': ['api', 'kerberos'],
                   'url': ['api', 'url']}

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
            'max_active': 10
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
            'ca_file': None,
            'verify_certs': True,
            'cert_file': None,
            'cert_type': 'PEM',
            'key_file': None,
            'key_type': 'PEM',
            'key_password': None,
        }
    }

    def __init__(self, config_file=None, **kwargs):
        super(Context, self).__init__()
        self.connected = False
        explicit_user = 'password' in kwargs or 'passwordfile' in kwargs or 'username' in kwargs
        explicit_kerberos = 'kerberos' in kwargs and kwargs.get('kerberos')
        if explicit_user and explicit_kerberos:
            raise ConfigurationError('both kerberos and login/password authentication requested')

        if 'url' in kwargs and 'sniff' not in kwargs:
            kwargs['sniff'] = False

        config = ConfigParser()
        if config_file is not None:
            config.read(config_file, encoding='utf-8')

        # Prepare the configuration with default settings
        self.current_config = copy.copy(Context.default_settings)

        # Read the configuration
        if len(config.sections()) != 0:
            for section in config.sections():
                for k,v in config.items(section):
                    if k in Context.boolean_options[section]:
                        self.current_config[section][k] = config.getboolean(section, k)
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
        elif explicit_kerberos:
            self.current_config['api']['username'] = None
            self.current_config['api']['password'] = None

        if self.current_config['api']['kerberos'] and self.current_config['kerberos'].get('keytab', None) is not None:
            import gssapi
            import os

            ccache = self.current_config['kerberos']['ccache']
            keytab = self.current_config['kerberos']['keytab']
            kname = self.current_config['kerberos']['principal']
            if kname is not None:
                kname = gssapi.Name(kname)

            gssapi.creds.Credentials(name=kname, usage='initiate', store={'ccache': ccache, 'client_keytab': keytab})
            os.environ['KRB5CCNAME'] = ccache
            self.current_config['api']['kerberos'] = True

        if self.current_config['api']['url'] == None:
            raise ConfigurationError('incomplete configuration, Elastic url not found')
        if self.current_config['api']['username'] is None and self.current_config['api']['kerberos'] is None:
            raise ConfigurationError('not enought authentication informations')

        if self.current_config['logging']['filters'] is not None and self.current_config['api']['debug']:
            self.filter = 0
            filters = [x.strip() for x in self.current_config['logging']['filters'].split(',')]
            for f in filters:
                self.filter |= CurlDebugType[f.upper()]

    def connect(self, parent=None):
        if parent is not None:
            self.multi_handle = parent.multi_handle
            self.curl_perform_task = parent.curl_perform_task
            self.loop = parent.loop
        else:
            self.loop = new_event_loop()
            self.multi_handle = PyCyrlMuliHander(self.current_config['api']['max_active'], loop=self.loop)
            self.curl_perform_task = None

        cnxprops={'multi_handle': self.multi_handle}
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
        verify_certs = self.current_config['ssl'].pop('verify_certs')
        self.escnx = Elasticsearch(self.current_config['api']['url'],
                                   transport_class=AsyncTransport,
                                   connection_class=PyCyrlConnection,
                                   verify_certs=verify_certs,
                                   ssl_opts=self.current_config['ssl'],
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
