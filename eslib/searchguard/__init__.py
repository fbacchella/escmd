from eslib.dispatcher import dispatcher, Dispatcher, command
from eslib.verb import DumpVerb


@dispatcher(object_name="searchguard")
class SearchGuardDispatcher(Dispatcher):

    def get(self, running, **kwargs):
        return {}

    def fill_parser(self, parser):
        parser.add_option("-i", "--impersonate", dest="impersonate", help="Impersonate as", default=None)

    def check_noun_args(self, running, impersonate=None, **kwargs):
        running.impersonate = impersonate
        return super().check_noun_args(running, **kwargs)


    async def search_guard_query(self, running, method, url, headers={}, **kwargs):
        if running.impersonate is not None:
            headers['sg_impersonate_as'] = running.impersonate
        return await self.api.escnx.transport.perform_request(method, '/_searchguard' + url, headers=headers, **kwargs)


@command(SearchGuardDispatcher, verb='whoami')
class Authinfo(DumpVerb):

    async def get(self, running, **kwargs):
        return {}

    async def execute(self, running, **kwargs):
        return await self.dispatcher.search_guard_query(running, 'GET', '/authinfo')

    def to_str(self, running, item):
        return super().to_str(running, (None, (None, item)))


@command(SearchGuardDispatcher, verb='debug')
class Debug(DumpVerb):

    async def get(self, running, **kwargs):
        return {}

    async def execute(self, running, **kwargs):
        return await self.dispatcher.search_guard_query(running, 'GET', '/auth/debug')

    def to_str(self, running, item):
        return super().to_str(running, (None, (None, item)))


@command(SearchGuardDispatcher, verb='authtoken')
class AuthToken(DumpVerb):

    def fill_parser(self, parser):
        parser.add_option("-n", "--name", dest="name", help="Token name", default=None)
        parser.add_option("-e", "--expires", dest="expires", help="Expires after", default=None)
        parser.add_option("-f", "--freeze_privileges", dest="freeze_privileges", help="Freeze privileges", default=None, action='store_true')

    def check_verb_args(self, running, *args, name=None, expires=None, freeze_privileges=None, **kwargs):
        if name is None:
            raise Exception('Name is mandatory')
        else:
            running.name = name
        running.freeze_privileges = freeze_privileges
        running.expires = expires
        running.requested = {
                "index_permissions":
                    [
                        {
                            "index_patterns": ["*"],
                            "allowed_actions": ["*"]
                        }
                    ],
                "cluster_permissions": ["*"]
            }
        return super().check_verb_args(running, *args, **kwargs)

    async def get(self, running, **kwargs):
        return {}

    async def execute(self, running, **kwargs):
        body = {
            "name": running.name,
            "requested": running.requested,
        }
        if running.expires is not None:
            body['expires_after'] = running.expires
        if running.freeze_privileges is not None:
            body['freeze_privileges'] = running.freeze_privileges
        return await self.dispatcher.search_guard_query(running, 'POST', '/authtoken', body=body)

    def to_str(self, running, item):
        if 'token' in item:
            return item['token']
        else:
            return super().to_str(running, (None, (None, item)))

