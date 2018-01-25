from eslib.dispatcher import dispatcher, command, Dispatcher
from eslib.verb import DumpVerb
from asyncio import coroutine
from json import dumps

@dispatcher(object_name="cluster")
class ClusterDispatcher(Dispatcher):
    pass

@command(ClusterDispatcher, verb='readsettings')
class IndiciesForceMerge(DumpVerb):

    def fill_parser(self, parser):
        parser.add_option("-k", "--only_keys", dest="only_keys", default=False, action='store_true')
        parser.add_option("-p", "--pretty", dest="pretty", default=False, action='store_true')

    @coroutine
    def get(self):
        val = yield from self.api.escnx.cluster.get_settings(include_defaults=True)
        return val
