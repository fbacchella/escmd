from asyncio import coroutine

from eslib.verb import List, DumpVerb, CatVerb
from eslib.dispatcher import dispatcher, command, Dispatcher


@dispatcher(object_name="node")
class NodesDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-n", "--node", dest="node_name", help="nodes filter")

    @coroutine
    def check_noun_args(self, running, node_name=None):
        running.node_name = node_name

    @coroutine
    def get(self, running):
        nodes = yield from self.api.escnx.nodes.info(running.node_name)
        return nodes['nodes']


@command(NodesDispatcher)
class NodesList(List):

    def to_str(self, running, item):
        name = item[0]
        value = item[1]
        return "%s %s '%s'" % (name, value['host'], ', '.join(value['roles']))


@command(NodesDispatcher, verb='dump')
class NodesDump(DumpVerb):
    pass


@command(NodesDispatcher)
class PluginsCat(CatVerb):

    def get_source(self):
        return self.api.escnx.cat.nodes
