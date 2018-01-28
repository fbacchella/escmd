from asyncio import coroutine

from eslib.verb import List, DumpVerb
from eslib.dispatcher import dispatcher, command, Dispatcher


@dispatcher(object_name="node")
class NodesDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-n", "--nodes", dest="nodes", help="nodes filter")

    @coroutine
    def get(self, nodes=None):
        nodes = yield from self.api.escnx.nodes.info(nodes)
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
