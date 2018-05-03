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
        if running.node_name is None or len(running.node_name) == 0:
            node_name = '*'
        else:
            node_name = running.node_name
        nodes = yield from self.api.escnx.nodes.info(node_name, metric=running.metrics)
        return nodes['nodes']


@command(NodesDispatcher)
class NodesList(List):

    @coroutine
    def check_verb_args(self, running, *args, **kwargs):
        running.metrics = 'host'
        yield from super().check_verb_args(running, *args, **kwargs)

    def to_str(self, running, item):

        value = item[1]
        value.pop('attributes')
        value.pop('build_hash')
        return "%s %s" % (value['name'], value)


@command(NodesDispatcher, verb='dump')
class NodesDump(DumpVerb):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-m", "--metric", dest="metrics", default=[], action='append')

    @coroutine
    def check_verb_args(self, running, *args, metrics=[], **kwargs):
        running.metrics = ','.join(metrics)
        yield from super().check_verb_args(running, *args, **kwargs)


@command(NodesDispatcher)
class PluginsCat(CatVerb):

    def get_source(self):
        return self.api.escnx.cat.nodes
