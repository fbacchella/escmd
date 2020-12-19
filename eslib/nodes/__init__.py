from asyncio import coroutine

from eslib.verb import List, DumpVerb, CatVerb
from eslib.dispatcher import dispatcher, command, Dispatcher

import json

filter = []
for i in ('name', 'transport_address', 'ip', 'host', 'version', 'roles'):
    filter.append('nodes.*.%s' % i)
default_filter_path = ','.join(filter)


@dispatcher(object_name="node", default_filter_path='nodes.*.name')
class NodesDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-n", "--node", dest="node_name", help="nodes filter")

    def check_noun_args(self, running, node_name=None, **kwargs):
        running.node_name = node_name
        return super().check_noun_args(running, node_name=node_name, **kwargs)

    @coroutine
    def get(self, running, node_name='_all', local_node=None, filter_path=None):
        nodes = yield from self.api.escnx.nodes.info(node_name, filter_path=filter_path)
        return nodes['nodes']


@command(NodesDispatcher)
class NodesList(List):

    @coroutine
    def action(self, element, running, filter_path=None, **kwargs):
        return super().action(element, running, filter_path=default_filter_path, **kwargs)

    def to_str(self, running, item):
        value = list(item[1].values())[0]
        name = value.pop('name')
        return "%s %s" % (name, value)


@command(NodesDispatcher, verb='dump')
class NodesDump(DumpVerb):

    def fill_parser(self, parser):
        super().fill_parser(parser)

    def check_verb_args(self, running, *args, **kwargs):
        running.metrics = None
        return super().check_verb_args(running, *args, **kwargs)


@command(NodesDispatcher, verb='stats')
class NodesStats(DumpVerb):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-m", "--metric", dest="metrics", default=[], action='append')

    def check_verb_args(self, running, *args, metrics=[], **kwargs):
        running.metrics = ','.join(metrics)
        return super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def action(self, element, running, *args, only_keys=False, **kwargs):
        val = yield from self.api.escnx.nodes.stats(element[0], metric=running.metrics, level='node')
        return val

    def to_str(self, running, value):
        for k, v in value[1]['nodes'].items():
            yield "{\"%s\": %s}" % (value[0][1], json.dumps(v, **running.formatting))


@command(NodesDispatcher)
class NodesCat(CatVerb):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-b", "--bytes", dest="bytes", default=False, action='store_true')
        parser.add_option("--full_id", dest="full_id", default=False, action='store_true')
        parser.add_option("-l", "--local", dest="local", default=False, action='store_true')

    def get_source(self):
        return self.api.escnx.cat.nodes
