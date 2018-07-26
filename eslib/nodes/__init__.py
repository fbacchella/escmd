from asyncio import coroutine

from eslib.verb import List, DumpVerb, CatVerb
from eslib.dispatcher import dispatcher, command, Dispatcher

import json

@dispatcher(object_name="node")
class NodesDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-n", "--node", dest="node_name", help="nodes filter")
        parser.add_option("-l", "--locale", dest="local_node", help="Command runs on local node", default=False, action='store_true')

    @coroutine
    def check_noun_args(self, running, local_node=False, node_name=''):
        if local_node:
            node_name = '_all'
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
        if 'attributes' in value: value.pop('attributes')
        if 'build_hash' in value: value.pop('build_hash')
        name = value.pop('name')
        return "%s %s" % (name, value)


@command(NodesDispatcher, verb='dump')
class NodesDump(DumpVerb):

    def fill_parser(self, parser):
        super().fill_parser(parser)

    @coroutine
    def check_verb_args(self, running, *args, metrics=[], **kwargs):
        running.metrics = None
        yield from super().check_verb_args(running, *args, **kwargs)


@command(NodesDispatcher, verb='stats')
class NodesStats(DumpVerb):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-m", "--metric", dest="metrics", default=[], action='append')

    @coroutine
    def check_verb_args(self, running, *args, metrics=[], **kwargs):
        running.metrics = ','.join(metrics)
        yield from super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def get(self, running):
        val = yield from self.api.escnx.nodes.info(node_id=running.node_name, metric='settings')
        nodes = {}
        for k,v in val['nodes'].items():
            nodes[k] = v['name']
        return nodes

    @coroutine
    def action(self, element, running, *args, only_keys=False, **kwargs):
        val = yield from self.api.escnx.nodes.stats(element[0], metric=running.metrics, level='node')
        return val

    def to_str(self, running, value):
        for k, v in value[1]['nodes'].items():
            yield "%s: %s" % (value[0][1], json.dumps(v, **running.formatting))


@command(NodesDispatcher)
class NodesCat(CatVerb):

    def fill_parser(self, parser):
        super(NodesCat, self).fill_parser(parser)
        parser.add_option("-l", "--local", dest="local", default=False, action='store_true')

    @coroutine
    def check_verb_args(self, running, *args, local=False, **kwargs):
        running.local = local
        yield from super().check_verb_args(running, *args, **kwargs)

    def get_source(self):
        return self.api.escnx.cat.nodes
