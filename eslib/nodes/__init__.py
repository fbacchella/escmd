import asyncio
from asyncio import coroutine

from eslib.verb import List, DumpVerb, CatVerb, Verb
from eslib.dispatcher import dispatcher, command, Dispatcher
from elasticsearch.exceptions import RequestError

import json
from random import shuffle

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
        infos = yield from self.api.escnx.nodes.info(element[0], human=True, filter_path=['nodes.*.name', 'nodes.*.version',
                                                                                          'nodes.*.transport_address', 'nodes.*.roles'])
        return infos

    def to_str(self, running, item):
        value = list(item[1]['nodes'].items())[0][1]
        name = value.pop('name')
        return "%s %s" % (name, value)


@command(NodesDispatcher, verb='dump')
class NodesDump(DumpVerb):

    def fill_parser(self, parser):
        super().fill_parser(parser)

    def check_verb_args(self, running, *args, **kwargs):
        running.metrics = None
        return super().check_verb_args(running, *args, **kwargs)


@command(NodesDispatcher, verb='rebalance')
class NodesRebalance(Verb):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-i", "--index", dest="indices", default=[], action='append')
        parser.add_option("-d", "--destinations", dest="destinations", default=[], action='append')

    def check_verb_args(self, running, *args, indices=[], destinations=[], **kwargs):
        running.indices = indices
        running.destinations = destinations
        return super().check_verb_args(running, *args, **kwargs)

    async def execute(self, running):
        (routing, nodes) = await asyncio.gather(self._resolve_route(running), self._resolve_nodes(running))
        # The set of shards to move
        shards = []
        for (i, ii) in routing['routing_table']['indices'].items():
            for (s, si) in ii['shards'].items():
                for sd in si:
                    if sd['node'] not in running.object and sd['state'] == 'STARTED':
                        continue
                    shards.append(sd)
        shuffle(shards)
        shuffle(nodes)
        commands = []
        i = 0
        for s in shards:
            source_node_name =  running.object[s['node']]['name']
            destination_node_name = nodes[i % len(nodes)]
            i += 1
            c = {'move': {'index': s['index'], 'shard': s['shard'], 'from_node': source_node_name, 'to_node': destination_node_name}}
            commands.append(c)
        try:
            status = await self.api.escnx.cluster.reroute({'commands': commands}, dry_run=False, metric='none', explain=True)
        except RequestError as ex:
            print(ex.status_code)
            for e in ex.info['error']['root_cause']:
                print(e['reason'])
            return ()
        return status

    async def _resolve_route(self, running):
        return await self.api.escnx.cluster.state(allow_no_indices=True, index=running.indices, filter_path='routing_table')

    async def _resolve_nodes(self, running):
        nodes = []
        val = await self.api.escnx.nodes.info('_all', filter_path='nodes.*.name')
        for k, v in val['nodes'].items():
            if v['name'] in running.destinations:
                nodes.append(v['name'])
        return nodes

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
            yield "{\"%s\": %s}" % (v['name'], json.dumps(v, **running.formatting))


@command(NodesDispatcher)
class NodesCat(CatVerb):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-b", "--bytes", dest="bytes", default=False, action='store_true')
        parser.add_option("--full_id", dest="full_id", default=False, action='store_true')
        parser.add_option("-l", "--local", dest="local", default=False, action='store_true')

    def get_source(self):
        return self.api.escnx.cat.nodes
