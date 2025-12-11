import json

from eslib.dispatcher import dispatcher, command, Dispatcher
from eslib.verb import Verb, CatVerb
from eslib.tree import TreeNode


@dispatcher(object_name="shard")
class ShardsDispatcher(Dispatcher):

    def check_noun_args(self, running):
        return {}

    async def get(self, running):
        return {}


@command(ShardsDispatcher, verb='move')
class ShardsMove(Verb):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-d", "--dry_run", dest="dry_run", default=False, action='store_true')
        parser.add_option("-e", "--explain", dest="explain", default=False, action='store_true')

    def check_verb_args(self, running, *args, allow_primary=False, **kwargs):
        running.assignemnts = []
        running.vector = []
        for i in args:
            index, shard, from_node, to_node = i.split(':')
            running.vector.append((index, shard, from_node, to_node))
            running.assignemnts.append({'move': {"index" : index, "shard" : int(shard), "from_node" : from_node, "to_node" : to_node}})
        return super().check_verb_args(running, **kwargs)

    async def get(self, running):
        return True

    async def execute(self, running, dry_run=False, explain=False):
        reroute_body = {'commands': running.assignemnts}
        return await self.api.escnx.cluster.reroute(body=reroute_body, metric=['version'], dry_run=dry_run, explain=explain)

    def to_str(self, running, value):
        acknowledged = value['acknowledged']
        explanations = value.get('explanations', None)
        xpl = []
        if explanations is not None:
            for c in explanations:
                c_cmd = c['command']
                decisions = []
                for d in c['decisions']:
                    if d['decision'] == 'NO':
                        decisions.append(d)
                xpl.append({'command': c_cmd, 'decisions': decisions})
            return json.dumps(xpl, indent=2, sort_keys=True)
        elif acknowledged:
            val = ""
            for a in running.vector:
                val = "%s:%s moved from %s to %s" % (a[0], a[1], a[2], a[3])
            return val
        else:
            return "Moved failed"


class ShardTreeNode(TreeNode):

    def _value_to_str(self, level):
        if level < 2:
            return self.value
        else:
            (allocation, version, node) = (None, None, '')
            for (k, v) in self.value.items():
                if k == 'allocation':
                    if v == 'primary':
                        allocation = '*'
                    elif v == 'unused':
                        allocation = '-'
                    else:
                        allocation = ' '
                elif k == 'version':
                    version = v
                elif isinstance(v, dict) and 'name' in v:
                    node = v['name']
            return "%s %s %s" %(allocation, version, node)


@command(ShardsDispatcher, verb='tree')
class ShardsTree(Verb):

    statuses = set(['green', 'yellow', 'red', 'all'])

    def fill_parser(self, parser):
        parser.add_option("-s", "--status", dest="status", default='all')
        parser.add_option("-i", "--indices", dest="indices", default='*')
        parser.add_option("-f", "--flat", dest="flat", default=False, action='store_true')

    def check_verb_args(self, running, *args, status='all', indices='*', flat=False, **kwargs):
        if not status in ShardsTree.statuses:
            raise Exception('unknow status: %s' % status)
        running.status = status
        running.indices = indices
        running.flat = flat
        return super().check_verb_args(running, *args, **kwargs)

    async def get(self, running):
        return await self.api.escnx.indices.shard_stores(index=running.indices, status=running.status)

    async def execute(self, running):
        tree = ShardTreeNode(None, running.flat)

        for index, shards in running.object['indices'].items():
            shards=shards['shards']
            if len(shards) == 0:
                continue
            index_node = ShardTreeNode(index, running.flat)
            tree.addchild(index_node)
            for snum, shard in shards.items():
                shard_node = ShardTreeNode(snum, running.flat)
                index_node.addchild(shard_node)
                for replica in shard['stores']:
                    replica_node = ShardTreeNode(replica, running.flat)
                    shard_node.addchild(replica_node)
        return tree

    def to_str(self, running, value):
        return '%s' % value


@command(ShardsDispatcher)
class ShardsCat(CatVerb):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-l", "--local", dest="local", default=False, action='store_true')

    def get_source(self):
        return self.api.escnx.cat.shards
