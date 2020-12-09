from asyncio import coroutine

from eslib.dispatcher import dispatcher, command, Dispatcher
from eslib.verb import Verb, CatVerb
from eslib.tree import TreeNode


@dispatcher(object_name="shard")
class ShardsDispatcher(Dispatcher):

    @coroutine
    def check_noun_args(self, running):
        pass

    @coroutine
    def get(self, running):
        return None


@command(ShardsDispatcher, verb='assign')
class ShardsAssign(Verb):

    def fill_parser(self, parser):
        parser.add_option("-p", "--allow_primary", dest="allow_primary", default=False, action='store_true')

    @coroutine
    def check_verb_args(self, running, *args, allow_primary=False, **kwargs):
        running.assignemnts = []
        for i in args:
            index, shard, node = i.split(':')
            running.assignemnts.append({'allocate': {"index" : index, "shard" : int(shard), "node" : node, 'allow_primary': allow_primary}})
        yield from super().check_verb_args(running, **kwargs)

    @coroutine
    def get(self, running):
        return True

    @coroutine
    def execute(self, running):
        reroute_body = {'commands': running.assignemnts}
        val = yield from self.api.escnx.cluster.reroute(body=reroute_body)
        return val


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
class ShardsList(Verb):

    statuses = set(['green', 'yellow', 'red', 'all'])

    def fill_parser(self, parser):
        parser.add_option("-s", "--status", dest="status", default='all')
        parser.add_option("-i", "--indices", dest="indices", default='*')

    @coroutine
    def check_verb_args(self, running, *args, status='all', indices='*', **kwargs):
        if not status in ShardsList.statuses:
            raise Exception('unknow status: %s' % status)
        running.status = status
        running.indices = indices
        yield from super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def get(self, running):
        return self.api.escnx.indices.shard_stores(index=running.indices, status=running.status)

    @coroutine
    def execute(self, running):
        tree = ShardTreeNode(None)

        for index, shards in running.object['indices'].items():
            shards=shards['shards']
            if len(shards) == 0:
                continue
            index_node = ShardTreeNode(index)
            tree.addchild(index_node)
            for snum, shard in shards.items():
                shard_node = ShardTreeNode(snum)
                index_node.addchild(shard_node)
                for replica in shard['stores']:
                    replica_node = ShardTreeNode(replica)
                    shard_node.addchild(replica_node)
        return tree

    def to_str(self, running, value):
        return '%s' % value

    @coroutine
    def no_get_elements(self, running):
        all_shards = []
        for index, shards in running.object['indices'].items():
            shards=shards['shards']
            if len(shards) == 0:
                continue
            for shard in shards.values():
                all_shards.append((index, shard))
            #print(index, shards.keys())
        return all_shards

    def no_to_str(self, running, item):
        name = item[0]
        value = item[1]
        return '{"%s": %s}' % (name, str(value))


@command(ShardsDispatcher)
class ShardsCat(CatVerb):

    def fill_parser(self, parser):
        super(ShardsCat, self).fill_parser(parser)
        parser.add_option("-l", "--local", dest="local", default=False, action='store_true')

    @coroutine
    def check_verb_args(self, running, *args, local=False, **kwargs):
        running.local = local
        yield from super().check_verb_args(running, *args, **kwargs)

    def get_source(self):
        return self.api.escnx.cat.shards
