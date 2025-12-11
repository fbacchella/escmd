 
from eslib.dispatcher import dispatcher, command, Dispatcher
from eslib.verb import ReadSettings, WriteSettings, DumpVerb, CatVerb
from elasticsearch.exceptions import RequestError

import json

@dispatcher(object_name="cluster")
class ClusterDispatcher(Dispatcher):

    def get(self, running, **kwargs):
        return {}


@command(ClusterDispatcher, verb='master')
class ClusterMaster(CatVerb):

    def fill_parser(self, parser):
        super(ClusterMaster, self).fill_parser(parser)
        parser.add_option("-l", "--local", dest="local", default=False, action='store_true')

    def check_verb_args(self, running, *args, format='text', local=False, **kwargs):
        running.local = local
        running.args = args
        return super().check_verb_args(running, *args, format='json', **kwargs)

    def get_source(self):
        return self.api.escnx.cat.master

    def to_str(self, running, item):
        if len(running.args) == 1 and isinstance(item, dict):
            return item[running.args[0]]
        else:
            return super().to_str(running, item[0])


@command(ClusterDispatcher, verb='health')
class ClusterHealth(DumpVerb):

    def fill_parser(self, parser):
        parser.add_option("-w", "--wait_for", dest="wait_for", default=None)
        parser.add_option("-l", "--level", dest="level", default='cluster')
        super().fill_parser(parser)

    async def get(self, running, **kwargs):
        return {}

    def check_verb_args(self, running, *args, wait_for=None, level=None, **kwargs):
        if wait_for is not None:
            waited_parts = wait_for.split(':')
            if len(waited_parts) == 2:
                (waited_type, waited_value) = waited_parts
                running.waited_type = waited_type
                running.waited_value = waited_value
            elif len(waited_parts) == 1:
                running.waited_type = 'status'
                running.waited_value = waited_parts[0]
        else:
            running.waited_type = None
        if not level in ('cluster', 'indices', 'shards'):
            raise Exception('Unknow level: "%s"' % level)
        return super().check_verb_args(running, *args, level=level, **kwargs)

    async def execute(self, running, level, pretty=None, **kwargs):
        waited = {}
        if running.waited_type is not None:
            waited['wait_for_' + running.waited_type] = running.waited_value
        waited['timeout'] = "%ds" % self.api.timeout
        val = await self.api.escnx.cluster.health(level=level, **waited, pretty=pretty)
        return val

    def to_str(self, running, item):
        return super().to_str(running, (None, (None, item)))

    def filter_dump(self, running, *args, **kwargs):
        if len(args) == 1:
            return args[0]
        else:
            return args[1]


@command(ClusterDispatcher, verb='allocation_explain')
class ClusterAllocationExplain(DumpVerb):

    def fill_parser(self, parser):
        parser.add_option("-i", "--index", dest="index", default=None)
        parser.add_option("-P", "--primary", dest="primary", default=None, action='store_true')
        parser.add_option("-s", "--shard", dest="shard", default=None)
        super().fill_parser(parser)

    def check_verb_args(self, running, *args, index=None, primary=None, shard=None, **kwargs):
        running.args = args
        running.index = index
        running.primary = primary
        running.shard = shard
        return super().check_verb_args(running, *args, **kwargs)

    async def execute(self, running, **kwargs):
        body = {}
        if running.index is not None:
            body['index'] = running.index
            body['shard'] = 0
            body['primary'] = True
        if running.shard is not None:
            body['shard'] = running.shard
        if running.primary is not None:
            body['primary'] = running.primary
        try:
            val = await self.api.escnx.cluster.allocation_explain(body=body)
        except RequestError as e:
            if e.error == 'illegal_argument_exception':
                reason = e.info.get('error', {}).get('reason','')
                if reason.startswith:
                    return None
                else:
                    raise e
            else:
                raise e
            val = None
        return val

    def to_str(self, running, item):
        return json.dumps(item, **running.formatting)


@command(ClusterDispatcher, verb='reroute')
class ClusterReroute(DumpVerb):

    def _action_allocate_empty_primary(self, index, shard, node, accept_data_loss=False):
        return  {"index" : index, "shard" : int(shard), "node" : node, "accept_data_loss": self.str_to_bool(accept_data_loss)}

    def _action_move(self, index, shard, from_node, to_node):
        return {"index" : index, "shard" : int(shard), "from_node" : from_node, "to_node" : to_node}

    reroute_action = {
        'allocate_empty_primary': _action_allocate_empty_primary,
        'move': _action_move,
    }

    def fill_parser(self, parser):
        parser.add_option("-f", "--retry_failed", dest="retry_failed", default=False, action='store_true')
        parser.add_option("--dry_run", dest="dry_run", default=False, action='store_true')
        super().fill_parser(parser)

    def check_verb_args(self, running, *args, **kwargs):
        running.assignemnts = []
        for i in args:
            action_args = i.split(':')
            action_name = action_args.pop(0)
            action = ClusterReroute.reroute_action[action_name](self, *action_args)
            running.assignemnts.append({action_name: action})
        if kwargs.get('retry_failed', None) is not None:
            running.retry_failed = kwargs['retry_failed']
        if kwargs.get('dry_run', None) is not None:
            running.dry_run = kwargs['dry_run']
        running.args = args
        return super().check_verb_args(running, *args, **kwargs)

    async def execute(self, running, **kwargs):
        reroute_body = {'commands': running.assignemnts}
        return await self.api.escnx.cluster.reroute(body=reroute_body, metric='none', retry_failed=running.retry_failed, dry_run=running.dry_run)

    def to_str(self, running, item):
        return ""

    def str_to_bool(self, s):
        if isinstance(s, bool):
            return s
        else:
            s = s.strip().lower()
            if s in ("true", "1", "yes", "y", "on"):
                return True
            elif s in ("false", "0", "no", "n", "off"):
                return False
            else:
                raise ValueError(f"Invalid boolean value : '{s}'")


@command(ClusterDispatcher, verb='readsettings')
class ClusterReadSettings(ReadSettings):

    async def get_elements(self, running):
        return (None,)

    async def get(self, running):
        return None

    async def get_settings(self, running, element):
        val = await self.api.escnx.cluster.get_settings(include_defaults=True, flat_settings=running.flat)
        return val


@command(ClusterDispatcher, verb='writesettings')
class ClusterWriteSettings(WriteSettings):

    def fill_parser(self, parser):
        def set_persistent(option, opt, value, parser):
            parser.values.persistent = True
            parser.values.transient = False
        def set_transient(option, opt, value, parser):
            parser.values.persistent = False
            parser.values.transient = True

        parser.add_option("-p", "--persistent", dest="persistent", default=False, action="callback", callback=set_persistent)
        parser.add_option("-t", "--transient", dest="transient", default=True, action="callback", callback=set_transient)
        return super().fill_parser(parser)

    def check_verb_args(self, running, *args, persistent=False, transient=True, **kwargs):
        running.destination = 'persistent' if persistent else 'transient'
        return super().check_verb_args(running, *args, **kwargs)

    async def get_elements(self, running):
        return (None,)

    async def get(self, running):
        return None

    async def action(self, element, running):
        values = running.values
        val = await self.api.escnx.cluster.put_settings(body={running.destination: values})
        return val

    def format(self, running, name, result):
        return None


@command(ClusterDispatcher, verb='ilm_status')
class ClusterIlmStatus(DumpVerb):

    async def get(self, running, **kwargs):
        return {}

    async def execute(self, running, **kwargs):
        val = await self.api.escnx.ilm.get_status()
        return val

    def to_str(self, running, item):
        return item['operation_mode']
