from asyncio import coroutine

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

    def check_verb_args(self, running, *args, local=False, **kwargs):
        running.local = local
        running.args = args
        return super().check_verb_args(running, *args, **kwargs)

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

    @coroutine
    def get(self, running, **kwargs):
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

    @coroutine
    def execute(self, running, level, pretty=None, **kwargs):
        waited = {}
        if running.waited_type is not None:
            waited['wait_for_' + running.waited_type] = running.waited_value
        val = yield from self.api.escnx.cluster.health(level=level, **waited, pretty=pretty)
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
        parser.add_option("-w", "--wait_for", dest="wait_for", default=None)
        super().fill_parser(parser)

    def check_verb_args(self, running, *args, **kwargs):
        running.args = args
        return super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def execute(self, running, **kwargs):
        try:
            val = yield from self.api.escnx.cluster.allocation_explain()
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

    def fill_parser(self, parser):
        parser.add_option("-f", "--retry_failed", dest="retry_failed", default=False, action='store_true')
        parser.add_option("--dry_run", dest="dry_run", default=False, action='store_true')
        super().fill_parser(parser)

    def check_verb_args(self, running, *args, **kwargs):
        if kwargs.get('retry_failed', None) is not None:
            running.retry_failed = kwargs['retry_failed']
        if kwargs.get('dry_run', None) is not None:
            running.dry_run = kwargs['dry_run']
        running.args = args
        return super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def execute(self, running, **kwargs):
        val = yield from self.api.escnx.cluster.reroute(retry_failed=running.retry_failed, dry_run=running.dry_run)
        return val

    def to_str(self, running, item):
        return json.dumps(item, **running.formatting)


@command(ClusterDispatcher, verb='readsettings')
class ClusterReadSettings(ReadSettings):

    @coroutine
    def get_elements(self, running):
        return (None,)

    @coroutine
    def get(self, running):
        return None

    @coroutine
    def get_settings(self, running, element):
        val = yield from self.api.escnx.cluster.get_settings(include_defaults=True, flat_settings=running.flat)
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

    @coroutine
    def get_elements(self, running):
        return (None,)

    @coroutine
    def get(self, running):
        return None

    @coroutine
    def action(self, element, running):
        values = running.values
        val = yield from self.api.escnx.cluster.put_settings(body={running.destination: values})
        return val

    def format(self, running, name, result):
        return None


@command(ClusterDispatcher, verb='ilm_status')
class ClusterHealth(DumpVerb):

    @coroutine
    def get(self, running, **kwargs):
        return {}

    @coroutine
    def execute(self, running, **kwargs):
        val = yield from self.api.escnx.ilm.get_status()
        return val

    def to_str(self, running, item):
        return item['operation_mode']
