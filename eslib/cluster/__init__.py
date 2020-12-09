from asyncio import coroutine

from eslib.dispatcher import dispatcher, command, Dispatcher
from eslib.verb import ReadSettings, WriteSettings, DumpVerb, CatVerb
from elasticsearch.exceptions import RequestError

import json

@dispatcher(object_name="cluster")
class ClusterDispatcher(Dispatcher):

    @coroutine
    def check_noun_args(self, running):
        pass


@command(ClusterDispatcher, verb='master')
class ClusterMaster(CatVerb):

    def fill_parser(self, parser):
        super(ClusterMaster, self).fill_parser(parser)
        parser.add_option("-l", "--local", dest="local", default=False, action='store_true')

    @coroutine
    def check_verb_args(self, running, *args, local=False, **kwargs):
        running.local = local
        running.args = args
        yield from super().check_verb_args(running, *args, **kwargs)

    def get_source(self):
        return self.api.escnx.cat.master

    def to_str(self, running, item):
        if len(running.args) == 1 and isinstance(item, dict):
            return item[running.args[0]]
        else:
            return super(ClusterMaster, self).to_str(running, item)


@command(ClusterDispatcher, verb='health')
class ClusterHealth(DumpVerb):

    def fill_parser(self, parser):
        parser.add_option("-w", "--wait_for", dest="wait_for", default=None)
        super().fill_parser(parser)

    @coroutine
    def get(self, running):
        return None

    @coroutine
    def check_verb_args(self, running, *args, **kwargs):
        if kwargs.get('wait_for', None) is not None:
            waited_parts = kwargs['wait_for'].split(':')
            if len(waited_parts) == 2:
                (waited_type, waited_value) = waited_parts
                running.waited_type = waited_type
                running.waited_value = waited_value
            elif len(waited_parts) == 1:
                running.waited_type = 'status'
                running.waited_value = waited_parts[0]

        else:
            running.waited_type = None
        running.args = args
        yield from super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def execute(self, running):
        waited = {}
        if running.waited_type is not None:
            waited['wait_for_' + running.waited_type] = running.waited_value
        val = yield from self.api.escnx.cluster.health(level='cluster', **waited)
        return val

    def to_str(self, running, item):
        if item is None:
            return None
        if len(running.args) == 1:
            if running.args[0] in item:
               return item[running.args[0]]
        if len(running.args) > 0:
            values = {}
            for i in running.args:
                if i in item:
                    values[i] = item[i]
            return json.dumps(values, **running.formatting)
        elif running.only_keys:
            return json.dumps(list(item.keys()), **running.formatting)
        else:
            return json.dumps(item, **running.formatting)


@command(ClusterDispatcher, verb='allocation_explain')
class ClusterAllocationExplain(DumpVerb):

    def fill_parser(self, parser):
        parser.add_option("-w", "--wait_for", dest="wait_for", default=None)
        super().fill_parser(parser)

    @coroutine
    def get(self, running):
        return None

    @coroutine
    def check_verb_args(self, running, *args, **kwargs):
        running.args = args
        yield from super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def execute(self, running):
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
        parser.add_option("-f", "--retry_failed", dest="retry_failed", default=False)
        parser.add_option("--dry_run", dest="dry_run", default=False)
        super().fill_parser(parser)

    @coroutine
    def get(self, running):
        return None

    @coroutine
    def check_verb_args(self, running, *args, **kwargs):
        if kwargs.get('retry_failed', None) is not None:
            running.retry_failed = kwargs['retry_failed']
        if kwargs.get('dry_run', None) is not None:
            running.dry_run = kwargs['dry_run']
        running.args = args
        yield from super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def execute(self, running):
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

    @coroutine
    def get_elements(self, running):
        return (None,)

    @coroutine
    def get(self, running):
        return None

    @coroutine
    def check_verb_args(self, running, *args, persistent=False, transient=True, **kwargs):
        running.destination = 'persistent' if persistent else 'transient'
        yield from super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def action(self, element, running):
        values = running.values
        val = yield from self.api.escnx.cluster.put_settings(body={running.destination: values})
        return val

    def format(self, running, name, result):
        return None
