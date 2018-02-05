from asyncio import coroutine

from eslib.dispatcher import dispatcher, command, Dispatcher
from eslib.verb import ReadSettings, WriteSettings


@dispatcher(object_name="cluster")
class ClusterDispatcher(Dispatcher):

    @coroutine
    def check_noun_args(self, running):
        pass


@command(ClusterDispatcher, verb='readsettings')
class ClusterReadSettings(ReadSettings):

    @coroutine
    def get(self, running):
        return None

    @coroutine
    def check_verb_args(self, running, *args, flat=False, **kwargs):
        running.args = args
        if len(args) > 0:
            flat = True
        yield from super().check_verb_args(running, *args, flat=flat,  **kwargs)

    @coroutine
    def get_elements(self, running):
        val = yield from self.api.escnx.cluster.get_settings(include_defaults=True, flat_settings=running.flat)
        return val.items()


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
    def check_verb_args(self, running, *args, persistent=False, transient=True, **kwargs):
        running.destination = 'persistent' if persistent else 'transient'
        yield from super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def execute(self, running):
        values = running.values
        val = yield from self.api.escnx.cluster.put_settings(body={running.destination: values})
        return val

