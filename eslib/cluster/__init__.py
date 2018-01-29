from asyncio import coroutine

from eslib.dispatcher import dispatcher, command, Dispatcher
from eslib.verb import DumpVerb, Verb, ReadSettings, WriteSettings


@dispatcher(object_name="cluster")
class ClusterDispatcher(Dispatcher):
    pass


@command(ClusterDispatcher, verb='readsettings')
class ClusterReadSettings(ReadSettings):

    @coroutine
    def get_elements(self, running, **kwargs):
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
    def validate(self, running, *args, persistent=False, transient= True, **kwargs):
        running.destination = 'persistent' if persistent else 'transient'
        return super().validate(running, *args,**kwargs)

    @coroutine
    def execute(self, running, *args, **kwargs):
        values = running.values
        val = yield from self.api.escnx.cluster.put_settings(body={running.destination: values})
        return val

