from asyncio import coroutine

from eslib.verb import Verb
from eslib.dispatcher import dispatcher, command, Dispatcher


@dispatcher(object_name="xpack")
class XPackDispatcher(Dispatcher):

    def execute(self):
        pass

    @coroutine
    def get(self, running):
        pass


@command(XPackDispatcher, verb='license')
class XPackLicense(Verb):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-d", "--delete", dest="delete", default=False, action='store_true')

    def check_verb_args(self, running, *args, delete=False, **kwargs):
        running.delete = delete
        return super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def execute(self, running):
        if running.delete:
            val = yield from self.api.escnx.xpack.license.delete()
        else:
            val = None
        return val

