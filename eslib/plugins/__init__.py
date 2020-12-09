from eslib.verb import CatVerb
from eslib.dispatcher import dispatcher, command, Dispatcher
from asyncio import coroutine
import datetime
import time


@dispatcher(object_name="plugin")
class PluginsDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-l", "--local", dest="local", help="tasks filter", default=False, action='store_true')

    @coroutine
    def check_noun_args(self, running, **kwargs):
        pass

    def noget(self, **kwargs):
        val = yield from self.api.escnx.cat.plugins(format='json')
        return val


@command(PluginsDispatcher)
class PluginsCat(CatVerb):

    def fill_parser(self, parser):
        super(PluginsCat, self).fill_parser(parser)
        parser.add_option("-l", "--local", dest="local", default=False, action='store_true')

    @coroutine
    def check_verb_args(self, running, *args, local=False, **kwargs):
        running.local = local
        yield from super().check_verb_args(running, *args, **kwargs)

    def get_source(self):
        return self.api.escnx.cat.plugins
