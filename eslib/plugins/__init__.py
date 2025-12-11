from eslib.verb import CatVerb
from eslib.dispatcher import dispatcher, command, Dispatcher


@dispatcher(object_name="plugin")
class PluginsDispatcher(Dispatcher):
    pass


@command(PluginsDispatcher)
class PluginsCat(CatVerb):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-l", "--local", dest="local", default=False, action='store_true')

    def get_source(self):
        return self.api.escnx.cat.plugins
