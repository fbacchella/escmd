from eslib.verb import Verb, List, DumpVerb
import json
from eslib.dispatcher import dispatcher, command, Dispatcher
from asyncio import coroutine


@dispatcher(object_name="template")
class TemplatesDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-n", "--name", dest="name", help="nodes filter")

    def get(self, name = '*'):
        val = yield from self.api.escnx.indices.get_template(name=name)
        return val


@command(TemplatesDispatcher)
class TemplatesList(List):
    pass


@command(TemplatesDispatcher, verb='dump')
class TemplatesDump(DumpVerb):
    pass


@command(TemplatesDispatcher, verb='put')
class TemplatesPut(Verb):

    def fill_parser(self, parser):
        parser.add_option("-f", "--template_file", dest="template_file_name", default=None)

    @coroutine
    def get(self, name = None):
        if name is None:
            return False
        val = yield from self.api.escnx.indices.exists_template(name=name)
        if not val:
            return name
        else:
            return None

    def validate(self, running, *args, **kwargs):
        print(vars(running))
        print(args)
        print(kwargs)
        return running.object is not None

    @coroutine
    def execute(self, running, template_file_name):
        with open(template_file_name, "r") as template_file:
            template = json.load(template_file)
        val = yield from self.api.escnx.indices.put_template(name=running.object, body=template)
        return val

    def to_str(self, value):
        return value.__str__()
