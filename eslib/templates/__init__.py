from eslib.verb import Verb, List, DumpVerb
import json
from eslib.dispatcher import dispatcher, command, Dispatcher
from asyncio import coroutine


@dispatcher(object_name="template")
class TemplatesDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-n", "--name", dest="name", help="nodes filter")

    def get(self, name = None):
        return name


class TemplateVerb(Verb):

    @coroutine
    def get_elements(self):
        val = yield from self.api.escnx.indices.get_template(name=self.object)
        return val


@command(TemplatesDispatcher)
class TemplatesList(List, TemplateVerb):

    def to_str(self, value):
        return value.__str__()


@command(TemplatesDispatcher, verb='dump')
class TemplatesDump(DumpVerb,TemplateVerb):
    pass


@command(TemplatesDispatcher, verb='put')
class TemplatesPut(TemplateVerb):

    def fill_parser(self, parser):
        parser.add_option("-f", "--template_file", dest="template_file_name", default=None)

    def action(self, object_name=None, object=None, template_file_name=None):
        with open(template_file_name, "r") as template_file:
            template = json.load(template_file)
        val = yield from self.api.escnx.indices.put_template(name=object_name, body=template)
        return val

    def to_str(self, value):
        return value.__str__()
