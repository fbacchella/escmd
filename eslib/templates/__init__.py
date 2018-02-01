from eslib.verb import Verb, List, DumpVerb
import json
from eslib.dispatcher import dispatcher, command, Dispatcher
from asyncio import coroutine


@dispatcher(object_name="template")
class TemplatesDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-n", "--name", dest="template_name", help="nodes filter")

    @coroutine
    def check_noun_args(self, running, template_name=None):
        running.template_name = template_name

    def get(self, running):
        val = yield from self.api.escnx.indices.get_template(name=running.template_name)
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
    def check_verb_args(self, running, *args, template_file_name=None, **kwargs):
        with open(template_file_name, "r") as template_file:
            running.template = json.load(template_file)
        yield from super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def get(self, running):
        return None

    @coroutine
    def execute(self, running):
        val = yield from self.api.escnx.indices.put_template(name=running.template_name, body=running.template)
        return val

    def to_str(self, running, value):
        return value.__str__()
