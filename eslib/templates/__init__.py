import eslib.verb
import json
from eslib.dispatcher import dispatcher, command, Dispatcher
import sys

@dispatcher(object_name="template")
class TemplatesDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-n", "--name", dest="name", help="nodes filter")

    def get(self, name = None):
        return name

@command(TemplatesDispatcher)
class TemplatesList(eslib.verb.List):

    def execute(self, *args, **kwargs):
        return self.api.escnx.indices.get_template().keys()


@command(TemplatesDispatcher, verb='dump')
class TemplatesDump(eslib.verb.Verb):

    def execute(self, *args, **kwargs):
        templates = self.api.escnx.indices.get_template(self.object)
        if self.object in templates:
            json.dump(templates[self.object], sys.stdout)
            return templates[self.object]
        else:
            return None

@command(TemplatesDispatcher, verb='put')
class TemplatesPut(eslib.verb.Verb):

    def fill_parser(self, parser):
        parser.add_option("-f", "--template_file", dest="template_file_name", default=None)

    def execute(self, template_file_name=None):
        with open(template_file_name, "r") as template_file:
            template = json.load(template_file)
        return self.api.escnx.indices.put_template(name=self.object, body=template)
