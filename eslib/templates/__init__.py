from eslib.verb import Verb, List, DumpVerb, RepeterVerb, CatVerb
from eslib.dispatcher import dispatcher, command, Dispatcher
from eslib.context import TypeHandling
from yaml import load
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


@dispatcher(object_name="template", default_filter_path='*.order')
class TemplatesDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-n", "--name", dest="template_name", default=None, help="nodes filter")

    def check_noun_args(self, running, template_name=None, **kwargs):
        running.template_name = template_name
        return super().check_noun_args(running, template_name=template_name, **kwargs)

    async def get(self, running, template_name=None, filter_path=None):
        return await self.api.escnx.indices.get_template(name=template_name, filter_path=filter_path)


@command(TemplatesDispatcher)
class TemplatesList(List):

    async def action(self, element, running):
        return await self.api.escnx.indices.get_template(element[0], filter_path='*.order,*.index_patterns')

    def to_str(self, running, item):
        name, value = list(item[1].items())[0]
        return "%s %s" % (name, value)


@command(TemplatesDispatcher, verb='dump')
class TemplatesDump(DumpVerb):
    pass


@command(TemplatesDispatcher, verb='put')
class TemplatesPut(Verb):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-f", "--template_file", dest="template_file_name", default=None)
        parser.add_option("-p", "--template_pattern", dest="template_pattern", default=[], action='append')
        parser.add_option("-t", "--type_name", dest="type_name", default=None)

    def check_verb_args(self, running, *args, template_file_name=None, template_pattern=None, type_name=None, **kwargs):
        if running.template_name is None:
            raise Exception("-n/--name mandatory is not defined")
        with open(template_file_name, "r") as template_file:
            running.template = load(template_file, Loader=Loader)
        if type_name is not None and self.api.type_handling == TypeHandling.DEPRECATED and 'mappings' in running.template and type_name in running.template['mappings']:
            # Removing an explicit give type from the mapping
            running.template['mappings'] = running.template['mappings'][type_name]
            running.with_type = None
        elif type_name is not None and self.api.type_handling == TypeHandling.TRANSITION:
            # Given a type_name, used to detect is type is present or not
            running.with_type = 'mappings' in running.template and type_name in running.template['mappings']
        elif type_name is not None and self.api.type_handling == TypeHandling.IMPLICIT:
            raise Exception("implicit type handling don't accept type_name argument")
        else:
            running.with_type = None
        if template_pattern is not None and len(template_pattern) > 0:
            running.template['index_patterns'] = template_pattern
            if 'template' in running.template:
                del running.template['template']
        return super().check_verb_args(running, *args, **kwargs)

    async def get(self, running, template_name, filter_path):
        return {}

    async def execute(self, running):
        val = await self.api.escnx.indices.put_template(name=running.template_name, body=running.template, include_type_name=running.with_type)
        return val

    def to_str(self, running, value):
        return "%s added" % running.template_name

"""
delete_template(*args, **kwargs)
Delete an index template by its name. http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html

Parameters:	
name – The name of the template
master_timeout – Specify timeout for connection to master
timeout – Explicit operation timeout
"""

@command(TemplatesDispatcher, verb='delete')
class TemplatesDelete(RepeterVerb):

    async def action(self, element, *args, **kwargs):
        return await self.api.escnx.indices.delete_template(name=element[0])

    def check_verb_args(self, running):
        if running.template_name == '*' or running.template_name == '__all':
            raise Exception("won't destroy everything, -n/--name mandatory")
        return super().check_verb_args(running)

    def format(self, running, name, result):
        return "%s deleted" % name


@command(TemplatesDispatcher)
class TemplatesCat(CatVerb):

    def fill_parser(self, parser):
        super(TemplatesCat, self).fill_parser(parser)
        parser.add_option("-l", "--local", dest="local", default=False, action='store_true')

    def get_source(self):
        return self.api.escnx.cat.templates
