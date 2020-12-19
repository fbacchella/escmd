from eslib.verb import Verb, List, DumpVerb, RepeterVerb, CatVerb
from yaml import load
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
from eslib.dispatcher import dispatcher, command, Dispatcher
from asyncio import coroutine


@dispatcher(object_name="template", default_filter_path='*.order')
class TemplatesDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-n", "--name", dest="template_name", default=None, help="nodes filter")

    def check_noun_args(self, running, template_name=None, **kwargs):
        running.template_name = template_name
        return super().check_noun_args(running, template_name=template_name, **kwargs)

    @coroutine
    def get(self, running, template_name=None, filter_path=None):
        val = yield from self.api.escnx.indices.get_template(name=template_name, filter_path=filter_path)
        return val


@command(TemplatesDispatcher)
class TemplatesList(List):

    @coroutine
    def action(self, element, running):
        template = yield from self.api.escnx.indices.get_template(element[0], filter_path='*.order,*.index_patterns')
        return template

    def to_str(self, running, item):
        name, value = list(item[1].items())[0]
        return "%s %s" % (name, value)


@command(TemplatesDispatcher, verb='dump')
class TemplatesDump(DumpVerb):
    pass


@command(TemplatesDispatcher, verb='put')
class TemplatesPut(Verb):

    def fill_parser(self, parser):
        parser.add_option("-f", "--template_file", dest="template_file_name", default=None)
        parser.add_option("-p", "--template_pattern", dest="template_pattern", default=[], action='append')

    def check_verb_args(self, running, *args, template_file_name=None, template_pattern=None, **kwargs):
        with open(template_file_name, "r") as template_file:
            running.template = load(template_file, Loader=Loader)
        if template_pattern is not None and len(template_pattern) > 0:
            running.template['index_patterns'] = template_pattern
            if 'template' in running.template:
                del running.template['template']
        return super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def get(self, running, template_name, filter_path):
        return {}

    @coroutine
    def execute(self, running):
        val = yield from self.api.escnx.indices.put_template(name=running.template_name, body=running.template)
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

    @coroutine
    def action(self, element, *args, **kwargs):
        val = yield from self.api.escnx.indices.delete_template(name=element[0])
        return val

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
