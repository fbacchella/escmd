from eslib.verb import List, DumpVerb, RepeterVerb
from eslib.dispatcher import dispatcher, command, Dispatcher
from asyncio import coroutine
import re
from json import dumps


@dispatcher(object_name="index")
class IndiciesDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-n", "--name", dest="name", help="index filter")

    @coroutine
    def get(self, name = '*'):
        val = yield from self.api.escnx.indices.get(index=name)
        return val

@command(IndiciesDispatcher)
class IndiciesList(List):
    pass


@command(IndiciesDispatcher, verb='forcemerge')
class IndiciesForceMerge(RepeterVerb):

    def fill_parser(self, parser):
        parser.add_option("-m", "--max_num_segments", dest="max_num_segments", help="Max num segmens", default=1)

    @coroutine
    def action(self, element, *args, max_num_segments=False, **kwargs):
        val = yield from self.api.escnx.indices.forcemerge(element[0], max_num_segments=max_num_segments, flush=True)
        return val

    def to_str(self, running, value):
        return "%s -> %s" % (list(running.object.keys())[0], value.__str__())


@command(IndiciesDispatcher, verb='delete')
class IndiciesDelete(RepeterVerb):

    @coroutine
    def get(self, name = None):
        if name == None:
            return '*'
        val = yield from self.api.escnx.indices.get(index=name)
        return val

    @coroutine
    def action(self, element, *args, **kwargs):
        val = yield from self.api.escnx.indices.delete(index=element[0])
        return val

    @coroutine
    def validate(self, running, *args, **kwargs):
        if running.object == '*':
            return False
        return running.object is not None

    def to_str(self, running, value):
        return "%s -> %s" % (list(running.object.keys())[0], value.__str__())


@command(IndiciesDispatcher, verb='reindex')
class IndiciesReindex(RepeterVerb):

    version_re = re.compile('(?P<indexseed>.*)\\.v(?P<numver>\\d+)')

    def fill_parser(self, parser):
        parser.add_option("-t", "--use_template", dest="template_name", help="Template to use for reindexing", default=None)
        parser.add_option("-v", "--version", dest="version")
        parser.add_option("-c", "--current_suffix", dest="current", default=None)
        parser.add_option("-s", "--separator", dest="separator", default='_')
        parser.add_option("-b", "--base_regex", dest="base_regex", default=None)

    def to_str(self, running, value):
        return dumps({value[0]: value[1]})

    @coroutine
    def action(self, element, running, version='next', current=None, separator='_', **kwargs):
        index_name = element[0]
        index = element[1]
        if running.base_regex is not None:
            match = running.base_regex.match(index_name)
            if len(match.groups()) == 1:
                base_name = match.group(1)
            else:
                raise Exception('invalid matching pattern ')
        else:
            base_name = index_name
        new_index_name = base_name + separator + version
        v = yield from self.api.escnx.indices.exists(index=new_index_name)
        if v:
            return ('does exists')
        if running.mappings is None:
            print("reusing mapping")
            mappings = index['mappings']
            settings = index['settings']
            old_replica = settings.get('index', {}).get('number_of_replicas', None)
            settings['index']['number_of_replicas'] = 0
        else:
            mappings = running.mappings
            settings = dict(running.settings)
            old_replica = running.old_replica
        for k in 'creation_date', 'uuid', 'provided_name':
            if k in settings.get('index'):
                del settings.get('index')[k]
        settings.get('index', {}).get('version', {'created': None}).pop('created')

        # Create the index with an empty mapping
        yield from self.api.escnx.indices.create(index=new_index_name, body={'settings': settings, "mappings": {}})

        # Moving mapping
        for i in mappings.keys():
            print("adding mapping for type", i)
            yield from self.api.escnx.indices.put_mapping(doc_type=i, index=new_index_name, body=mappings[i],
                                               update_all_types=True)

        # Doing the reindexation
        reindex_status = yield from self.api.escnx.reindex(
            body={"conflicts": "proceed", 'source': {'index': index_name, 'sort': '_doc', 'size': 10000},
                  'dest': {'index': new_index_name, 'version_type': 'external'}})

        # Ensure the minimum segments
        self.api.escnx.indices.forcemerge(new_index_name, max_num_segments=1, flush=False)

        # Reset the good replica number
        if old_replica is not None and old_replica != 0:
            yield from self.api.escnx.indices.put_settings(index=new_index_name,
                                                body={'index': {'number_of_replicas': old_replica}})

        # Moving aliases
        aliases = index['aliases']
        aliases_actions = []
        if len(aliases) > 0:
            for a in aliases:
                aliases_actions.append({'remove': {'index': index_name, 'alias': a}})
                aliases_actions.append({'add': {'index': new_index_name, 'alias': a}})
            yield from self.api.escnx.indices.update_aliases(body={
                'actions': aliases_actions
            })

        # the old index was not suffixed, destroy it and keep it's name as an alias
        if base_name == index_name:
            yield from self.api.escnx.indices.delete(index=index_name)
            yield from self.api.escnx.indices.update_aliases(body={
                'actions': [
                    {'add': {'index': new_index_name, 'alias': index_name}}
                ]
            })
        return (index_name, reindex_status)


    @coroutine
    def validate(self, running, *args, template_name=None, **kwargs):
        running.settings = {}
        running.mappings = None
        running.old_replica = None
        if template_name is not None:
            templates = yield from self.api.escnx.indices.get_template(name=template_name)
            if template_name in templates:
                template = templates[template_name]
                settings = template['settings']
                mappings = template['mappings']
                old_replica = settings.get('index', {}).get('number_of_replicas', None)
                settings['index']['number_of_replicas'] = 0
                running.mappings = mappings
                running.old_replica = old_replica
                running.settings = settings

        if kwargs.get('base_regex', None) is not None:
            running.base_regex = re.compile(kwargs['base_regex'])
        else:
            running.base_regex = None
        return super().validate(running, *args, **kwargs)


@command(IndiciesDispatcher, verb='settings')
class IndiciesSettings(RepeterVerb):

    setting_re = re.compile('(?P<setting>[a-z0-9\\._]+)=(?P<value>.*)')

    def execute(self, *args, **kwargs):
        settings = {}
        for i in args:
            matched = IndiciesSettings.setting_re.match(i)
            if matched is None:
                continue
            base = settings
            path = matched.group('setting').split('.')
            for j in path[:-1]:
                base[j] = {}
                base = base[j]
            try:
                base[path[-1]] = int(matched.group('value'))
            except ValueError:
                base[path[-1]] = matched.group('value')
        if len(settings) == 0:
            return False
        print(self.api.escnx.indices.put_settings(body=settings, index=self.object))


@command(IndiciesDispatcher, verb='dump')
class IndiciesDump(DumpVerb):
    pass
