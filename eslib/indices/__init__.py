from eslib.verb import Verb, DumpVerb, RepeterVerb, ReadSettings, WriteSettings, CatVerb, List
from eslib.dispatcher import dispatcher, command, Dispatcher
from eslib.exceptions import ESLibError
from asyncio import coroutine
from json import loads, dumps
from elasticsearch.exceptions import NotFoundError, ElasticsearchException
import re
from yaml import load
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


@dispatcher(object_name="index")
class IndicesDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-n", "--name", dest="name", help="index filter")
        parser.add_option("--ignore_unavailable", dest="ignore_unavailable", help="ignore unavailable", default=None, action='store_true')
        parser.add_option("--allow_no_indices", dest="allow_no_indices", help="allow no indices", default=None, action='store_true')
        parser.add_option("--expand_wildcards", dest="expand_wildcards", help="expand wildcards", default=None)

    @coroutine
    def check_noun_args(self, running, name='_all', ignore_unavailable=None, allow_no_indices=None, expand_wildcards= None, **kwargs):
        running.index_name = name
        running.ignore_unavailable = ignore_unavailable
        running.allow_no_indices = allow_no_indices
        running.expand_wildcards = expand_wildcards

    @coroutine
    def get(self, running):
        val = yield from self.api.escnx.indices.get(index=running.index_name,
                                                    expand_wildcards=running.expand_wildcards,
                                                    allow_no_indices=running.allow_no_indices,
                                                    ignore_unavailable=running.ignore_unavailable
        )
        return val


@command(IndicesDispatcher, verb='list')
class IndiciesList(List):

    template = lambda self, x, y: "%s\t%12d\t%4d\t%12d" % (x, y['indices'][x]['primaries']['docs']['count'], y['indices'][x]['primaries']['segments']['count'], y['indices'][x]['primaries']['store']['size_in_bytes'])

    @coroutine
    def get_elements(self, running):
        vals = []
        for i in running.object.keys():
            stats = yield from self.api.escnx.indices.stats(index=i)
            vals.append((i, stats))
        return vals


@command(IndicesDispatcher, verb='forcemerge')
class IndiciesForceMerge(RepeterVerb):

    def fill_parser(self, parser):
        parser.add_option("-m", "--max_num_segments", dest="max_num_segments", help="Max num segmens", default=1)
        parser.add_option("-t", "--notranslog", dest="notranslog", help="Remove translog", default=False, action='store_true')
        parser.add_option("-c", "--codec", dest="codec", default=None)

    @coroutine
    def check_verb_args(self, running, *args, max_num_segments=1, notranslog=False, codec=None, **kwargs):
        running.max_num_segments = max_num_segments
        running.notranslog = notranslog
        if codec == 'best_compression' or codec == 'default':
            running.codec = codec
        elif codec is not None:
            return False
        else:
            running.codec = None

        yield from super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def execute(self, running):
        if running.notranslog:
            yield from self.api.escnx.indices.put_settings(index=running.index_name,
                                                           body={'index': {'translog': {'flush_threshold_size': '56b'}}})
        if running.codec is not None:
            yield from self.api.escnx.indices.close(index=running.index_name)
            yield from self.api.escnx.indices.put_settings(index=running.index_name,
                                                           body={'index': {'codec': running.codec}})
            yield from self.api.escnx.indices.open(index=running.index_name, wait_for_active_shards='all')

        val = yield from self.api.escnx.indices.forcemerge(running.index_name, max_num_segments=running.max_num_segments, flush=True)
        return val

    def result_is_valid(self, result):
        return result.get('_shards',{}).get('failed', -1) == 0

    def to_str(self, running, value):
        merged = "successful: %d, failed: %d\nmerged:\n" % (value['_shards']['successful'], value['_shards']['failed'])
        for i in running.object.keys():
            merged += "  %s\n" % i
        return merged


@command(IndicesDispatcher, verb='delete')
class IndiciesDelete(RepeterVerb):

    @coroutine
    def action(self, element, *args, **kwargs):
        val = yield from self.api.escnx.indices.delete(index=element[0])
        return val

    @coroutine
    def check_verb_args(self, running, ):
        if running.index_name == '*':
            raise Exception("won't destroy everything, -n/--name mandatory")

    def format(self, running, name, result):
        return "%s deleted" % name


@command(IndicesDispatcher, verb='reindex')
class IndiciesReindex(RepeterVerb):

    def fill_parser(self, parser):
        parser.add_option("-t", "--use_template", dest="template_name", help="Template to use for reindexing", default=None)
        parser.add_option("-p", "--prefix", dest="prefix", default='')
        parser.add_option("-s", "--suffix", dest="suffix", default='')
        parser.add_option("-i", "--infix_regex", dest="infix_regex", default=None)

    @coroutine
    def check_verb_args(self, running, *args, template_name=None, infix_regex=None, prefix='', suffix='', keep_mapping=False, **kwargs):
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

        if infix_regex is not None:
            running.infix_regex = re.compile(infix_regex)
        else:
            running.infix_regex = None
        running.prefix = prefix
        running.suffix = suffix
        return super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def action(self, element, running):
        index_name = element[0]
        index = element[1]
        if running.infix_regex is not None:
            match = running.infix_regex.match(index_name)
            if match is not None and len(match.groups()) == 1:
                infix = match.group(1)
            else:
                raise ESLibError('invalid matching pattern')
        else:
            infix = index_name
        new_index_name = running.prefix + infix + running.suffix
        v = yield from self.api.escnx.indices.exists(index=new_index_name)
        if v:
            raise ESLibError('%s already exists' % new_index_name)

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
        settings.get('index', {}).pop('version', None)

        # Create the index with an empty mapping
        yield from self.api.escnx.indices.create(index=new_index_name, body={'settings': settings, "mappings": mappings})

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

        yield from self.api.escnx.indices.delete(index=index_name)

        # the old index was not suffixed, keep it's name as an alias
        if infix == index_name:
            yield from self.api.escnx.indices.update_aliases(body={
                'actions': [
                    {'add': {'index': new_index_name, 'alias': index_name}}
                ]
            })
        return reindex_status


@command(IndicesDispatcher, verb='dump')
class IndiciesDump(DumpVerb):
    pass


@command(IndicesDispatcher, verb='readsettings')
class IndiciesReadSettings(ReadSettings):

    @coroutine
    def get_elements(self, running):
        val = yield from self.api.escnx.cat.indices(index=running.index_name, format='json', h='index')
        return map(lambda x: x['index'], val)

    @coroutine
    def get_settings(self, running, element):
        # Can't use get_settings filtering of settings, because if settings name are given, or even '_all', flat_settings don't work any more
        index_entry = yield from self.api.escnx.indices.get_settings(index=element, include_defaults=True, flat_settings=running.flat)
        #index_entry = yield from self.api.escnx.indices.get(index=element, include_defaults=True, flat_settings=running.flat)
        index_name, index_data = next(iter(index_entry.items()))
        new_settings = {}
        if running.flat:
            # remove 'index.' at the beginning of settings names
            new_settings['defaults'] = {k.replace('index.', ''): v for k, v in index_data['defaults'].items()}
            new_settings['settings'] = {k.replace('index.', ''): v for k, v in index_data['settings'].items()}
        else:
            new_settings['defaults'] = index_data['defaults']['index']
            new_settings['settings'] = index_data['settings']['index']
        return new_settings


@command(IndicesDispatcher, verb='writesettings')
class IndiciesWriteSettings(WriteSettings):

    @coroutine
    def get(self, running):
        val = yield from self.api.escnx.cat.indices(index=running.index_name, format='json', h='index')
        return val

    @coroutine
    def get_elements(self, running):
        index_names = []
        for i in running.object:
            index_names.append((i['index'], None))
        return index_names

    @coroutine
    def action(self, element, running):
        val = yield from self.api.escnx.indices.put_settings(body=running.values, index=element[0])
        return val

    def format(self, running, name, result):
        return "%s set" % (name)


@command(IndicesDispatcher, verb='addmapping')
class IndiciesAddMapping(RepeterVerb):

    def fill_parser(self, parser):
        parser.add_option("-f", "--mapping_file", dest="mapping_file_name", help="The file with the added mapping", default=None)
        parser.add_option("-t", "--type", dest="type", help="The type to add the mapping to", default='_default')

    @coroutine
    def check_verb_args(self, running, *args, mapping_file_name=None, type='_default_', **kwargs):
        with open(mapping_file_name, "r") as mapping_file:
            running.mapping = load(mapping_file)
        running.type = type
        yield from super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def action(self, element, running):
        val = yield from self.api.escnx.indices.put_mapping(index=element[0], body={"properties": running.mapping}, doc_type=running.type)
        return val

    def to_str(self, running, value):
        return "%s -> %s" % (list(running.object.keys())[0], value.__str__())


@command(IndicesDispatcher, verb='getfields')
class IndicesGetFieldMapping(RepeterVerb):

    def fill_parser(self, parser):
        parser.add_option("-t", "--type", dest="type", help="The type to add the mapping to", default='_default')
        parser.add_option("-f", "--flat", dest="flat", default=False, action='store_true')
        parser.add_option("-p", "--pretty", dest="pretty", default=False, action='store_true')

    @coroutine
    def check_verb_args(self, running, *args, flat=False, type='_default_', **kwargs):
        running.type = type
        running.flat = flat
        yield from super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def get(self, running):
        val = yield from self.api.escnx.cat.indices(index=running.index_name, format='json', h='index')
        return val

    @coroutine
    def get_elements(self, running):
        index_names = []
        for i in running.object:
            index_names.append((i['index'], None))
        return index_names

    @coroutine
    def action(self, element, running):
        try:
            val = yield from self.api.escnx.indices.get_mapping(index=element[0], doc_type=running.type)
            return val
        except elasticsearch.exceptions.NotFoundError as e:
            return e

    def result_is_valid(self, result):
        return not isinstance(result, elasticsearch.exceptions.ElasticsearchException)

    def format(self, running, index, result):
        mappings = result[index]
        if running.flat:
            yield from self.flatten(index, mappings['mappings'][running.type]['properties'])
        else:
            yield dumps({index: mappings['mappings'][running.type]['properties']})

    def flatten(self, prefix, fields):
        separator = '.' if len(prefix) > 0 else ''
        for k,v in fields.items():
            if isinstance(v, dict) and 'properties' in v :
                yield from self.flatten(prefix + separator + str(k), v['properties'])
            elif isinstance(v, dict)  and 'type' in v :
                yield prefix + separator + str(k) + ": " + v['type']


@command(IndicesDispatcher)
class IndiciesCat(CatVerb):

    def fill_parser(self, parser):
        super(IndiciesCat, self).fill_parser(parser)
        parser.add_option("-l", "--local", dest="local", default=False, action='store_true')

    @coroutine
    def check_verb_args(self, running, *args, local=False, **kwargs):
        running.local = local
        yield from super().check_verb_args(running, *args, **kwargs)

    def get_source(self):
        return self.api.escnx.cat.indices

"""create(**kwargs)
Create an index in Elasticsearch. http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html

Parameters:	
index – The name of the index
body – The configuration for the index (settings and mappings)
master_timeout – Specify timeout for connection to master
timeout – Explicit operation timeout
update_all_types – Whether to update the mapping for all fields with the same name across all types or not
wait_for_active_shards – Set the number of active shards to wait for before the operation returns.
"""

@command(IndicesDispatcher, verb='create')
class IndicesCreate(Verb):

    def fill_parser(self, parser):
        parser.add_option("-b", "--settingsbody", dest="max_num_segments", help="Max num segmens", default=1)

    @coroutine
    def check_verb_args(self, running, *args, max_num_segments=1, **kwargs):
        running.max_num_segments = max_num_segments
        yield from super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def get(self, running):
        return None

    @coroutine
    def execute(self, running):
        val = yield from self.api.escnx.indices.create(running.index_name)
        return val

    def to_str(self, running, value):
        name = value.get('index', None)
        status = value.get('acknowledged', False)
        if status and name is not None:
            return '%s created' % name
        else:
            return dumps(value)


@command(IndicesDispatcher, verb='stats')
class IndicesStats(DumpVerb):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-m", "--metrics", dest="metrics", help="Metrics list[]", default=None, action='append')
        parser.add_option("-f", "--flat", dest="flat", help="Flatten stats", default=False, action='store_true')

    @coroutine
    def check_verb_args(self, running, *args, metrics=None, flat=False, **kwargs):
        if metrics is not None:
            running.metrics = ','.join(metrics)
        else:
            running.metrics = None
        running.flat = flat
        yield from super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def get(self, running):
        return None

    @coroutine
    def execute(self, running):
        val = yield from self.api.escnx.indices.stats(running.index_name, metric=running.metrics)
        return val

    def to_str(self, running, value):
        if running.flat:
            pass
            #value = {k.replace('index.', ''): v for k, v in index_data['defaults'].items()}
        else:
            return dumps(value)
