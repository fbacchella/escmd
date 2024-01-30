import json

from eslib.context import TypeHandling
from eslib.verb import Verb, DumpVerb, RepeterVerb, ReadSettings, WriteSettings, CatVerb, List
from eslib.dispatcher import dispatcher, command, Dispatcher
from eslib.exceptions import ESLibError
from asyncio import coroutine
from json import loads, dumps
from elasticsearch.exceptions import NotFoundError, ElasticsearchException, RequestError
import re
from yaml import load
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


@dispatcher(object_name="index", default_filter_path='*.settings.index.uuid')
class IndicesDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-n", "--name", dest="index_name", help="index filter", default='_all')
        parser.add_option("--ignore_unavailable", dest="ignore_unavailable", help="ignore unavailable", default=None, action='store_true')
        parser.add_option("--allow_no_indices", dest="allow_no_indices", help="allow no indices", default=None, action='store_true')
        parser.add_option("--expand_wildcards", dest="expand_wildcards", help="expand wildcards", default=None, action='store_true')

    def check_noun_args(self, running, index_name='_all', **kwargs):
        running.index_name = index_name
        return super().check_noun_args(running, index_name=index_name, **kwargs)

    @coroutine
    def get(self, running, index_name='_all', expand_wildcards=None, allow_no_indices=None, ignore_unavailable=None, filter_path='*'):
        val = yield from self.api.escnx.indices.get(index=index_name,
                                                    expand_wildcards=expand_wildcards,
                                                    allow_no_indices=allow_no_indices,
                                                    ignore_unavailable=ignore_unavailable,
                                                    filter_path=filter_path
        )
        return val


@command(IndicesDispatcher, verb='list')
class IndiciesList(List):

    template = lambda self, x, y: "%s\t%12d\t%4d\t%12s" % (x, y['indices'][x]['primaries']['docs']['count'], y['indices'][x]['primaries']['segments']['count'], y['indices'][x]['primaries']['store']['size'])

    @coroutine
    def action(self, element, running):
        stats = yield from self.api.escnx.indices.stats(index=element[0], human=True)
        return stats


@command(IndicesDispatcher, verb='forcemerge')
class IndiciesForceMerge(Verb):

    def fill_parser(self, parser):
        parser.add_option("-m", "--max_num_segments", dest="max_num_segments", help="Max num segmens", default=1)
        parser.add_option("-t", "--notranslog", dest="notranslog", help="Remove translog", default=False, action='store_true')
        parser.add_option("-c", "--codec", dest="codec", default=None)

    def check_verb_args(self, running, *args, max_num_segments=1, notranslog=False, codec=None, **kwargs):
        running.max_num_segments = max_num_segments
        running.notranslog = notranslog
        if codec == 'best_compression' or codec == 'default':
            running.codec = codec
        elif codec is not None:
            raise Exception('Unknown codec')
        else:
            running.codec = None

        return super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def get(self, running, index_name, filter_path):
        running.index_name = index_name
        val = yield from super().get(running, index_name=index_name, filter_path=filter_path)
        return val

    @coroutine
    def execute(self, running):
        if running.notranslog:
            yield from self.api.escnx.indices.put_settings(index=running.index_name,
                                                           body={'index': {'translog': {'flush_threshold_size': '56b'}}})
        if running.codec is not None:
            yield from self.api.escnx.indices.close(index=running.index_name)
            yield from self.api.escnx.indices.put_settings(index=running.index_name,
                                                           body={'index': {'codec': running.codec}})
            yield from self.api.escnx.indices.open(index=running.index_name, wait_for_active_shards='all',
                                                   timeout="%ds" % self.api.timeout)
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

    def check_verb_args(self, running):
        if running.index_name == '*' or running.index_name == '_all':
            raise Exception("won't destroy everything, -n/--name mandatory")
        return super().check_verb_args(running)

    def format(self, running, name, result):
        return "%s deleted" % name


@command(IndicesDispatcher, verb='reindex')
class IndiciesReindex(RepeterVerb):
    """
    Used to reindex indices.
    To reindex everything for a give indices prefix, one can use:
    ./escmd -c sg.ini -U https://localhost index -n "$PREFIX-*,<-$PREFIX-{now/d}>" reindex -s $NEXT -i '(.*?)[a-z]?$' -c 'best_compression'
    where $PREFIX is the prefix for reindexed indices and $NEXT is a one letter suffix incremented with each reindexation
    """
    def fill_parser(self, parser):
        parser.add_option("-t", "--use_template", dest="template_name", help="Template to use for reindexing", default=None)
        parser.add_option("-p", "--prefix", dest="prefix", default='')
        parser.add_option("-k", "--keepmapping", dest="keep_mapping", default=False, action='store_true')
        parser.add_option("-s", "--suffix", dest="suffix", default='')
        parser.add_option("-i", "--infix_regex", dest="infix_regex", default=None)
        parser.add_option("-c", "--codec", dest="codec", default=None)

    def check_noun_args(self, running, **kwargs):
        return super().check_noun_args(running, filter_path='*.settings.index.lifecycle,*.settings.index.uuid,*.aliases', **kwargs)

    def check_verb_args(self, running, *args, template_name=None, infix_regex=None, prefix='', suffix='', keep_mapping=False,
                        codec=None,
                        **kwargs):
        running.settings = {}
        running.mappings = None
        running.old_replica = None
        if codec == 'best_compression' or codec == 'default':
            running.codec = codec
        elif codec is not None:
            return False
        else:
            running.codec = None
        if template_name is not None:
            running.template_name = template_name
            running.keep_mapping = False
        else:
            running.template_name = None
            running.keep_mapping = keep_mapping

        if infix_regex is not None:
            running.infix_regex = re.compile(infix_regex)
        else:
            running.infix_regex = None
        running.prefix = prefix
        running.suffix = suffix
        return super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def action(self, element, running):
        if running.template_name is not None:
            templates = yield from self.api.escnx.indices.get_template(name=running.template_name)
            if running.template_name in templates:
                template = templates[running.template_name]
                settings = template['settings']
                mappings = template['mappings']
                old_replica = settings.get('index', {}).get('number_of_replicas', None)
                settings['index']['number_of_replicas'] = 0
                running.mappings = mappings
                running.old_replica = old_replica
                running.settings = settings
            else:
                raise ESLibError('No matching template found')

        index_name = element[0]
        index_data = element[1]
        if running.infix_regex is not None:
            match = running.infix_regex.match(index_name)
            if match is not None and len(match.groups()) >= 1:
                infix = match.group(1)
            else:
                raise ESLibError('invalid matching pattern')
        else:
            infix = index_name
        new_index_name = running.prefix + infix + running.suffix
        v = yield from self.api.escnx.indices.exists(index=new_index_name)
        if v:
            raise ESLibError('%s already exists' % new_index_name)

        if running.keep_mapping:
            index = yield from self.api.escnx.indices.get(index_name, filter_path='*.mappings')
            for i in index.items():
                mappings = i[1]['mappings']
            settings = {'index': {}}
        elif running.mappings is not None:
            mappings = running.mappings
            settings = dict(running.settings)
        else:
            mappings = {}
            settings = {'index': {}}
        for k in 'creation_date', 'uuid', 'provided_name', 'version':
            if k in settings.get('index', {}):
                del settings.get('index')[k]

        new_index_settings = settings
        if running.codec:
            new_index_settings['index']['codec'] = running.codec

        # if lifecycle managed, extract informations for later processing
        if 'lifecycle' in index_data['settings']['index']:
            lifecycle = index_data['settings']['index']['lifecycle']
            del index_data['settings']['index']['lifecycle']
        else:
            lifecycle = None

        for i in (yield from self.api.escnx.ilm.explain_lifecycle(index_name)).values():
            ilm = [x for x in i.values()][0]
            if ilm['managed'] and ilm['action'] != 'complete':
                raise ESLibError('%s currently in life cycle processing' % index_name)

        # Create the index
        yield from self.api.escnx.indices.create(index=new_index_name, body={'settings': new_index_settings, "mappings": mappings})

        # Doing the reindexation
        reindex_status = yield from self.api.escnx.reindex(
            body={"conflicts": "proceed", 'source': {'index': index_name, 'sort': '_doc', 'size': 10000},
                  'dest': {'index': new_index_name, 'version_type': 'external'}})

        # Ensure the minimum segments
        yield from self.api.escnx.indices.forcemerge(new_index_name, max_num_segments=1, flush=True)

        if lifecycle is not None:
            yield from self.api.escnx.indices.put_settings(index=new_index_name, body={'index': {'lifecycle': lifecycle}})

        if ilm['managed']:
            for i in (yield from self.api.escnx.ilm.explain_lifecycle(new_index_name)).values():
                ilm_new = [x for x in i.values()][0]
            new_step_command = {
                "current_step": {
                    "phase": ilm_new['phase'],
                    "action": "complete",
                    "name": "complete"
                },
                "next_step": {
                    "phase": ilm['phase'],
                    "action": "complete",
                    "name": "complete"
                }
            }
            self.api.escnx.ilm.move_to_step(new_index_name, new_step_command)

        # Only delete if not failure
        if len(reindex_status['failures']) == 0:
            # Moving aliases
            aliases = index_data['aliases']
            aliases_actions = []
            if len(aliases) > 0:
                for a in aliases:
                    aliases_actions.append({'remove': {'index': index_name, 'alias': a}})
                    aliases_actions.append({'add': {'index': new_index_name, 'alias': a}})
                yield from self.api.escnx.indices.update_aliases(body={'actions': aliases_actions})

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
    def get_elements(self, running):
        index_names = []
        for i in running.object:
            index_names.append((i, None))
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
        parser.add_option("-t", "--type", dest="type", help="The type to add the mapping to", default='_default_')

    def check_verb_args(self, running, *args, mapping_file_name=None, type='_default_', **kwargs):
        with open(mapping_file_name, "r") as mapping_file:
            running.mapping = load(mapping_file, Loader=Loader)
        if 'mappings' in running.mapping:
            running.mapping = running.mapping['mappings']
        if type in running.mapping:
            running.mapping = running.mapping[type]
        if 'properties' in running.mapping:
            running.properties = running.mapping['properties']
        else:
            raise Exception("type not found in mapping")
        running.type = type
        return super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def action(self, element, running):
        val = yield from self.api.escnx.indices.put_mapping(index=element[0], body={"properties": running.properties}, doc_type=running.type)
        return val

    def to_str(self, running, value):
        return "%s -> %s" % (list(running.object.keys())[0], value.__str__())


@command(IndicesDispatcher, verb='getfields')
class IndicesGetFieldMapping(RepeterVerb):

    def fill_parser(self, parser):
        parser.add_option("-t", "--type", dest="type", help="The type to add the mapping to", default='_default')
        parser.add_option("-f", "--flat", dest="flat", default=False, action='store_true')
        parser.add_option("-p", "--pretty", dest="pretty", default=False, action='store_true')

    def check_verb_args(self, running, *args, flat=False, type='_default_', **kwargs):
        running.type = type
        running.flat = flat
        return super().check_verb_args(running, *args, **kwargs)

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
        except NotFoundError as e:
            return e

    def result_is_valid(self, result):
        return not isinstance(result, ElasticsearchException)

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

    def check_verb_args(self, running, *args, local=False, **kwargs):
        running.local = local
        return super().check_verb_args(running, *args, **kwargs)

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

    def check_verb_args(self, running, *args, max_num_segments=1, **kwargs):
        running.max_num_segments = max_num_segments
        return super().check_verb_args(running, *args, **kwargs)

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

    def check_verb_args(self, running, *args, metrics=None, flat=False, **kwargs):
        if metrics is not None:
            running.metrics = ','.join(metrics)
        else:
            running.metrics = None
        running.flat = flat
        return super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def action(self, element, running, *args, only_keys=False, **kwargs):
        val = yield from self.api.escnx.indices.stats(element[0], metric=running.metrics)
        return val

    def to_str(self, running, value):
        if running.flat:
            pass
            #value = {k.replace('index.', ''): v for k, v in index_data['defaults'].items()}
        else:
            return dumps(value[1])


@command(IndicesDispatcher, verb='explain_lifecycle')
class IndicesExplainLifecycl(DumpVerb):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-e", "--only_errors", dest="only_errors", help="filters the indices included in the response to ones in an ILM error state, implies only_managed", default=None, action='store_true')
        parser.add_option("-m", "--only_managed", dest="only_managed", help="filters the indices included in the response to ones managed by ILM", default=None, action='store_true')

    def check_verb_args(self, running, *args, only_errors=None, only_managed=None, **kwargs):
        running.only_errors = only_errors
        running.only_managed = only_managed
        return super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def action(self, element, running, *args, only_keys=False, **kwargs):
        val = yield from self.api.escnx.ilm.explain_lifecycle(element[0], only_errors=running.only_errors, only_managed=running.only_managed)
        return val

    def to_str(self, running, value):
        return dumps(value[1])


@command(IndicesDispatcher, verb='retry_policy')
class IndicesExplainLifecycl(RepeterVerb):

    @coroutine
    def action(self, element, running, *args, only_keys=False, **kwargs):
        val = yield from self.api.escnx.ilm.retry(element[0])
        return val

    def to_str(self, running, value):
        return dumps(value[1])


@command(IndicesDispatcher, verb='updatemapping')
class IndicesUpdateMapping(RepeterVerb):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-t", "--template_file", dest="template_file_name", default=None)
        parser.add_option("-m", "--mapping_file", dest="template_file_name", default=None)
        parser.add_option("-d", "--doc_type", dest="doc_type", default=None)

    def check_verb_args(self, running, *args, template_file_name=None, mapping_file=None, doc_type=None, **kwargs):
        if template_file_name is not None:
            with open(template_file_name, "r") as template_file:
                template = load(template_file, Loader=Loader)
            if doc_type is not None and self.api.type_handling == TypeHandling.DEPRECATED and 'mappings' in template and doc_type in template['mappings']:
                # Removing an explicit give type from the mapping
                running.mappings = template['mappings'][doc_type]
                running.with_type = None
                running.doc_type = None
            elif doc_type is not None and self.api.type_handling == TypeHandling.TRANSITION:
                # Given a type_name, used to detect is type is present or not
                running.with_type = 'mappings' in template and doc_type in template['mappings']
                if running.with_type:
                    running.mappings = template['mappings'][doc_type]
                    running.doc_type = doc_type
                else:
                    running.mappings = template['mappings']
                    running.doc_type = None
            elif doc_type is not None and self.api.type_handling == TypeHandling.IMPLICIT:
                raise Exception("implicit type handling don't accept type_name argument")
            else:
                running.mappings = template['mappings']
                running.doc_type = None
        elif mapping_file is not None:
            with open(template_file_name, "r") as template_file:
                running.mapping = load(template_file, Loader=Loader)
        return super().check_verb_args(running, *args, **kwargs)

    async def action(self, element, running):
        return await self.api.escnx.indices.put_mapping(running.mappings, index=element[0], doc_type=running.doc_type)

    def to_str(self, running, value):
        name = value[0][0]
        status = value[1].get('acknowledged', False)
        if status and name is not None:
            return '%s mapping updated' % name
        elif status and name is not None:
            return {name: value}
        else:
            return value


@command(IndicesDispatcher, verb='allocation_explain')
class ClusterAllocationExplain(DumpVerb):

    def fill_parser(self, parser):
        parser.add_option("-P", "--primary", dest="primary", default=None, action='store_true')
        parser.add_option("-s", "--shard", dest="shard", default=None)
        super().fill_parser(parser)

    def check_verb_args(self, running, *args, index=None, primary=None, shard=None, **kwargs):
        running.args = args
        running.index = index
        running.primary = primary
        running.shard = shard
        return super().check_verb_args(running, *args, **kwargs)

    async def action(self, element, running):
        body = {'index': element[0], 'shard': 0, 'primary': True}
        if running.shard is not None:
            body['shard'] = running.shard
        if running.primary is not None:
            body['primary'] = running.primary
        try:
            val = self.api.escnx.cluster.allocation_explain(body=body)
        except RequestError as e:
            if e.error == 'illegal_argument_exception':
                reason = e.info.get('error', {}).get('reason','')
                if reason.startswith:
                    return None
                else:
                    raise e
            else:
                raise e
            val = None
        return await val

    def to_str(self, running, item):
        return json.dumps(item, **running.formatting)
