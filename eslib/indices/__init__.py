from eslib.verb import Verb, DumpVerb, RepeterVerb, ReadSettings, WriteSettings, CatVerb
from eslib.dispatcher import dispatcher, command, Dispatcher
from asyncio import coroutine
import re
from json import dumps, load


@dispatcher(object_name="index")
class IndiciesDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-n", "--name", dest="name", help="index filter")

    @coroutine
    def check_noun_args(self, running, name='_all'):
        running.index_name = name

    @coroutine
    def get(self, running):
        val = yield from self.api.escnx.indices.get(index=running.index_name)
        return val


@command(IndiciesDispatcher, verb='list')
class IndiciesList(RepeterVerb):

    @coroutine
    def action(self, element, running):
        val = yield from self.api.escnx.indices.stats(index=element[0])
        return val

    def to_str(self, running, item):
        for i,j in item['indices'].items():
            return "%s\t%12d\t%4d" % (i,j['primaries']['docs']['count'],j['total']['segments']['count'])


@command(IndiciesDispatcher, verb='forcemerge')
class IndiciesForceMerge(RepeterVerb):

    def fill_parser(self, parser):
        parser.add_option("-m", "--max_num_segments", dest="max_num_segments", help="Max num segmens", default=1)

    @coroutine
    def check_verb_args(self, running, *args, max_num_segments=1, **kwargs):
        running.max_num_segments = max_num_segments
        yield from super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def action(self, element, running):
        val = yield from self.api.escnx.indices.forcemerge(element[0], max_num_segments=running.max_num_segments, flush=True)
        return val

    def to_str(self, running, value):
        return "%s -> %s" % (list(running.object.keys())[0], value.__str__())


@command(IndiciesDispatcher, verb='delete')
class IndiciesDelete(RepeterVerb):

    @coroutine
    def action(self, element, *args, **kwargs):
        val = yield from self.api.escnx.indices.delete(index=element[0])
        return val

    @coroutine
    def check_verb_args(self, running, ):
        if running.index_name == '*':
            raise Exception("won't destroy everything, -n/--name mandatory")

    def to_str(self, running, value):
        return "%s -> %s" % (list(running.object.keys())[0], value.__str__())


@command(IndiciesDispatcher, verb='reindex')
class IndiciesReindex(RepeterVerb):

    def fill_parser(self, parser):
        parser.add_option("-t", "--use_template", dest="template_name", help="Template to use for reindexing", default=None)
        parser.add_option("-p", "--prefix", dest="prefix", default='')
        parser.add_option("-s", "--suffix", dest="suffix", default='')
        parser.add_option("-i", "--infix_regex", dest="infix_regex", default=None)

    @coroutine
    def check_verb_args(self, running, *args, template_name=None, infix_regex=None, prefix='', suffix='', **kwargs):
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
            if len(match.groups()) == 1:
                infix = match.group(1)
            else:
                raise Exception('invalid matching pattern ')
        else:
            infix = index_name
        new_index_name = running.prefix + infix + running.suffix
        v = yield from self.api.escnx.indices.exists(index=new_index_name)
        if v:
            return (new_index_name, 'does exists')
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

        yield from self.api.escnx.indices.delete(index=index_name)

        # the old index was not suffixed, keep it's name as an alias
        if infix == index_name:
            yield from self.api.escnx.indices.update_aliases(body={
                'actions': [
                    {'add': {'index': new_index_name, 'alias': index_name}}
                ]
            })
        yield from self.api.escnx.indices.forcemerge(element[0], max_num_segments=1, flush=True)
        return (index_name, reindex_status)

    def to_str(self, running, value):
        return dumps({value[0]: value[1]})


@command(IndiciesDispatcher, verb='dump')
class IndiciesDump(DumpVerb):
    pass


@command(IndiciesDispatcher, verb='readsettings')
class IndiciesReadSettings(ReadSettings):

    @coroutine
    def get(self, running):
        val = yield from self.api.escnx.cat.indices(index=running.index_name, format='json', h='index')
        return val

    @coroutine
    def get_elements(self, running):
        indices = []
        for i in running.object:
            index_entry = yield from self.api.escnx.indices.get(index=i['index'], include_defaults=True, flat_settings=running.flat)
            index_name, index_data = next(iter(index_entry.items()))
            if running.flat:
                # remove 'index.' at the beginning of settings names
                new_settings = {}
                for k,v in index_data['settings'].items():
                    new_settings[k.replace('index.', '')] = v
                indices.append((index_name, new_settings))
            else:
                indices.append((index_name, index_data['settings']['index']))
        return indices

    def to_str(self, running, item):
        if len(running.object) > 1:
            if running.flat:
                k,v = item
                v = {"%s/%s" % (k, i): j for i,j in v.items()}
                return super().to_str(running, (k, v))
            else:
                k, v = item
                return super().to_str(running, ('', {k: v}))
        else:
            return super().to_str(running, item)


@command(IndiciesDispatcher, verb='writesettings')
class IndiciesWriteSettings(WriteSettings, RepeterVerb):

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


@command(IndiciesDispatcher, verb='addmapping')
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


@command(IndiciesDispatcher)
class IndiciesCat(CatVerb):

    def get_source(self):
        return self.api.escnx.cat.indices
