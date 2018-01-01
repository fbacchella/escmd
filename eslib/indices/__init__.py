import eslib.verb
from elasticsearch.exceptions import RequestError
from eslib.dispatcher import dispatcher, command, Dispatcher

import re
import json

@dispatcher(object_name="index")
class IndiciesDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-n", "--name", dest="name", help="index filter")

    def get(self, name=None):
        return name


@command(IndiciesDispatcher)
class IndiciesList(eslib.verb.List):

    def execute(self, *args, **kwargs):
        return self.api.escnx.cat.indices(), self.next

    def next(self, value):
        for i in value:
            yield i,None

    def to_str(self, value):
        if isinstance(value, dict):
            if value.get('acknowledged', False):
                return 'acknowledged'
            else:
                return json.dumps(value)
        else:
            return str(value)


@command(IndiciesDispatcher, verb='forcemerge')
class IndiciesForceMerge(eslib.verb.Verb):

    def fill_parser(self, parser):
        parser.add_option("-m", "--max_num_segments", dest="max_num_segments", help="Max num segmens", default=1)

    def execute(self, max_num_segments=1):
        self.max_num_segments = max_num_segments
        return self.api.escnx.indices.get(index=self.object, filter_path=['*.settings.index.provided_name']), self.next

    def next(self, value):
        for i in value:
            index_name = value['index']
            yield self.api.escnx.indices.forcemerge(index_name, max_num_segments=self.max_num_segments, flush=True), None


@command(IndiciesDispatcher, verb='delete')
class IndiciesDelete(eslib.verb.Verb):

    def execute(self, *args, **kwargs):
        return self.api.escnx.indices.delete(index=self.object)


@command(IndiciesDispatcher, verb='reindex')
class IndiciesReindex(eslib.verb.Verb):

    version_re = re.compile('(?P<indexseed>.*)\\.v(?P<numver>\\d+)')

    def fill_parser(self, parser):
        parser.add_option("-t", "--use_template", dest="template_name", help="Template to use for reindexing", default=None)
        parser.add_option("-v", "--version", dest="version")
        parser.add_option("-c", "--current_suffix", dest="current", default=None)
        parser.add_option("-s", "--separator", dest="separator", default='_')

    def execute(self, template_name=None, version='next', current=None, separator='_'):
        template = None
        mappings = None
        settings = None
        if template_name is not None:
            templates = self.api.escnx.indices.get_template(name=template_name)
            if template_name in templates:
                template = templates[template_name]
                settings = template['settings']
                mappings = template['mappings']
                old_replica = settings.get('index', {}).get('number_of_replicas', None)
                settings['index']['number_of_replicas'] = 0
        for (index_name, index) in self.api.escnx.indices.get(index=self.object).items():
            try:
                new_index_name = index_name + separator + version
                if self.api.escnx.indices.exists(index=new_index_name):
                    continue
                print('reindexing', index_name)
                if template is None:
                    print("reusing mapping")
                    mappings = index['mappings']
                    settings = index['settings']
                    old_replica = settings.get('index', {}).get('number_of_replicas', None)
                    settings['index']['number_of_replicas'] = 0
                self.api.escnx.indices.create(index=new_index_name, body={'settings': settings})
                for i in mappings.keys():
                    print("adding mapping for type", i)
                    self.api.escnx.indices.put_mapping(doc_type=i, index=new_index_name, body=mappings[i], update_all_types=True)
                reindex_status =  self.api.escnx.reindex(body={"conflicts": "proceed", 'source': {'index': index_name, 'sort': '_doc', 'size': 1000}, 'dest': {'index': new_index_name, 'version_type': 'external'}})
                self.api.escnx.indices.forcemerge(new_index_name, max_num_segments=1, flush=False)
                if old_replica is not None and old_replica != 0:
                    self.api.escnx.indices.put_settings(index=new_index_name, body={'index': {'number_of_replicas': old_replica }})
                if current is not None:
                    current_name = index_name + separator + current
                    if self.api.escnx.indices.exists_alias(name=current_name):
                        print(self.api.escnx.indices.delete_alias(index='_all', name=current_name))
                        print(self.api.escnx.indices.put_alias(index=new_index_name, name=current_name))
                self.api.escnx.indices.put_alias(index=new_index_name, name=index_name + separator + "reindexed" )
                yield reindex_status
            except RequestError as e:
                if 'failures' in e.info and 'cause' in e.info['failures']:
                    message = e.info['failures']['cause']
                elif 'error' in e.info and  'root_cause' in e.info['error']['root_cause']:
                    message = e.error + ", " + e.info['error']['root_cause'][0]['reason']
                else:
                    message = "%s" % e
                print('failed to reindex %s: %s' % (index_name, message))



@command(IndiciesDispatcher, verb='settings')
class IndiciesSettings(eslib.verb.Verb):

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
