import optparse
import json
import collections
import re
from asyncio import coroutine
from asyncio import ensure_future, wait
from elasticsearch.exceptions import TransportError, ElasticsearchException
from eslib.exceptions import resolve_exception, ESLibError

# Find the best implementation available on this platform
try:
    from io import StringIO
except:
    from io import StringIO


class Verb(object):
    """A abstract class, used to implements actual verb"""
    def __init__(self, dispatcher):
        self.api = dispatcher.api
        self.dispatcher = dispatcher

    def fill_parser(self, parser):
        pass

    def parse(self, args):
        parser = optparse.OptionParser(usage=self.__doc__)
        self.fill_parser(parser)

        (verb_options, verb_args) = parser.parse_args(args)
        return (verb_options, verb_args)

    def check_noun_args(self, running, **kwargs):
        """
        Check and eventually parse the argument given to the noun.
        Default to delegate that action to the dispatcher
        :param running: where to store the running context
        :param kwargs:
        :return:
        """
        return self.dispatcher.check_noun_args(running, **kwargs)

    def check_verb_args(self, running, *args, **kwargs):
        return kwargs

    @coroutine
    def get(self, running, **kwargs):
        val = yield from self.dispatcher.get(running, **kwargs)
        return val

    @coroutine
    def execute(self, running):
        raise NotImplementedError

    def to_str(self, running, value):
        if value is True:
            return "success"
        elif isinstance(value, dict):
            if value.get('acknowledged', False):
                return 'acknowledged'
            else:
                return json.dumps(value)
        else:
            return str(value)


class RepeterVerb(Verb):

    @coroutine
    def execute(self, running, *args, **kwargs):
        try:
            elements = yield from self.get_elements(running)
        except Exception as ex:
            # ex needs to be passed as argument, because of the 'yield from' magic,
            # it's not defined when defining the function
            def enumerator(ex):
                ex.source = object
                yield ex
            return enumerator(ex)
        coros = []
        for e in elements:
            task = ensure_future(self.action(e, running), loop=self.api.loop)
            task.element = e
            coros.append(task)
        if len(coros) > 0:
            done, pending = yield from wait(coros, loop=self.api.loop)
            def enumerator():
                for i in done:
                    if i.exception() is not None:
                        ex = i.exception()
                        ex.context = (type(ex), ex, ex.__traceback__)
                        if isinstance(ex, TransportError):
                            yield resolve_exception(ex)
                        else:
                            yield ex
                    else:
                        yield (i.element, i.result())
            return enumerator()
        else:
            return None

    @coroutine
    def action(self, element, running):
        raise NotImplementedError

    @coroutine
    def get_elements(self, running):
        return running.object.items()

    def to_str(self, running, value):
        if value[0] is not None:
            name = value[0][0]
        else:
            name = None
        result = value[1]
        if self.result_is_valid(result):
            return self.format(running, name, result)
        elif isinstance(result, (ElasticsearchException, ESLibError)):
            return "failed %s: %s" % (name, result.info['error']['reason'])
        else:
            return "%s -> %s" % (name, json.dumps(result))

    def result_is_valid(self, result):
        return not isinstance(result, (ElasticsearchException, ESLibError)) and result.get('acknowledged', False)

    def format(self, running, name, result):
        raise NotImplementedError


class DumpVerb(RepeterVerb):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-k", "--only_keys", dest="only_keys", default=False, action='store_true')
        parser.add_option("-p", "--pretty", dest="pretty", default=False, action='store_true')
        super().fill_parser(parser)

    def check_verb_args(self, running, *args, pretty=False, only_keys=False, **kwargs):
        if pretty:
            running.formatting = {'indent': 2, 'sort_keys': True}
        else:
            running.formatting = {}
        running.only_keys = only_keys
        running.attrs = args
        return super().check_verb_args(running, pretty=pretty, **kwargs)

    @coroutine
    def get(self, running, **kwargs):
        val = yield from self.dispatcher.get(running, **kwargs)
        return val

    @coroutine
    def action(self, element, running, *args, only_keys=False, **kwargs):
        curs = element[1]
        for i in running.attrs:
            if not isinstance(curs, dict) or curs is None:
                break
            curs = curs.get(i, None)
        if only_keys and isinstance(curs, dict):
            return (element[0], curs.keys())
        else:
            return (element[0], curs)

    def to_str(self, running, result):
        item = result[1]
        identity = item[0]
        values = item[1]
        if item is None:
            return None
        elif len(running.attrs) == 1:
            if running.attrs[0] in values:
                content = values[running.attrs[0]]
            else:
                content = None
        elif len(running.attrs) > 0:
            interested_in = {i: values[i] for i in running.attrs if i in values}
            content = self.filter_dump(running, interested_in)
        elif running.only_keys:
            content = self.filter_dump(identity, list(values))
        else:
            content = self.filter_dump(identity, values)
        return json.dumps(content, **running.formatting)

    def filter_dump(self, running, *args, **kwargs):
        if len(args) == 1:
            return args
        elif len(args) == 2:
            return {args[0]: args[1]}


class List(RepeterVerb):
    verb = "list"
    template = "{name!s}"

    def check_verb_args(self, running, template=None, **kwargs):
        running.template = template
        return super().check_verb_args(running, **kwargs)

    def fill_parser(self, parser):
        super(List, self).fill_parser(parser)
        parser.add_option("-t", "--template", dest="template", default=None)

    @coroutine
    def action(self, element, running, **kwargs):
        node_info = self.dispatcher.get(running, **kwargs)
        return node_info

    def to_str(self, running, item):
        name = item[0][0]
        value = item[1]
        template = running.template
        if template is None:
            template = self.template
        if isinstance(template, str):
            return template.format(name=name, **value)
        else:
            return str(template(name, value))


class CatVerb(Verb):
    verb = "cat"

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-H", "--headers", dest="h", default=None)
        parser.add_option("-f", "--format", dest="format", default='text')
        parser.add_option("-p", "--pretty", dest="pretty", default=False, action='store_true')

    def check_verb_args(self, running, *args, pretty=False, **kwargs):
        if pretty:
            running.formatting = {'indent': 2, 'sort_keys': True}
        else:
            running.formatting = {}
        return super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def get(self, running, **kwargs):
        return {}

    @coroutine
    def execute(self, running, **kwargs):
        val = yield from self.get_source()(**kwargs)
        return val

    def to_str(self, running, item):
        if isinstance(item, str):
            return item
        else:
            return json.dumps(item, **running.formatting)


class Remove(Verb):
    verb = "remove"

    def execute(self, *args, **kwargs):
        return self.object.remove()


class RemoveForce(Remove):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-f", "--force", dest="force", help="Force", default=False, action='store_true')


    def execute(self, *args, **kwargs):
        return self.object.remove(**kwargs)


class ReadSettings(DumpVerb):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-f", "--flat", dest="flat", default=False, action='store_true')

    def check_verb_args(self, running, *args, flat=False, **kwargs):
        running.flat = flat if len(args) == 0 else True
        running.settings = set(args)
        return super().check_verb_args(running, **kwargs)

    @coroutine
    def action(self, element, running, *args, only_keys=False, **kwargs):
        settings = yield from self.get_settings(running, element)
        if len(running.settings) > 0:
            new_settings = {}
            for category, values in settings.items():
                cat_values = {k: v for k,v in values.items() if k in running.settings }
                if len(cat_values) != 0:
                    new_settings[category] = cat_values
            return new_settings
        else:
            return settings

    def to_str(self, running, item):
        source, settings = item
        # If only one object to be inspected, don't output it's name
        if running.object is not None and len(running.object) == 1:
            source = None
        if len(settings) == 0:
            yield None
        elif isinstance(settings, dict):
            if running.flat:
                for i in settings.values():
                    for k,v in i.items():
                        yield source, (k,v)
            else:
                yield json.dumps({source: settings}, **running.formatting)
        elif isinstance(settings, tuple):
            prefix = "%s " % source if source is not None else ""
            yield "%s%s: %s" % (prefix, settings[0], settings[1])


class WriteSettings(RepeterVerb):

    keyvalue_re = re.compile(r'^([-a-zA-Z0-9_\.]+)=(.*)$')
    keydepth_re = re.compile(r'^([-a-zA-Z0-9_]+)(?:\.(.*))?$')

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option('-f', '--settings_file', dest='settings_file_name', default=None)

    def check_verb_args(self, running, *args, settings_file_name=None, **kwargs):
        values = {}
        if settings_file_name is not None:
            with open(settings_file_name, "r") as settings_file:
                values = json.load(settings_file)
        for i in args:
            try:
                k, v = next(iter(WriteSettings.keyvalue_re.finditer(i))).groups()
                if v == 'null':
                    v = None
                failed = False
                self._dict_merge(values, self._path_to_dict(k, v))
            except StopIteration:
                failed = True
        if failed:
            raise Exception('invalid key ' + i)
        running.values = values
        return super().check_verb_args(running, **kwargs)

    def _path_to_dict(self, path, value, current_dict = None):
        if current_dict is None:
            current_dict = {}
        current_key, next_path = next(WriteSettings.keydepth_re.finditer(path)).groups()
        if next_path is None:
            current_dict[current_key] = value
        else:
            current_dict[current_key] = self._path_to_dict(next_path, value, {})
        return current_dict

    # see https://gist.github.com/angstwad/bf22d1822c38a92ec0a9
    def _dict_merge(self, dct, merge_dct):
        """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
        updating only top-level keys, dict_merge recurses down into dicts nested
        to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
        ``dct``.

        :param dct: dict onto which the merge is executed
        :param merge_dct: dct merged into dct
        :return: None
        """
        for k, v in merge_dct.items():
            if (k in dct and isinstance(dct[k], dict)
                    and isinstance(merge_dct[k], collections.Mapping)):
                self._dict_merge(dct[k], merge_dct[k])
            else:
                dct[k] = merge_dct[k]
