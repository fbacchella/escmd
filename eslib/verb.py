import optparse
import json
import collections
import re
from asyncio import coroutine
from asyncio import ensure_future, wait

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

    @coroutine
    def check_noun_args(self, running, **kwargs):
        """
        Check and eventually parse the argument given to the noun.
        Default to delegate that action to the dispatcher
        :param running: where to store the running context
        :param kwargs:
        :return:
        """
        yield from self.dispatcher.check_noun_args(running, **kwargs)

    @coroutine
    def check_verb_args(self, running, *args, **kwargs):
        pass

    @coroutine
    def get(self, running):
        val = yield from self.dispatcher.get(running)
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

    def status(self):
        """A default status command to run on success"""
        return 0;

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
            task = ensure_future(self.action(e, running))
            coros.append(task)
        if len(coros) > 0:
            done, pending = yield from wait(coros)
            def enumerator():
                for i in done:
                    if i.exception() is not None:
                        ex = i.exception()
                        #ex.source = i.object
                        ex.context = (type(ex), ex, ex.__traceback__)
                        yield ex
                    else:
                        yield i.result()
            return enumerator()
        else:
            return None

    @coroutine
    def action(self, running):
        raise NotImplementedError

    @coroutine
    def get_elements(self, running):
        return running.object.items()


class DumpVerb(RepeterVerb):

    def fill_parser(self, parser):
        parser.add_option("-k", "--only_keys", dest="only_keys", default=False, action='store_true')
        parser.add_option("-p", "--pretty", dest="pretty", default=False, action='store_true')

    @coroutine
    def check_verb_args(self, running, *args, pretty=False, only_keys=False, **kwargs):
        if pretty:
            running.formatting = {'indent': 2, 'sort_keys': True}
        else:
            running.formatting = {}
        running.only_keys = only_keys
        running.args_vector = args
        yield from super().check_verb_args(running, **kwargs)

    @coroutine
    def action(self, element, running, *args, only_keys=False, **kwargs):
        curs = element[1]
        for i in running.args_vector:
            if not isinstance(curs, dict) or curs is None:
                break
            curs = curs.get(i, None)
        if only_keys and isinstance(curs, dict):
            return (element[0], curs.keys())
        else:
            return (element[0], curs)

    def to_str(self, running, item):
        if item is None:
            return None
        elif running.only_keys:
            return json.dumps({item[0]: list(item[1])}, **running.formatting)
        else:
            return json.dumps({item[0]: item[1]}, **running.formatting)


class List(RepeterVerb):
    verb = "list"
    template = "{name!s} {id!s}"

    @coroutine
    def check_verb_args(self, running, *args, template=None, **kwargs):
        running.template = template
        super().check_verb_args(*args, **kwargs)

    def fill_parser(self, parser):
        super(List, self).fill_parser(parser)

    def execute(self, running):
        val = yield from self.get_elements(running)
        def enumerator():
            for i in val:
                yield i
        return enumerator()

    def to_str(self, running, item):
        name = item[0]
        value = item[1]
        return "%s %s" % (name, list(value.keys()))


class Remove(Verb):
    verb = "remove"

    def execute(self, *args, **kwargs):
        return self.object.remove()


class RemoveForce(Remove):

    def fill_parser(self, parser):
        parser.add_option("-f", "--force", dest="force", help="Force", default=False, action='store_true')


    def execute(self, *args, **kwargs):
        return self.object.remove(**kwargs)


class ReadSettings(DumpVerb):

    def fill_parser(self, parser):
        parser.add_option("-k", "--only_keys", dest="only_keys", default=False, action='store_true')
        parser.add_option("-p", "--pretty", dest="pretty", default=False, action='store_true')
        parser.add_option("-f", "--flat", dest="flat", default=False, action='store_true')

    @coroutine
    def get(self):
        raise NotImplementedError

    @coroutine
    def check_verb_args(self, running, *args, flat=False, **kwargs):
        running.flat = flat
        running.settings = []
        for i in args:
            setting_path = i.split('.')
            running.settings.append(setting_path)
        return super().check_verb_args(running, **kwargs)

    @coroutine
    def action(self, element, running, *args, only_keys=False, **kwargs):
        values = []
        if len(running.settings) > 0:
            for i in running.settings:
                curs = {element[0]: element[1]}
                for j in i:
                    if not isinstance(curs, dict) or curs is None:
                        break
                    curs = curs.get(j, None)
                if curs is not None:
                    if only_keys and isinstance(curs, dict):
                        values.append({'.'.join(i): curs.keys()})
                    else:
                        values.append({'.'.join(i): curs})
        else:
            values.append({element[0]: element[1]})
        return values

    def to_str(self, running, item):
        if len(item) == 0:
            yield None
        else:
            for i in item:
                (k, v) = next(iter(i.items()))
                if running.flat:
                    for (sk, sv) in v.items():
                        yield "%s=%s" % (sk, sv)
                    return
                elif not isinstance(v, str):
                    if running.only_keys:
                        v = json.dumps(list(v.keys()), **running.formatting)
                    else:
                        v = json.dumps(v, **running.formatting)
                    yield '{"%s": %s}' % (k, v)


class WriteSettings(Verb):

    keyvalue_re = re.compile(r'(^[a-z0-9_\.]+)=(.*)$')
    keydepth_re = re.compile(r'^([a-z0-9_]+)(?:\.(.*))?$')

    def fill_parser(self, parser):
        parser.add_option('-f', '--settings_file', dest='settings_file_name', default=None)

    @coroutine
    def get(self, running):
        return None

    @coroutine
    def check_verb_args(self, running, *args, settings_file_name=None, **kwargs):
        values = {}
        if settings_file_name is not None:
            with open(settings_file_name, "r") as settings_file:
                values = json.load(settings_file)
        for i in args:
            k, v = next(iter(WriteSettings.keyvalue_re.finditer(i))).groups()
            self._dict_merge(values, self._path_to_dict(k, v))
        running.values = values
        super().check_verb_args(running, **kwargs)

    def _path_to_dict(self, path, value, current_dict = {}):
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
