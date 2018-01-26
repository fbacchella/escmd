import optparse
import json
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

    @coroutine
    def validate(self, running, *args, **kwargs):
        return running.object is not None

    @coroutine
    def get(self, **object_options):
        val = yield from self.dispatcher.get(**object_options)
        return val

    @coroutine
    def execute(self, running, *args, **kwargs):
        raise NotImplementedError

    def status(self):
        """A default status command to run on success"""
        return 0;

class RepeterVerb(Verb):

    @coroutine
    def execute(self, running, *args, **kwargs):
        try:
            elements = yield from self.get_elements(running, **kwargs)
        except Exception as ex:
            # ex needs to be passed as argument, because of the 'yield from' magic,
            # it's not defined when defining the function
            def enumerator(ex):
                ex.source = object
                yield ex
            return enumerator(ex)
        coros = []
        for e in elements:
            task = ensure_future(self.action(e, running, args_vector=args, **kwargs))
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
    def action(self, **kwargs):
        raise NotImplementedError

    @coroutine
    def get_elements(self, running, **kwargs):
        return running.object.items()


class DumpVerb(RepeterVerb):

    def fill_parser(self, parser):
        parser.add_option("-k", "--only_keys", dest="only_keys", default=False, action='store_true')
        parser.add_option("-p", "--pretty", dest="pretty", default=False, action='store_true')

    @coroutine
    def validate(self, running, *args, pretty=False, only_keys=False, **kwargs):
        if pretty:
            running.formatting = {'indent': 2, 'sort_keys': True}
        else:
            running.formatting = {}
        running.only_keys = only_keys
        return super().validate(running, *args, **kwargs)

    @coroutine
    def action(self, element, *args, only_keys=False, **kwargs):
        curs = element
        for i in args:
            if not isinstance(curs, dict) or curs is None:
                break
            curs = curs.get(i, None)
        if only_keys and isinstance(curs, dict):
            return curs.keys()
        else:
            return curs

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
    def validate(self, running, template=None):
        running.template = template
        return True

    def fill_parser(self, parser):
        super(List, self).fill_parser(parser)
        parser.add_option("-t", "--template", dest="template", help="template for output formatting, default to %s" % self.template)

    def execute(self, running, template=None):
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
