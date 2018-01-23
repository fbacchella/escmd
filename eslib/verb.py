import optparse
import json
from asyncio import Future, coroutine
from elasticsearch.compat import string_types
from collections import Iterable
from queue import Queue
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
        self.dispatcher = object
        self.object = None
        self.waiting_futures = set()
        self.results = Queue()

    def fill_parser(self, parser):
        pass

    def validate(self):
        """try to validate the object needed by the commande, should be overriden if the no particular object is expected"""
        return True

    def uses_template(self):
        return False

    def parse(self, args):
        parser = optparse.OptionParser(usage=self.__doc__)
        self.fill_parser(parser)

        (verb_options, verb_args) = parser.parse_args(args)
        return (verb_options, verb_args)

    def execute(self, *args, **kwargs):
        kwargs = yield from self.filter_args(**kwargs)
        try:
            val = yield from self.get_elements()
        except Exception as ex:
            # ex needs to be passed as argument, because of the yield from magic
            # It's not defined when defining the function
            def enumerator(ex):
                ex.source = self.object
                yield ex
            return enumerator(ex)
        #, filter_path=['*.settings.index.provided_name']
        coros = []
        for name, object in val.items():
            task = ensure_future(self.action(object_name=name, object=object, args_vector=args, **kwargs))
            task.object_name = name
            coros.append(task)
        # No matching object, try with empty option
        if len(coros) == 0:
            task = ensure_future(self.action(name=self.object, **kwargs))
            task.object = name
            coros.append(task)

        if len(coros) > 0:
            done, pending = yield from wait(coros)
            def enumerator():
                for i in done:
                    if i.exception() is not None:
                        ex = i.exception()
                        ex.source = i.object_name
                        ex.context = (type(ex), ex, ex.__traceback__)
                        yield ex
                    else:
                        yield i.object_name, i.result()
            return enumerator()
        else:
            return ()

    @coroutine
    def filter_args(self, **kwargs):
        return kwargs

    @coroutine
    def get_elements(self):
        raise NameError('Not implemented')

    @coroutine
    def action(self, **kwargs):
        raise NameError('Not implemented')

    def to_str(self, value):
        if value is True:
            return "success"
        elif isinstance(value, dict):
            if value.get('acknowledged', False):
                return 'acknowledged'
            else:
                return json.dumps(value)
        else:
            return str(value)

    def get(self, lister, **kwargs):
        return lister.get(**kwargs)

    def status(self):
        """A default status command to run on success"""
        return 0;


class RepeterVerb(Verb):

    def validate(self, *args, **kwargs):
        return self.object is not None

    def get(self, lister, **kwargs):
        return lister.list(**kwargs)


class List(RepeterVerb):
    verb = "list"
    template = "{name!s} {id!s}"

    def validate(self, *args, **kwargs):
        return True

    def fill_parser(self, parser):
        super(List, self).fill_parser(parser)
        parser.add_option("-t", "--template", dest="template", help="template for output formatting, default to %s" % self.template)

    def execute(self, template=None):
        val = yield from self.get_elements()
        def enumerator():
            for i in val.items():
                yield i
        return enumerator()

    def to_str(self, item):
        return item



class Remove(Verb):
    verb = "remove"

    def execute(self, *args, **kwargs):
        return self.object.remove()


class RemoveForce(Remove):

    def fill_parser(self, parser):
        parser.add_option("-f", "--force", dest="force", help="Force", default=False, action='store_true')


    def execute(self, *args, **kwargs):
        return self.object.remove(**kwargs)
