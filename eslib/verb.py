import optparse
import json
from asyncio import Future
from elasticsearch.compat import string_types
from collections import Iterable
from eslib.asynctransport import QueryIterator

# Find the best implementation available on this platform
try:
    from io import StringIO
except:
    from io import StringIO


def callback(cb):
    def decorator(old_method):
        def new_method(self, *args, **kwargs):
            returned = old_method(self, *args, **kwargs)
            return returned, cb.__get__(self)
        return new_method
    return decorator


class Verb(object):
    """A abstract class, used to implements actual verb"""
    def __init__(self, dispatcher):
        self.api = dispatcher.api
        self.dispatcher = object
        self.object = None
        self.waiting_futures = set()
        self.results_futures = []

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

    def start(self, scheduler, data, callback):
        self.scheduler = scheduler
        self.add_async_query(data, callback)

    def forward(self, future, callback):
        data = future.result()
        forwarded = False
        ex = None
        while callback is not None:
            try:
                data, callback = callback(data)
            except Exception as e:
                ex = e
                break
            for i in data if data is not None and not isinstance(data, QueryIterator) and isinstance(data, Iterable) and not isinstance(data, string_types) else (data,):
                if isinstance(i, QueryIterator):
                    self.add_async_query(i, callback)
                    forwarded = True
                else:
                    pass
            if forwarded:
                callback = None
        if not forwarded:
            result_future = Future()
            if ex is not None:
                result_future.set_exception(ex)
            else:
                result_future.set_result(data)
            self.results_futures.append(result_future)

    def add_async_query(self, delayed_async_query, callback):
        delayed_async_query.futur_result.add_done_callback(lambda x: self.forward(x, callback))
        self.scheduler.schedule(delayed_async_query)


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
        if template is not None:
            self.template = template

        for i in self.object:
            yield i

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
