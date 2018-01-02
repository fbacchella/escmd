import optparse
import json
from asyncio import Future, coroutine, iscoroutine, iscoroutinefunction
from eslib import ESLibError

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

    def run_async_query(self, delayed_async_query, callback=None):
        new_future = Future()
        try_generator = delayed_async_query(new_future)
        try:
            (connection, method, url, params, body, headers, ignore, timeout) = next(try_generator)
            while True:
                future_perform = lambda: connection.perform_request(method, url, params, body, headers=headers, ignore=ignore, timeout=timeout)
                (connection, method, url, params, body, headers, ignore, timeout) = try_generator.send(future_perform)
        except StopIteration:
            pass

        @coroutine
        def wrapping_coroutine():
            result = yield from new_future
            return result

        if callback is not None:
            def forward(same_future):
                data = same_future.result()
                next_delayed_async_query_list, next_callback = callback(data)
                if next_delayed_async_query_list is not None:
                    for i in next_delayed_async_query_list:
                        if callable(i):
                            self.run_async_query(i, next_callback)
            new_future.add_done_callback(forward)
        self.waiting_futures.add(wrapping_coroutine())

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
