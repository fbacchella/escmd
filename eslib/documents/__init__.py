from asyncio import coroutine
from json import loads, dumps

from eslib.dispatcher import dispatcher, command, Dispatcher
from eslib.verb import Verb, RepeterVerb, DumpVerb
from eslib.exceptions import ESLibScriptError

import elasticsearch.exceptions

@dispatcher(object_name="document")
class DocumentDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-i", "--index", dest="index_name", help="index filter", action="store", type="string")
        parser.add_option("-I", "--document_id", dest="id", help="document id", action="store", type="string")
        parser.add_option("-q", "--query", dest="query", help="search query", action="store", type="string")
        parser.add_option("-S", "--size", dest="size", help="size", action="store", type="int")
        parser.add_option("-t", "--type", dest="doc_type", help="Document type", action="store", default=None)

    @coroutine
    def check_noun_args(self, running, id=None, index_name='*', query=None, size=50, doc_type='_all'):
        running.index_name = index_name
        running.id = id
        if query is not None:
            running.query = loads(query)
        else:
            running.query = None
        running.size = size
        running.doc_type = doc_type

    @coroutine
    def get(self, running):
        if running.id is not None:
            val = yield from self.api.escnx.get(index=running.index_name, id=running.id, doc_type=running.doc_type)
        elif running.search is not None:
            val = yield from self.api.escnx.search(index=running.index_name, body=running.search, size=running.size, doc_type=running.doc_type)
        else:
            val = None
        return val


@command(DocumentDispatcher, verb='dump')
class IndiciesDump(DumpVerb):

    @coroutine
    def get(self, running):
        val = yield from super().get(running)
        # Required a single document, return it as a list
        if running.id is not None:
            val = {running.id: val}
        return val

@command(DocumentDispatcher, verb='update_by_query')
class DocumentUpdateByQuery(Verb):

    def fill_parser(self, parser):
        parser.add_option("-s", "--script", dest="script", help="langage:script", default=None)
        parser.add_option("-S", "--scroll_size", dest="scroll_size", help="scroll size", default=None, action="store", type="int")
        super().fill_parser(parser)

    @coroutine
    def check_verb_args(self, running, *args, script=None, scroll_size=None, **kwargs):
        if running.doc_type == '_all' or running.doc_type is None:
            running.doc_type = ''
        if script is not None:
            running.langage, running.expr = script.split(':', 1)
        else:
            running.langage = None
        running.scroll_size = scroll_size
        yield from super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def get(self, running):
        return None

    @coroutine
    def execute(self, running):
        body={}
        if running.langage is not None:
            body['script'] = {'lang': running.langage, 'source': running.expr}
        if running.query is not None:
            body['query'] = running.query
        try:
            val = yield from self.api.escnx.update_by_query(index=running.index_name, body=body, size=None, scroll_size=running.scroll_size,
                                                            doc_type=running.doc_type)
            return val
        except elasticsearch.exceptions.TransportError as e:
            if e.error == 'script_exception' and e.status_code == 500:
                raise ESLibScriptError(e)
            else:
                raise e

    def to_str(self, running, value):
        return dumps(value)


@command(DocumentDispatcher, verb='list')
class IndiciesList(RepeterVerb):

    @coroutine
    def get_elements(self, running, **kwargs):
        hits = []
        for i in running.object['hits']['hits']:
            hits.append(i['_id'])
        return hits

    def to_str(self, running, item):
        return "%s" % (item.keys())

    @coroutine
    def action(self, element, running):
        return element
