from elasticsearch import Transport
from asyncio import Future, coroutine

class QueryIterator(object):

    def __init__(self, transport, method, url, headers, params, body):
        self.futur_result = Future()
        self.query_iterator = transport.perform_async_request(self.futur_result, method, url, headers, params, body)

    def __iter__(self):
        (self.connection,
         self.method,
         self.url,
         self.params,
         self.body,
         self.headers,
         self.ignore,
         self.timeout) = self.query_iterator.send(None)

        @coroutine
        def generator():
            curl_future = Future()
            self.connection.perform_request(self.method, self.url, self.params, self.body, headers=self.headers,
                                            ignore=self.ignore,
                                            timeout=self.timeout, future=curl_future)
            yield from curl_future
            (self.connection,
             self.method,
             self.url,
             self.params,
             self.body,
             self.headers,
             self.ignore,
             self.timeout) = self.query_iterator.send(curl_future.result)

        return generator()

    def schedule(self, waiting_list):
        waiting_list.add(iter(self))

class AsyncTransport(Transport):

    def perform_request(self, method, url, headers=None, params=None, body=None):
        return QueryIterator(self, method, url, headers, params, body)
