from elasticsearch import Transport
from asyncio import Future


class AsyncTransport(Transport):

    def perform_request(self, method, url, headers=None, params=None, body=None):
        futur_result = Future()
        query_iterator = self.perform_async_request(futur_result, method, url, headers, params, body)
        (connection,
         next_method,
         next_url,
         next_params,
         next_body,
         next_headers,
         ignore,
         timeout) = query_iterator.send(None)
        while True:
            curl_future = Future()
            yield from connection.perform_request(method, next_url, next_params, next_body,
                                                       headers=next_headers,
                                                       ignore=ignore,
                                                       timeout=timeout, future=curl_future)
            try:
                (connection,
                 next_method,
                 next_url,
                 next_params,
                 next_body,
                 next_headers,
                 ignore,
                 timeout) = query_iterator.send(curl_future.result)
            except StopIteration:
                break

        return futur_result.result()


