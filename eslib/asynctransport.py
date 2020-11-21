from elasticsearch import Transport, TransportError, ConnectionTimeout, RequestError, AuthorizationException, AuthenticationException
from asyncio import Future


class AsyncTransport(Transport):

    def __init__(self, *args, loop=None, **kwargs):
        super(AsyncTransport, self).__init__(*args, **kwargs)
        self.loop = loop

    def perform_async_request(self, future, method, url, headers=None, params=None, body=None):
        """
        Perform the actual request. Retrieve a connection from the connection
        pool, pass all the information to it's perform_request method and
        return the data.

        If an exception was raised, mark the connection as failed and retry (up
        to `max_retries` times).

        If the operation was succesful and the connection used was previously
        marked as dead, mark it as live, resetting it's failure count.

        :arg method: HTTP method to use
        :arg url: absolute url (without host) to target
        :arg headers: dictionary of headers, will be handed over to the
            underlying :class:`~elasticsearch.Connection` class
        :arg params: dictionary of query parameters, will be handed over to the
            underlying :class:`~elasticsearch.Connection` class for serialization
        :arg body: body of the request, will be serializes using serializer and
            passed to the connection
        """
        if body is not None:
            body = self.serializer.dumps(body)

            # some clients or environments don't support sending GET with body
            if method in ('HEAD', 'GET') and self.send_get_body_as != 'GET':
                # send it as post instead
                if self.send_get_body_as == 'POST':
                    method = 'POST'

                # or as source parameter
                elif self.send_get_body_as == 'source':
                    if params is None:
                        params = {}
                    params['source'] = body
                    body = None

        if body is not None:
            try:
                body = body.encode('utf-8', 'surrogatepass')
            except (UnicodeDecodeError, AttributeError):
                # bytes/str - no need to re-encode
                pass

        ignore = ()
        timeout = None
        if params:
            timeout = params.pop('request_timeout', None)
            ignore = params.pop('ignore', ())
            if isinstance(ignore, int):
                ignore = (ignore, )

        for attempt in range(self.max_retries + 1):
            connection = self.get_connection()
            future_result = yield (connection, method, url, params, body, headers, ignore, timeout)
            # The first used to return params return with no future_result, so skip it
            if future_result is None:
                continue

            try:
                status, headers_out, data = future_result()
            except (RequestError, AuthorizationException, AuthenticationException) as e:
                future.set_exception(e)
                break
            except TransportError as e:
                if method == 'HEAD' and e.status_code == 404:
                    future.set_result(False)
                    break
                retry = False
                if isinstance(e, ConnectionTimeout):
                    retry = self.retry_on_timeout
                elif isinstance(e, ConnectionError):
                    retry = True
                elif e.status_code in self.retry_on_status:
                    retry = True

                if retry:
                    # only mark as dead if we are retrying
                    self.mark_dead(connection)
                    # raise exception on last retry
                    if attempt == self.max_retries:
                        future.set_exception(e)
                        break
                else:
                    future.set_exception(e)
                    break
            else:
                if method == 'HEAD':
                    future.set_result(200 <= status < 300)
                else:
                    try:
                        if data:
                            data = self.deserializer.loads(data, headers_out.get('content-type'))
                        future.set_result(data)
                    except Exception as e:
                        future.set_exception(e)
                break

    def perform_request(self, method, url, headers=None, params=None, body=None):
        futur_result = Future(loop=self.loop)
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
            curl_future = Future(loop=self.loop)
            yield from connection.perform_request(method, next_url, next_params, next_body,
                                                       headers=next_headers,
                                                       ignore=ignore, future=curl_future)
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


