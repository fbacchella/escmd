from elasticsearch import Transport

class AsyncTransport(Transport):

    def perform_request(self, method, url, headers=None, params=None, body=None):
        return lambda x: self.perform_async_request(x, method, url, headers, params, body)
