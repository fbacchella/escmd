import elasticsearch.exceptions

class ESLibError(Exception):
    def __init__(self, error_message, value={}, exception=None):
        self.value = value
        if error_message is None:
            self.error_message = value
        else:
            self.error_message = error_message
        if exception is not None:
            self.exception = exception

    def __str__(self):
        return str(self.error_message)


class ESLibNotFoundError(ESLibError):
    def __init__(self, e, *args, **kwargs):
        error_info = e.info.pop('error', {})
        resource_id = error_info.pop('resource.id', None)
        _id = e.info.pop('_id', None)
        _index = e.info.pop('_index', None)
        if resource_id is not None:
            message="Resource '%s' not found" % resource_id
        elif _id is not None and _index is not None:
            message = "Document '%s/%s' not found" % (_index, _id)
        else:
            print(e.info)
            message = "Resource not found"
        super().__init__(message, *args, exception=e, **kwargs)


class ESLibHttpNotFoundError(ESLibError):
    def __init__(self, e, *args, url=None, **kwargs):
        if url is not None:
            message="URL '%s' not found" % url
        else:
            message="URL not found"
        super().__init__(message, *args, exception=e, **kwargs)


# Reserver for non ES-errors (proxy error, 404 not issued by ES
class ESLibTransportError(ESLibError):
    pass


class ESLibProxyError(ESLibTransportError):
    pass


class ESLibConnectiontError(ESLibError):
    pass


class ESLibTimeoutError(ESLibTransportError):
    def __init__(self, e):
        self.url = e.info
        self.duration = e.args[3]
        super().__init__(e.error, exception=e)


class ESLibRequestError(ESLibError):
    def __init__(self, e, *args, **kwargs):
        super().__init__(e.info['error']['reason'], *args, exception=e, value=e.info['error'], **kwargs)


class ESLibScriptError(ESLibError):
    def __init__(self, e, *args, **kwargs):
        error_info = e.info.pop('error', {})
        script_stack = error_info.pop('script_stack', [])
        reason = error_info.pop('reason', '')
        caused_by = error_info.pop('caused_by', {})
        caused_by_reason = caused_by.pop('reason', None)
        message = "script error"
        if caused_by_reason is not None:
            message += ": %s" % caused_by_reason
        else:
            message += ": %s" % reason
        if len(script_stack) > 0:
            message += "\n" + "\n".join(script_stack)
        super().__init__(message, *args, exception=e, **kwargs)


class ESLibConflictError(ESLibError):
    def __init__(self, e, *args, **kwargs):
        failures  = e.info.pop('failures', [])
        message = "conflict error\n"
        if len(failures) > 0:
            message += "%d failures\n" % len(failures)
            message += "first one:\n%s\n" % failures[0]
        message += "%s\n" % e.info
        super().__init__(message, *args, exception=e, **kwargs)


def _get_notfound_error(ex):
    print(type(ex), ex)
    if not ex.info.get('elasticerror', True):
        url = None
        if len(ex.args) >= 4:
            url = ex.args[3]
        return ESLibHttpNotFoundError(ex, url=url)
    else:
        return ESLibNotFoundError(ex)


def _get_transport_error(e):
    if e.status_code == 502:
        return ESLibProxyError(e)
    else:
        return e

ex_mapping = {
        elasticsearch.exceptions.ConflictError: lambda e: ESLibConflictError(e),
        elasticsearch.exceptions.RequestError: lambda e: ESLibRequestError(e),
        elasticsearch.exceptions.NotFoundError: lambda e: _get_notfound_error(e),
        elasticsearch.exceptions.TransportError: lambda e: _get_transport_error(e),
        elasticsearch.exceptions.ConnectionTimeout: lambda e: ESLibTimeoutError(e),
}

def resolve_exception(ex):
    if type(ex) in ex_mapping:
        return ex_mapping[type(ex)](ex)
    else:
        return ex
