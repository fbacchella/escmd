import elasticsearch.exceptions


class PyCurlException(elasticsearch.exceptions.ConnectionError):
    def __str__(self):
        return "curl error %s on URL %s" % (
            self.error,
            self.info,
        )


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


class ESLibConnectionError(ESLibTransportError):
    def __init__(self, exception=None):
        self.url = exception.info
        super().__init__(exception.error, exception=exception)


class ESLibTimeoutError(ESLibConnectionError):
    def __init__(self, exception=None):
        self.duration = exception.args[3]
        super().__init__(exception=exception)


class ESLibAuthorizationException(ESLibError):
    def __init__(self, e, *args, **kwargs):
        super().__init__(self._resolve_reason(e), *args, exception=e, value=self._resolve_error(e), **kwargs)

    def _resolve_error(self, e):
        if 'error' in e.info:
            return e.info['error']
        else:
            return {}

    def _resolve_reason(self, e):
        info = self._resolve_error(e)
        if 'reason' in info:
            return info['reason']
        else:
            return 'Access denied'


class ESLibAuthenticationException(ESLibError):
    def __init__(self, e, *args, **kwargs):
        url = e.args[3].split('?', 1)[0]
        super().__init__(e.error + " " + url, *args, exception=e, **kwargs)


class ESLibBatchError(ESLibError):
    def __init__(self, e, *args, **kwargs):
        failures = e.info['failures']
        super().__init__('%d Error%s in batch' % (len(failures), 's' if len(failures) > 0 else ''), *args, exception=e, value='Batch error', **kwargs)


class ESLibRequestError(ESLibError):
    def __init__(self, e, *args, **kwargs):
        if (e.status_code >= 400 and e.status_code< 500) or ('elasticerror' in e.info and e.info['elasticerror'] == True):
            super().__init__(e.info['error']['reason'], *args, exception=e, value=e.info['error'], **kwargs)
        else:
            super().__init__('HTTP failure: ' + e.error, *args, exception=e, value=e.status_code, **kwargs)


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


class ESLibPyCurlError(ESLibError):
    def __init__(self, e, *args, **kwargs):
        super().__init__(str(e), *args, exception=e, **kwargs)


def _get_notfound_error(ex):
    # same kind of exception returned for internal Elastic errors or pure HTTP error
    # The only difference is the type of ex.info.error: dict for internal Elastic errors or str
    if 'error' in ex.info and isinstance(ex.info['error'], dict):
        return ESLibNotFoundError(ex)
    else:
        url = None
        if len(ex.args) >= 4:
            url = ex.args[3]
        return ESLibHttpNotFoundError(ex, url=url)


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
        elasticsearch.exceptions.AuthorizationException: lambda e: ESLibAuthorizationException(e),
        elasticsearch.exceptions.AuthenticationException: lambda e: ESLibAuthenticationException(e),
        elasticsearch.exceptions.ConnectionTimeout: lambda e: ESLibTimeoutError(e),
        elasticsearch.exceptions.ConnectionError: lambda e: ESLibConnectionError(e),
}

def resolve_exception(ex):
    if isinstance(ex, elasticsearch.exceptions.TransportError) and ex.info is not None and 'failures' in ex.info:
        return ESLibBatchError(ex)
    if type(ex) in ex_mapping:
        return ex_mapping[type(ex)](ex)
    else:
        return ex
