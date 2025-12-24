import pycurl
from enum import IntEnum
from logging import Logger


class CurlDebugType(IntEnum):
    TEXT = 1
    HEADER = 2
    DATA = 4
    SSL = 8


def curl_debug_handler(debug_filter, logger, debug_type, data):
    """
    This is the implementation of the cURL debug callback.
    :param debug_type:  what kind of data to debug.
    :param data: the data to debug.
    """

    prefix = {pycurl.INFOTYPE_TEXT: '   ' if CurlDebugType.TEXT & debug_filter else False,
              pycurl.INFOTYPE_HEADER_IN: '<  ' if CurlDebugType.HEADER & debug_filter else False,
              pycurl.INFOTYPE_HEADER_OUT: '>  ' if CurlDebugType.HEADER & debug_filter else False,
              pycurl.INFOTYPE_DATA_IN: '<< ' if CurlDebugType.DATA & debug_filter else False,
              pycurl.INFOTYPE_DATA_OUT: '>> ' if CurlDebugType.DATA & debug_filter else False,
              pycurl.INFOTYPE_SSL_DATA_IN: '<S ' if CurlDebugType.SSL & debug_filter else False,
              pycurl.INFOTYPE_SSL_DATA_OUT: '>S ' if CurlDebugType.SSL & debug_filter else False
            }[debug_type]
    if prefix is False:
        return

    # Some versions of PycURL provide the debug data as strings, and
    # some as arrays of bytes, so we need to check the type of the
    # provided data and convert it to strings before trying to
    # manipulate it with the "replace", "strip" and "split" methods:
    if not debug_type == pycurl.INFOTYPE_SSL_DATA_IN and not pycurl.INFOTYPE_SSL_DATA_OUT:
        text = data.decode('utf-8', 'replace') if type(data) == bytes else data
    else:
        text = data.decode('unicode_escape', 'replace') if type(data) == bytes else data

    # Split the debug data into lines and send a debug message for
    # each line:
    lines = [x for x in text.replace('\r\n', '\n').split('\n') if len(x) > 0]
    for line in lines:
        if isinstance(logger, Logger):
            logger.debug("%s%s" % (prefix, line))
        else:
            print("%s%s" % (prefix, line), file=logger)
