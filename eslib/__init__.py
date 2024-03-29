import re

def join_default(val, default):
    for key, value in default.items():
        if key not in val and value is not None:
            val[key] = value


units = {
    'T': 1099511627776,
    'G': 1073741824,
    'M': 1048576,
    'K': 1024,
    'k': 1024,
    '': 1,
}
size_re = re.compile('(\\d+)([TGMKk]?)')

def parse_size(input_size, out_suffix="", default_suffix=None):
    if isinstance(input_size, str):
        matcher = size_re.match("%s" % input_size)
        if matcher is not None:
            value = float(matcher.group(1))
            suffix = matcher.group(2)
            if suffix == '' and default_suffix is not None:
                suffix = default_suffix
            return int(value * units[suffix] / units[out_suffix])
    else:
        return input_size


def create_re():
    re_elements = []
    for count in (8, 4, 4, 4, 12):
        re_elements.append('([0-9]|[a-z]){%d}' % count)
    return re.compile('^' + '-'.join(re_elements) + '$')


id_re = create_re()

def is_id(try_id):
    return isinstance(try_id, str) and id_re.match(try_id) is not None


dispatchers = { }

import eslib.indices
import eslib.nodes
import eslib.templates
import eslib.tasks
import eslib.cluster
import eslib.documents
import eslib.shards
import eslib.plugins
import eslib.xpack
import eslib.policies
import eslib.searchguard
import eslib.ilm
