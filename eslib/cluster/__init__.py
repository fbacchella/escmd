from eslib.dispatcher import dispatcher, command, Dispatcher
from eslib.verb import DumpVerb
from asyncio import coroutine
from json import dumps

@dispatcher(object_name="cluster")
class ClusterDispatcher(Dispatcher):
    pass

@command(ClusterDispatcher, verb='readsettings')
class ClusterReadSettings(DumpVerb):

    def fill_parser(self, parser):
        parser.add_option("-k", "--only_keys", dest="only_keys", default=False, action='store_true')
        parser.add_option("-p", "--pretty", dest="pretty", default=False, action='store_true')
        parser.add_option("-f", "--flat", dest="flat", default=False, action='store_true')

    @coroutine
    def get_elements(self, running, **kwargs):
        val = yield from self.api.escnx.cluster.get_settings(include_defaults=True, flat_settings=running.flat)
        return val.items()

    @coroutine
    def get(self):
        return True

    @coroutine
    def validate(self, running, *args, flat=False, **kwargs):
        running.settings = []
        running.flat = flat
        for i in args:
            setting_path = i.split('.')
            running.settings.append(setting_path)
        return super().validate(running, **kwargs)

    @coroutine
    def action(self, element, running, *args, only_keys=False, **kwargs):
        values = []
        if len(running.settings) > 0:
            for i in running.settings:
                curs = {element[0]: element[1]}
                for j in i:
                    if not isinstance(curs, dict) or curs is None:
                        break
                    curs = curs.get(j, None)
                if curs is not None:
                    if only_keys and isinstance(curs, dict):
                        values.append({'.'.join(i): curs.keys()})
                    else:
                        values.append({'.'.join(i): curs})
        else:
            values.append({element[0]: element[1]})
        return values

    def to_str(self, running, item):
        if len(item) == 0:
            yield None
        else:
            for i in item:
                (k, v) = next(iter(i.items()))
                if running.flat:
                    for (sk, sv) in v.items():
                        yield "%s: %s" % (sk, sv)
                    return
                elif not isinstance(v, str):
                    if running.only_keys:
                        v = dumps(list(v.keys()), **running.formatting)
                    else:
                        v = dumps(v, **running.formatting)
                yield "%s: %s" % (k, v)

