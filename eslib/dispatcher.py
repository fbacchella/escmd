from eslib import ESLibError, dispatchers
from eslib.running import Running
from asyncio import coroutine


def command(dispatcher_class, verb=None):
    def decorator(command_class):
        if verb is not None:
            command_class.verb = verb
        dispatchers[dispatcher_class.object_name].verbs[command_class.verb] = command_class
        return command_class
    return decorator


def dispatcher(object_name):
    def decorator(dispatcher_class):
        dispatcher_class.object_name = object_name
        setattr(dispatcher_class, 'verbs', {})
        dispatchers[object_name] = dispatcher_class

        return dispatcher_class
    return decorator


class Dispatcher(object):

    def __init__(self):
        self._api = None

    def add_command(self, new_command):
        self.verbs[new_command.verb] = new_command

    def fill_parser(self, parser):
        pass

    def execute(self):
        raise NotImplementedError

    @coroutine
    def get(self, **object_options):
        raise NotImplementedError

    def get_cmd(self, verb):
        if verb in self.verbs:
            cmd_class = self.verbs[verb]
            cmd = cmd_class(self)
            return cmd
        else:
            return None

    def run_phrase(self, verb, object_options={}, object_args=[]):
        cmd = self.get_cmd(verb)
        if cmd is None:
            raise ESLibError("unknown verb %s" % verb)
        object = yield from cmd.get(**object_options)
        running = Running(cmd, object)
        try:
            (verb_options, verb_args) = cmd.parse(object_args)
        except SystemExit:
            return None
        #try:
        validated = yield from cmd.validate(running, *verb_args, **vars(verb_options))
        if not validated:
            raise ESLibError("validation failed")
        #except TypeError:
        #    raise ESLibError("validation failed")
        # transform options to a dict and removed undeclared arguments or empty enumerations
        # but keep empty string
        verb_options = {k: v for k, v in list(vars(verb_options).items())
                        if v is not None and (isinstance(v, str) or not hasattr(v, '__len__') or len(v) != 0)}
        running.result = yield from cmd.execute(running, *verb_args, **verb_options)
        return running




