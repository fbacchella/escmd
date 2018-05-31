from eslib.exceptions import ESLibError, ESLibNotFoundError
from eslib import dispatchers
from eslib.running import Running
from asyncio import coroutine
import elasticsearch.exceptions

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
    def check_noun_args(self, running, **kwargs):
        raise NotImplementedError

    @coroutine
    def get(self, running):
        raise NotImplementedError

    def get_cmd(self, verb):
        if verb in self.verbs:
            cmd_class = self.verbs[verb]
            cmd = cmd_class(self)
            return cmd
        else:
            return None

    def clean_options(self, options):
        """
        transform options to a dict and removed undeclared arguments or empty enumerations but keep empty string
        :param options:
        :return:
        """
        return {k: v for k, v in list(vars(options).items())
                        if v is not None and (isinstance(v, str) or not hasattr(v, '__len__') or len(v) != 0)}

    def run_phrase(self, verb, object_options={}, object_args=[]):
        cmd = self.get_cmd(verb)
        running = Running(cmd)
        if cmd is None:
            raise ESLibError("unknown verb %s" % verb)
        try:
            (verb_options, verb_args) = cmd.parse(object_args)
        except SystemExit:
            return None
        verb_options = self.clean_options(verb_options)
        yield from cmd.check_noun_args(running, **object_options)
        yield from cmd.check_verb_args(running, *verb_args, **verb_options)
        running.object = yield from cmd.get(running)
        running.result = yield from cmd.execute(running)
        return running




