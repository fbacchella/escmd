from eslib.verb import Verb, List, DumpVerb, RepeterVerb, CatVerb
from eslib.dispatcher import dispatcher, command, Dispatcher
from yaml import load
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
from asyncio import coroutine


@dispatcher(object_name="policy")
class PoliciesDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-n", "--name", dest="policy_name", default=None, help="policies filter")

    def check_noun_args(self, running, policy_name=None, **kwargs):
        running.policy_name = policy_name
        return super().check_noun_args(running, policy_name=policy_name, **kwargs)

    @coroutine
    def get(self, running, policy_name=None, filter_path=None):
        val = yield from self.api.escnx.ilm.get_lifecycle(policy=policy_name)
        return val


@command(PoliciesDispatcher)
class PoliciesList(List):

    @coroutine
    def action(self, element, running):
        return {element[0]: element[1]}

    def to_str(self, running, item):
        name, value = list(item[1].items())[0]
        return name


@command(PoliciesDispatcher, verb='dump')
class PoliciesDump(DumpVerb):
    pass


@command(PoliciesDispatcher, verb='put')
class PoliciesPut(Verb):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-f", "--policy_file", dest="policy_file_name", default=None)

    def check_verb_args(self, running, *args, policy_file_name=None, **kwargs):
        if running.policy_name is None:
            raise Exception("-n/--name mandatory is not defined")
        if policy_file_name is None:
            raise Exception("-f/--policy_file mandatory is not defined")
        with open(policy_file_name, "r") as policy_file:
            running.policy = load(policy_file, Loader=Loader)
        if 'policy' not in running.policy:
            running.policy = {'policy': running.policy}
        return super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def get(self, running, **kwargs):
        return {}

    @coroutine
    def execute(self, running):
        val = yield from self.api.escnx.ilm.put_lifecycle(policy=running.policy_name, body=running.policy)
        return val

    def to_str(self, running, value):
        return "%s added" % running.policy_name


@command(PoliciesDispatcher, verb='delete')
class PoliciesDelete(RepeterVerb):

    @coroutine
    def action(self, element, *args, **kwargs):
        val = yield from self.api.escnx.ilm.delete_lifecycle(policy=element[0])
        return val

    def format(self, running, name, result):
        return "%s deleted" % name
