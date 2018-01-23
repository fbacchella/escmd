from eslib.verb import Verb, List
from eslib.dispatcher import dispatcher, command, Dispatcher
from asyncio import coroutine
import time

@dispatcher(object_name="task")
class TasksDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-i", "--id", dest="id", help="tasks filter")

    def get(self, name=None):
        return name

class TasksVerb(Verb):

    @coroutine
    def get_elements(self):
        val = yield from self.api.escnx.tasks.get(task_id=self.object)
        return val


@command(TasksDispatcher)
class TasksList(List):

    def extract(self, value):
        return len(value.popitem()[1]), None

    def execute(self, *args, **kwargs):
        val = yield from self.api.escnx.tasks.list()
        by_nodes = val['nodes']
        def enumerator():
            for n,data in by_nodes.items():
                for tid,tdata in data['tasks'].items():
                    yield tdata
        return enumerator()

    def to_str(self, value):
        if not isinstance(value, dict):
            return None
        elif not value.get('type', None) == 'direct':
            return None
        else:
            running_time_in_nanos = int(value.get('running_time_in_nanos', -1))
            start_time_in_millis = int(value.get('start_time_in_millis', -1))
            action = value.get('action', '')
            id = value.get('id', '')
            running_time = running_time_in_nanos / 1e9
            start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time_in_millis/1000))
            return "%s %s %s %s" % (id, action, running_time, start_time)
