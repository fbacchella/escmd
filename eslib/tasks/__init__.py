from eslib.verb import Verb, List, DumpVerb
from eslib.dispatcher import dispatcher, command, Dispatcher
from asyncio import coroutine
import datetime
import time


@dispatcher(object_name="task")
class TasksDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-i", "--id", dest="name", help="tasks filter")

    def get(self, name=None):
        val = yield from self.api.escnx.tasks.get(task_id=name)
        return val


class TreeNode(object):
    def __init__(self, value):
        self.value = value
        self.children = []

    def addchild(self, child):
        self.children.append(child)

    def __repr__(self, level=-1):
        if self.value is not None:
            ret = "    "*level+self._value_to_str()+"\n"
        else:
            ret = ''
        for child in self.children:
            ret += child.__repr__(level+1)
        return ret

    def _value_to_str(self):
        node_name = self.value.get('node_name', '')
        node_id = self.value.get('node', '')
        running_time_in_nanos = int(self.value.get('running_time_in_nanos', -1))
        start_time_in_millis = int(self.value.get('start_time_in_millis', -1))
        action = self.value.get('action', '')
        id = self.value.get('id', '')
        running_time = datetime.timedelta(seconds=running_time_in_nanos / 1e9)
        start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time_in_millis / 1000))
        return "%s/%s:%-8s %-30s %s %s" % (node_name, node_id, id, action, running_time, start_time)


@command(TasksDispatcher)
class TasksList(List):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-a", "--actions", dest="actions", default='')
        parser.add_option("-g", "--group_by", dest="group_by", default='nodes')

    @coroutine
    def get(self, name=None):
        return None

    @coroutine
    def execute(self, running, *args, **kwargs):
        val = yield from super().execute(running)
        tree = TreeNode(None)
        nodes = { '' : tree }
        try_parent = { '' : None }
        task_infos = {}
        for value in val:
            node, node_info = value
            node_name = node_info['name']
            tasks = node_info['tasks']
            for task in tasks.values():
                id = task['id']
                node = task['node']
                task_key = "%s:%s" % (node, id)
                try_parent[task_key] = task.get('parent_task_id', '')
                task['node_name'] = node_name
                task_infos[task_key] = task
        while len(try_parent) > 0:
            next_try_parent = {}
            for node, parent in try_parent.items():
                if parent in nodes:
                    node_tree = TreeNode(task_infos[node])
                    nodes[parent].addchild(node_tree)
                    nodes[node] = node_tree
                elif parent is not None:
                    next_try_parent[node] = parent
            try_parent = next_try_parent
        return tree

    @coroutine
    def validate(self, running, *args, actions='', group_by='nodes', **kwargs):
        val = yield from self.api.escnx.tasks.list(actions=actions, group_by=group_by)
        running.object = val
        running.group_by = group_by
        return True

    @coroutine
    def get_elements(self, running, **kwargs):
        return running.object['nodes'].items()

    def to_str(self, running, value):
        return '%s' % value


@command(TasksDispatcher, verb='dump')
class IndiciesDump(DumpVerb):
    pass
