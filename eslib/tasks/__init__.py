from eslib.verb import Verb, DumpVerb, CatVerb
from eslib.dispatcher import dispatcher, command, Dispatcher
from eslib.tree import TreeNode
from asyncio import coroutine
import datetime
import time


@dispatcher(object_name="task")
class TasksDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-i", "--id", dest="task_id", help="tasks filter")

    @coroutine
    def check_noun_args(self, running, task_id=None):
        running.task_id = task_id

    @coroutine
    def get(self, running):
        val = yield from self.api.escnx.tasks.get(task_id=running.task_id)
        return val


class TaskTreeNode(TreeNode):

    def _value_to_str(self, level):
        node_name = self.value.get('node_name', '')
        node_id = self.value.get('node', '')
        running_time_in_nanos = int(self.value.get('running_time_in_nanos', -1))
        start_time_in_millis = int(self.value.get('start_time_in_millis', -1))
        action = self.value.get('action', '')
        id = self.value.get('id', '')
        running_time = datetime.timedelta(seconds=int(running_time_in_nanos / 1e9))
        start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(round(start_time_in_millis / 1000, 0)))
        return "%s:%-*s %-50s %s %s %s" % (node_id, 12-level*TreeNode.identation, id, action, node_name, running_time, start_time)


@command(TasksDispatcher, verb='tree')
class TasksList(Verb):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-a", "--actions", dest="actions", default='')
        parser.add_option("-g", "--group_by", dest="group_by", default='nodes')

    @coroutine
    def check_verb_args(self, running, *args, actions='', group_by='nodes', **kwargs):
        running.group_by = group_by
        running.actions = actions
        yield from super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def get(self, running):
        val = yield from self.api.escnx.tasks.list(actions=running.actions, group_by=running.group_by)
        return val

    @coroutine
    def execute(self, running):
        tree = TaskTreeNode(None)
        nodes = { '' : tree }
        try_parent = { '' : None }
        task_infos = {}
        for node, node_info in running.object['nodes'].items():
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
                    node_tree = TaskTreeNode(task_infos[node])
                    nodes[parent].addchild(node_tree)
                    nodes[node] = node_tree
                elif parent is not None:
                    next_try_parent[node] = parent
            try_parent = next_try_parent
        return tree

    def to_str(self, running, value):
        return '%s' % value


@command(TasksDispatcher, verb='dump')
class IndiciesDump(DumpVerb):

    @coroutine
    def check_verb_args(self, running, *args, actions='', group_by='nodes', **kwargs):
        running.group_by = group_by
        running.actions = actions
        yield from super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def get_elements(self, running, **kwargs):
        tasks_dict = {}
        for node, node_info in running.object['nodes'].items():
            tasks = node_info['tasks']
            for task in tasks.values():
                id = task['id']
                node = task['node']
                task_key = "%s:%s" % (node, id)
                tasks_dict[task_key] = task
        return tasks_dict.items()
