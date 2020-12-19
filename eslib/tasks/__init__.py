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

    def check_noun_args(self, running, task_id=None, **kwargs):
        running.task_id = task_id
        return super().check_noun_args(running, task_id=task_id, **kwargs)

    @coroutine
    def get(self, running, task_id=None, **kwargs):
        val = yield from self.api.escnx.tasks.get(task_id=task_id)
        return val


class TaskTreeNode(TreeNode):

    def _value_to_str(self, level):
        node_name = self.value.get('node_name', '')
        running_time_in_nanos = int(self.value.get('running_time_in_nanos', -1))
        action = self.value.get('action', '')
        running_time = datetime.timedelta(seconds=int(running_time_in_nanos / 1e9))
        indentation = 35 - level*TreeNode.identation - len(action)
        while indentation < 0:
            indentation +=16
        return "%s %-*s%s%-*s%s" % \
               (action, indentation, '', node_name, 10-len(node_name), '', running_time)


@command(TasksDispatcher, verb='tree')
class TasksTree(Verb):

    def fill_parser(self, parser):
        super().fill_parser(parser)
        parser.add_option("-a", "--actions", dest="actions", default='')
        parser.add_option("-g", "--group_by", dest="group_by", default='nodes')

    #task_id is not used in tree, swallow it
    def check_noun_args(self, running, task_id=None, **kwargs):
        return super().check_verb_args(running, **kwargs)

    def check_verb_args(self, running, group_by=None, actions=None, **kwargs):
        running.group_by = group_by
        running.actions = actions
        return super().check_verb_args(running, **kwargs)

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
            if len(try_parent) == len(next_try_parent):
                break
            try_parent = next_try_parent
        return tree

    def to_str(self, running, value):
        return '%s' % value


@command(TasksDispatcher, verb='dump')
class TaskDump(DumpVerb):

    def check_verb_args(self, running, *args, actions='', group_by='nodes', **kwargs):
        running.group_by = group_by
        running.actions = actions
        return super().check_verb_args(running, *args, **kwargs)

    @coroutine
    def get_elements(self, running):
        tasks_dict = {}
        for node, node_info in running.object['nodes'].items():
            tasks = node_info['tasks']
            for task in tasks.values():
                id = task['id']
                node = task['node']
                task_key = "%s:%s" % (node, id)
                tasks_dict[task_key] = task
        return tasks_dict.items()


@command(TasksDispatcher)
class TaskCat(CatVerb):

    def get_source(self):
        return self.api.escnx.cat.tasks
