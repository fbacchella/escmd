import optparse
import eslib
import tests
from eslib.escmd import get_parser
from eslib.context import Context


class MiscTestCase(tests.TestCaseProvider):

    def test_list_nouns(self):
        self.assertTrue('cluster' in eslib.dispatchers)
        self.assertTrue('index' in eslib.dispatchers)
        self.assertTrue('node' in eslib.dispatchers)
        self.assertTrue('task' in eslib.dispatchers)
        self.assertTrue('template' in eslib.dispatchers)
        self.assertTrue('plugin' in eslib.dispatchers)
        self.assertTrue('shard' in eslib.dispatchers)

    def test_verb_instance(self):
        for dispatch_class in eslib.dispatchers.values():
            dispatch = dispatch_class()
            dispatch.api = self.ctx

            parser_object = optparse.OptionParser()
            parser_object.disable_interspersed_args()
            dispatch.fill_parser(parser_object)
            try:
                parser_object.parse_args(['-h'])
            except SystemExit:
                pass
            for verb_name, verb_class in dispatch.verbs.items():
                running = self._run_action(dispatch, verb_name, object_args=['-h'])
                self.assertIsNone(running)

    def test_list_verb(self):
        for i in ('index', 'template', 'node'):
            dispatcher = eslib.dispatchers[i]()
            dispatcher.api = self.ctx
            self.assertIsNotNone(self._run_action(dispatcher, 'list'))

    def test_cat_verb(self):
        for i in ('shard', 'task', 'index', 'node', 'plugin', 'template'):
            dispatcher = eslib.dispatchers[i]()
            dispatcher.api = self.ctx
            self.assertIsNotNone(self._run_action(dispatcher, 'cat'))

    def test_cat_nodes_json(self):
        dispatcher = eslib.dispatchers['node']()
        dispatcher.api = self.ctx
        running = self._run_action(dispatcher, 'cat', object_args=['-f', 'json'])
        for i in running.object:
            self.assertIsInstance(i, dict)

    def test_cat_task_json(self):
        dispatcher = eslib.dispatchers['task']()
        dispatcher.api = self.ctx
        running = self._run_action(dispatcher, 'cat', object_args=['-f', 'json'])
        for i in running.object:
            self.assertIsInstance(i, dict)

    def test_cat_shard_json(self):
        dispatcher = eslib.dispatchers['shard']()
        dispatcher.api = self.ctx
        running = self._run_action(dispatcher, 'cat', object_args=['-f', 'json'])
        for i in running.object:
            self.assertIsInstance(i, dict)

    def test_tree(self):
        for tree_noun in ('shard', 'task'):
            dispatcher = eslib.dispatchers[tree_noun]()
            dispatcher.api = self.ctx
            running = self._run_action(dispatcher, 'tree')
            for i in running.object:
                pass


    def test_default_parser_args(self):
        parser = get_parser()
        kwargs={}
        arg_options = dict(Context.arg_options)
        for o in parser.option_list:
            if o.dest is None or o.dest == 'config_file':
                continue
            else:
                self.assertTrue(o.dest in Context.arg_options)
                path = arg_options.pop(o.dest)
                default = Context.default_settings[path[0]][path[1]]
                kwargs[o.dest] = default
        ctx = Context(**kwargs)
        # Some interal Context args that are not reachable from command line
        arg_options.pop('transport_class')
        arg_options.pop('connection_class')
        arg_options.pop('sniff')
        self.assertEqual(0, len(arg_options))
        self.assertTrue(ctx.connect())


    def test_empty_context(self):
        ctx = Context()
        self.assertTrue(ctx.connect())
