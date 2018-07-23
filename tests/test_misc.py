import optparse
import eslib
import tests


class MiscTestCase(tests.TestCaseProvider):

    def test_list_nouns(self):
        self.assertTrue('cluster' in eslib.dispatchers)
        self.assertTrue('index' in eslib.dispatchers)
        self.assertTrue('node' in eslib.dispatchers)
        self.assertTrue('task' in eslib.dispatchers)
        self.assertTrue('template' in eslib.dispatchers)

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

    def test_write_settings_cluster(self):
        dispatcher = eslib.dispatchers['cluster']()
        def tester(running, j):
            self.assertIsInstance(j, tuple)
            source, settings = j
            self.assertIsNone(source)
            self.assertIsInstance(settings, dict)
        self.action_write_settings(dispatcher, ['cluster.routing.allocation.enable=all'], tester)

    def test_cat_nodes_json(self):
        dispatcher = eslib.dispatchers['node']()
        dispatcher.api = self.ctx
        running = self._run_action(dispatcher, 'cat', object_args=['-f', 'json'])
        for i in running.object:
            self.assertIsInstance(i, dict)

    def test_cat_template_json(self):
        dispatcher = eslib.dispatchers['template']()
        dispatcher.api = self.ctx
        running = self._run_action(dispatcher, 'cat', object_args=['-f', 'json'])
        for i in running.object:
            self.assertIsInstance(i, dict)

    def test_cat_template_json_local(self):
        dispatcher = eslib.dispatchers['template']()
        dispatcher.api = self.ctx
        running = self._run_action(dispatcher, 'cat', object_args=['-f', 'json', '-l'])
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

