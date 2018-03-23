import asyncio
import unittest
import optparse
import eslib
from eslib import context

class DispatchersTestCase(unittest.TestCase):

    def setUp(self):
        self.ctx = context.Context(url='localhost:9200', sniff=False, debug=False)
        self.ctx.connect()
        for i in self.ctx.perform_query(self.ctx.escnx.indices.create(id(self))):
            pass

    def tearDown(self):
        self.ctx.disconnect()

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
        for i in ('task', 'index', 'template', 'node'):
            dispatcher = eslib.dispatchers[i]()
            dispatcher.api = self.ctx
            self.assertIsNotNone(self._run_action(dispatcher, 'list'))

    def test_read_settings_cluster_json(self):
        dispatcher = eslib.dispatchers['cluster']()
        settings_keys = ['persistent', 'transient', 'defaults']
        def tester(running, j):
            self.assertIsInstance(j, tuple)
            k,v = j
            self.assertIsInstance(v, dict)
            self.assertTrue(k in settings_keys)
        self.action_read_settings(dispatcher, [], tester)

    def test_read_settings_cluster_flat(self):
        dispatcher = eslib.dispatchers['cluster']()
        settings_keys = ['persistent', 'transient', 'defaults']
        def tester(running, j):
            self.assertIsInstance(j, tuple)
            k, v = j
            self.assertIsInstance(v, dict)
            self.assertTrue(k in settings_keys)
            for l in v.values():
                self.assertNotIsInstance(l, dict)
        self.action_read_settings(dispatcher, ['-f'], tester)

    def test_read_settings_cluster_flat_single(self):
        dispatcher = eslib.dispatchers['cluster']()
        settings_keys = ['persistent', 'transient', 'defaults']
        def tester(running, j):
            self.assertIsInstance(j, tuple)
            k, v = j
            self.assertIsInstance(v, dict)
            self.assertTrue(k in settings_keys)
            if len(v) == 1:
                self.assertRegex(next(running.cmd.to_str(running, j)), '[a-z-]+')
            for l in v.values():
                self.assertNotIsInstance(l, dict)
        self.action_read_settings(dispatcher, ['-f', 'cluster.name'], tester)

    def test_read_settings_index_json(self):
        dispatcher = eslib.dispatchers['index']()
        def tester(running, j):
            self.assertIsInstance(j, tuple)
            k,v = j
            self.assertIsInstance(v, dict)
        self.action_read_settings(dispatcher, [], tester)

    def test_read_settings_index_flat(self):
        dispatcher = eslib.dispatchers['index']()
        def tester(running, j):
            self.assertIsInstance(j, tuple)
            k, v = j
            self.assertIsInstance(v, dict)
            for l in v.values():
                self.assertNotIsInstance(l, dict)
        self.action_read_settings(dispatcher, ['-f'], tester)

    def test_read_settings_index_flat_single(self):
        dispatcher = eslib.dispatchers['index']()
        settings_keys = ['persistent', 'transient', 'defaults']
        def tester(running, j):
            self.assertIsInstance(j, tuple)
            k, v = j
            self.assertIsInstance(v, dict)
            if len(v) == 1:
                self.assertEqual(next(running.cmd.to_str(running, j)), '1')
            for l in v.values():
                self.assertNotIsInstance(l, dict)
        self.action_read_settings(dispatcher, ['-f', 'number_of_replicas'], tester)

    def action_read_settings(self, dispatcher, object_args, tester):
        dispatcher.api = self.ctx
        running = self._run_action(dispatcher, 'readsettings', object_args=object_args)
        for j in running.result:
            tester(running, j)

    def test_cat_indices(self):
        dispatcher = eslib.dispatchers['index']()
        dispatcher.api = self.ctx
        running = self._run_action(dispatcher, 'cat', object_args=['-f', 'json'])
        for i in running.object:
            pass

    def test_tree(self):
        for tree_noun in ('shard', 'task'):
            dispatcher = eslib.dispatchers[tree_noun]()
            dispatcher.api = self.ctx
            running = self._run_action(dispatcher, 'tree')
            for i in running.object:
                pass

    def _run_action(self, dispatcher, verb, object_options={}, object_args=[]):
        query = dispatcher.run_phrase(verb, object_options, object_args)
        running = dispatcher.api.perform_query(query)
        return running

