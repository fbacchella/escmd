import asyncio
import unittest
import optparse
import eslib
from eslib import context
import json


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
        for i in ('index', 'template', 'node'):
            dispatcher = eslib.dispatchers[i]()
            dispatcher.api = self.ctx
            self.assertIsNotNone(self._run_action(dispatcher, 'list'))

    def test_read_settings_cluster_json(self):
        dispatcher = eslib.dispatchers['cluster']()
        settings_keys = ['persistent', 'transient', 'defaults']
        def tester(running, j):
            self.assertIsInstance(j, tuple)
            source, settings = j
            self.assertIsNone(source)
            self.assertIsInstance(settings, dict)
            step1 = next(running.cmd.to_str(running, j))
            result = json.loads(step1)
            self.assertIsInstance(result, dict)
        self.action_read_settings(dispatcher, [], tester)

    def test_read_settings_cluster_flat(self):
        dispatcher = eslib.dispatchers['cluster']()
        settings_keys = ['persistent', 'transient', 'defaults']
        def tester(running, j):
            self.assertIsInstance(j, tuple)
            source, settings = j
            self.assertIsNone(source)
            self.assertIsInstance(settings, dict)
            for k,v in settings.items():
                self.assertTrue(k in settings_keys)
                self.assertIsInstance(v, dict)
        self.action_read_settings(dispatcher, ['-f'], tester)

    def test_read_settings_cluster_flat_single(self):
        dispatcher = eslib.dispatchers['cluster']()
        settings_keys = ['persistent', 'transient', 'defaults']
        def tester(running, j):
            self.assertIsInstance(j, tuple)
            source, settings = j
            self.assertIsNone(source)
            self.assertIsInstance(settings, dict)
            self.assertEqual(1, len(settings))
            step1 = next(running.cmd.to_str(running, j))
            step2 = next(running.cmd.to_str(running, step1))
            self.assertEqual("cluster.name: elasticsearch", step2)
        self.action_read_settings(dispatcher, ['-f', 'cluster.name'], tester)

    def test_read_settings_index_json(self):
        dispatcher = eslib.dispatchers['index']()
        def tester(running, j):
            self.assertIsInstance(j, tuple)
            source, settings = j
            self.assertIsNotNone(source)
            self.assertIsInstance(settings, dict)
            step1 = next(running.cmd.to_str(running, j))
            result = json.loads(step1)
            self.assertIsInstance(result, dict)
        self.action_read_settings(dispatcher, [], tester)

    def test_read_settings_index_flat(self):
        dispatcher = eslib.dispatchers['index']()
        def tester(running, j):
            self.assertIsInstance(j, tuple)
            source, settings = j
            self.assertIsNotNone(source)
            self.assertIsInstance(settings, dict)
            for step1 in running.cmd.to_str(running, j):
                step2 = next(running.cmd.to_str(running, step1))
                #print('test_read_settings_index_flat', step2)
                self.assertRegex(step2, '[0-9]+ [a-zA_Z0-9_\.]+: .*')
        self.action_read_settings(dispatcher, ['-f'], tester)

    def test_read_settings_index_flat_single(self):
        dispatcher = eslib.dispatchers['index']()
        def tester(running, j):
            self.assertIsInstance(j, tuple)
            source, settings = j
            self.assertIsNotNone(source)
            self.assertIsInstance(settings, dict)
            for step1 in running.cmd.to_str(running, j):
                step2 = next(running.cmd.to_str(running, step1))
                #print('test_read_settings_index_flat', step2)
                self.assertRegex(step2, '[0-9]+ [a-zA_Z0-9_\.]+: 1')
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


