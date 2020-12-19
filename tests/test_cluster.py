import eslib

import json
import tests


class ClusterTestCase(tests.TestCaseProvider):

    def test_write_settings_cluster(self):
        dispatcher = eslib.dispatchers['cluster']()
        def tester(running, j):
            self.assertIsInstance(j, tuple)
            source, settings = j
            self.assertIsNone(source)
            self.assertIsInstance(settings, dict)
        self.action_write_settings(dispatcher, ['cluster.routing.allocation.enable=all'], tester)

    def test_read_settings_cluster_json(self):
        dispatcher = eslib.dispatchers['cluster']()
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
        def tester(running, j):
            self.assertIsInstance(j, tuple)
            source, settings = j
            self.assertIsNone(source)
            self.assertIsInstance(settings, dict)
            self.assertEqual(1, len(settings))
            step1 = next(running.cmd.to_str(running, j))
            step2 = next(running.cmd.to_str(running, step1))
            self.assertRegex(step2, "cluster.name: .+r")
        self.action_read_settings(dispatcher, ['-f', 'cluster.name'], tester)

    def test_cluster_master(self):
        dispatcher = eslib.dispatchers['cluster']()
        dispatcher.api = self.ctx
        running = self._run_action(dispatcher, 'master', object_args=['-f', 'json'])
        for i in running.object:
            self.assertIsInstance(i, dict)
            self.assertIn('id', i)
            self.assertIn('host', i)
            self.assertIn('node', i)
            self.assertIn('ip', i)

    def test_cluster_health(self):
        dispatcher = eslib.dispatchers['cluster']()
        dispatcher.api = self.ctx
        running = self._run_action(dispatcher, 'health')
        self.assertIn('status', running.result)
        self.assertIn('number_of_nodes', running.result)
