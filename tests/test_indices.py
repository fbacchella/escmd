import eslib
import json
import tests


class IndicesTestCase(tests.TestCaseProvider):

    def test_cat_indices_json(self):
        dispatcher = eslib.dispatchers['index']()
        dispatcher.api = self.ctx
        running = self._run_action(dispatcher, 'cat', object_args=['-f', 'json'])
        for i in running.object:
            self.assertIsInstance(i, dict)

    def test_cat_indices_with_headers(self):
        dispatcher = eslib.dispatchers['index']()
        dispatcher.api = self.ctx
        running = self._run_action(dispatcher, 'cat', object_args=['-f', 'json', '-H', 'status,docs.count'])
        for i in running.object:
            self.assertEqual(2, len(i))
            self.assertIsInstance(i, dict)
            self.assertEqual(2, len(i))
            self.assertIn('status', i.keys())
            self.assertIn('docs.count', i.keys())

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
                self.assertRegex(step2, '(\..*|[0-9]+) [a-zA_Z0-9_\.]+: .*')
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
                self.assertRegex(step2, '(\..*|[0-9]+) [a-zA_Z0-9_\.]+: 1')
        self.action_read_settings(dispatcher, ['-f', 'number_of_replicas'], tester)

