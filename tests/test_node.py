import eslib
from eslib.exceptions import ESLibRequestError
import tests


class NodeTestCase(tests.TestCaseProvider):

    def test_cat_json(self):
        dispatcher = eslib.dispatchers['node']()
        dispatcher.api = self.ctx
        running = self._run_action(dispatcher, 'cat', object_args=['-f', 'json'])
        print(running.object)
        self.assertIsInstance(running.object, list)
        self.assertEqual(len(running.object), 1)
        for i in running.object:
            self.assertIsInstance(i, dict)
            self.assertIn('jdk', i)
            self.assertIn('node.role', i)

    def test_dump(self):
        dispatcher = eslib.dispatchers['node']()
        dispatcher.api = self.ctx
        running = self._run_action(dispatcher, 'dump')
        print(running.object)
        self.assertIsInstance(running.object, dict)
        self.assertEqual(len(running.object), 1)
        for n, data in running.object.items():
            self.assertIsInstance(n, str)
            self.assertIsInstance(data, dict)
            print(data.keys())
            self.assertIn('os', data)
            self.assertIn('http', data)
            self.assertIn('ip', data)
            self.assertIn('jvm', data)

    def test_stats(self):
        dispatcher = eslib.dispatchers['node']()
        dispatcher.api = self.ctx
        running = self._run_action(dispatcher, 'stats', object_args=['-m', 'breaker', '-m', 'fs'])
        self.assertIsInstance(running.object, dict)
        self.assertEqual(len(running.object), 1)
        for i in running.result:
            for k, v in i[1]['nodes'].items():
                self.assertIn('breakers', v)
                self.assertIn('fs', v)

    def test_bad_stats(self):
        dispatcher = eslib.dispatchers['node']()
        dispatcher.api = self.ctx
        try:
            running = self._run_action(dispatcher, 'stats', object_args=['-m', 'not_a_metric'])
            raise next(running.result)
            self.fail("Exception not thrown")
        except ESLibRequestError as e:
            self.assertRegex(str(e), 'request \[/_nodes/.+/stats/not_a_metric\] contains unrecognized metric: \[not_a_metric\]')
