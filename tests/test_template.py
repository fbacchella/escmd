import eslib

import tests
import os
from pathlib import Path

class TemplateTestCase(tests.TestCaseProvider):

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

    def test_put_and_delete(self):
        current_path = Path(os.path.dirname(os.path.realpath(__file__)))
        template_path = current_path / "resources" / "template.yaml"
        dispatcher = eslib.dispatchers['template']()
        dispatcher.api = self.ctx
        running = self._run_action(dispatcher, 'put', object_options={'template_name': 'logs'}, object_args=['-f', str(template_path), '-p', 'logs-*'])
        print(vars(running))
        running = self._run_action(dispatcher, 'delete', object_options={'template_name': 'logs'})
        print(vars(running))
