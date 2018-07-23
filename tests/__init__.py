import unittest
from eslib import context
from eslib.pycurlconnection import PyCyrlConnection
from eslib.asynctransport import AsyncTransport
import tests.esmock

class TestCaseProvider(unittest.TestCase):

    def setUp(self):
        transport_class=AsyncTransport
        connection_class=PyCyrlConnection
        self.ctx = context.Context(url='localhost:9200', sniff=False, debug=False, transport_class=transport_class, connection_class=connection_class)
        self.ctx.connect()
        for i in self.ctx.perform_query(self.ctx.escnx.indices.create(id(self))):
            pass

    def tearDown(self):
        self.ctx.disconnect()

    def _run_action(self, dispatcher, verb, object_options={}, object_args=[]):
        query = dispatcher.run_phrase(verb, object_options, object_args)
        running = dispatcher.api.perform_query(query)
        return running

    def action_read_settings(self, dispatcher, object_args, tester):
        dispatcher.api = self.ctx
        running = self._run_action(dispatcher, 'readsettings', object_args=object_args)
        for j in running.result:
            tester(running, j)

    def action_write_settings(self, dispatcher, object_args, tester):
        dispatcher.api = self.ctx
        running = self._run_action(dispatcher, 'writesettings', object_args=object_args)
        for j in running.result:
            tester(running, j)


if __name__ == '__main__':
    print('running in main')
    unittest.main()
