import unittest
from eslib import context, dispatchers
from eslib.pycurlconnection import PyCyrlConnection
from eslib.asynctransport import AsyncTransport
from eslib.exceptions import ESLibTimeoutError

class TestESFailures(unittest.TestCase):

    def test_timeout(self):
        transport_class = AsyncTransport
        connection_class = PyCyrlConnection
        try:
            self.ctx = context.Context(url='http://169.254.169.254:9200', sniff=False, debug=False, transport_class=transport_class,
                                       timeout=1,
                                       connection_class=connection_class)
            self.ctx.connect()
            self.fail()
        except ESLibTimeoutError as ex:
            self.assertIsInstance(ex.duration, float)
        finally:
            self.ctx.disconnect()


if __name__ == '__main__':
    print('running in main')
    unittest.main()
