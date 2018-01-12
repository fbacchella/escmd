
from asyncio import get_event_loop, wait, FIRST_COMPLETED
from eslib.pycurlconnection import multi_handle
from eslib.asynctransport import QueryIterator


class Scheduler(object):

    def __init__(self):
        self.loop = get_event_loop()
        self.waiting_tasks = set()

    def schedule(self, task):
        if isinstance(task, QueryIterator):
            self.waiting_tasks.add(iter(task))

    def execute(self, *futures):
        print(futures)

        def coro():
            done, pending = yield from wait(futures, return_when=FIRST_COMPLETED, loop=self.loop)
            return done, pending

        return self.loop.run_until_complete(coro())

    def run(self):
        while True:
            running_futurs = set()
            while len(self.waiting_tasks) > 0:
                running_futurs.add(self.waiting_tasks.pop())
            done, still_waiting = self.execute(multi_handle.perform(), *running_futurs)
            self.waiting_tasks.union(still_waiting)
            if len(self.waiting_tasks) == 0 and len(running_futurs) == 0:
                break

