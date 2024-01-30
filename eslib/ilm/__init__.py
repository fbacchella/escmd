from eslib.dispatcher import dispatcher, Dispatcher, command
from eslib.verb import DumpVerb, List


@dispatcher(object_name="ilm")
class IlmDispatcher(Dispatcher):

    def get(self, running, **kwargs):
        return {}

    def fill_parser(self, parser):
        pass

    def check_noun_args(self, running, **kwargs):
        return super().check_noun_args(running, **kwargs)


@command(IlmDispatcher, verb='list')
class IlmList(List):

    def fill_parser(self, parser):
        super(List, self).fill_parser(parser)
        parser.add_option("-m", "--machinreadable", dest="human", default=True, action='store_false')

    def check_verb_args(self, running, *args, human=True, template=None, **kwargs):
        running.human = human
        return super().check_verb_args(running, *args, template=template, **kwargs)

    async def get_elements(self, running):
        return await self.api.escnx.ilm.get_lifecycle(human=running.human)

    async def action(self, element, running):
        return element

    def to_str(self, running, item):
        return item[0]

@command(IlmDispatcher, verb='stop')
class IlmStop(DumpVerb):

    async def get(self, running, **kwargs):
        return {}

    async def execute(self, running, **kwargs):
        return await self.api.escnx.ilm.stop()

    def to_str(self, running, item):
        if item['acknowledged']:
            return 'ILM stopped'
        else:
            return item


@command(IlmDispatcher, verb='start')
class IlmStart(DumpVerb):

    async def get(self, running, **kwargs):
        return {}

    async def execute(self, running, **kwargs):
        return await self.api.escnx.ilm.start()

    def to_str(self, running, item):
        if item['acknowledged']:
            return 'ILM started'
        else:
            return item


@command(IlmDispatcher, verb='status')
class IlmStatus(DumpVerb):

    async def get(self, running, **kwargs):
        return {}

    async def execute(self, running, **kwargs):
        return await self.api.escnx.ilm.get_status()

    def to_str(self, running, item):
        return item['operation_mode']
