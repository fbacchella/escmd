import eslib.verb

from eslib.dispatcher import dispatcher, command, Dispatcher


@dispatcher(object_name="node")
class NodesDispatcher(Dispatcher):

    def fill_parser(self, parser):
        parser.add_option("-n", "--nodes", dest="nodes", help="nodes filter")

    def get(self, nodes=None):
        if nodes is not None:
            nodes = self.api.escnx.nodes.info(nodes)
            print(nodes)


@command(NodesDispatcher)
class NodesList(eslib.verb.List):

    def execute(self, *args, **kwargs):
        return self.api.escnx.cat.nodes()
