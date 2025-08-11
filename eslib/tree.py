class TreeNode(object):

    identation = 2

    def __init__(self, value):
        self.value = value
        self.parent = None
        self.children = []

    def addchild(self, child):
        self.children.append(child)
        child.parent = self

    def __repr__(self, level=-1):
        if self.value is not None:
            ret = " "* TreeNode.identation * level + self._value_to_str(level)+"\n"
        else:
            ret = ''
        for child in self.children:
            ret += child.__repr__(level+1)
        return ret

    def _value_to_str(self, level):
        raise NotImplementedError


