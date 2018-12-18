class Graph(object):
    def __init__(self, nodeSet, linkSet):
        self.ns_, self.ls_ = nodeSet, linkSet

    def __bool__(self):
        return bool(self.ns_) and bool(self.ls_)

    @property
    def nodeSet(self):
        return self.ns_

    @property
    def n(self):
        return len(self.ns_)
    
    @property
    def m(self):
        return len(self.ls_)

    def coverage(self, u=None, direction='out'):
        if bool(self):
            if u is None:
                return self.m/float(self.n ** 2)
            else:
                return linkSet.degree(v, direction=direction)/float(self.n ** 2)
        else:
            return 0.

