from __future__ import absolute_import
from itertools import chain
from itertools import permutations
from itertools import combinations
from collections import Iterable
from warnings import warn

from .multi_df_utils import _closed_to_tuple
from .node_set_s import NodeSetS
from .time_set_df import TimeSetDF
from .temporal_node_set_df import TemporalNodeSetDF
from stream_graph import ABC
from stream_graph.collections import NodeCollection
from stream_graph.collections import LinkCollection
from stream_graph.collections import TimeCollection
from stream_graph.collections import TimeGenerator
from stream_graph.exceptions import UnrecognizedTemporalNodeSet


class TemporalNodeSetB(ABC.TemporalNodeSet):
    """Implementation of ABC.TemporalNodeSet using class combination.

    All nodes defined by a nodeset, exist in all the time domain defined by timeset.

    Parameters:
    -----------
    nodeset: ABC.NodeSet

    timeset: ABC.TimeSet

    discrete: bool or None

    """
    def __init__(self, nodeset=None, timeset=None, discrete=None):
        is_none = [a is not None for a in [timeset, nodeset]]
        if all(is_none):
            if isinstance(nodeset, ABC.NodeSet):
                self.nodeset_ = nodeset
            else:
                self.nodeset_ = NodeSetS(nodeset)

            if isinstance(timeset, ABC.TimeSet):
                if discrete is not None:
                    warn('The discrete signature is derived from the one of the given timeset')
                self.timeset_ = timeset
            else:
                if discrete is None:
                    discrete = True
                self.timeset_ = TimeSetDF(timeset, discrete=discrete)

        elif any(is_none):
            raise ValueError('All arguments should have values or be None')

    def __str__(self):
        if bool(self):
            out = [('Node-Set', str(self.nodeset_))]
            out += [('Time-Set', str(self.timeset_))]
            header = ['Mixed-TemporalNodeset']
            header += [len(header[0])*'=']
            return '\n\n'.join(['\n'.join(header)] + ['\n'.join([a, len(a)*'-', b]) for a, b in out])
        else:
            out = ["Empty Mixed-TemporalNodeset"]
            out = [out[0] + "\n" + len(out[0])*'-']
            if not hasattr(self, 'nodeset_'):
                out += ['- Node-Set: None']
            elif not bool(self.nodeset_):
                out += ['- Node-Set: Empty']
            if not hasattr(self, 'timeset_'):
                out += ['- Time-Set: None']
            elif not bool(self.timeset_):
                out += ['- Time-Set: Empty']
            return '\n\n  '.join(out)

    @property
    def discrete(self):
        return (None if not bool(self.timeset_) else self.timeset_.discrete)

    @property
    def nodeset(self):
        if hasattr(self, 'nodeset_'):
            return self.nodeset_.copy()
        else:
            return NodeSetS()

    @property
    def timeset(self):
        if hasattr(self, 'timeset_'):
            return self.timeset_.copy()
        else:
            return TimeSetDF()

    @property
    def size(self):
        if bool(self):
            return self.timeset_.size * self.n
        else:
            return 0.

    @property
    def n(self):
        if bool(self):
            return self.nodeset_.size
        else:
            return 0

    @property
    def total_time(self):
        if bool(self):
            return self.timeset_.size
        else:
            return 0

    @property
    def total_common_time(self):
        n = self.n
        return n*(n-1)*self.timeset_.size

    @property
    def number_of_instants(self):
        if self.timeset_.instantaneous:
            return self.timeset_.number_of_instants * self.n
        else:
            raise AttributeError('Instantaneous \'TemporalNodeSetB\' has no attribute \'number_of_instants\'')

    @property
    def instantaneous(self):
        return self.timeset_.instantaneous

    def __contains__(self, u):
        assert type(u) is tuple and len(u) is 2
        if (not bool(self)) or (u[0] is None and u[1] is None):
            return False
        return ((u[0] is None or u[0] in self.nodeset_) and
                (u[1] is None or u[1] in self.timeset_))

    def node_duration(self, u=None):
        if u is None:
            return NodeCollection({u: self.total_time for u in self.nodeset_})
        else:
            if u in self.nodeset_:
                return self.total_time
            else:
                return 0.

    def common_time(self, u=None):
        if u is None:
            return NodeCollection({u: (self.n-1) * self.total_time for u in self.nodeset_})
        elif isinstance(u, Iterable):
            return NodeCollection({u: (self.n-1) * self.total_time for u in set(self.nodeset_) & set(u)})

        if bool(self) and u in self.nodeset_:
            return (self.n-1) * self.total_time
        return 0.

    def common_time_pair(self, l=None):
        if l is None:
            return LinkCollection({(u, v): self.total_time for u, v in combinations(self.nodeset_, 2)})
        elif isinstance(l, Iterable) and not (isinstance(l, tuple) and len(l) == 2 and any(not isinstance(a, Iterable) for a in l)):
            nodes, links = set(self.nodeset_) & set(a for u, v in l for a in (u, v)), set(l)
            return LinkCollection({(u, v): self.total_time for u, v in permutations(nodes, 2) if (u, v) in links})

        if bool(self) and l[0] in self.nodeset_ and l[1] in self.nodeset_:
            return self.total_time
        return 0.

    def issuperset(self, tns):
        if isinstance(tns, ABC.TemporalNodeSet):
            if bool(self) or bool(tns):
                if isinstance(tns, TemporalNodeSetB):
                    return (self.timeset_.issuperset(tns.timeset_) and self.nodeset_.issuperset(tns.nodeset_))
                else:
                    return TemporalNodeSetDF(self).issuperset(tns)
            else:
                return True
        else:
            raise UnrecognizedTemporalNodeSet('ns')
        return False

    def nodes_at(self, t=None):
        if t is None:
            if bool(self):
                return constant_time_generator(self.timeset, self.nodeset, NodeSetS(), self.discrete, self.instantaneous)
            else:
                return TimeGenerator()
        else:
            if bool(self) and (t in self.timeset_):
                return self.nodeset
            else:
                return NodeSetS()

    def n_at(self, t=None):
        if t is None:
            if bool(self):
                return constant_time_generator(self.timeset, self.n, 0, self.discrete, self.instantaneous)
            else:
                return TimeCollection()
        else:
            if bool(self) and (t in self.timeset_):
                return self.n
            else:
                return 0

    def times_of(self, u=None):
        if u is None:
            return NodeCollection({u: self.timeset for u in self.nodeset_})
        if bool(self) and u in self.timeset_:
            return self.timeset
        else:
            return TimeSetDF()

    def __iter__(self):
        if self.instantaneous:
            for a in self.nodeset_:
                for t in self.timeset_:
                    yield (a, t)
        else:
            for a in self.nodeset_:
                for key in self.timeset_:
                    yield (a, ) + key

    def __bool__(self):
        return hasattr(self, 'nodeset_') and hasattr(self, 'timeset_') and bool(self.nodeset_) and bool(self.timeset_)

    def __and__(self, ns):
        if isinstance(ns, ABC.TemporalNodeSet):
            assert ns.discrete == self.discrete
            if isinstance(ns, TemporalNodeSetB):
                if ns and bool(self):
                    return TemporalNodeSetB(timeset=self.timeset_ & ns.timeset_, nodeset=self.nodeset_ & ns.nodeset_)
            else:
                return ns & self
        else:
            raise UnrecognizedTemporalNodeSet('right operand')
        return TemporalNodeSetB()

    def __or__(self, ns):
        if isinstance(ns, ABC.TemporalNodeSet):
            if not bool(self):
                return ns.copy()
            assert ns.discrete == self.discrete
            if isinstance(ns, TemporalNodeSetB):
                if bool(ns):
                    return TemporalNodeSetB(timeset=self.timeset_ | ns.timeset_, nodeset=self.nodeset_ | ns.nodeset_)
                else:
                    return self.copy()
            else:
                return ns | self
        else:
            raise UnrecognizedTemporalNodeSet('right operand')
        return TemporalNodeSetB()

    def __sub__(self, tns):
        if isinstance(tns, ABC.TemporalNodeSet):
            if bool(self):
                if bool(tns):
                    assert tns.discrete == self.discrete
                    if isinstance(tns, TemporalNodeSetB):
                        # If all time-stamps appear at all times also for the second temporal-node-set.
                        ns_in = self.nodeset_ & tns.nodeset_
                        # For the subset of nodes that are common
                        if bool(ns_in):
                            # Take the difference in time.
                            ts_diff = self.timeset_ - tns.timeset_
                            # Calculate their difference in time
                            if bool(ts_diff):
                                # First take the difference in nodes with all the times.
                                tnsa_ = TemporalNodeSetB(nodeset=self.nodeset_ - tns.nodeset_, timeset=self.timeset_)
                                # And append it to the common nodes with their difference in time.
                                tnsb_ = TemporalNodeSetB(nodeset=ns_in, timeset=ts_diff)
                                iter_ = chain(iter(tnsa_), iter(tnsb_))
                                nsa = TemporalNodeSetDF(iter_, disjoint_intervals=True, discrete=self.discrete)
                                return nsa
                        ns_diff = self.nodeset_ - tns.nodeset_
                        if bool(ns_diff):
                            return TemporalNodeSetB(nodeset=ns_diff, timeset=self.timeset_)
                    else:
                        try:
                            return tns.__rsub__(self)
                        except (AttributeError, NotImplementedError):
                            return TemporalNodeSetDF(self, discrete=self.discrete) - TemporalNodeSetDF(tns, discrete=self.discrete)
            else:
                return self.copy()
        else:
            raise UnrecognizedTemporalNodeSet('right operand')
        return TemporalNodeSetB()

    def _to_discrete(self, bins, bin_size):
        timeset, bins = self.timeset_.discretize(bins, bin_size)
        return self.__class__(timeset=timeset, nodeset=self.nodeset), bins


def constant_time_generator(timeset, obj, empty_object, discrete, instantaneous):
    if instantaneous:
        def generate(timeset):
            for t in timeset:
                yield (t, obj)
    elif discrete:
        def generate(timeset):
            for ts, tf in timeset:
                yield (ts, obj)
                yield (tf + 1, empty_object)
    else:
        def generate(timeset):
            for ts, tf, it in timeset:
                s, f = _closed_to_tuple(it)
                yield ((ts, s), obj)
                yield ((tf, not f), empty_object)

    return TimeGenerator(generate(timeset), discrete=discrete, instantaneous=instantaneous)
