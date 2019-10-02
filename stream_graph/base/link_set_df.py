from __future__ import absolute_import
import pandas as pd
import itertools


from stream_graph import ABC
from stream_graph.exceptions import UnrecognizedLinkSet
from stream_graph.exceptions import UnrecognizedDirection
from .utils import make_algebra
from .node_set_s import NodeSetS
from stream_graph.collections import NodeCollection
from collections import Counter
from collections import defaultdict
from six import iteritems
from operator import add
from .dataframes.algorithms.utils.misc import hinge_loss


class LinkSetDF(ABC.LinkSet):
    """DataFrame implementation of the ABC.LinkSet

    Parameters
    ----------
    df: pandas.DataFrame or Iterable, default=None
        If a DataFrame it should contain two columns for u and v.
        If an Iterable it should produce :code:`(u, v)` tuples of NodeId (int or str).

    no_duplicates: Bool, default=False
        Defines if the input could contain duplicate tuples.

    sort_by: A non-empty subset of ['u', 'v'], default=['u', 'v']
        The order of the DataFrame elements by which they will be produced when iterated.

    """
    def __init__(self, df=None, no_duplicates=True, sort_by=None, weighted=False, merge_function=None, operation_functions=None):
        # Add a check for dataframe style
        if df is None:
            not_empty = False
        elif isinstance(df, pd.DataFrame):
            not_empty = not df.empty
        else:
            not_empty = len(df) > 0
        if weighted:
            self.algebra = make_algebra(operation_functions)
            self.algebra['m'] = (sum if merge_function is None else merge_function)
            assert callable(self.algebra['m'])
        if not_empty:
            if not isinstance(df, pd.DataFrame):
                df = list(df)
            self.df_ = pd.DataFrame(df)
            if weighted:
                if len(self.df_.columns) == 3:
                    # If weighted with 3 columns
                    try:
                        # See if the dataframe already contains the valid columns names.
                        self.df_ = self.df_[['u', 'v', 'w']]
                    except Exception:
                        # In a different case just set the column names in the expected order
                        self.df_.columns = ['u', 'v', 'w']
                elif len(self.df_.columns) == 2:
                    # If weighted, but has only two columns (has not weights)
                    try:
                        self.df_ = self.df_[['u', 'v']]
                    except Exception:
                        self.df_.columns = ['u', 'v']
                    # Set all weights to 1.
                    self.df_['w'] = 1
                else:
                    raise ValueError('If weighted is True, input should be an iterable of at least 2 and at most 3 elements.')
            else:
                if len(self.df_.columns) == 2:
                    # If not weighted and we have exactly 2 columns
                    try:
                        self.df_ = self.df_[['u', 'v']]
                    except Exception:
                        # In a different case just set the column names in the expected order
                        self.df_.columns = ['u', 'v']
                elif len(self.df_.columns) == 3:
                    # If not weighted and we have more than 2 columns
                    try:
                        # we have to keep only the column associated with links
                        self.df_ = self.df_[['u', 'v']]
                    except Exception:
                        self.df_.drop(columns=self.df_.columns[2], inplace=True)
                        self.df_.columns = ['u', 'v']
                else:
                    raise ValueError('If weighted is False, input should be an iterable of exactly 2 elements.')
            self.sort_by = sort_by
            self.weighted_ = weighted
            if not no_duplicates:
                # Handle duplicates in case the user signifies that we may have
                self.merge_.reindex_
        else:
            self.weighted_ = weighted

    @property
    def to_unweighted(self):
        """Return an unweighted version of this LinkSetDF."""
        if self.weighted:
            return LinkSetDF(self.df.drop(columns=['w']),
                             no_duplicates=True, weighted=False)
        else:
            return self.copy()

    @property
    def to_weighted(self):
        """Return an weighted version of this LinkSetDF."""
        if self.weighted:
            return self.copy()
        else:
            df = self.df.copy()
            # If there are no weights set them to 1.
            df['w'] = 1
            return LinkSetDF(df.reindex(columns=['u', 'v', 'w']), no_duplicates=True, weighted=True)

    @property
    def weighted(self):
        """Defines if the object is weighted."""
        return self.weighted_

    def __bool__(self):
        return hasattr(self, 'df_') and not self.df_.empty

    def __eq__(self, obj):
        """Compare equality in the level of data."""
        if self.weighted == obj.weighted:
            return set(self) == set(obj)
        else:
            return False

    @property
    def nodeset(self):
        from stream_graph import NodeSetS
        return NodeSetS(set(itertools.chain(self.df_['u'], self.df_['v'])))

    @property
    def is_sorted_(self):
        return (hasattr(self, 'sort_by_') and hasattr(self, 'sorted_') and self.sorted_) or self.sort_by_ is None

    @property
    def sort_by(self):
        if hasattr(self, 'sort_by_'):
            return self.sort_by_
        else:
            return None

    @property
    def reindex_(self):
        # Important for python 2 compatibility
        self.df_ = self.df_.reindex(columns=['u', 'v'] + (['w'] if self.weighted else []))
        return self

    @sort_by.setter
    def sort_by(self, val):
        if not (hasattr(self, 'sort_by_') and self.sort_by_ == val):
            self.sorted_ = False
            self.sort_by_ = val

    @property
    def sort_(self):
        if not self.is_sorted_:
            self.df_ = self.df_.sort_values(by=self.sort_by)
        return self

    @property
    def merge_(self):
        if self.weighted:
            self.df_ = merge_weights(self.df_, merge_function=self.algebra['m'])
        else:
            self.df_.drop_duplicates(inplace=True)
        return self

    @property
    def size(self):
        if bool(self):
            return self.df.shape[0]
        else:
            return 0

    @property
    def _weighted_size(self):
        return self.df.w.sum()

    @property
    def df(self):
        if bool(self):
            return self.df_
        else:
            return pd.DataFrame(columns=['u', 'v'])

    def neighbors_of(self, u=None, direction='out'):
        if u is None:
            # A dictionary containing for its node it's set of neighbors
            neighbors = defaultdict(set)
            # Define a function for adding for its node it's neighbor.
            if direction == 'out':
                def add(u, v):
                    neighbors[u].add(v)
            elif direction == 'in':
                def add(u, v):
                    neighbors[v].add(u)
            elif direction == 'both':
                def add(u, v):
                    neighbors[u].add(v)
                    neighbors[v].add(u)
            else:
                raise UnrecognizedDirection()
            for key in iter(self):
                # Parse all elements.
                add(key[0], key[1])
            # Return a node-collection of nodesets.
            return NodeCollection({u: NodeSetS(s) for u, s in iteritems(neighbors)})
        else:
            # In case we want only one element
            if direction == 'out':
                # Extract the series of elements.
                s = self.df[self.df.u == u].v
            elif direction == 'in':
                s = self.df[self.df.v == u].u
            elif direction == 'both':
                s = itertools.chain(self.df[self.df.u == u].v, self.df[self.df.v == u].u)
            else:
                raise UnrecognizedDirection()
            # Return a Nodeset.
            return NodeSetS(s)

    def _degree_unweighted(self, u=None, direction='out'):
        if u is None:
            # Initialize iterators
            if direction == 'out':
                iter_ = (k[0] for k in self)
            elif direction == 'in':
                iter_ = (k[1] for k in self)
            elif direction == 'both':
                # Avoid double occurencies for degree i.e. (2, 1), (1, 2)
                iter_ = (a[0] for a in set(ks for k in self for ks in [k[:2], (k[1], k[0])]))
            else:
                raise UnrecognizedDirection()

            # Count how many times each node appears, as an indicator of how many neighbors it has.
            return NodeCollection(Counter(iter_))
        else:
            if direction == 'out':
                return (self.df.u == u).sum()
            elif direction == 'in':
                return (self.df.v == u).sum()
            elif direction == 'both':
                # Avoid double occurencies for degree i.e. (2, 1), (1, 2)
                return len(set(itertools.chain(self.df[self.df.u == u].v, self.df[self.df.v == u].u)))
            else:
                raise UnrecognizedDirection()

    def _degree_weighted(self, u=None, direction='out'):
        if u is None:
            # Use a Counter to count the total weight sum.
            degrees = Counter()
            if direction == 'out':
                def add(key):
                    degrees[key[0]] += key[2]
            elif direction == 'in':
                def add(key):
                    degrees[key[1]] += key[2]
            elif direction == 'both':
                # Double occorencies do not matter here
                def add(key):
                    degrees[key[0]] += key[2]
                    degrees[key[1]] += key[2]
            else:
                raise UnrecognizedDirection()
            for key in iter(self):
                add(key)
            return NodeCollection(degrees)
        else:
            # Cast funciton to return the appropriate weigth series.
            def cast(index):
                return self.df.w[index]
            if direction == 'out':
                return cast(self.df.u == u).sum()
            elif direction == 'in':
                return cast(self.df.v == u).sum()
            elif direction == 'both':
                return cast((self.df.u == u) | (self.df.v == u)).sum()
            else:
                raise UnrecognizedDirection()

    def __contains__(self, l):
        assert isinstance(l, tuple) and len(l) == 2
        return ((self.df.u == l[0]) & (self.df.v == l[1])).any()

    def __and__(self, ls):
        if isinstance(ls, ABC.LinkSet):
            if bool(ls) and bool(self):
                if not isinstance(ls, LinkSetDF):
                    try:
                        return ls & self
                    except NotImplementedError:
                        ls = LinkSetDF(ls, weighted=ls.weighted)
                if self.weighted and ls.weighted:
                    return LinkSetDF(intersect_weights(self.df, ls.df, self.algebra['i']), weighted=True)
                else:
                    sa = set(self.to_unweighted.df.itertuples(name=None, index=False))
                    sb = set(ls.to_unweighted.df.itertuples(name=None, index=False))
                    return LinkSetDF(sa & sb)
        else:
            raise UnrecognizedLinkSet('ls')
        return LinkSetDF()

    def __or__(self, ls):
        if isinstance(ls, ABC.LinkSet):
            if not bool(self):
                return ls.copy()
            elif bool(ls):
                if not isinstance(ls, LinkSetDF):
                    try:
                        return ls | self
                    except NotImplementedError:
                        ls = LinkSetDF(ls, weighted=ls.weighted)
                if self.weighted and ls.weighted:
                    return LinkSetDF(union_weights(self.df, ls.df, self.algebra['u']), weighted=True, no_duplicates=True)
                else:
                    sa = set(self.to_unweighted.df.itertuples(name=None, index=False))
                    sb = set(ls.to_unweighted.df.itertuples(name=None, index=False))
                    return LinkSetDF(sa | sb)
            else:
                return self.copy()
        else:
            raise UnrecognizedLinkSet('ls')

    def __sub__(self, ls):
        if isinstance(ls, ABC.LinkSet):
            if bool(self):
                if bool(ls):
                    if not isinstance(ls, LinkSetDF):
                        try:
                            return ls.__rsub__(self)
                        except (AttributeError, NotImplementedError):
                            ls = LinkSetDF(ls, weighted=ls.weighted)
                    if self.weighted and ls.weighted:
                        return LinkSetDF(difference_weights(self.df, ls.df, self.algebra['d']), weighted=True)
                    else:
                        sa = set(self.to_unweighted.df.itertuples(name=None, index=False))
                        sb = set(ls.to_unweighted.df.itertuples(name=None, index=False))
                        return LinkSetDF(sa - sb)
                else:
                    return self.copy()
        else:
            raise UnrecognizedLinkSet('ls')
        return NodeSetS()

    def __iter__(self):
        return self.df.itertuples(index=False, name=None)

    def issuperset(self, ls):
        if not isinstance(ls, ABC.LinkSet):
            raise UnrecognizedLinkSet('ls')
        if self.weighted and ls.weighted and self.algebra['s'] is not None:
            assert callable(self.algebra['s'])
            issuper_function = self.algebra['s']
            data = {(u, v): w for u, v, w in self}
            for u, v, wb in ls:
                wa = data.get((u, v), None)
                if wa is None or not issuper_function(wa, wb):
                    return False
            return True
        else:
            sa = ({(u, v) for (u, v, _) in self} if self.weighted else set(self))
            sb = ({(u, v) for (u, v, _) in ls} if ls.weighted else set(ls))
            return sa.issuperset(sb)


def iter_df(df):
    return df.itertuples(index=False, name=None)


def merge_weights(df, merge_function):
    data = defaultdict(list)
    for u, v, w in iter_df(df):
        data[(u, v)].append(w)
    return pd.DataFrame(list((u, v, merge_function(w)) for (u, v), w in iteritems(data)), columns=['u', 'v', 'w'])


def union_weights(dfa, dfb, union_function):
    union_function = (add if union_function is None else union_function)
    assert callable(union_function)
    data = {(u, v): w for u, v, w in iter_df(dfa)}
    for u, v, wb in iter_df(dfb):
        wa = data.get((u, v), None)
        if wa is None:
            # Add from the second if it not exists in the first
            data[(u, v)] = wb
        else:
            # Take the union in common points
            data[(u, v)] = union_function(wa, wb)
    return pd.DataFrame(list((u, v, w) for (u, v), w in iteritems(data)), columns=['u', 'v', 'w'])


def intersect_weights(dfa, dfb, intersection_function):
    intersection_function = (min if intersection_function is None else intersection_function)
    assert callable(intersection_function)
    data = {(u, v): w for u, v, w in iter_df(dfa)}
    data_it = dict()
    for u, v, wb in iter_df(dfb):
        wa = data.get((u, v), None)
        if wa is not None:
            # Take only the intersection in common points
            wi = intersection_function(wa, wb)
            if wi is not None:
                data_it[(u, v)] = wi
    return pd.DataFrame(list((u, v, w) for (u, v), w in iteritems(data_it)), columns=['u', 'v', 'w'])


def difference_weights(dfa, dfb, difference_function):
    difference_function = (hinge_loss if difference_function is None else difference_function)
    data = {(u, v): w for u, v, w in iter_df(dfa)}
    for u, v, wb in iter_df(dfb):
        wa = data.get((u, v), None)
        if wa is not None:
            # Calculate the difference function in common points
            data[(u, v)] = difference_function(wa, wb)

    # if we have Nones they will be omitted
    return pd.DataFrame(list((u, v, w) for (u, v), w in iteritems(data) if w is not None), columns=['u', 'v', 'w'])
