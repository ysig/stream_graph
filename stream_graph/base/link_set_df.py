from __future__ import absolute_import
import pandas as pd
import itertools

from stream_graph import ABC
from stream_graph.exceptions import UnrecognizedLinkSet
from stream_graph.exceptions import UnrecognizedDirection
from .node_set_s import NodeSetS
from stream_graph.collections import NodeCollection
from collections import Counter
from collections import defaultdict
from six import iteritems

class LinkSetDF(ABC.LinkSet):
    """DataFrame implementation of the ABC.LinkSet"""
    def __init__(self, df=None, no_duplicates=False, sort_by=['u', 'v'], weighted=False):
        """Initialize a LinkSet.

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
        # Add a check for dataframe style
        if df is None:
            not_empty = False
        elif isinstance(df, pd.DataFrame):
            not_empty = not df.empty
        else:
            not_empty = len(df) > 0
        if not_empty:
            if not isinstance(df, pd.DataFrame):
                df = list(df)
            self.df_ = pd.DataFrame(df)
            if weighted:
                if len(self.df_.columns) == 3:
                    self.df_.columns = ['u', 'v', 'w']
                elif len(self.df_.columns) == 2:
                    self.df_.columns = ['u', 'v']
                    self.df_['w'] = 1
                else:
                    raise ValueError('If weighted is True, input should be an iterable of at least 2 and at most 3 elements.')
            else:
                if len(self.df_.columns) == 2:
                    self.df_.columns = ['u', 'v']
                elif len(self.df_.columns) == 3:
                    self.df_.drop(columns=self.df_.columns[2], inplace=True)
                    self.df_.columns = ['u', 'v']
                else:
                    print(self.df_)
                    raise ValueError('If weighted is False, input should be an iterable of exactly 2 elements.')
                                    
            self.sort_by = sort_by
            self.merged_ = no_duplicates
            self.weighted_ = weighted

    @property
    def to_unweighted(self):
        if self.weighted:
            return LinkSetDF(self.df.drop(columns=['w']),
                             no_duplicates=True, weighted=False)
        else:
            return self.copy()

    @property
    def to_weighted(self):
        if self.weighted:
            return self.copy()            
        else:
            dfm = self.df
            dfm['w'] = 1
            return LinkSetDF(dfm.reindex(columns=['u', 'v', 'w']), no_duplicates=True, weighted=True)

    @property
    def weighted(self):
        if bool(self):
            return self.weighted_
        else:
            return None

    def __bool__(self):
        return hasattr(self, 'df_') and not self.df_.empty

    @property
    def is_merged_(self):
        return hasattr(self, 'merged_') and self.merged_

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
        if not self.is_merged_:
            if self.weighted:
                self.df_ = merge_weights(self.df_)
            else:
                self.df_ = self.df_.drop_duplicates()
            self.merged_ = True
        return self

    @property
    def size(self):
        if bool(self):
            return self.mdf.shape[0]
        else:
            return 0

    @property
    def _size_weighted(self):
        return self.mdf.w.sum()

    @property
    def df(self):
        if bool(self):
            return self.merge_.sort_.reindex_.df_
        else:
            return pd.DataFrame(columns=['u', 'v'])

    @property
    def mdf(self):
        if bool(self):
            return self.merge_.reindex_.df_
        else:
            return pd.DataFrame(columns=['u', 'v'])

    def neighbors(self, u=None, direction='out'):
        if u is None:
            neighbors = defaultdict(set)
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
                add(key[0], key[1])
            return NodeCollection({u: NodeSetS(s) for u, s in iteritems(neighbors)})
        else:
            if direction == 'out':
                s = self.df[self.df.u == u].v.values.flat
            elif direction == 'in':
                s = self.df[self.df.v == u].u.values.flat
            elif direction == 'both':
                s = itertools.chain(self.df[self.df.u == u].v.values.flat,
                                    self.df[self.df.v == u].u.values.flat)
            else:
                raise UnrecognizedDirection()
            return NodeSetS(s)

    def _degree_unweighted(self, u=None, direction='out'):
        if u is None:
            degrees = Counter()
            if direction == 'out':
                def add(key):
                    degrees[key[0]] += 1
            elif direction == 'in':
                def add(key):
                    degrees[key[1]] += 1
            elif direction == 'both':
                def add(key):
                    degrees[key[0]] += 1
                    degrees[key[1]] += 1
            else:
                raise UnrecognizedDirection()
            for key in iter(self):
                add(key)
            return NodeCollection(degrees)
        else:
            if direction == 'out':
                return (self.df.u == u).sum()
            elif direction == 'in':
                return (self.df.v == u).sum()
            elif direction == 'both':
                return ((self.df.u == u) | (self.df.v == u)).sum()
            else:
                raise UnrecognizedDirection()

    def _degree_weighted(self, u=None, direction='out'):
        if u is None:
            degrees = Counter()
            if direction == 'out':
                def add(key):
                    degrees[key[0]] += key[2]
            elif direction == 'in':
                def add(key):
                    degrees[key[1]] += key[2]
            elif direction == 'both':
                def add(key):
                    degrees[key[0]] += key[2]
                    degrees[key[1]] += key[2]
            else:
                raise UnrecognizedDirection()
            for key in iter(self):
                add(key)
            return NodeCollection(degrees)
        else:
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
                    return LinkSetDF(intersect_weights(self.df, ls.df), weighted=True)
                else:
                    return LinkSetDF(pd.merge(self.to_unweighted.df, ls.to_unweighted.df, how='inner'))
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
                    return LinkSetDF(merge_weights(self.df.append(ls.df, ignore_index=False, sort=False)), weighted=True, no_duplicates=True)
                else:
                    return LinkSetDF(pd.merge(self.to_unweighted.df, ls.to_unweighted.df, how='outer'), no_duplicates=True)
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
                        return LinkSetDF(difference_weights(self.df, ls.df), weighted=True)
                    else:
                        df_uw, ls_uw = self.to_unweighted.df, ls.to_unweighted.df
                        df_it = pd.merge(df_uw, ls_uw, how='inner')
                        return LinkSetDF(df_uw.append(df_it, sort=False, ignore_index=True).drop_duplicates(keep=False),
                                         sort_by=self.sort_by, no_duplicates=True)
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
        return self.size >= ls.size and (self & ls).size == ls.size


def iter_df(df):
    return df.itertuples(index=False, name=None)

def merge_weights(df):
    data = Counter()
    for u, v, w in iter_df(df):
        data[(u, v)] += w
    return pd.DataFrame(list((u, v, w) for (u, v), w in iteritems(data)), columns=['u', 'v', 'w'])

def intersect_weights(dfa, dfb):
    data = Counter()
    for u, v, w in iter_df(dfa):
        data[(u, v)] += w
    data_it = Counter()
    for u, v, w in iter_df(dfb):
        wb = data[(u, v)]
        if wb > 0:
            if (u, v) in data_it:
                data_it[(u, v)] += w
            else:
                data_it[(u, v)] += wb + w
    return pd.DataFrame(list((u, v, w) for (u, v), w in iteritems(data_it)), columns=['u', 'v', 'w'])

def difference_weights(dfa, dfb):
    b_keys = set((u, v) for u, v, _ in iter_df(dfb))
    data_diff = Counter()
    for u, v, w in iter_df(dfa):
        if (u, v) not in b_keys:
            data_diff[(u, v)] += w
    return pd.DataFrame(list((u, v, w) for (u, v), w in iteritems(data_diff)), columns=['u', 'v', 'w'])
