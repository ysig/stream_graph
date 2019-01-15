import pandas as pd
import itertools

from stream_graph import ABC
from stream_graph.exceptions import UnrecognizedLinkSet
from stream_graph.exceptions import UnrecognizedDirection
from stream_graph.set.node_set_s import NodeSetS


class LinkSetDF(ABC.LinkSet):
    def __init__(self, df=None, no_duplicates=False, sort_by=['u', 'v']):
        # Add a check for dataframe style
        if df is not None:
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(list(df), columns=['u', 'v'])
            self.df_ = df
            self.sort_by = sort_by
            self.merged_ = no_duplicates

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
        self.df_ = self.df_.reindex(columns=['u', 'v'])
        return self

    @sort_by.setter
    def sort_by(self, val):
        if not (hasattr(self, 'sort_by_') and self.sort_by_ == val):
            self.sorted_ = False
            self.sort_by_ = val

    @property
    def sort_df_(self):
        if not self.is_sorted_:
            self.df_ = self.df_.sort_values(by=self.sort_by)
        return self

    @property
    def merge_df_(self):
        if not self.is_merged_:
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
    def df(self):
        return self.merge_df_.sort_df_.reindex_.df_

    @property
    def mdf(self):
        return self.merge_df_.reindex_.df_

    def neighbors(self, u, direction='out'):
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

    def degree(self, u, direction='out'):
        if direction == 'out':
            return (self.df.u == u).sum()
        elif direction == 'in':
            return (self.df.v == u).sum()
        elif direction == 'both':
            return ((self.df.u == u) | (self.df.v == u)).sum()
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
                        ls = LinkSetDF(ls)
                return LinkSetDF(pd.merge(self.df, ls.df, how='inner'))
        else:
            raise UnrecognizedLinkSet('ls')
        return NodeSetS()

    def __or__(self, ls):
        if isinstance(ls, ABC.LinkSet):
            if not bool(self):
                return ls.copy()
            elif bool(ls):
                if not isinstance(ls, LinkSetDF):
                    try:
                        return ls | self
                    except NotImplementedError:
                        ls = LinkSetDF(ls)
                return LinkSetDF(pd.merge(self.df, ls.df, how='outer'))
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
                            ls = LinkSetDF(ls)

                    return LinkSetDF(self.df.append(pd.merge(self.df, ls.df, how='inner'),
                                                    ignore_index=True).drop_duplicates(keep=False),
                                     sort_by=self.sort_by)
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
