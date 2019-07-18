from __future__ import absolute_import
import pandas as pd
from six import string_types
from collections import Iterable
from .algorithms.discrete_interval import merge_no_key as merge_no_key_
from .algorithms.discrete_interval import merge_by_key as merge_by_key_
from .algorithms.discrete_interval import union_no_key as union_no_key_
from .algorithms.discrete_interval import union_on_key as union_on_key_
from .algorithms.discrete_interval import union_by_key as union_by_key_
from .algorithms.discrete_interval import intersection_no_key as intersection_no_key_
from .algorithms.discrete_interval import intersection_by_key as intersection_by_key_
from .algorithms.discrete_interval import intersection_on_key as intersection_on_key_
from .algorithms.discrete_interval import difference_no_key as difference_no_key_
from .algorithms.discrete_interval import difference_by_key as difference_by_key_
from .algorithms.discrete_interval import difference_on_key as difference_on_key_
from .algorithms.discrete_interval import issuper_no_key as issuper_no_key_
from .algorithms.discrete_interval import issuper_by_key as issuper_by_key_
from .algorithms.discrete_interval import nonempty_intersection_no_key as nonempty_intersection_no_key_
from .algorithms.discrete_interval import nonempty_intersection_by_key as nonempty_intersection_by_key_
from .algorithms.discrete_interval import cartesian_intersection as cartesian_intersection_
from .algorithms.discrete_interval import map_intersection as map_intersection_
from .algorithms.discrete_interval import interval_intersection_size as interval_intersection_size_


class DIntervalDF(pd.DataFrame):
    def __init__(self, *args, **kargs):
        disjoint_intervals = kargs.pop('disjoint_intervals', None)
        super(DIntervalDF, self).__init__(*args, **kargs)
        if not self.empty:
            assert 'ts' in self.columns
            assert self.ts.dtype.kind == 'i'

            if 'tf' not in self.columns:
                self['tf'] = self['ts']
            else:
                assert self.tf.dtype.kind == 'i'

            from .weighted_discrete_interval_df import DIntervalWDF
            if len(args) and isinstance(args[0], DIntervalWDF) or (isinstance(kargs.get('data', None), DIntervalWDF)):
                self.drop(columns=['w'])
                self.merge(inplace=True)
            elif len(args) and isinstance(args[0], DIntervalDF) or (isinstance(kargs.get('data', None), DIntervalDF)):
                if disjoint_intervals:
                    self.merge(inplace=True)
            elif disjoint_intervals is False:
                self.merge(inplace=True)

    def copy(self, *args, **kargs):
        return DIntervalDF(super(DIntervalDF, self).copy(*args, **kargs))

    def drop(self, *args, **kargs):
        merge = kargs.pop('merge', True)
        out = super(DIntervalDF, self).drop(*args, **kargs)
        if isinstance(out, pd.DataFrame):
            if 'ts' in out.columns:
                if 'tf' in out.columns:
                    out = DIntervalDF(out, disjoint_intervals=(not merge))
                else:
                    from instantaneous_df import InstantaneousDF
                    out = InstantaneousDF(out, no_duplicates=(not merge))
        return out

    def append(self, *args, **kargs):
        merge = kargs.pop('merge', False)
        out = super(DIntervalDF, self).append(*args, **kargs)
        if merge:
            self.merge(inplace=True)
        return out

    def itertuples(self, index=False, name=None):
        columns = sorted(list(set(self.columns) - {'ts', 'tf'})) + ['ts', 'tf']
        return super(DIntervalDF, self).reindex(columns=columns).itertuples(index=index, name=name)

    def __getitem__(self, index):
        out = super(DIntervalDF, self).__getitem__(index)
        if isinstance(out, pd.DataFrame):
            if 'ts' in out.columns and 'tf' in out.columns:
                out = DIntervalDF(out)
        return out

    def get_ni_columns(self, on_column):
        if on_column is None:
            columns = self.columns
        elif (not isinstance(on_column, Iterable) or isinstance(on_column, string_types)) and on_column in self.columns:
            columns = [on_column]
        elif isinstance(on_column, Iterable):
            columns = list(c for c in on_column)
        cols = sorted(list(set(c for c in columns) - {'ts', 'tf'}))
        if on_column is not None:
            assert all(c in self.columns for c in cols)
        return cols

    @property
    def events(self):
        columns = sorted(list(set(self.columns) - {'ts', 'tf'}))
        dfp = self[columns + ['ts']].rename(columns={"ts": "t"})
        dfp['start'] = True
        dfpv = self[columns + ['tf']].rename(columns={"tf": "t"})
        dfpv['start'] = False
        return dfp.append(dfpv, ignore_index=True, sort=False).sort_values(by=['t', 'start'], ascending=[True, False])

    def measure_time(self):
        return (self.tf - self.ts + 1).sum()

    def df_at(self, t):
        return self[self.index_at(t)]

    def df_at_interval(self, ts, tf):
        return self[self.index_at_interval(ts, tf)]

    def count_at(self, t):
        return self.index_at(t).sum()

    def index_at(self, t):
        return (self.ts <= t) & (self.tf >= t)

    def index_at_interval(self, ts, tf):
        assert ts <= tf
        return (self.ts <= ts) & (self.tf >= tf)

    def _save_or_return(self, df, inplace, on_column=None, disjoint_intervals=True):
        if df is None:
            df = self.__class__(columns=self.columns)
        elif isinstance(df, list):
            assert on_column is not None
            df = self.__class__(df, columns=on_column + ['ts', 'tf'], disjoint_intervals=disjoint_intervals)

        if inplace and df is not self:
            return self._update_inplace(df._data)
        else:
            return (df.copy() if df is self else df)

    def merge(self, inplace=False):
        on_column = self.get_ni_columns(None)
        if not len(on_column):
            df = merge_no_key_(self)
        else:
            df = merge_by_key_(self)
        return self._save_or_return(df, inplace, on_column)

    def union(self, df, on_column=None, by_key=True, inplace=False):
        if df.empty:
            return self._save_or_return(self, inplace)

        assert not (not by_key and df is None)
        on_column = self.get_ni_columns(on_column)

        if not len(on_column):
            df = union_no_key_(self, df)
        elif by_key:
            df = union_by_key_(self, df)
        else:
            df = union_on_key_(self, df)
        return self._save_or_return(df, inplace, on_column)

    def intersection(self, df, on_column=None, by_key=True, inplace=False):
        if df is None or df.empty:
            return self._save_or_return(None, inplace)

        assert not (not by_key and df is None)
        on_column = self.get_ni_columns(on_column)

        if not len(on_column):
            df = intersection_no_key_(self, df)
        elif by_key:
            df = intersection_by_key_(self, df)
        else:
            df = intersection_on_key_(self, df)
        return self._save_or_return(df, inplace, on_column)

    def difference(self, dfb, on_column=None, by_key=True, inplace=False):
        if self.empty or dfb.empty:
            return self._save_or_return(self, inplace)

        on_column = self.get_ni_columns(on_column)
        if not len(on_column):
            df = difference_no_key_(self, dfb)
        elif by_key:
            df = difference_by_key_(self, dfb)
        else:
            df = difference_on_key_(self, dfb)
        return self._save_or_return(df, inplace, on_column)

    def issuper(self, dfb, on_column=None, by_key=True):
        on_column = self.get_ni_columns(on_column)
        if not len(on_column):
            return issuper_no_key_(self, dfb)
        elif by_key:
            return issuper_by_key_(self, dfb)
        else:
            # Should function as well
            return issuper_no_key_(self, dfb)

    def nonempty_intersection(self, bdf, on_column="u", by_key=True):
        on_column = self.get_ni_columns(on_column)
        if not len(on_column):
            return nonempty_intersection_no_key_(self, bdf)
        elif by_key:
            return nonempty_intersection_by_key_(self, bdf)
        else:
            # Should function as well
            return nonempty_intersection_no_key_(self, bdf)

    def cartesian_intersection(self, base_df):
        assert set(base_df.columns) == {'u', 'ts', 'tf'}
        assert set(self.columns) == {'u', 'v', 'ts', 'tf'}

        u_set = set(base_df[['u']].values.flat)
        out = cartesian_intersection_(self[self.u.isin(u_set) & self.v.isin(u_set)], base_df)
        return self._save_or_return(out, False, on_column=['u', 'v'])

    def map_intersection(self, base_df):
        return self.__class__(map_intersection_(self, base_df), columns=['u', 'ts', 'tf'])

    def intersection_size(self, b):
        return interval_intersection_size_(self, b)

    @property
    def limits(self):
        return (self.ts.min(), self.tf.max())
