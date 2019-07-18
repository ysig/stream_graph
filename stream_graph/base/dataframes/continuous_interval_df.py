from __future__ import absolute_import
import pandas as pd
from six import string_types
from collections import Iterable

from .algorithms.continuous_interval import merge_no_key, merge_by_key as merge_by_key
from .algorithms.continuous_interval import union_no_key, union_by_key, union_on_key
from .algorithms.continuous_interval import intersection_no_key, intersection_by_key, intersection_on_key
from .algorithms.continuous_interval import difference_no_key, difference_by_key, difference_on_key
from .algorithms.continuous_interval import issuper_no_key, issuper_by_key
from .algorithms.continuous_interval import nonempty_intersection_no_key, nonempty_intersection_by_key
from .algorithms.continuous_interval import cartesian_intersection as cartesian_intersection_
from .algorithms.continuous_interval import map_intersection as map_intersection_
from .algorithms.continuous_interval import interval_intersection_size as interval_intersection_size_


class CIntervalDF(pd.DataFrame):
    def __init__(self, *args, **kargs):
        disjoint_intervals = kargs.pop('disjoint_intervals', None)
        super(CIntervalDF, self).__init__(*args, **kargs)
        assert 'ts' in self.columns
        if 'tf' not in self.columns:
            self['tf'] = self['ts']
        if 's' not in self.columns:
            self['s'] = True
        if 'f' not in self.columns:
            self['f'] = True

        from .weighted_continuous_interval_df import CIntervalWDF
        if len(args) and isinstance(args[0], CIntervalWDF) or (isinstance(kargs.get('data', None), CIntervalWDF)):
            self.drop(columns=['w'], merge=True)
        elif len(args) and isinstance(args[0], CIntervalDF) or (isinstance(kargs.get('data', None), CIntervalDF)):
            if disjoint_intervals:
                self.merge(inplace=True)
        elif disjoint_intervals is False:
            self.merge(inplace=True)

    def copy(self, *args, **kargs):
        return CIntervalDF(super(CIntervalDF, self).copy(*args, **kargs))

    def drop(self, *args, **kargs):
        merge = kargs.pop('merge', True)
        out = super(CIntervalDF, self).drop(*args, **kargs)
        if isinstance(out, pd.DataFrame):
            if {'ts', 'tf', 's', 'f'}.issubset(set(out.columns)):
                out = CIntervalDF(out, disjoint_intervals=(not merge))
        return out

    def append(self, *args, **kargs):
        merge = kargs.pop('merge', False)
        out = super(CIntervalDF, self).append(*args, **kargs)
        if merge:
            self.merge(inplace=True)
        return out

    def itertuples(self, index=False, name=None, bounds=False):
        columns = sorted(list(set(self.columns) - {'ts', 'tf', 's', 'f'})) + ['ts', 'tf']
        if bounds:
            columns += ['s', 'f']
            return super(CIntervalDF, self).reindex(columns=columns).itertuples(index=index, name=name)
        else:
            return super(CIntervalDF, self).reindex(columns=columns).itertuples(index=index, name=name)

    def __getitem__(self, index):
        out = super(CIntervalDF, self).__getitem__(index)
        if isinstance(out, pd.DataFrame):
            if 'ts' in out.columns and 'tf' in out.columns:
                out = CIntervalDF(out)
        return out

    def get_ni_columns(self, on_column):
        if on_column is None:
            columns = self.columns
        elif (not isinstance(on_column, Iterable) or isinstance(on_column, string_types)) and on_column in self.columns:
            columns = [on_column]
        elif isinstance(on_column, Iterable):
            columns = list(c for c in on_column)
        cols = sorted(list(set(c for c in columns) - {'ts', 'tf', 's', 'f'}))
        if on_column is not None:
            assert all(c in self.columns for c in cols)
        return cols

    @property
    def events(self):
        columns = sorted(list(set(self.columns) - {'ts', 'tf', 's', 'f'}))
        dfp = self[columns + ['ts']].rename(columns={"ts": "t"})
        dfp['start'] = True
        dfpv = self[columns + ['tf']].rename(columns={"tf": "t"})
        dfpv['start'] = False
        return dfp.append(dfpv, ignore_index=True, sort=False).sort_values(by=['t', 'start'], ascending=[True, False])

    @property
    def events_bounds(self):
        columns = sorted(list(set(self.columns) - {'ts', 'tf', 's', 'f'}))
        dfp = self[columns + ['ts', 's']].rename(columns={"ts": "t", 's':'closed'})
        dfp['start'] = True
        dfpv = self[columns + ['tf', 'f']].rename(columns={"tf": "t", 'f':'closed'})
        dfpv['start'] = False
        return dfp.append(dfpv, ignore_index=True, sort=False).sort_values(by=['t', 'start'])

    def measure_time(self):
        return (self.tf - self.ts).sum()

    def df_at(self, t):
        return self[self.index_at(t)]

    def df_at_interval(self, ts, tf, it=None):
        return self[self.index_at_interval(ts, tf, it)]

    def count_at(self, t):
        return self.index_at(t).sum()

    def index_at(self, t):
        return self.index_at_interval(t, t)

    def index_at_interval(self, ts, tf, it=None):
        assert ts <= tf
        l, r = (it in ['left', 'both'], it in ['right', 'both'])
        return ((self.ts < ts) & (self.tf > tf)) | ((self.ts == ts) & (self.s | l)) | ((self.tf == tf) & (self.f | r))

    def _save_or_return(self, df, inplace, on_column, disjoint_intervals=True):
        if df is None:
            df = self.__class__(columns=self.columns)
        else:
            df = self.__class__(df, columns=on_column+['ts', 'tf', 's', 'f'], disjoint_intervals=disjoint_intervals)

        if inplace and df is not self:
            return self._update_inplace(df._data)
        else:
            return (df.copy() if df is self else df)

    def merge(self, inplace=False):
        on_column = self.get_ni_columns(None)
        data = (self if inplace else self.copy())
        if not len(on_column):
            df = merge_no_key(data)
        else:
            df = merge_by_key(data)
        return self._save_or_return(df, inplace, on_column)

    def union(self, df, on_column=None, by_key=True, inplace=False):
        if df.empty:
            return self._save_or_return(self, inplace)

        assert not (not by_key and df is None)
        on_column = self.get_ni_columns(on_column)

        if not len(on_column):
            df = union_no_key(self, df)
        elif by_key:
            df = union_by_key(self, df)
        else:
            df = union_on_key(self, df)
        return self._save_or_return(df, inplace, on_column)

    def intersection(self, df=None, on_column=None, by_key=True, inplace=False):
        if df is None or df.empty:
            return self._save_or_return(None, inplace)

        assert not (not by_key and df is None)
        on_column = self.get_ni_columns(on_column)

        if not len(on_column):
            df = intersection_no_key(self, df)
        elif by_key:
            df = intersection_by_key(self, df)
        else:
            df = intersection_on_key(self, df)
        return self._save_or_return(df, inplace, on_column)

    def difference(self, dfb, on_column=None, by_key=True, inplace=False):
        if self.empty or dfb.empty:
            return self._save_or_return(self, inplace)

        on_column = self.get_ni_columns(on_column)
        if not len(on_column):
            df = difference_no_key(self, dfb)
        elif by_key:
            df = difference_by_key(self, dfb)
        else:
            df = difference_on_key(self, dfb)
        return self._save_or_return(df, inplace, on_column)

    def issuper(self, dfb, on_column=None, by_key=True):
        on_column = self.get_ni_columns(on_column)
        if not len(on_column):
            return issuper_no_key(self, dfb)
        elif by_key:
            return issuper_by_key(self, dfb)
        else:
            # Should function as well
            return issuper_no_key(self, dfb)

    def nonempty_intersection(self, bdf, on_column="u", by_key=True):
        on_column = self.get_ni_columns(on_column)
        if not len(on_column):
            return nonempty_intersection_no_key(self, bdf)
        elif by_key:
            return nonempty_intersection_by_key(self, bdf)
        else:
            # Should function as well
            return nonempty_intersection_no_key(self, bdf)

    def cartesian_intersection(self, base_df):
        assert set(base_df.columns) == {'u', 'ts', 'tf', 's', 'f'}
        assert set(self.columns) == {'u', 'v', 'ts', 'tf', 's', 'f'}
        nodes = set(u for u in base_df['u'])
        out = cartesian_intersection_(self[self.u.isin(nodes) & self.v.isin(nodes)], base_df)
        return self.__class__(out, columns=['u', 'v', 'ts', 'tf', 's', 'f'], disjoint_intervals=True)

    def map_intersection(self, base_df):
        assert set(base_df.columns) == {'u', 'ts', 'tf', 's', 'f'}
        assert set(self.columns) == {'u', 'v', 'ts', 'tf', 's', 'f'}
        nodes = set(u for u in base_df['u'])
        out = map_intersection_(self[self.u.isin(nodes)], base_df)
        return self.__class__(out, columns=['u', 'ts', 'tf', 's', 'f'], disjoint_intervals=True)

    def intersection_size(self, b):
        return interval_intersection_size_(self, b)

    @property
    def limits(self):
        ts, its = min((key[-4], not key[-2]) for key in self.itertuples(bounds=True))
        tf, itf = max((key[-3], not key[-1]) for key in self.itertuples(bounds=True))
        return (ts, tf, not its, itf)
