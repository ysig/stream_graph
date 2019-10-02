from __future__ import absolute_import
import pandas as pd
import operator
from six import string_types
from collections import Iterable
from .algorithms.utils.misc import hinge_loss, truer, min_sumer, oner, first
from .algorithms.weighted_continuous_interval import merge_no_key, merge_by_key, union_no_key, union_on_key, union_by_key, intersection_no_key, intersection_by_key, intersection_on_key, difference_no_key, difference_by_key, difference_on_key
from .algorithms.weighted_continuous_interval import issuper_no_key, issuper_by_key, issuper_on_key
from .algorithms.weighted_continuous_interval import nonempty_intersection_no_key, nonempty_intersection_by_key, nonempty_intersection_on_key
from .algorithms.weighted_continuous_interval import cartesian_intersection as cartesian_intersection_
from .algorithms.weighted_continuous_interval import interval_intersection_size as interval_intersection_size_


class CIntervalWDF(pd.DataFrame):
    def __init__(self, *args, **kargs):
        disjoint_intervals = kargs.pop('disjoint_intervals', None)
        merge_function = kargs.pop('merge_function', None)
        assert merge_function is None or callable(merge_function)
        super(CIntervalWDF, self).__init__(*args, **kargs)
        assert 'ts' in self.columns
        if 'tf' not in self.columns:
            self['tf'] = self['ts']
        if 's' not in self.columns:
            self['s'] = False
        if 'f' not in self.columns:
            self['f'] = True
        if 'w' not in self.columns:
            self['w'] = 1

        self.merge_function = (sum if merge_function is None else merge_function)
        if not self.empty:
            from .continuous_interval_df import CIntervalDF
            if len(args) and isinstance(args[0], CIntervalWDF):
                self.merge_function = args[0].merge_function
                if disjoint_intervals is not False:
                    self.merge(inplace=True)
            elif isinstance(kargs.get('data', None), CIntervalWDF):
                self.merge_function = kargs['data'].merge_function
                if disjoint_intervals is not False:
                    self.merge(inplace=True)
            elif len(args) and (isinstance(args[0], CIntervalDF) or isinstance(kargs.get('data', None), CIntervalDF)):
                self.merge(inplace=True)
            elif disjoint_intervals is False:
                self.merge(inplace=True)

    def copy(self, *args, **kargs):
        return CIntervalWDF(super(CIntervalWDF, self).copy(*args, **kargs))

    def drop(self, *args, **kargs):
        merge = kargs.pop('merge', True)
        out = super(CIntervalWDF, self).drop(*args, **kargs)
        if isinstance(out, pd.DataFrame):
            if {'ts', 'tf', 's', 'f'}.issubset(set(out.columns)):
                if 'w' in out.columns:
                    out = CIntervalWDF(out, disjoint_intervals=(not merge), merge_function=self.merge_function)
                else:
                    from stream_graph.base.dataframes import CIntervalDF
                    out = CIntervalDF(out, disjoint_intervals=(not merge))
        return out

    def append(self, *args, **kargs):
        merge = kargs.pop('merge', False)
        out = super(self.__class__, self).append(*args, **kargs)
        if merge:
            self.merge(inplace=True)
        return out

    def itertuples(self, index=False, name=None, weights=False, bounds=False):
        cols = sorted(list(set(self.columns) - {'ts', 'tf', 's', 'f', 'w'})) + ['ts', 'tf']
        if bounds:
            cols.extend(['s', 'f'])
        if weights:
            cols.append('w')
        return super(CIntervalWDF, self).reindex(columns=cols).itertuples(index=index, name=name)

    def __getitem__(self, index):
        out = super(CIntervalWDF, self).__getitem__(index)
        if isinstance(out, pd.DataFrame):
            if {'ts', 'tf', 's', 'f'}.issubset(set(out.columns)):
                if 'w' in out.columns:
                    # do you need to transfer merge_function?
                    out = self.__class__(out, disjoint_intervals=(set(out.columns) == set(self.columns)), merge_function=self.merge_function)
                else:
                    from stream_graph.base.dataframe import CIntervalDF
                    out = CIntervalDF(out, disjoint_intervals=False)
            # else 'ts' in out.columns:
            #     if 'w' in out.columns:
            #         out = InstantaneousWDF(out)
            #     else:
            #         out = InstantaneousDF(out)

        return out

    def get_ni_columns(self, on_column):
        if on_column is None:
            columns = self.columns
        elif (not isinstance(on_column, Iterable) or isinstance(on_column, string_types)) and on_column in self.columns:
            columns = [on_column]
        elif isinstance(on_column, Iterable):
            columns = list(c for c in on_column)
        cols = sorted(list(set(c for c in columns) - {'ts', 'tf', 's', 'f', 'w'}))
        if on_column is not None:
            assert all(c in self.columns for c in cols)
        return cols

    @property
    def events(self):
        columns = sorted(list(set(self.columns) - {'ts', 'tf', 's', 'f'}))
        dfp = self[columns + ['ts', 'w']].rename(columns={"ts": "t"})
        dfp['start'] = True
        dfpv = self[columns + ['tf', 'w']].rename(columns={"tf": "t"})
        dfpv['start'] = False
        return dfp.append(dfpv, ignore_index=True, sort=False).sort_values(by=['t', 'start'], ascending=[True, False])

    @property
    def events_bounds(self):
        columns = sorted(list(set(self.columns) - {'ts', 'tf', 's', 'f'}))
        dfp = self[columns + ['ts', 's', 'w']].rename(columns={"ts": "t", 's': 'closed'})
        dfp['start'] = True
        dfpv = self[columns + ['tf', 'f', 'w']].rename(columns={"tf": "t", 'f': 'closed'})
        dfpv['start'] = False
        return dfp.append(dfpv, ignore_index=True, sort=False).sort_values(by=['t', 'start'])

    def measure_time(self, weights=False):
        if weights:
            return (self.tf - self.ts) * self.w.sum()
        else:
            return (self.tf - self.ts).sum()

    def sort_values(self, by, axis=0, ascending=True, inplace=False, kind='quicksort', na_position='last'):
        df = super(self.__class__, self).sort_values(by, axis, ascending, inplace, kind, na_position)
        if not inplace:
            return self.__class__(df, merge_function=self.merge_function)

    def df_at(self, t):
        return self[self.index_at(t)]

    def df_at_interval(self, ts, tf, it=None):
        return self[self.index_at_interval(ts, tf)]

    def count_at(self, t, weights=False):
        if weights:
            return self.w[self.index_at(t)].sum()
        else:
            return self.index_at(t).sum()

    def index_at(self, t):
        return (self.ts <= t) & (self.tf >= t)

    def index_at_interval(self, ts, tf, it=None):
        assert ts <= tf
        l, r = (it in ['left', 'both'], it in ['right', 'both'])
        return ((self.ts < ts) & (self.tf > tf)) | ((self.s | l) & (self.ts == ts)) | ((self.f | r) & (self.tf == tf))

    def _save_or_return(self, df, inplace, on_column, disjoint_intervals=True):
        if df is None:
            df = self.__class__(columns=self.columns, merge_function=self.merge_function)
        elif isinstance(df, list):
            df = self.__class__(df, columns=on_column + ['ts', 'tf', 's', 'f', 'w'], disjoint_intervals=disjoint_intervals, merge_function=self.merge_function)

        if inplace and df is not self:
            return self._update_inplace(df._data)
        else:
            return (df.copy() if df is self else df)

    def merge(self, inplace=False):
        on_column = self.get_ni_columns(None)

        if not len(on_column):
            df = merge_no_key(self, self.merge_function)
        else:
            df = merge_by_key(self, self.merge_function)

        return self._save_or_return(df, inplace, on_column)

    def union(self, df, on_column=None, by_key=True, inplace=False, union_function=None):
        if df.empty:
            return self._save_or_return(self, inplace)

        assert not (not by_key and df is None)

        if union_function is None:
            union_function = operator.add
        else:
            assert callable(union_function)

        on_column = self.get_ni_columns(on_column)
        if not len(on_column):
            df = union_no_key(self, df, union_function)
        elif by_key:
            df = union_by_key(self, df, union_function)
        else:
            df = union_on_key(self, df, union_function)
        return self._save_or_return(df, inplace, on_column)

    def intersection(self, dfb, on_column=None, by_key=True, inplace=False, intersection_function=None):
        # Maybe allow signalling of ignore value with None
        if dfb.empty:
            return self._save_or_return(None, inplace)

        assert not (not by_key and dfb is None)

        if intersection_function is None:
            intersection_function = min
        elif intersection_function == 'unweighted':
            intersection_function = first
        else:
            assert callable(intersection_function)

        on_column = self.get_ni_columns(on_column)
        if not len(on_column):
            df = intersection_no_key(self, dfb, intersection_function)
        elif by_key:
            df = intersection_by_key(self, dfb, intersection_function)
        else:
            df = intersection_on_key(self, dfb, intersection_function)
        return self._save_or_return(df, inplace, on_column)

    def difference(self, dfb, on_column=None, by_key=True, inplace=False, difference_function=None):
        # Maybe allow signalling of ignore value with None
        if self.empty or dfb.empty:
            return self._save_or_return(self, inplace)

        if difference_function is None:
            difference_function = hinge_loss
        else:
            assert callable(difference_function)

        on_column = self.get_ni_columns(on_column)
        if not len(on_column):
            df = difference_no_key(self, dfb, difference_function)
        elif by_key:
            df = difference_by_key(self, dfb, difference_function)
        else:
            df = difference_on_key(self, dfb, difference_function)

        return self._save_or_return(df, inplace, on_column)

    def issuper(self, dfb, on_column=None, by_key=True, issuper_function=None):
        if issuper_function is None:
            issuper_function = operator.ge
        elif issuper_function == 'unweighted':
            issuper_function = truer
        else:
            assert callable(issuper_function)

        on_column = self.get_ni_columns(on_column)
        if not len(on_column):
            return issuper_no_key(self, dfb, issuper_function)
        elif by_key:
            return issuper_by_key(self, dfb, issuper_function)
        else:
            # Should function as well
            return issuper_on_key(self, dfb, issuper_function)

    def nonempty_intersection(self, dfb, on_column="u", by_key=True, nonempty_intersection_function=None):
        if nonempty_intersection_function is None:
            nonempty_intersection_function = operator.ge
        elif nonempty_intersection_function == 'unweighted':
            nonempty_intersection_function = truer
        else:
            assert callable(nonempty_intersection_function)

        on_column = self.get_ni_columns(on_column)
        if not len(on_column):
            return nonempty_intersection_no_key(self, dfb, nonempty_intersection_function)
        elif by_key:
            return nonempty_intersection_by_key(self, dfb, nonempty_intersection_function)
        else:
            # Should function as well
            return nonempty_intersection_on_key(self, dfb, nonempty_intersection_function)

    def cartesian_intersection(self, base_df, cartesian_intersection_function=None):
        assert (set(base_df.columns) - {'u', 'ts', 'tf', 's', 'f'}).issubset({'w'})
        assert set(self.columns) == {'u', 'v', 'ts', 'tf', 's', 'f', 'w'}

        if cartesian_intersection_function is None:
            cartesian_intersection_function = min
        elif cartesian_intersection_function == 'unweighted':
            cartesian_intersection_function = oner
        else:
            assert callable(cartesian_intersection_function)

        u_set = set(base_df[['u']].values.flat)
        out = cartesian_intersection_(self[self.u.isin(u_set) & self.v.isin(u_set)], base_df, cartesian_intersection_function)
        return self.__class__(out, columns=['u', 'v', 'ts', 'tf', 's', 'f', 'w'], disjoint_intervals=True)

    def map_intersection(self, base_df):
        # Not implemented for weighted
        return self.drop(columns='w').map_intersection(base_df)

    def intersection_size(self, df, discrete=False, interval_intersection_function=None):
        # cache = [Counter, Counter, None, 0]
        if interval_intersection_function is None:
            interval_intersection_function = min_sumer
        elif interval_intersection_function == 'unweighted':
            interval_intersection_function = oner
        else:
            assert callable(interval_intersection_function)

        return interval_intersection_size_(self, df, interval_intersection_function)

    @property
    def limits(self):
        ts, its = min((key[-4], not key[-2]) for key in self.itertuples(bounds=True))
        tf, itf = max((key[-3], not key[-1]) for key in self.itertuples(bounds=True))
        return (ts, tf, not its, itf)
