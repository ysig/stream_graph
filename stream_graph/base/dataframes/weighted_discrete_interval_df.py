from __future__ import absolute_import
import pandas as pd
import operator
from six import string_types
from collections import Iterable
from .algorithms.weighted_discrete_interval import merge_no_key, merge_by_key
from .algorithms.weighted_discrete_interval import union_no_key, union_by_key, union_on_key
from .algorithms.weighted_discrete_interval import intersection_no_key, intersection_by_key, intersection_on_key
from .algorithms.weighted_discrete_interval import difference_no_key, difference_by_key, difference_on_key
from .algorithms.weighted_discrete_interval import issuper_no_key, issuper_by_key, issuper_on_key
from .algorithms.weighted_discrete_interval import nonempty_intersection_by_key, nonempty_intersection_no_key, nonempty_intersection_on_key
from .algorithms.weighted_discrete_interval import cartesian_intersection as cartesian_intersection_
from .algorithms.weighted_discrete_interval import interval_intersection_size as interval_intersection_size_
from .algorithms.utils.misc import hinge_loss, noner, first, truer, min_sumer, oner


class DIntervalWDF(pd.DataFrame):
    def __init__(self, *args, **kargs):
        disjoint_intervals = kargs.pop('disjoint_intervals', None)

        super(DIntervalWDF, self).__init__(*args, **kargs)
        assert 'ts' in self.columns
        assert self.ts.dtype.kind == 'i'

        if 'tf' not in self.columns:
            self['tf'] = self['ts']
        else:
            assert self.tf.dtype.kind == 'i'

        if not self.empty:
            from .discrete_interval_df import DIntervalDF
            if len(args) and isinstance(args[0], DIntervalWDF) or (isinstance(kargs.get('data', None), DIntervalWDF)):
                if disjoint_intervals:
                    self.merge(inplace=True)
            elif len(args) and isinstance(args[0], DIntervalDF) or (isinstance(kargs.get('data', None), DIntervalDF)):
                self['w'] = 1
                self.merge(inplace=True)
            elif disjoint_intervals is False:
                if 'w' not in self.columns:
                    self['w'] = 1
                self.merge(inplace=True)

    def copy(self, *args, **kargs):
        return DIntervalWDF(super(DIntervalWDF, self).copy(*args, **kargs))

    def drop(self, *args, **kargs):
        merge = kargs.pop('merge', True)
        out = super(DIntervalWDF, self).drop(*args, **kargs)
        if isinstance(out, pd.DataFrame):
            if 'ts' in out.columns:
                if 'tf' in out.columns:
                    if 'w' in out.columns:
                        out = DIntervalWDF(out, disjoint_intervals=(not merge))
                    else:
                        from .discrete_interval_df import DIntervalDF
                        out = DIntervalDF(out, disjoint_intervals=(not merge))
#                else:
#                    if 'w' in out.columns:
#                        out = InstantaneousWDF(out)
#                    else:
#                        out = InstantaneousDF(out)
        return out

    def itertuples(self, index=False, name=None, weights=False):
        columns = sorted(list(set(self.columns) - {'ts', 'tf', 'w'})) + ['ts', 'tf']
        if weights:
            columns.append('w')

        return super(DIntervalWDF, self).reindex(columns=columns).itertuples(index=index, name=name)

    def __getitem__(self, index):
        out = super(DIntervalWDF, self).__getitem__(index)
        if isinstance(out, pd.DataFrame):
            if 'ts' in out.columns and 'tf' in out.columns:
                if 'w' in out.columns:
                    out = DIntervalWDF(out, disjoint_intervals=(set(out.columns) == set(self.columns)))
                else:
                    from .discrete_interval_df import DIntervalDF
                    out = DIntervalDF(out, disjoint_intervals=False)
            #else 'ts' in out.columns:
            #    if 'w' in out.columns:
            #        out = InstantaneousWDF(out)
            #    else:
            #        out = InstantaneousDF(out)

        return out

    def get_ni_columns(self, on_column):
        if on_column is None:
            columns = self.columns
        elif (not isinstance(on_column, Iterable) or isinstance(on_column, string_types)) and on_column in self.columns:
            columns = [on_column]
        elif isinstance(on_column, Iterable):
            columns = list(c for c in on_column)
        cols = sorted(list(set(c for c in columns) - {'ts', 'tf', 'w'}))
        if on_column is not None:
            assert all(c in self.columns for c in cols)
        return cols

    @property
    def events(self):
        return _events_not_sorted(self, True).sort_values(by=['t', 'start'])

    def append(self, *args, **kargs):
        merge = kargs.pop('merge', False)
        out = super(self.__class__, self).append(*args, **kargs)
        if merge:
            self.merge(inplace=True)
        return out

    def measure_time(self, weights=False):
        if weights:
            return ((self.tf - self.ts + 1)*self.w).sum()
        else:
            return (self.tf - self.ts + 1).sum()

    def df_at(self, t):
        return self[self.index_at(t)]

    def df_at_interval(self, ts, tf):
        return self[self.index_at_interval(ts, tf)]

    def count_at(self, t, weights=False):
        if weights:
            return self.w[self.index_at(t)].sum()
        else:
            return self.index_at(t).sum()

    def index_at(self, t):
        assert type(t) is int
        return (self.ts <= t) & (self.tf >= t)

    def index_at_interval(self, ts, tf):
        assert ts <= tf and type(ts) is int and type(tf) is int
        return (self.ts <= ts) & (self.tf >= tf)

    def _save_or_return(self, df, inplace, on_column=None, disjoint_intervals=True):
        if df is None:
            df = self.__class__(columns=self.columns)
        elif isinstance(df, list):
            assert on_column is not None
            df = self.__class__(df, columns=on_column + ['ts', 'tf', 'w'], disjoint_intervals=disjoint_intervals)

        if inplace and df is not self:
            return self._update_inplace(df._data)
        else:
            return (df.copy() if df is self else df)

    def merge(self, inplace=False, merge_function=None):
        if self.empty:
            return self._save_or_return(self, inplace)

        if merge_function is None:
            merge_function = sum
        else:
            assert callable(merge_function)

        on_column = self.get_ni_columns(None)
        if not len(on_column):
            df = merge_no_key(self, merge_function)
        else:
            df = merge_by_key(self, merge_function)
        return self._save_or_return(df, inplace, on_column)

    def union(self, df, on_column=None, by_key=True, inplace=False, union_function=None):
        if df.empty:
            return self._save_or_return(self, inplace)

        if union_function is None:
            union_function = operator.add
        else:
            assert callable(union_function)

        assert not (not by_key and df is None)

        on_column = self.get_ni_columns(on_column)
        if not len(on_column):
            df = union_no_key(self, df, union_function)
        elif by_key:
            df = union_by_key(self, df, union_function)
        else:
            df = union_on_key(self, df, union_function)
        return self._save_or_return(df, inplace, on_column)

    def intersection(self, dfb, on_column=None, by_key=True, inplace=False, intersection_function=None):
        if dfb.empty:
            return self._save_or_return(None, inplace)

        if intersection_function is None:
            intersection_function = max
        elif intersection_function == 'unweighted':
            intersection_function = first
        else:
            assert callable(intersection_function)

        assert not (not by_key and dfb is None)
        on_column = self.get_ni_columns(on_column)
        if not len(on_column):
            df = intersection_no_key(self, dfb, intersection_function)
        elif by_key:
            df = intersection_by_key(self, dfb, intersection_function)
        else:
            df = intersection_on_key(self, dfb, intersection_function)
        return self._save_or_return(df, inplace, on_column)

    def difference(self, dfb, on_column=None, by_key=True, inplace=False, difference_function=None):
        if difference_function is None:
            difference_function = hinge_loss
        elif difference_function == 'unweighted':
            difference_function = noner
        else:
            assert callable(difference_function)

        if self.empty or dfb.empty:
            return self._save_or_return(self, inplace)

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
            import operator
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
        assert set(self.columns) == {'u', 'v', 'ts', 'tf', 'w'}

        if cartesian_intersection_function is None:
            cartesian_intersection_function = max
        elif cartesian_intersection_function == 'unweighted':
            cartesian_intersection_function = truer
        else:
            assert callable(cartesian_intersection_function)

        u_set = set(base_df[['u']].values.flat)
        out = cartesian_intersection_(self[self.u.isin(u_set) & self.v.isin(u_set)], base_df, cartesian_intersection_function)

        return self.__class__(out, columns=(['u', 'v', 'ts', 'tf', 'w']), disjoint_intervals=True)

    def map_intersection(self, base_df):
        # Not implemented for weighted
        return self.drop(columns='w').map_intersection(base_df)

    def interval_intersection_size(self, b, intersection_function=None):
        # cache = [Counter, Counter, None, 0]
        if intersection_function is None:
            intersection_function = min_sumer
        elif intersection_function == 'unweighted':
            intersection_function = oner
        else:
            assert callable(intersection_function)

        return interval_intersection_size_(self, b, intersection_function)

    @property
    def limits(self):
        return (self.ts.min(), self.tf.max())
