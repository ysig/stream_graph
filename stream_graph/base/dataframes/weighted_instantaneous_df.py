import pandas as pd
import operator
from collections import Iterable
from collections import defaultdict
from six import iteritems
from six import string_types
from .algorithms.weighted_instantaneous import union_on_key as union_on_key_
from .algorithms.weighted_instantaneous import union_by_key as union_by_key_
from .algorithms.weighted_instantaneous import intersection_by_key as intersection_by_key_
from .algorithms.weighted_instantaneous import intersection_on_key as intersection_on_key_
from .algorithms.weighted_instantaneous import difference_by_key as difference_by_key_
from .algorithms.weighted_instantaneous import difference_on_key as difference_on_key_
from .algorithms.weighted_instantaneous import issuper_by_key, issuper_on_key
from .algorithms.weighted_instantaneous import nonempty_intersection_by_key, nonempty_intersection_on_key
from .algorithms.utils.misc import hinge_loss, noner, first, min_sumer


class InstantaneousWDF(pd.DataFrame):
    def __init__(self, *args, **kargs):
        no_duplicates = kargs.pop('no_duplicates', None)
        merge_function = kargs.pop('merge_function', sum)
        assert callable(merge_function)
        super(self.__class__, self).__init__(*args, **kargs)
        assert 'ts' in self.columns

        self.merge_function = merge_function
        if not self.empty:
            from .instantaneous_df import InstantaneousDF
            if len(args) and isinstance(args[0], self.__class__) or (isinstance(kargs.get('data', None), self.__class__)):
                if no_duplicates:
                    self.merge(inplace=True)
            elif len(args) and isinstance(args[0], InstantaneousDF) or (isinstance(kargs.get('data', None), InstantaneousDF)):
                self['w'] = 1
                self.merge(inplace=True)
            elif no_duplicates is False:
                if 'w' not in self.columns:
                    self['w'] = 1
                self.merge(inplace=True)

    def copy(self, *args, **kargs):
        return self.__class__(super(self.__class__, self).copy(*args, **kargs))

    def drop(self, *args, **kargs):
        merge = kargs.pop('merge', True)
        out = super(self.__class__, self).drop(*args, **kargs)
        if isinstance(out, pd.DataFrame):
            if 'ts' in out.columns:
                if 'w' in out.columns:
                    out = InstantaneousWDF(out, no_duplicates=(not merge), merge_function=self.merge_function)
                else:
                    from .instantaneous_df import InstantaneousDF
                    no_duplicates = set(self.columns) - {'w'} == set(out.columns)
                    out = InstantaneousDF(out, no_duplicates=no_duplicates and (not merge))
        return out

    def append(self, *args, **kargs):
        merge = kargs.pop('merge', False)
        out = super(self.__class__, self).append(*args, **kargs)
        if isinstance(out, pd.DataFrame):
            if 'ts' in out.columns:
                out = self.__class__(out, no_duplicates=(not merge), merge_function=self.merge_function)
        return out

    def itertuples(self, index=False, name=None, weights=False):
        if weights:
            columns = sorted(list(set(self.columns) - {'ts', 'w'})) + ['ts', 'w']
        else:
            columns = sorted(list(set(self.columns) - {'ts', 'w'})) + ['ts']
        return super(self.__class__, self).reindex(columns=columns).itertuples(index=index, name=name)

    def __getitem__(self, index):
        out = super(self.__class__, self).__getitem__(index)
        if isinstance(out, pd.DataFrame):
            if 'ts' in out.columns:
                if 'w' in out.columns:
                    out = InstantaneousWDF(out, no_duplicates=set(out.columns) == set(self.columns), merge_function=self.merge_function)
                else:
                    from .instantaneous_df import InstantaneousDF
                    out = InstantaneousDF(out)
        return out

    def get_ni_columns(self, on_column):
        if on_column is None:
            columns = self.columns
        elif (not isinstance(on_column, Iterable) or isinstance(on_column, string_types)) and on_column in self.columns:
            columns = [on_column]
        elif isinstance(on_column, Iterable):
            columns = list(c for c in on_column)
        cols = sorted(list(set(c for c in columns) - {'ts', 'w'}))
        if on_column is not None:
            assert all(c in self.columns for c in cols)
        return cols, len(cols) == len(self.columns) - 1

    def df_at(self, t):
        return self[self.index_at(t)]

    def count_at(self, t, weights=False):
        if weights:
            return self.w[self.index_at(t)].sum()
        else:
            return self.index_at(t).sum()

    def index_at(self, t):
        return (self.ts == t)

    def _save_or_return(self, df, inplace, on_column=None, no_duplicates=True):
        if df is None:
            df = InstantaneousWDF(columns=self.columns, merge_function=self.merge_function)
        elif isinstance(df, list):
            assert on_column is not None
            df = self.__class__(df, columns=on_column + ['ts', 'w'], no_duplicates=no_duplicates, merge_function=self.merge_function)

        if inplace and df is not self:
            return self._update_inplace(df._data)
        else:
            return (df.copy() if df is self else df)

    def merge(self, inplace=False):
        if self.empty:
            return self._save_or_return(self, inplace)

        on_column, _ = self.get_ni_columns(None)
        columns = on_column + ['ts', 'w']
        data = defaultdict(list)
        for key in self[columns].itertuples(weights=True):
            data[key[:-1]].append(key[-1])

        out = list(key + (self.merge_function(ws), ) for key, ws in iteritems(data))
        return self._save_or_return(out, inplace, on_column=on_column)

    def union(self, df, on_column=None, by_key=True, inplace=False, union_function=None):
        if df.empty:
            return self._save_or_return(self, inplace)

        if union_function is None:
            import operator
            union_function = operator.add
        else:
            assert callable(union_function)

        on_column, _ = self.get_ni_columns(on_column)
        if not len(on_column) or by_key:
            df = union_by_key_(self, df, union_function)
        else:
            df = union_on_key_(self, df, union_function)
        return self._save_or_return(df, inplace, on_column)

    def intersection(self, df, on_column=None, by_key=True, inplace=False, intersection_function=None):
        assert not (not by_key and df is None)

        weighted = True
        if intersection_function is None:
            intersection_function = max
        elif intersection_function == 'unweighted':
            weighted = False
        else:
            assert callable(intersection_function)

        on_column, _ = self.get_ni_columns(on_column)
        if not len(on_column) or by_key:
            if df.empty:
                df = None
            else:
                df = intersection_by_key_(self, df, intersection_function)
        else:
            if weighted:
                ts = dict(pd.DataFrame(df[['ts', 'w']]).itertuples(index=False, name=None) if isinstance(df, pd.DataFrame) else df)
                df = (intersection_on_key_(self, ts, intersection_function) if len(ts) else None)
            else:
                ts = set(df.ts if isinstance(df, pd.DataFrame) else df)
                df = self[self.ts.isin(ts)] if len(ts) else None
        return self._save_or_return(df, inplace, on_column)

    def difference(self, dfb, on_column=None, by_key=True, inplace=False, difference_function=None):
        weighted = True
        if difference_function is None:
            difference_function = hinge_loss
        elif difference_function == 'unweighted':
            difference_function = noner
        else:
            assert callable(difference_function)

        if self.empty or dfb.empty:
            return self._save_or_return(self, inplace)
        on_column, _ = self.get_ni_columns(on_column)
        if not len(on_column) or by_key:
            df = difference_by_key_(self, dfb, difference_function)
        else:
            if weighted:
                ts = dict(pd.DataFrame(dfb[['ts', 'w']]).itertuples(index=False, name=None) if isinstance(dfb, pd.DataFrame) else dfb)
                df = (difference_on_key_(self, ts, difference_function) if len(ts) else self)
            else:
                ts = set(dfb.ts.values.flat if isinstance(dfb, pd.DataFrame) else dfb)
                df = self[~self.ts.isin(ts)] if len(ts) else self
        return self._save_or_return(df, inplace, on_column)

    def issuper(self, dfb, on_column=None, by_key=True, issuper_function=None):
        weighted = True
        if issuper_function is None:
            import operator
            issuper_function = operator.ge
        elif issuper_function == 'unweighted':
            weighted = False
        else:
            assert callable(issuper_function)

        if weighted:
            on_column = self.get_ni_columns(on_column)
            if not len(on_column) or by_key:
                return issuper_by_key(self, dfb, issuper_function)
            else:
                if isinstance(dfb, pd.DataFrame):
                    ts = dict(pd.DataFrame(dfb[['ts', 'w']]).itertuples(index=False, name=None))
                else:
                    ts = dict(t for t in dfb)
                return issuper_on_key(self, ts, issuper_function)
        else:
            if 'w' in dfb.columns:
                return self.drop(columns='w').issuper(dfb.drop(columns='w'), on_column=on_column, by_key=by_key)
            else:
                return self.drop(columns='w').issuper(dfb, on_column=on_column, by_key=by_key)

    def nonempty_intersection(self, dfb, on_column="u", by_key=True, nonempty_intersection_function=None):
        weighted = True
        if nonempty_intersection_function is None:
            nonempty_intersection_function = operator.ge
        elif nonempty_intersection_function == 'unweighted':
            weighted = False
        else:
            assert callable(nonempty_intersection_function)

        if weighted:
            on_column = self.get_ni_columns(on_column)
            if not len(on_column) or by_key:
                return nonempty_intersection_by_key(self, dfb, nonempty_intersection_function)
            else:
                if isinstance(dfb, pd.DataFrame):
                    ts = dict(pd.DataFrame(dfb[['ts', 'w']]).itertuples(index=False, name=None))
                else:
                    ts = dict(t for t in dfb)
                return nonempty_intersection_on_key(self, ts, nonempty_intersection_function)
        else:
            if 'w' in dfb.columns:
                return self.drop(columns='w').nonempty_intersection(dfb.drop(columns='w'), on_column=on_column, by_key=by_key)
            else:
                return self.drop(columns='w').nonempty_intersection(dfb, on_column=on_column, by_key=by_key)

    def cartesian_intersection(self, base_df, cartesian_intersection_function=None):
        weighted = True
        if cartesian_intersection_function is None:
            cartesian_intersection_function = max
        elif cartesian_intersection_function == 'unweighted':
            weighted = False
        else:
            assert callable(cartesian_intersection_function)

        if weighted:
            times = defaultdict(dict)
            for u, ts, w in base_df.itertuples(weights=True):
                times[u][ts] = w

            out = list()
            for u, v, ts, w in self.itertuples(weights=True):
                tus = times.get(u, None)
                if tus is not None:
                    wu = tus.get(ts, None)
                    if wu is not None:
                        tvs = times.get(v, None)
                        if tvs is not None:
                            wv = tvs.get(ts, None)
                            if wv is not None:
                                wf = cartesian_intersection_function(wu, wv, w)
                                if wf is not None:
                                    out.append((u, v, ts, wf))
        else:
            times = defaultdict(set)
            for u, ts in base_df[['u', 'ts']].itertuples():
                times[u].add(ts)

            out = list()
            for u, v, ts, w in self.itertuples(index=False, name=None, weights=True):
                tus = times.get(u, None)
                if tus is not None and ts in tus:
                    tvs = times.get(v, None)
                    if tvs is not None and ts in tvs:
                        out.append((u, v, ts, w))

        return self.__class__(out, columns=['u', 'v', 'ts', 'w'], no_duplicates=True)

    def map_intersection(self, base_df):
        return self.drop(columns='w').map_intersection(base_df)

    def intersection_size(self, dfb=None, discrete=True, interval_intersection_function=None):
        if interval_intersection_function != 'unweighted':
            if interval_intersection_function is None:
                interval_intersection_function = min_sumer
            else:
                assert callable(interval_intersection_function)
            if discrete:
                def constructor():
                    return (list(), list())
                d = defaultdict(constructor)
                for key in self.itertuples(weights=True):
                    d[key[-2]][0].append(key[-1])
                for key in dfb.itertuples(weights=True):
                    d[key[-2]][1].append(key[-1])
                return sum(interval_intersection_function(w[0], w[1]) for _, w in iteritems(d) if len(w[1]))
        else:
            if 'w' in dfb.columns:
                return self.drop(columns='w').interval_intersection_size(dfb.drop(columns='w'), discrete=discrete)
            else:
                return self.drop(columns='w').interval_intersection_size(dfb, discrete=discrete)

    @property
    def limits(self):
        return (self.ts.min(), self.ts.max())
