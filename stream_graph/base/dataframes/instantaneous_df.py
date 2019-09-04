import pandas as pd
from collections import Iterable
from collections import Counter
from collections import defaultdict
from six import iteritems
from six import string_types
from .algorithms.utils.misc import set_tuple


class InstantaneousDF(pd.DataFrame):
    def __init__(self, *args, **kargs):
        no_duplicates = kargs.pop('no_duplicates', None)
        super(InstantaneousDF, self).__init__(*args, **kargs)
        assert 'ts' in self.columns

        if not self.empty:
            from .weighted_instantaneous_df import InstantaneousWDF
            if len(args) and isinstance(args[0], InstantaneousWDF) or (isinstance(kargs.get('data', None), InstantaneousWDF)):
                self.drop(columns=['w'])
            elif len(args) and isinstance(args[0], self.__class__) or (isinstance(kargs.get('data', None), self.__class__)):
                if no_duplicates:
                    self.merge(inplace=True)
            elif no_duplicates is False:
                self.merge(inplace=True)

    def copy(self, *args, **kargs):
        return self.__class__(super(InstantaneousDF, self).copy(*args, **kargs))

    def drop(self, *args, **kargs):
        merge = kargs.pop('merge', True)
        out = super(InstantaneousDF, self).drop(*args, **kargs)
        if 'ts' in out.columns:
            return self.__class__(out, no_duplicates=(not merge))
        return out

    def append(self, *args, **kargs):
        merge = kargs.pop('merge', False)
        out = super(InstantaneousDF, self).append(*args, **kargs)
        if isinstance(out, pd.DataFrame):
            if 'ts' in out.columns:
                out = self.__class__(out)
                if merge:
                    out.merge(inplace=True)
        return out

    def sort_values(self, by, axis=0, ascending=True, inplace=False, kind='quicksort', na_position='last'):
        df = super(self.__class__, self).sort_values(by, axis, ascending, inplace, kind, na_position)
        if not inplace:
            return self.__class__(df)

    def itertuples(self, index=False, name=None):
        columns = sorted(list(set(self.columns) - {'ts'})) + ['ts']
        return super(InstantaneousDF, self).reindex(columns=columns).itertuples(index=index, name=name)

    def __getitem__(self, index):
        out = super(InstantaneousDF, self).__getitem__(index)
        if isinstance(out, pd.DataFrame):
            if 'ts' in out.columns:
                out = self.__class__(out, no_duplicates=(set(out.columns) == set(self.columns)))
        return out

    def get_ni_columns(self, on_column):
        if on_column is None:
            columns = self.columns
        elif (not isinstance(on_column, Iterable) or isinstance(on_column, string_types)) and on_column in self.columns:
            columns = [on_column]
        elif isinstance(on_column, Iterable):
            columns = list(c for c in on_column)
        cols = sorted(list(set(c for c in columns) - {'ts'}))
        if on_column is not None:
            assert all(c in self.columns for c in cols)
        return cols, len(cols) == len(self.columns) - 1

    def df_at(self, t):
        return self[self.index_at(t)]

    def count_at(self, t):
        return self.index_at(t).sum()

    def index_at(self, t):
        return (self.ts == t)

    def _save_or_return(self, df, inplace, on_column=None, no_duplicates=True):
        if df is None:
            df = self.__class__(columns=self.columns)
        elif isinstance(df, list):
            df = self.__class__(df, columns=on_column + ['ts'], no_duplicates=no_duplicates)

        if inplace and df is not self:
            return self._update_inplace(df._data)
        else:
            return (df.copy() if df is self else df)

    def merge(self, inplace=False):
        return self.drop_duplicates(inplace=inplace)

    def union(self, df, on_column=None, by_key=True, inplace=False):
        assert not (not by_key and df is None)
        if df.empty:
            return self._save_or_return(self, inplace)

        on_column, ac = self.get_ni_columns(on_column)
        if not len(on_column) or by_key:
            if ac:
                df = self.append(df, ignore_index=True, sort=False)
            else:
                df = self[on_column + ['ts']].append(df[on_column + ['ts']], ignore_index=True, sort=False)
            return self.append(df, ignore_index=True, sort=False).drop_duplicates(inplace=inplace)
        else:
            ts = set(df.ts if isinstance(df, pd.DataFrame) else df)
            data = defaultdict(set)
            for key in (self if ac else self[on_column + ['ts']]).itertuples():
                k, t = key[:-1], key[-1]
                data[k].add(t)

            df = list(key + (t,) for key, t_set in iteritems(data) for t in t_set.union(ts))
            return self._save_or_return(df, inplace, on_column=on_column)

    def intersection(self, df, on_column=None, by_key=True, inplace=False):
        assert not (not by_key and df is None)

        on_column, ac = self.get_ni_columns(on_column)
        if not len(on_column) or by_key:
            if df.empty:
                self._save_or_return(None, inplace)

            if ac:
                df = self.append(df, ignore_index=True, sort=False)
            else:
                dfa = self[on_column + ['ts']].drop_duplicates()
                dfb = df[on_column + ['ts']].drop_duplicates()
                df = dfa.append(dfb, ignore_index=True, sort=False)
            df = df[df.duplicated(keep='first')]
        else:
            ts = (set(df['ts']) if isinstance(df, pd.DataFrame) else set(df))
            if not len(ts):
                self._save_or_return(None, inplace)

            df = (self if ac else self[on_column + ['ts']])
            df = df[df.ts.isin(ts)].drop_duplicates()
        return self._save_or_return(df, inplace)

    def difference(self, dfb, on_column=None, by_key=True, inplace=False):
        if self.empty or dfb.empty:
            return self._save_or_return(self, inplace)
        on_column, ac = self.get_ni_columns(on_column)
        if not len(on_column) or by_key:
            if ac:
                dfa = self[on_column + ['ts']].drop_duplicates()
                dfb = dfb[on_column + ['ts']].drop_duplicates()
            else:
                dfa = self
            df = dfa[~dfa.append(dfb, ignore_index=False, sort=False).duplicated(keep=False)[:dfa.shape[0]]]
        else:
            ts = set(dfb.ts if isinstance(dfb, pd.DataFrame) else dfb)
            data = defaultdict(set)
            for key in (self if ac else self[on_column + ['ts']]).itertuples():
                k, t = key[:-1], key[-1]
                data[k].add(t)

            df = list(key + (t,) for key, t_set in iteritems(data) for t in t_set.difference(ts))

        return self._save_or_return(df, inplace, on_column=on_column)

    def issuper(self, dfb, on_column=None, by_key=True):
        on_column, ac = self.get_ni_columns(on_column)
        if not len(on_column) or by_key:
            return dfb.append(self, ignore_index=True, sort=False).duplicated(keep=False)[:dfb.shape[0]].all()
        else:
            data, ts = defaultdict(set), set(dfb.ts if isinstance(dfb, pd.DataFrame) else dfb)
            for key in (self if ac else self[on_column + ['ts']]).itertuples():
                k, t = key[:-1], key[-1]
                data[k].add(t)
            return all(t_set.issuperset(ts) for key, t_set in iteritems(data))

    def nonempty_intersection(self, bdf, on_column="u", by_key=True):
        on_column, ac = self.get_ni_columns(on_column)
        if not len(on_column) or by_key:
            return self.append(bdf, ignore_index=True, sort=False).duplicated(keep='first').any()
        else:
            ts = set(bdf.ts if isinstance(bdf, pd.DataFrame) else bdf)
            for key in (self if ac else self[on_column + ['ts']]).itertuples():
                if key[-1] in ts:
                    return True
            return False

    def cartesian_intersection(self, base_df):
        # Base
        base_d = defaultdict(set)
        for u, t in base_df.itertuples():
            base_d[u].add(t)

        df = None
        if len(base_d):
            base_set = set(base_d.keys())

            # My-Data
            data = defaultdict(set)
            for key in self.itertuples():
                u, v, t = key
                if {u, v}.issubset(base_set):
                    data[(u, v)].add(t)

            if len(data):
                df = list(key + (t, ) for key, t_set in iteritems(data) for t in t_set.intersection(base_d[key[0]]).intersection(base_d[key[1]]))

        return self._save_or_return(df, inplace=False, on_column=['u', 'v'])

    def map_intersection(self, base_df):
        # Base
        base_d = defaultdict(set)
        for u, t in base_df.itertuples():
            base_d[u].add(t)

        df = None
        if len(base_d):
            # My-Data
            data = defaultdict(set_tuple)
            for key in self.itertuples():
                u, v, t = key
                if u in base_d:
                    data[v][0].update(base_d[u])
                    data[v][1].add(t)
            if len(data):
                df = list((v, t) for v, s in iteritems(data) for t in s[0].intersection(s[1]))

        return self._save_or_return(df, inplace=False, on_column=['u'])

    def intersection_size(self, b=None):
        ca, cb = Counter(self.ts), Counter(b.ts)
        return sum(v*cb[k] for k, v in iteritems(ca))

    @property
    def limits(self):
        return (self.ts.min(), self.ts.max())
