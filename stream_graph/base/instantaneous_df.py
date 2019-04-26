import pandas as pd
import numpy as np
from collections import Iterable
from collections import Counter
from collections import defaultdict
from six import iteritems
from six import string_types
from numbers import Real

class InstantaneousDF(pd.DataFrame):
    def __init__(self, *args, **kargs):
        super(InstantaneousDF, self).__init__(*args, **kargs)
        assert 'ts' in self.columns

    def copy(self, *args, **kargs):
        return InstantaneousDF(super(InstantaneousDF, self).copy(*args, **kargs))

    def drop(self, *args, **kargs):
        out = super(InstantaneousDF, self).drop(*args, **kargs)
        if isinstance(out, pd.DataFrame):
            if 'ts' in out.columns:
                out = InstantaneousDF(out)
        return out

    def itertuples(self, index=False, name=None):
        columns = sorted(list(set(self.columns) - {'ts'})) + ['ts']
        return super(InstantaneousDF, self).reindex(columns=columns).itertuples(index=index, name=name)

    def __getitem__(self, index):
        out = super(InstantaneousDF, self).__getitem__(index)
        if isinstance(out, pd.DataFrame):
            if 'ts' in out.columns:
                out = InstantaneousDF(out)
        return out

    def get_ni_columns(self, on_column):
        if on_column is None:
            columns = self.columns
        elif (not isinstance(on_column, Iterable) or isinstance(on_column, string_types)) and on_column in self.columns:
            columns = [c]
        elif isinstance(on_column, Iterable):
            columns = list(c for c in on_column)
        cols = sorted(list(set(c for c in columns) - {'ts'}))
        if on_column is not None:
            assert all(c in self.columns for c in cols)
        return cols

    def df_at(self, t):
        return self[self.index_at(t)]

    def count_at(self, t):
        return self.index_at(t).sum()

    def index_at(self, t):
        return (self.ts == t)

    def _save_or_return(self, df, inplace):
        if df is None:
            df = InstantaneousDF(columns=self.columns)

        if inplace and df is not self:
            return self._update_inplace(df._data)
        else:
            return (df.copy() if df is self else df)  

    def merge(self, inplace=False):
        return self.drop_duplicates(inplace=inplace)

    def union(self, df, on_column=None, by_key=True, inplace=False):
        assert not (not by_key and df is None)
        
        on_column = self.get_ni_columns(on_column)
        if not len(on_column) or by_key:
            if df.empty:
                df = self
            else:
                if len(on_column) == len(self.columns) -1:
                    df = self.append(df, ignore_index=True, sort=False)
                else:
                    df = self[on_column + ['ts']].append(df[on_column + ['ts']], ignore_index=True, sort=False)
                return self.append(df, ignore_index=True, sort=False).drop_duplicates(inplace=inplace)
        else:
            if df.empty:
                df = self
            else:
                ts = (df[['ts']] if isinstance(df, pd.DataFrame) else pd.DataFrame(list(df), columns='ts'))
                def append_dd(dfa, dfb):
                    return dfa[['ts']].append(dfb, ignore_index=True, sort=False).drop_duplicates()
                df = InstantaneousDF(self.groupby(on_column).apply(append_dd, ts)).gby_format()

        return self._save_or_return(df, inplace)

    def intersect(self, df, on_column=None, by_key=True, inplace=False):
        assert not (not by_key and df is None)

        on_column = self.get_ni_columns(on_column)
        if not len(on_column) or by_key:
            if df.empty:
                self._save_or_return(None, inplace)
            if len(on_column) != len(self.columns) - 1:
                dfa = self[on_column + ['ts']].drop_duplicates()
                dfb = dfb[on_column + ['ts']].drop_duplicates()
                df = dfa.append(dfb, ignore_index=True, sort=False)
            else:
                df = self.append(df, ignore_index=True, sort=False)
            df = df[df.duplicated(keep='first')]
        else:
            ts = (set(df.ts) if isinstance(df, pd.DataFrame) else set(df))
            if not len(ts):
                return self._save_or_return(None, inplace)

            df = self[on_column + ['ts']]
            df = df[df.ts.isin(ts)].drop_duplicates()
        return self._save_or_return(df, inplace)

    def difference(self, dfb, on_column=None, by_key=True, inplace=False):
        if self.empty or dfb.empty:
            return self._save_or_return(self, inplace)
        on_column = self.get_ni_columns(on_column)
        if not len(on_column) or by_key:
            if len(on_column) != len(self.columns) -1:
                dfa = self[on_column + ['ts']].drop_duplicates()
                dfb = dfb[on_column + ['ts']].drop_duplicates()
            else:
                dfa = self
            df = dfa[~dfa.append(dfb, ignore_index=False, sort=False).duplicated(keep=False)[:dfa.shape[0]]]
        else:
            ts = pd.DataFrame((dfb.ts.values if isinstance(dfb, pd.DataFrame) else list(dfb)), columns=['ts'])
            if ts.empty:
                return self._save_or_return(self, inplace)

            def append_rd(dfa, dfb):
                dfa = dfa[['ts']]
                return dfa[~dfa.append(dfb, ignore_index=False, sort=False).duplicated(keep=False)[:dfa.shape[0]]]
            df = InstantaneousDF(self.groupby(on_column).apply(append_rd, ts)).gby_format() 

        return self._save_or_return(df, inplace)

    def gby_format(self):
        ndf = self.reset_index()
        return ndf[[col for col in ndf.columns if not (len(col) > 6 and col[:5]=='level')]]

    def issuper(self, dfb, on_column=None, by_key=True):
        on_column = self.get_ni_columns(on_column)
        if not len(on_column) or by_key:
            return dfb.append(self, ignore_index=True, sort=False).duplicated(keep=False)[:dfb.shape[0]].all()
        else:
            def append_dup(dfa, dfb):
                return dfb.append(dfa, ignore_index=True, sort=False).duplicated(keep=False)[:dfb.shape[0]].all()
            return self.groupby(on_column).apply(append_dup, dfb).all()

    def nonempty_intersection(self, bdf, on_column="u", by_key=True):
        on_column = self.get_ni_columns(on_column)
        if not len(on_column) or by_key:
            return self.append(bdf, ignore_index=True, sort=False).duplicated(keep='first').any()
        else:
            def append_ad(dfa, dfb):
                return not dfa.empty and dfa.append(dfb, ignore_index=True, sort=False).duplicated(keep='first').any()
            return self.groupby(on_column).apply(append_ad, bdf).all()

    def cartesian_intersect(self, base_df):
        base_df = base_df.set_index('u')
        df = self[self.u.isin(base_df.index) & self.v.isin(base_df.index)]
        def cartesian_kd(df, base):
            u, v = df.name
            dfu = base.loc[u, ['ts']]
            dfv = base.loc[v, ['ts']]
            dft = df[['ts']]
            dfi = dft.append(dfu, ignore_index=True, sort=True)
            df = dfi[dfi.duplicated(keep='first')].append(dfv, ignore_index=True, sort=True)
            df = df[df.duplicated(keep='first')]
            if not df.empty:
                return df
        return InstantaneousDF(df.groupby(['u', 'v']).apply(cartesian_kd, base_df)).gby_format()

    def map_intersect(self, base_df):
        base_df = base_df.set_index('u')
        df = self[self.u.isin(base_df.index)]
        def map_kd(df, base):
            u, _ = df.name
            dfu = base.loc[u, ['ts']]
            df = df[['ts']]
            df = df.append(dfu, ignore_index=True, sort=True)
            df = df[df.duplicated(keep='first')]
            if not df.empty:
                return df
        return InstantaneousDF(df.groupby(['u', 'v']).apply(map_kd, base_df)).gby_format().drop(columns=['u']).rename(columns={'v': 'u'})

    def interval_intersection_size(self, b=None, discrete=False):
        if discrete:
            return b.join(self[['ts']].set_index('ts'), on='ts', how='inner').shape[0]
        else:
            #Instantaneous links have no size
            return 0


class InstantaneousWDF(pd.DataFrame):
    def __init__(self, *args, **kargs):
        super(self.__class__, self).__init__(*args, **kargs)
        assert 'ts' in self.columns
        if 'w' not in self.columns:
            self['w'] = 1

    def copy(self, *args, **kargs):
        return self.__class__(super(self.__class__, self).copy(*args, **kargs))

    def drop(self, *args, **kargs):
        out = super(self.__class__, self).drop(*args, **kargs)
        if isinstance(out, pd.DataFrame):
            if 'ts' in out.columns:
                if 'w' in out.columns:
                    out = InstantaneousWDF(out)
                else:
                    out = InstantaneousDF(out)           
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
                    out = InstantaneousWDF(out)
                else:
                    out = InstantaneousDF(out)
        return out

    def get_ni_columns(self, on_column):
        if on_column is None:
            columns = self.columns
        elif (not isinstance(on_column, Iterable) or isinstance(on_column, string_types)) and on_column in self.columns:
            columns = [c]
        elif isinstance(on_column, Iterable):
            columns = list(c for c in on_column)
        cols = sorted(list(set(c for c in columns) - {'ts', 'w'}))
        if on_column is not None:
            assert all(c in self.columns for c in cols)
        return cols

    def df_at(self, t):
        return self[self.index_at(t)]

    def count_at(self, t, weights=False):
        if weights:
            return self.w[self.index_at(t)].sum()
        else:
            return self.index_at(t).sum()

    def index_at(self, t):
        return (self.ts == t)

    def _save_or_return(self, df, inplace):
        if df is None:
            df = InstantaneousWDF(columns=self.columns)

        if inplace and df is not self:
                return self._update_inplace(df._data)
        else:
            return (df.copy() if df is self else df)

    def _union_by_key(self, df, on_column):
        if df is None:
            df = self
        else:
            df = self.append(df, ignore_index=True, sort=False)
        columns = on_column + ['ts', 'w']
        data = Counter()
        for key in df[columns].itertuples(index=False, name=None, weights=True):
            data[key[:-1]] += key[-1]

        return self.__class__(list(key + (w, ) for key, w in iteritems(data)), columns=columns)        

    def _union_on_key(self, df, on_column):
        df = (self.__class__(df) if not isinstance(df, self.__class__) else df)
        df_d = {ts: w for ts, w in df.itertuples(weights=True)}

        data, columns = defaultdict(Counter), on_column + ['ts']
        if len(df_d):
            for key in set(key[:-1] for key in self[columns].itertuples(name=None, index=False)):
                for ts, w in iteritems(df_d):
                    data[key][ts] += w
        columns += ['w']

        for key in self[columns].itertuples(index=False, name=None, weights=True):
            ts, w = key[-2:]
            data[key[:-2]][ts] += w

        return self.__class__(list(key + (ts, w) for key, ct in iteritems(data) for ts, w in iteritems(ct)), columns=columns)

    def merge(self, inplace=False):
        return self._save_or_return(self._union_by_key(None, self.get_ni_columns(None)), inplace)

    def union(self, df, on_column=None, by_key=True, inplace=False):
        if df.empty:
            return self._save_or_return(self, inplace)
        
        on_column = self.get_ni_columns(on_column)
        if not len(on_column) or by_key:
            df = self._union_by_key(df, on_column)
        else:
            df = self._union_on_key(df, on_column)
        return self._save_or_return(df, inplace)

    def _intersect_by_key(self, df, on_column):
        if not isinstance(df, self.__class__):
            df = self.__class__(df)

        columns = on_column + ['ts', 'w']
        data = Counter()
        for key in self[columns].itertuples(index=False, name=None, weights=True):
            data[key[:-1]] += key[-1]
        data_it = Counter()
        for key in df[columns].itertuples(index=False, name=None, weights=True):
            k, w = key[:-1], key[-1]
            wb = data[k]
            if wb > 0:
                if data_it[k] > 0:
                    data_it[k] += w
                else:
                    data_it[k] += wb + w
        return self.__class__(list(key + (w, ) for key, w in iteritems(data_it)), columns=columns)

    def _intersect_on_key_weighted(self, df, on_column):
        data, columns = Counter(), on_column + ['ts', 'w']
        for key in self[columns].itertuples(index=False, name=None, weights=True):
            k, w = key[:-1], key[-1]
            if k[-1] in df:
                data[k] += (w if data[k] > .0 else w + df[k[-1]])
        return self.__class__(list(key + (w, ) for key, w in iteritems(data)), columns=columns)

    def _intersect_on_key_unweighted(self, df, on_column):
        data, columns = Counter(), on_column + ['ts', 'w']
        for key in self[columns].itertuples(index=False, name=None, weights=True):
            k, w = key[:-1], key[-1]
            if k[-1] in df:
                data[k] += w
        return self.__class__(list(key + (w, ) for key, w in iteritems(data)), columns=columns)

    def intersect(self, df, on_column=None, by_key=True, inplace=False):
        assert not (not by_key and df is None)

        on_column = self.get_ni_columns(on_column)
        if not len(on_column) or by_key:
            df = (None if df.empty else self._intersect_by_key(df, on_column))
        else:
            weighted = False
            if isinstance(df, pd.DataFrame):
                if 'w' in df.columns:
                    ts = dict(pd.DataFrame(df[['ts', 'w']]).itertuples(index=False, name=None))
                    weighted = True
                else:
                    ts = set(df.ts.values.flat)
            else:
                ts = list(df)
                if any(not isinstance(t, (int, Real)) for t in ts):
                    weighted = True
                    ts = dict(t for t in ts)
                else:
                    ts = set(ts)
                    
            if weighted:
                df = self._intersect_on_key_weighted(ts, on_column) if len(ts) else None
            else:
                df = self._intersect_on_key_unweighted(ts, on_column) if len(ts) else None
        return self._save_or_return(df, inplace)

    def _difference_by_key(self, df, on_column):
        columns = on_column + ['ts']
        data_diff, b_keys = Counter(), set(key for key in df[columns].itertuples(name=None, index=False))
        columns += ['w']
        for key in self.itertuples(index=False, name=None, weights=True):
            k, w = key[:-1], key[-1]
            if k not in b_keys:
                data_diff[k] += w
        return self.__class__(list(k + (w, ) for k, w in iteritems(data_diff)), columns=columns)

    def _difference_on_key(self, df, on_column):
        data, columns = Counter(), on_column + ['ts', 'w']
        for key in self.itertuples(index=False, name=None, weights=True):
            k, w = key[:-1], key[-1]
            if k[-1] not in df:
                data[k] += w
        return self.__class__(list(key + (w, ) for key, w in iteritems(data)), columns=columns)

    def difference(self, dfb, on_column=None, by_key=True, inplace=False):
        if self.empty or dfb.empty:
            return self._save_or_return(self, inplace)
        on_column = self.get_ni_columns(on_column)
        if not len(on_column) or by_key:
            df = self._difference_by_key(dfb, on_column)
        else:
            if isinstance(dfb, pd.DataFrame):
                ts = set(dfb.ts.values.flat)
            else:
                ts = set((t[0] if isinstance(t, (tuple, list)) else t) for t in dfb)
            df = self._difference_on_key(ts, on_column)
        return self._save_or_return(df, inplace)

    def gby_format(self):
        ndf = self.reset_index()
        return ndf[[col for col in ndf.columns if not (len(col) > 6 and col[:5]=='level')]]

    def issuper(self, dfb, on_column=None, by_key=True):
        if 'w' in dfb.columns:
            return self.drop(columns='w').issuper(dfb.drop(columns='w'), on_column=on_column, by_key=by_key)
        else:
            return self.drop(columns='w').issuper(dfb, on_column=on_column, by_key=by_key)

    def nonempty_intersection(self, bdf, on_column="u", by_key=True):
        if w in dfb.columns:
            return self.drop(columns='w').nonempty_intersection(dfb.drop(columns='w'), on_column=on_column, by_key=by_key)
        else:
            return self.drop(columns='w').nonempty_intersection(dfb, on_column=on_column, by_key=by_key)

    def cartesian_intersect(self, base_df):
        times = defaultdict(set)
        for u, ts in base_df[['u', 'ts']].itertuples(index=False, name=None):
            times[u].add(ts)

        data, set_times_nodes = Counter(), set(times.keys())
        df = self[self.u.isin(set_times_nodes) & self.v.isin(set_times_nodes)]
        for u, v, ts, w in df.itertuples(index=False, name=None, weights=True):
            if ts in times[u] and ts in times[v]:
                data[(u, v, ts)] += w

        return self.__class__(list(key + (w, ) for key, w in iteritems(data)), columns=['u', 'v', 'ts', 'w'])

    def map_intersect(self, base_df):
        return self.drop(columns='w').map_intersect(base_df)

    def interval_intersection_size(self, b=None, discrete=False):
        if w in dfb.columns:
            return self.drop(columns='w').interval_intersection_size(b.drop(columns='w'), discrete=discrete)
        else:
            return self.drop(columns='w').interval_intersection_size(b, discrete=discrete)
