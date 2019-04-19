import pandas as pd
import numpy as np
from itertools import chain
from six import iteritems
from six import string_types
from collections import Iterable
from collections import Counter
from collections import defaultdict
from .instantaneous_df import InstantaneousDF
from .instantaneous_df import InstantaneousWDF

class IntervalDF(pd.DataFrame):
    def __init__(self, *args, **kargs):
        super(IntervalDF, self).__init__(*args, **kargs)
        assert 'ts' in self.columns
        if 'tf' not in self.columns:
            self['tf'] = self['ts']
        if len(args) and isinstance(args[0], IntervalWDF):
            self.drop(columns=['w'])

    def copy(self, *args, **kargs):
        return IntervalDF(super(IntervalDF, self).copy(*args, **kargs))

    def drop(self, *args, **kargs):
        out = super(IntervalDF, self).drop(*args, **kargs)
        if isinstance(out, pd.DataFrame):
            if 'ts' in out.columns:
                if 'tf' in out.columns:
                    out = IntervalDF(out)
                else:
                    out = InstantaneousDF(out)
        return out

    def itertuples(self, index=False, name=None):
        columns = sorted(list(set(self.columns) - {'ts', 'tf'})) + ['ts', 'tf']
        return super(IntervalDF, self).reindex(columns=columns).itertuples(index=index, name=name)

    def __getitem__(self, index):
        out = super(IntervalDF, self).__getitem__(index)
        if isinstance(out, pd.DataFrame):
            if 'ts' in out.columns and 'tf' in out.columns:
                out = IntervalDF(out)
        return out

    def get_ni_columns(self, on_column):
        if on_column is None:
            columns = self.columns
        elif (not isinstance(on_column, Iterable) or isinstance(on_column, string_types)) and on_column in self.columns:
            columns = [c]
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
        return dfp.append(dfpv, ignore_index=True, sort=False).sort_values(by=['t', 'start'])

    def measure_time(self, discrete=False):
        if discrete:
            return (self.tf - self.ts + 1).sum()
        else:
            return (self.tf - self.ts).sum()

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

    def _save_or_return(self, df, inplace):
        if df is None:
            df = self.IntervalDF(columns=self.columns)

        if inplace and df is not self:
            return self._update_inplace(df._data)
        else:
            return (df.copy() if df is self else df)  

    def merge(self, inplace=False):
        on_column = self.get_ni_columns(None)
        if not len(on_column):
            df = merge_intervals_base(self)
        else:
            df = self.groupby(on_column).apply(merge_intervals_base).gby_format()
        return self._save_or_return(df, inplace)

    def union(self, df=None, on_column=None, by_key=True, inplace=False):#supper slow
        if df.empty:
            return self._save_or_return(self, inplace)

        assert not (not by_key and df is None)        
        on_column = self.get_ni_columns(on_column)

        if not len(on_column):
            df = merge_intervals_base(self.append(df, ignore_index=True, sort=False))
        elif by_key:
            df = self.append(df, ignore_index=True, sort=False).groupby(on_column).apply(merge_intervals_base).gby_format()
        else:
            def append_merge(dfa, dfb):
                return merge_intervals_base(dfa.append(dfb, ignore_index=True, sort=False))
            df = self.groupby(on_column).apply(append_merge, df).gby_format()
        return self._save_or_return(df, inplace)

    def intersect(self, df=None, on_column=None, by_key=True, inplace=False):
        if df is None or df.empty:
            return self._save_or_return(None, inplace)

        assert not (not by_key and df is None)
        on_column = self.get_ni_columns(on_column)

        if not len(on_column):
            df = intersect_intervals_base(self.append(df, ignore_index=True, sort=False))
        elif by_key:
            df = self.append(df, ignore_index=True, sort=False).groupby(on_column).apply(intersect_intervals_base).gby_format()   
        else:
            def append_intersect(dfa, dfb):
                return intersect_intervals_base(dfa.append(dfb, ignore_index=True, sort=False))
            df = self.groupby(on_column).apply(append_intersect, df).gby_format()     
        return self._save_or_return(df, inplace)

    def difference(self, dfb, on_column=None, by_key=True, inplace=False):
        if self.empty or dfb.empty:
            return self._save_or_return(self, inplace)

        on_column = self.get_ni_columns(on_column)
        if not len(on_column):
            df = difference_base(self, dfb)
        elif by_key:
            nr, nb = self.shape[0], dfb.shape[0]
            mdf = self.append(dfb, ignore_index=True, sort=False)
            mdf['r'] = np.concatenate((np.full((nr,), True), np.full((nb,), False)))

            def trans(df):
                tr, tb = df[df.r], df[~df.r]
                if tr.empty or tb.empty:
                    return tr
                return difference_base(tr, tb)

            df = mdf.groupby(on_column).apply(trans).gby_format()
        else:
            df = self.groupby(on_column).apply(difference_base, dfb).gby_format() 
        return self._save_or_return(df, inplace)

    def gby_format(self):
        ndf = self.reset_index()
        return ndf[[col for col in ndf.columns if not (len(col) > 6 and col[:5]=='level')]]

    def issuper(self, dfb, on_column=None, by_key=True):
        on_column = self.get_ni_columns(on_column)
        if on_column is None:
            return issuper_base(self, dfb)
        elif by_key:
            dfr = self
            if not isinstance(on_column, list):
                on_column = ['on_column']
            for col in on_column:
                    dfr = dfr[dfr[col].isin(dfb[col])]
            if dfr.empty:
                return False
            common_df = dfr.append(dfb, ignore_index=True, sort=False)
            common_df['r'] = np.concatenate((np.full((dfr.shape[0],), True),
                                             np.full((dfb.shape[0],), False)))
            def split_issuper(df):
                tr, tb = df[df.r], df[~df.r]
                return issuper_base(tr, tb)      
            return common_df.groupby(on_column).apply(split_issuper).all()
        else:
            return self.groupby(on_column).apply(issuper_base, dfb).all()

    def nonempty_intersection(self, bdf, on_column="u", by_key=True):
        on_column = self.get_ni_columns(on_column)
        if on_column is None:
            return nonempty_intersection_base(self, dfb)
        elif by_key:
            common_df = dfr.append(dfb, ignore_index=True, sort=False)
            common_df['r'] = np.concatenate((np.full((dfr.shape[0],), True),
                                             np.full((dfb.shape[0],), False)))
            def split_nonempty_intersection(df):
                tr, tb = df[df.r], df[~df.r]
                return nonempty_intersection_base(tr, tb)
            return common_df.groupby(on_column).apply(split_nonempty_intersection).all()
        else:
            def append_nei(dfa, dfb):
                return not dfa.empty and nonempty_intersection_base(dfa[['ts', 'tf']].append(dfb, ignore_index=True, sort=False))
            return self.groupby(on_column).apply(nonempty_intersection_base, bdf).all()

    def cartesian_intersect(self, base_df):
        base_df = base_df.set_index('u')
        df = self[self.u.isin(base_df.index) & self.v.isin(base_df.index)]
        return df.groupby(['u', 'v']).apply(cartesian_intersect_base, base_df).gby_format()

    def map_intersect(self, base_df):
        base_df = base_df.set_index('u')
        df = self[self.u.isin(base_df.index)]
        return df.groupby(['u', 'v']).apply(map_intersect_base, base_df).gby_format().drop(columns=['u']).rename(columns={'v': 'u'})

    def interval_intersection_size(self, b=None, discrete=False):
        a = self
        tsa, tfa, la = a.ts.values, a.tf.values, a.shape[0]
        if b is None:
            tsb, tfb, lb = tsa, tfa, la
        else:
            tsb, tfb, lb = b.ts.values, b.tf.values, b.shape[0]
        dt = np.concatenate((tsa, tsb, tfa, tfb))
        tap = np.concatenate((np.full((la,), -2), np.full((lb,), -1), np.full((la,), 1), np.full((lb,), 2)))
        tbp = np.concatenate((np.full((la,), -1), np.full((lb,), 0), np.full((la,), 0), np.full((lb,), 1)))
        index = np.lexsort((tap, dt))
        tap[:la] = 0
        tap[-lb:] = 0
        cnt_a, cnt_b = np.cumsum(tap[index]), np.cumsum(tbp[index])
        d = np.diff(dt[index])
        if discrete:
            cnt = (d * cnt_a[1:] * cnt_b[1:])
            return -np.sum(cnt) + (cnt < 0).sum()
        else:
            return -np.sum((d * cnt_a[1:] * cnt_b[1:]))

def difference_base(tr, tb):
    tb = tb[(tb.ts != tb.tf)]
    if tb.empty:
        return IntervalDF({'ts': tr.ts.values, 'tf': tr.tf.values})
    elif not tr.empty:
        tsr, tsb, tfr, tfb = tr.ts.values, tb.ts.values, tr.tf.values, tb.tf.values
        lr, lb = tsr.shape[0], tsb.shape[0]
        dt = np.concatenate((tsr, tsb, tfb, tfr))
        tii = np.concatenate((np.full((lr,), -1), np.full((lb,), -1), np.full((lb,), 1), np.full((lr,), 1)))
        tbp = np.concatenate((np.full((lr,), False), np.full((lb,), True), np.full((lb,), True), np.full((lr,), False)))
        index = np.lexsort((tii, tbp, dt))
        cr, dts, tbps = np.cumsum(tii[index]), dt[index], tbp[index]
        idxn = np.where(((cr[1:] == -2) & tbps[1:]) |
                        ((cr[1:] == 0) & ~tbps[1:]))[0] #(trps[:-1] == trps[1:])
        if idxn.size:
            df = pd.DataFrame({'ts': dts[idxn], 'tf': dts[idxn+1]})
            return IntervalDF(df[df.ts != df.tf])
    else:
        return IntervalDF(columns=['ts', 'tf'])

def map_intersect_base(df, base):
    u, _ = df.name
    dfu = base.loc[u, ['ts', 'tf']]
    df = intersect_intervals_base(df.append(dfu, ignore_index=True, sort=True))
    if not df.empty:
        return df

def merge_intervals_base(df):
    if not df.empty:
        a = np.concatenate((df.ts.values, df.tf.values))
        b = np.concatenate((np.full((df.shape[0],), fill_value=-1), np.full((df.shape[0],), fill_value=+1)))
        idx = np.lexsort((b, a))
        cs = np.cumsum(b[idx])
        fi = np.where(cs == 0)[0]
        if len(fi):
            ii = np.concatenate(([0], fi[:-1] + 1))
        else:
            ii = np.array([0])
        aidx = a[idx]
        return IntervalDF({'ts': aidx[ii], 'tf': aidx[fi]})
    else:
        return df

def intersect_intervals_base(df):
    # Assumes that intervals with common elements come from different dataframes
    if not df.empty:
        a = np.concatenate((df.ts.values, df.tf.values))
        b = np.concatenate((np.full((df.shape[0],), fill_value=-1), np.full((df.shape[0],), fill_value=+1)))
        idx = np.lexsort((b, a))
        cs = np.cumsum(b[idx])
        iidx = np.where((cs[1:] == -1) & (cs[:-1] == -2))[0]
        if iidx.size:
            sa = a[idx]
            return IntervalDF({'ts': sa[iidx], 'tf': sa[iidx+1]})
    return IntervalDF(columns=['ts', 'tf'])
    
def issuper_base(tr, tb):
    if tr.empty or tb.empty:
        return False
    tsr, tsb, tfr, tfb = tr.ts.values, tb.ts.values, tr.tf.values, tb.tf.values
    lr, lb = tsr.shape[0], tsb.shape[0]
    dt = np.concatenate((tsr, tsb, tfb, tfr))
    tbp = np.concatenate((np.full((lr,), -2), np.full((lb,), -1), np.full((lb,), 1), np.full((lr,), 2)))
    trp = np.concatenate((np.full((lr,), -1), np.full((lb,), 0), np.full((lb,), 0), np.full((lr,), 1)))
    index = np.lexsort((tbp, dt))
    tbp[:lr], tbp[-lr:] = 0, 0
    cr, cb = np.cumsum(trp[index]), np.cumsum(tbp[index])
    return np.all((cr != 0) | (cb == 0))

def nonempty_intersection_base(df):
    if not df.empty:
        a = np.concatenate((df.ts.values, df.tf.values))
        b = np.concatenate((np.full((df.shape[0],), fill_value=-1), np.full((df.shape[0],), fill_value=+1)))
        idx = np.lexsort((b, a))
        cs = np.cumsum(b[idx])
        return np.any((cs[1:] == -1) & (cs[:-1] == -2))
    else:
        return False

def cartesian_intersect_base(df, base):
    u, v = df.name
    dfu = base.loc[u, ['ts', 'tf']]
    dfv = base.loc[v, ['ts', 'tf']]
    dft = df[['ts', 'tf']]
    dfi = intersect_intervals_base(dft.append(dfu, ignore_index=True, sort=True))
    df = intersect_intervals_base(dfi.append(dfv, ignore_index=True, sort=True))
    if not df.empty:
        return df

def _events_not_sorted(df, weighted=False):
    cw = (['w'] if weighted else [])
    columns = sorted(list(set(df.columns) - ({'ts', 'tf'} | set(cw))))    
    dfp = df[columns + ['ts'] + cw].rename(columns={"ts": "t"})
    dfp['start'] = True
    dfpv = df[columns + ['tf'] + cw].rename(columns={"tf": "t"})
    dfpv['start'] = False
    return dfp.append(dfpv, ignore_index=True, sort=False)


class IntervalWDF(pd.DataFrame):
    def __init__(self, *args, **kargs):
        super(IntervalWDF, self).__init__(*args, **kargs)
        assert 'ts' in self.columns
        if 'tf' not in self.columns:
            self['tf'] = self['ts']
        if 'w' not in self.columns:
            self['w'] = 1

    def copy(self, *args, **kargs):
        return IntervalWDF(super(IntervalWDF, self).copy(*args, **kargs))

    def drop(self, *args, **kargs):
        out = super(IntervalWDF, self).drop(*args, **kargs)
        if isinstance(out, pd.DataFrame):
            if 'ts' in out.columns:
                if 'tf' in out.columns:
                    if 'w' in out.columns:
                        out = IntervalWDF(out)
                    else:
                        out = IntervalDF(out)
                else:
                    if 'w' in out.columns:
                        out = InstantaneousWDF(out)
                    else:
                        out = InstantaneousDF(out)
        return out

    def itertuples(self, index=False, name=None, weights=False):
        if weights:
            columns = sorted(list(set(self.columns) - {'ts', 'tf', 'w'})) + ['ts', 'tf', 'w']
        else:
            columns = sorted(list(set(self.columns) - {'ts', 'tf', 'w'})) + ['ts', 'tf']
        return super(IntervalWDF, self).reindex(columns=columns).itertuples(index=index, name=name)

    def __getitem__(self, index):
        out = super(IntervalWDF, self).__getitem__(index)
        if isinstance(out, pd.DataFrame):
            if 'ts' in out.columns and 'tf' in out.columns:
                if 'w' in out.columns:
                    out = IntervalWDF(out)
                else:
                    out = IntervalDF(out)
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
            columns = [c]
        elif isinstance(on_column, Iterable):
            columns = list(c for c in on_column)
        cols = sorted(list(set(c for c in columns) - {'ts', 'tf', 'w'}))
        if on_column is not None:
            assert all(c in self.columns for c in cols)
        return cols

    @property
    def events(self):
        return _events_not_sorted(self, True).sort_values(by=['t', 'start'])

    def measure_time(self, discrete=False):
        if discrete:
            return (self.tf - self.ts + 1).sum()
        else:
            return (self.tf - self.ts).sum()

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
        return (self.ts <= t) & (self.tf >= t)

    def index_at_interval(self, ts, tf):
        assert ts <= tf
        return (self.ts <= ts) & (self.tf >= tf)

    def _save_or_return(self, df, inplace):
        if df is None:
            df = self.IntervalDF(columns=self.columns)

        if inplace and df is not self:
            return self._update_inplace(df._data)
        else:
            return (df.copy() if df is self else df)

    def _union_time_only(self, df=None):
        if df is None:
            df = self 
        else:
            df = self.append(df, ignore_index=True, sort=False)
        events = df.events[['t', 'start', 'w']]
        e, _, w_current = events.iloc[0]
        p = None
        for t, f, w in events.iloc[1:]:
            if t > e:
                if w_current != 0:
                    if p is not None and out[p] == w_current:
                        out[p] = out[p][0] + (t, w_current)
                    else:
                        p = len(out)
                        out.append((e, t, w_current))
                e = t
            wcurrent += (w if f else -w)
        return self.__class__(out, columns=['ts', 'tf', 'w'])

    def _union_by_key(self, df, on_column):
        if df is None:
            df = self 
        else:
            df = self.append(df, ignore_index=True, sort=False)
        w_current, e, prev, out = defaultdict(float), dict(), dict(), list()
        for col in df.events[['t', 'start', 'w'] + on_column].itertuples(index=False, name=None):
            t, f, w = col[:3]
            key = col[3:]
            if key not in e:
                e[key] = t
            elif t > e[key]:
                wck = w_current[key]
                if wck != 0:
                    pk = prev.get(key, None)
                    if pk is not None and out[pk][-1] == wck:
                        out[pk] = out[pk][:-2] + (t, wck)
                    else:
                        prev[key] = len(out)
                        out.append(key + (e[key], t, wck))
                else:
                    prev.pop(key, None)
                e[key] = t
            w_current[key] += (w if f else -w)
        return self.__class__(out, columns=(on_column + ['ts', 'tf', 'w']))

    def merge(self, inplace=False):
        on_column = self.get_ni_columns(None)
        if not len(on_column):
            df = self._union_time_only()
        else:
            df = self._union_by_key(None, on_column)
        return self._save_or_return(df, inplace)

    def _union_on_key(self, df, on_column):
        df = (self.__class__(df) if not isinstance(df, self.__class__) else df)
           
        # Extract information and possible keys
        iter_ = list((False, ) + x for x in _events_not_sorted(self, True)[['t', 'start'] + on_column + ['w']].itertuples(index=False, name=None))
        all_keys = list(set(tup[3:-1] for tup in iter_))
        iter_ += list((True, ) + x for x in _events_not_sorted(df, False)[['t', 'start', 'w']].itertuples(index=False, name=None))

        # Sort List
        def order(x):
            return (x[1], x[2], x[0])

        w_current, e, prev, out = defaultdict(float), defaultdict(float), dict(), list()
        b_area, et = False, tuple()
        for col in sorted(iter_, key=order):
            _, t, f = col[:3]
            k, w = col[3:-1], col[-1]
            keys = ([k] if len(k) else all_keys)
            for key in keys:
                w_total = w_current[key] + w_current[et]
                pk = prev.get(key, None)
                if t > e[key]:
                    if w_total != .0:
                        if pk is not None and out[pk][-1] == w_total:
                            out[pk] = out[pk][:-2] + (t, w_total)
                        else:
                            prev[key] = len(out)
                            out.append(key + (e[key], t, w_total))
                    else:
                        prev.pop(key, None)
                e[key] = t
            w_current[k] += (w if f else -w)
        return self.__class__(out, columns=(on_column + ['ts', 'tf', 'w']))

    def union(self, df, on_column=None, by_key=True, inplace=False):
        if df.empty:
            return self._save_or_return(self, inplace)

        assert not (not by_key and df is None)

        on_column = self.get_ni_columns(on_column)
        if not len(on_column):
            df = self._union_time_only(df)
        elif by_key:
            df = self._union_by_key(df, on_column)
        else:
            df = self._union_on_key(df, on_column)
        return self._save_or_return(df, inplace)

    def _intersect_time_only(self, dfb):
        df = self.append(dfb, ignore_index=True, sort=False)
        df['r'] = np.concatenate((np.full((self.shape[0],), True), np.full((dfb.shape[0],), False)))

        # Intersect
        events = df.events[['t', 'start', 'r', 'w']]
        e, f, w, r = events.iloc[0]
        w_current = (set(), set())
        w_current[int(r)].add(w)
        prev = None
        for t, f, w, r in events.iloc[1:]:
            if t > e:
                if all(len(w) for w in w_current):
                    wc = sum(w_current[0]) + sum(w_current[1])
                    if prev is not None and out[prev][-1] == wc:
                        out[prev] = (out[prev][0], t, wc)
                    else:
                        prev = len(out)
                        out.append((e, t, wc))
                else:
                    prev = None
                e = t
            if f:
                # start
                w_current[int(r)].add(w)
            else:
                w_current[int(r)].remove(w)
        return self.__class__(out, columns=['ts', 'tf', 'w'])

    def _intersect_by_key(self, dfb, on_column):
        df = self.append(dfb, ignore_index=True, sort=False)
        df['r'] = np.concatenate((np.full((self.shape[0],), True), np.full((dfb.shape[0],), False)))

        def constr():
            return (set(), set())

        # Intersect
        w_current, e, prev, out = defaultdict(constr), dict(), dict(), list()

        for col in df.events[['t', 'start', 'w', 'r'] + on_column].itertuples(index=False, name=None):
            t, f, w, r = col[:4]
            key = col[4:]
            if key not in e:
                e[key] = t
            elif t > e[key]:
                if all(len(w) for w in w_current[key]):
                    wa, wb = w_current[key]
                    wc = sum(wa) + sum(wb)
                    pk = prev.get(key, None)
                    if pk is not None and out[pk][-1] == wc:
                        out[prev] = out[prev:-2] + (t, wc)
                    else:
                        prev[key] = len(out)
                        out.append(key + (e[key], t, wc))
                else:
                    prev.pop(key, None)
                e[key] = t
            if f:
                # start
                w_current[key][int(r)].add(w)
            else:
                w_current[key][int(r)].remove(w)
        return self.__class__(out, columns=(on_column + ['ts', 'tf', 'w']))

    def _intersect_on_key(self, dfb, on_column):
        w_current, e, prev, out = dict(), dict(), dict(), list()
        iter_ = list((True, ) + x for x in _events_not_sorted(self, True)[['t', 'start'] + on_column + ['w']].itertuples(index=False, name=None))
        if 'w' in dfb:
            iter_ += list((False, ) + x for x in _events_not_sorted(dfb, False)[['t', 'start', 'w']].itertuples(index=False, name=None))
        else:
            iter_ += list((False, ) + x for x in _events_not_sorted(dfb, False)[['t', 'start']].itertuples(index=False, name=None))
        def get(x):
            return (x[1], x[2], x[0])

        b_area, wb = False, 0.
        for col in sorted(iter_, key=get):
            _, t, f = col[:3]
            key = col[3:]
            if len(key) > 1:
                w, key = key[-1], key[:-1]
                wc = w_current.pop(key, .0)
                if key in e and t > e[key] and b_area:
                    wck = wc + wb
                    if wck != 0:
                        pk = prev.get(key, None)
                        if pk is not None and out[pk][-1] == wck:
                            out[prev] = out[prev:-2] + (t, wck)
                        else:
                            prev[key] = len(out)
                            out.append(key + (e[key], t, wck))
                    else:
                        prev.pop(key, None)
                e[key] = t
                wc += (w if f else -w)
                if wc != .0:
                    w_current[key] = wc                        
            else:
                w, b_area = (key[0] if len(key) else 0), f
                if not f:
                    for k, w in iteritems(w_current):
                        if t > e[k]:
                            wck = w + wb
                            if wck != 0:
                                pk = prev.get(key, None)
                                if pk is not None and out[pk][-1] == wck:
                                    out[prev] = out[prev:-2] + (t, wck)
                                else:
                                    prev[key] = len(out)
                                    out.append(key + (e[key], t, wck))
                            else:
                                prev.pop(key, None)
                            e[k] = t
                else:
                    for k in w_current.keys():
                        e[k] = t                    
                wb += (w if f else -w)
                
        return self.__class__(out, columns=(on_column + ['ts', 'tf', 'w']))

    def intersect(self, dfb, on_column=None, by_key=True, inplace=False):
        if dfb.empty:
            return self._save_or_return(None, inplace)

        assert not (not by_key and dfb is None)
        on_column = self.get_ni_columns(on_column)
        if not len(on_column):
            df = self._intersect_time_only(dfb)
        elif by_key:
            df = self._intersect_by_key(dfb, on_column)
        else:
            df = self._intersect_on_key(dfb, on_column)
        return self._save_or_return(df, inplace)

    def _difference_time_only(self, dfb):
        # Difference
        df = self.append(dfb, ignore_index=True, sort=False)
        df['r'] = np.concatenate((np.full((self.shape[0],), False), np.full((dfb.shape[0],), True)))

        events = df.events[['t', 'start', 'r', 'w']]            
        e, f, w, r = events.iloc[0]
        b_area = r
        for t, f, r, w in events.iloc[1:]:
            if t > e and not b_area and w_current != 0:
                out.append((e, t, w_current))
            if r:
                b_area = f
            else:
                w_current += (w if f else -w)
            e = t
        return self.__class__(out, columns=['ts', 'tf', 'w'])  

    def _difference_by_key(self, dfb, on_column):
        df = self.append(dfb, ignore_index=True, sort=False)
        df['r'] = np.concatenate((np.full((self.shape[0],), False), np.full((dfb.shape[0],), True)))

        w_current, e, out, b_area = defaultdict(float), dict(), list(), defaultdict(bool)
        for col in df.events[['t', 'start', 'r', 'w'] + on_column].itertuples(index=False, name=None):
            t, f, r, w = col[:4]
            key = col[4:]
            if key in e and t > e[key] and not b_area[key] and w_current[key] != 0:
                out.append(key + (e[key], t, w_current[key]))
            if r:
                b_area[key] = f
            else:
                w_current[key] += (w if f else -w)
            e[key] = t
        return self.__class__(out, columns=(on_column + ['ts', 'tf', 'w']))

    def _difference_on_key(self, dfb, on_column):
        w_current, e, out = dict(), dict(), list()
        iter_ = list((True, ) + x for x in _events_not_sorted(self, True)[['t', 'start'] + on_column + ['w']].itertuples(index=False, name=None))
        iter_ += list((False, ) + x for x in _events_not_sorted(dfb, True)[['t', 'start']].itertuples(index=False, name=None))
        def key(x):
            return (x[1], x[2], x[0])
        b_area = False
        for col in sorted(iter_, key=key):
            _, t, f = col[:3]
            key = col[3:]
            if len(key):
                w = key[-1]
                key = key[:-1]
                if key not in e:
                    e[key] = t
                elif t > e[key] and w_current[key] != 0 and b_area and len(key):
                    out.append(key + (e[key], t, w_current[key]))
                e[key] = t
                wc = w_current.pop(key, .0) + (w if f else -w)
                if wc != .0:
                    w_current[key] = wc    
            else:
                if f:
                    for k, w in iteritems(w_current):
                        if t > e[k]:
                            out.append(k + (e[k], t, w))
                            e[k] = t
                else:
                    for k in w_current.keys():
                        e[k] = t
                b_area = not f
        return self.__class__(out, columns=(on_column + ['ts', 'tf', 'w']))

    def difference(self, dfb, on_column=None, by_key=True, inplace=False):
        if self.empty or dfb.empty:
            return self._save_or_return(self, inplace)

        on_column = self.get_ni_columns(on_column)
        if not len(on_column):
            return self._difference_time_only(dfb)
        elif by_key:
            return self._difference_by_key(dfb, on_column)
        else:
            return self._difference_on_key(dfb, on_column)

        return self._save_or_return(df, inplace)

    def gby_format(self):
        ndf = self.reset_index()
        return ndf[[col for col in ndf.columns if not (len(col) > 6 and col[:5]=='level')]]

    def issuper(self, dfb, on_column=None, by_key=True):
        if 'w' in dfb.columns:
            return self.drop(columns='w').issuper(dfb.drop(columns='w'), on_column=on_column, by_key=by_key)
        else:
            return self.drop(columns='w').issuper(dfb, on_column=on_column, by_key=by_key)

    def nonempty_intersection(self, dfb, on_column="u", by_key=True):
        if 'w' in dfb.columns:
            return self.drop(columns='w').nonempty_intersection(dfb.drop(columns='w'), on_column=on_column, by_key=by_key)
        else:
            return self.drop(columns='w').nonempty_intersection(dfb, on_column=on_column, by_key=by_key)

    def cartesian_intersect(self, base_df):
        assert set(self.columns) == {'u', 'v', 'ts', 'tf', 'w'} and set(base_df.columns) == {'u', 'ts', 'tf'}
    
        u_set = set(base_df[['u']].values.flat)
        df = self[self.u.isin(u_set) & self.v.isin(u_set)]

        # Intersect
        columns = sorted(list(set(self.columns) - {'ts', 'tf'}))
        w_current, e, b_area = defaultdict(dict), dict(), defaultdict(bool)
        iter_a = ((True, ) + x for x in _events_not_sorted(df, True)[['t', 'start', 'u', 'v', 'w']].itertuples(index=False, name=None))
        iter_b = ((False, ) + x  for x in _events_not_sorted(base_df, False)[['t', 'start', 'u']].itertuples(index=False, name=None))
        iter_ = list(chain(iter_a, iter_b))
        def key(x):
            return (x[1], not x[0], not x[2])

        out = list()
        for col in sorted(iter_, key=key):
            _, t, f = col[:3]
            key = col[3:]
            if len(key) == 3:
                u, v, w = key
                wc = w_current[u].pop(v, .0)
                if (u, v) in e and t > e[(u, v)] and wc != 0 and b_area[u] and b_area[v]:
                    out.append((u, v, e[(u, v)], t, wc))
                e[(u, v)] = t
                wc += (w if f else -w)
                if wc != .0:
                    w_current[u][v] = wc
            else:
                u = key[0]
                if not f:
                    for v, w in iteritems(w_current[u]):
                        if b_area[v]:
                            out.append((u, v, e[(u, v)], t, w))
                            out.append((v, u, e[(v, u)], t, w))
                            e[(u, v)] = t
                b_area[u] = f            
        return IntervalWDF(out, columns=['u', 'v', 'ts', 'tf', 'w'])

    def map_intersect(self, base_df):
        return self.drop(columns='w').map_intersect(base_df)

    def interval_intersection_size(self, b=None, discrete=False):
        if w in dfb.columns:
            return self.drop(columns='w').interval_intersection_size(b.drop(columns='w'), discrete=discrete)
        else:
            return self.drop(columns='w').interval_intersection_size(b, discrete=discrete)

