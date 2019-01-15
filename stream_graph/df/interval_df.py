import pandas as pd
import numpy as np
from collections import Iterable

class IntervalDF(pd.DataFrame):
    def __init__(self, *args, **kargs):
        super(IntervalDF, self).__init__(*args, **kargs)
        assert 'ts' in self.columns and 'tf' in self.columns

    def copy(self, *args, **kargs):
        return IntervalDF(super(IntervalDF, self).copy(*args, **kargs))

    def drop(self, *args, **kargs):
        out = super(IntervalDF, self).drop(*args, **kargs)
        if isinstance(out, pd.DataFrame):
            if 'ts' in out.columns and 'tf' in out.columns:
                out = IntervalDF(out)
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
            ret = list(set(self.columns) - {'ts', 'tf'})
            if len(ret):
                return ret
            return None
        elif isinstance(on_column, Iterable):
            assert all(c in self.columns for c in on_column)
            return on_column
        else:
            assert c in self.columns
            return on_column

    def measure_time(self):
        return (self.tf - self.ts).sum()

    def df_at(self, t):
        return self[self.index_at(t)]

    def df_at_interval(self, ts, tf):
        return self[self.index_at_interval(ts, tf)]

    def df_count_at(df, t):
        return self.index_at(t).sum()

    def index_at(self, t):
        return (self.ts <= t) & (self.tf >= t)

    def index_at_interval(self, ts, tf):
        assert ts <= tf
        return (self.ts <= ts) & (self.tf >= tf)

    def _save_or_return(self, df, inplace):
        if df is None:
            df = self.IntervalDF(columns=self.columns)

        if inplace:
            return self._update_inplace(df._data)
        else:
            return df        

    def union(self, df=None, on_column=None, by_key=True, inplace=False):#supper slow
        assert not (not by_key and df is None)
        
        on_column = self.get_ni_columns(on_column)
        if on_column is None:
            if df is None:
                df = merge_intervals_base(self)
            else:
                df = merge_intervals_base(self.append(df, ignore_index=True, sort=False))
        elif by_key:
            if df is None:
                df = self
            else:
                df = self.append(df, ignore_index=True, sort=False)
            df = df.groupby(on_column).apply(merge_intervals_base).gby_format()
        else:
            if df.empty:
                df = self
            else:
                def append_merge(dfa, dfb):
                    return merge_intervals_base(dfa.append(dfb, ignore_index=True, sort=False))
                df = self.groupby(on_column).apply(append_merge, df).gby_format()

        return self._save_or_return(df, inplace)

    def intersect(self, df=None, on_column=None, by_key=True, inplace=False):
        assert not (not by_key and df is None)

        on_column = self.get_ni_columns(on_column)
        if on_column is None:
            if df is None:
                df = intersect_intervals_base(self)
            else:
                df = intersect_intervals_base(self.append(df, ignore_index=True, sort=False))
        elif by_key:
            if df is None:
                df = self
            else:
                df = self.append(df, ignore_index=True, sort=False)
            df = df.groupby(on_column).apply(intersect_intervals_base).gby_format()   
        else:
            if df.empty:
                df = IntervalDF(columns=self.columns)
            else:
                def append_intersect(dfa, dfb):
                    return intersect_intervals_base(dfa.append(dfb, ignore_index=True, sort=False))
                df = self.groupby(on_column).apply(append_intersect, df).gby_format()     

        return self._save_or_return(df, inplace)

    def difference(self, dfb, on_column=None, by_key=True, inplace=False):
        if self.empty or dfb.empty:
            return self.copy()

        on_column = self.get_ni_columns(on_column)
        if on_column is None:
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
            if dfb.empty:
                df = None
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
            return self.groupby(on_column).apply(nonempty_intersection_base, dft).all()

    def cartesian_intersect(self, base_df):
        base_df = base_df.set_index('u')
        df = self[self.u.isin(base_df.index) & self.v.isin(base_df.index)]
        return df.groupby(['u', 'v']).apply(cartesian_intersect_base, base_df).gby_format()

    def map_intersect(self, base_df):
        base_df = base_df.set_index('u')
        df = self[self.u.isin(base_df.index)]
        return df.groupby(['u', 'v']).apply(map_intersect_base, base_df).gby_format().drop(columns=['u']).rename(columns={'v': 'u'})

    def interval_intersection_size(self, b=None):
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
        return -np.sum(d * cnt_a[1:] * cnt_b[1:])

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

