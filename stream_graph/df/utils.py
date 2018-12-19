"""A file containing utilities for dataframes representing intervals."""
import pandas as pd
import numpy as np

from stream_graph import API
from stream_graph.df.node_stream_df import NodeStreamDF
from stream_graph.df.time_set_df import TimeSetDF


def intersect_intervals(df):
    # Assumes that intervals with common elements come from different dataframes
    if not df.empty:
        a = np.concatenate((df.ts.values, df.tf.values))
        b = np.concatenate((np.full((df.shape[0],), fill_value=-1), np.full((df.shape[0],), fill_value=+1)))
        idx = np.lexsort((b, a))
        cs = np.cumsum(b[idx])
        iidx = np.where((cs[1:] == -1) & (cs[:-1] == -2))[0]
        if iidx.size:
            sa = a[idx]
            return pd.DataFrame({'ts': sa[iidx], 'tf': sa[iidx+1]})
    return pd.DataFrame(columns=['ts', 'tf'])


def intersect_intervals_b(df, dfb):
    # Assumes that intervals with common elements come from different dataframes
    if df.empty:
        return pd.DataFrame(columns=['ts', 'tf'])
    return intersect_intervals(df.append(dfb, ignore_index=True,  sort=False))


def intersect_intervals_df(df, on_column="u"):
    return gby_format(df.groupby(on_column).apply(intersect_intervals))


def intersect_intervals_with_df(df, dfb, on_column="u"):
    if dfb.empty or df.empty:
        return pd.DataFrame(columns=df.columns)
    return gby_format(df.groupby(on_column).apply(intersect_intervals_b, dfb))


def merge_intervals_b(df, bdf):
    if not df.empty:
        return merge_intervals(df[['ts', 'tf']].append(bdf[['ts', 'tf']], ignore_index=True, sort=False))
    else:
        return df


def merge_intervals_with_df(df, bdf, on_column="u"):
    return gby_format(df.groupby(on_column).apply(merge_intervals_b, bdf))


def merge_intervals(df):
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
        return pd.DataFrame({'ts': aidx[ii], 'tf': aidx[fi]})
    else:
        return df


def merge_intervals_df(df, on_column="u"):#supper slow
    return gby_format(df.groupby(on_column).apply(merge_intervals))


def interval_intersection_size(a, b=None):
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


def measure_time(df):
    return (df.tf - df.ts).sum()


def df_at(df, t):
    return df[df_index_at(df, t)]

def df_at_interval(df, ts, tf):
    return df[df_index_at_interval(df, ts, tf)]

def df_count_at(df, t):
    return df_index_at(df, t).sum()


def df_index_at(df, t):
    return (df.ts <= t) & (df.tf >= t)


def df_index_at_interval(df, ts, tf):
    assert ts <= tf
    return (df.ts <= ts) & (df.tf >= tf)


def df_fit_to_time_bounds(df, tmn, tmx):
    if df.empty:
        return df
    assert tmn <= tmx
    tdf = df[(df.tf > tmn) & (df.ts < tmx)].copy()
    tdf.ts.clip_lower(tmn, inplace=True)
    tdf.tf.clip_upper(tmx, inplace=True)
    return tdf


def issuper_df(dfr, dfb, on_column='u'):
    dfr = dfr[dfr.u.isin(dfb.u)]
    if dfr.empty:
        return False
    common_df = dfr.append(dfb, ignore_index=True, sort=True)
    common_df['r'] = np.concatenate((np.full((dfr.shape[0],), True),
                                     np.full((dfb.shape[0],), False)))
    return common_df.groupby(on_column).apply(issuper).all()


def common_time_with_df(df, bdf, on_column="u"):
    if bdf.empty:
        return pd.DataFrame(columns=on_column)
    df = common_df.groupby(on_column).apply(common_time, bdf).reset_index()
    return df[df[0]].drop(columns=[0])


def common_time(df, bdf):
    return not df.empty and common_time_b(df[['ts', 'tf']].append(bdf, ignore_index=True))


def common_time_b(df):
    if not df.empty:
        a = np.concatenate((df.ts.values, df.tf.values))
        b = np.concatenate((np.full((df.shape[0],), fill_value=-1), np.full((df.shape[0],), fill_value=+1)))
        idx = np.lexsort((b, a))
        cs = np.cumsum(b[idx])
        return np.any((cs[1:] == -1) & (cs[:-1] == -2))
    else:
        return False


def issuper_b(tr, tb):
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


def issuper(df):
    #import ipdb; ipdb.set_trace()
    tr, tb = df[df.r], df[~df.r]
    return issuper_b(tr, tb)


def issuper_with_df(dfr, dft, on_column='u'):
    if dft.empty:
        return True
    return dfr.groupby(on_column).apply(issuper_b, dft).all()


def cartesian_intersect(df, base):
    u, v = df.name
    dfu = base.loc[u, ['ts', 'tf']]
    dfv = base.loc[v, ['ts', 'tf']]
    dft = df[['ts', 'tf']]
    i1 = intersect_intervals(dft.append(dfu, ignore_index=True, sort=True))
    i2 = intersect_intervals(dft.append(dfv, ignore_index=True, sort=True))
    df = merge_intervals(i1.append(i2, ignore_index=True, sort=True))
    if not df.empty:
        return df


def difference(df):
    tr, tb = df[df.r], df[~df.r]
    if tr.empty or tb.empty:
        return tr
    return difference_b(tr, tb)

def difference_b(tr, tb):
    #import pdb; pdb.set_trace()
    tb = tb[(tb.ts != tb.tf)]
    if tb.empty:
        return pd.DataFrame({'ts': tr.ts.values, 'tf': tr.tf.values})
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
            return pd.DataFrame({'ts': dts[idxn], 'tf': dts[idxn+1]})
    else:
        return pd.DataFrame(columns=['ts', 'tf'])


def difference_df(dfr, dfb, on_column='u'):
    if dfr.empty or dfb.empty:
        return dfr
    nr, nb = dfr.shape[0], dfb.shape[0]
    mdf = dfr.append(dfb, ignore_index=True, sort=False)
    mdf['r'] = np.concatenate((np.full((nr,), True), np.full((nb,), False)))
    return gby_format(mdf.groupby(on_column).apply(difference))


def difference_with_df(dfr, dfb, on_column='u'):
    if dfr.empty or dfb.empty:
        return dfr
    return gby_format(dfr.groupby(on_column).apply(difference_b, dfb))


def cartesian_intersect_df(df, base_df):
    base_df = base_df.set_index('u')
    df = df[df.u.isin(base_df.index) & df.v.isin(base_df.index)]
    return gby_format(df.groupby(['u', 'v']).apply(cartesian_intersect, base_df))


def map_intersect(df, base):
    u, _ = df.name
    dfu = base.loc[u, ['ts', 'tf']]
    df = intersect_intervals(df.append(dfu, ignore_index=True, sort=True))
    if not df.empty:
        return df


def map_intersect_df(df, base_df):
    base_df = base_df.set_index('u')
    df = df[df.u.isin(base_df.index)]
    return gby_format(df.groupby(['u', 'v']).apply(map_intersect, base_df)).drop(columns=['u']).rename(columns={'v': 'u'})


def ns_to_df(ns):
    if ns:
        if isinstance(ns, NodeStreamDF):
            return ns.df
        else:
            return pd.DataFrame(list(ns), columns=["u", "ts", "tf"])
    else:
        return pd.DataFrame(list(), columns=["u", "ts", "tf"])

def ts_to_df(ts):
    if bool(ns):
        if isinstance(ns, TimeSetDF):
            return ts.df
        else:
            return pd.DataFrame(list(ts), columns=["u", "ts", "tf"])
    else:
        return pd.DataFrame(list(), columns=["u", "ts", "tf"])

def nsr_disjoint_union(nodes, min_time, max_time, ba, bb):
    return NodeStreamDF().set_df(pd.DataFrame(iter((n, mn, mx)
                                                   for n in nodes
                                                   for mn, mx
                                                   in (ba, bb)),
                                             columns=["u", "ts", "tf"]),
                                 min_time=min_time,
                                 max_time=max_time)


def gby_format(df):
    ndf = df.reset_index()
    return ndf[[col for col in ndf.columns if not (len(col) > 6 and col[:5]=='level')]]
