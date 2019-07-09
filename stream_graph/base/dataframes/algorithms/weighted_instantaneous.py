from .utils.misc import iter_dmerge_
from collections import defaultdict
from six import iteritems


def union_by_key(dfa, dfb, union_function):
    data = dict()
    for key in dfa.itertuples(weights=True):
        data[key[:-1]] = key[-1]

    for key in dfb.itertuples(weights=True):
        d = data.get(key[:-1], None)
        if d is None:
            data[key[:-1]] = key[-1]
        else:
            data[key[:-1]] = union_function(data[key[:-1]], key[-1])

    return list(key + (w, ) for key, w in iteritems(data))


def union_on_key(df, df_base, union_function):
    db = {ts: w for ts, w in df_base.itertuples(weights=True)}
    if len(db):
        data = defaultdict(dict)
        for key in df.itertuples(weights=True):
            data[key[:-2]][key[-2]] = key[-1]
        return list(key + (t, w) for key, da in iteritems(data) for t, w in iter_dmerge_(da, db, union_function))
    else:
        return df


def intersection_by_key(dfa, dfb, intersection_function):
    data = dict()
    if dfa.shape[0] > dfb.shape[0]:
        dfa, dfb = dfb, dfa

        def apply(a, b):
            return intersection_function(b, a)
    else:
        def apply(a, b):
            return intersection_function(a, b)

    for key in dfa.itertuples(weights=True):
        data[key[:-1]] = key[-1]

    out = list()
    for key in dfb.itertuples(weights=True):
        k, w = key[:-1], key[-1]
        v = data.get(k, None)
        if v is not None:
            out.append(k + (apply(v, w), ))

    return out


def intersection_on_key(df, bd, intersection_function):
    out = list()
    for key in df.itertuples(weights=True):
        k, w = key[:-1], key[-1]
        wb = bd.get(k[-1], None)
        if wb is not None:
            out.append(k + (intersection_function(w, wb),))
    return out


def difference_by_key(dfa, dfb, difference_function):
    out = list()
    db = {key[:-1]: key[-1] for key in dfb.itertuples(weights=True)}
    for key in dfa.itertuples(weights=True):
        k, wa = key[:-1], key[-1]
        wb = db.get(k, None)
        if wb is not None:
            w = difference_function(wa, wb)
            if w is not None:
                out.append(k + (w,))
        else:
            out.append(key)

    return out


def difference_on_key(dfa, db, difference_function):
    out = list()
    for key in dfa.itertuples(weights=True):
        k, w = key[:-1], key[-1]
        wb = db.get(k[-1], None)
        if wb is not None:
            w = difference_function(w, wb)
            if w is not None:
                out.append(k + (w,))
        else:
            out.append(key)
    return out


def issuper_by_key(dfa, dfb, issuper_function):
    da = dict()
    for key in dfa.itertuples(weights=True):
        da[key[:-1]] = key[-1]
    for key in dfb.itertuples(weights=True):
        w = da.get(key[:-1], None)
        if w is None or not issuper_function(w, key[-1]):
            return False
    return True


def issuper_on_key(dfa, ts, issuper_function):
    for key in dfa.itertuples(weights=True):
        w = ts.get(key[-2], None)
        if w is None or not issuper_function(w, key[-1]):
            return False
    return True


def nonempty_intersection_by_key(dfa, dfb, nonempty_intersection_function):
    da = dict()
    for key in dfa.itertuples(weights=True):
        da[key[:-1]] = key[-1]

    for key in dfb.itertuples(weights=True):
        w = da.get(key[:-1], None)
        if w is not None and nonempty_intersection_function(w, key[-1]):
            return True
    return False


def nonempty_intersection_on_key(dfa, ts, nonempty_intersection_function):
    for key in dfa.itertuples(weights=True):
        w = ts.get(key[-2], None)
        if w is not None and nonempty_intersection_function(key[-1], w):
            return True
    return False
