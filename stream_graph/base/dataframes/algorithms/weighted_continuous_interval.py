from __future__ import absolute_import
from six import iteritems, viewkeys
from collections import Counter, defaultdict
from .utils.misc import counter_to_iter, get_key_set
from .utils.bounds import valid_interval, events, events_uni
from .utils.orderings import b_order_0_1n2_2 as merge_order
from .utils.orderings import rb_order_1_3_0n3 as difference_order
from .utils.orderings import rb_order_1_3_2n3_0n3 as issuper_order
from .utils.orderings import rb_order_1_3_2n3 as intersection_order
from .utils.orderings import rb_order_nonempty_intersection as nei_order
from .utils.orderings import r_order_1_2_0 as iis_order


def add_prev(key, cache, out, value, id=-1):
    # Valid Interval
    prev = cache[id]
    if prev is not None:
        ts_p, tf_p, bs_p, bf_p, w_p = out[prev][-5:]
        if value[0] == tf_p and value[2] != bf_p and w_p == value[4]:
            out[prev] = key + (ts_p, value[1], bs_p, value[3], w_p)
            return
    cache[id] = len(out)
    out.append(key + value)


# Merge (order [ < ( < ) < ] # maybe list instead of weight
def merge_cache(key, cache, event, out, merge_function):
    t, closed, s, w = event
    if s:
        if cache[1] is None:
            cache[1] = (t, closed)
        elif cache[1][0] < t or (cache[1][0] == t and cache[1][1] != closed):
            add_prev(key, cache, out, (cache[1][0], t, cache[1][1], not closed, merge_function(counter_to_iter(cache[0]))))
            cache[1] = (t, closed)
        cache[0][w] += 1
    else:
        assert cache[1] is not None
        value = (cache[1][0], t, cache[1][1], closed)
        if valid_interval(value):
            add_prev(key, cache, out, value + (merge_function(counter_to_iter(cache[0])), ))
        cache[0][w] -= 1
        if cache[0][w] == 0:
            cache[0].pop(w)
        if len(cache[0]):
            cache[1] = (t, not closed)
        else:
            cache[1] = None


def merge_cache_constructor():
    return [Counter(), None, None]


def merge_no_key(df, merge_function):
    out, cache = [], merge_cache_constructor()
    for event in events_uni(df, merge_order, weights=True):
        merge_cache(tuple(), cache, event, out, merge_function)

    return out


def merge_by_key(df, merge_function):
    # Internal
    out, cache = [], defaultdict(merge_cache_constructor)
    for event in events_uni(df, merge_order, weights=True):
        ev, key = event[:4], event[4:]
        merge_cache(key, cache[key], ev, out, merge_function)

    return out


# Union [assumes merged interval-df]
# cache = [w_a, w_b, break, prev]
def union_cache_constructor():
    return [None, None, None, None]


def update_cache_union(key, cache, event, out, union_function):
    t, closed, s, w = event
    if s:
        if cache[0] is None:
            cache[0], cache[2] = w, (t, closed)
        else:
            assert cache[1] is None
            cache[1] = w
            if cache[2][0] < t or (cache[2][0] == t and cache[2][1] != closed):
                add_prev(key, cache, out, (cache[2][0], t, cache[2][1], not closed, w))
                cache[2] = (t, closed)

    else:
        if cache[1] is None:
            value = (cache[2][0], t, cache[2][1], closed, cache[0])
            if valid_interval(value):
                add_prev(key, cache, out, value)
            cache[0], cache[2] = None, None
        else:
            value = (cache[2][0], t, cache[2][1], closed,)
            if valid_interval(value):
                add_prev(key, cache, out, value + (union_function(cache[0], cache[1]),))
            cache[0], cache[1], cache[2] = w, None, (t, not closed)


def union_no_key(a, b, union_function):
    out, cache = [], union_cache_constructor()
    for event in events(a, b, merge_order, weights=True):
        update_cache_union(tuple(), cache, event, out, union_function)

    return out


def union_by_key(a, b, union_function):
    # Internal
    out, cache = [], defaultdict(union_cache_constructor)
    for event in events(a, b, merge_order, weights=True):
        ev, key = event[:4], event[4:]
        update_cache_union(key, cache[key], ev, out, union_function)

    return out


def union_on_key(df, kdf, union_function):
    # Extract information and possible keys
    all_keys = get_key_set(df)

    out, cache = [], defaultdict(union_cache_constructor)
    for col in events(df, kdf, merge_order, weights=True):
        ev, k = col[:4], col[4:]
        keys = ([k] if len(k) > 0 else all_keys)
        for key in keys:
            update_cache_union(key, cache[key], ev, out, union_function)

    return out


# Intersection [assumes merged interval-df]
# cache = [w_a, w_b, break]
def intersection_cache_constructor():
    return [None, None, None]


def intersection_cache(key, cache, event, out, intersection_fun):
    r, t, closed, s, w = event
    if s:
        cache[int(r)] = w
        if cache[int(not r)] is not None:
            cache[2] = (t, closed)
    else:
        if cache[2] is not None:
            if cache[2][0] != t or (cache[2][1] and closed):
                w = intersection_fun(cache[1], cache[0])
                if w is not None:
                    out.append(key + (cache[2][0], t, cache[2][1], closed, w))
            cache[2] = None
        cache[int(r)] = None


def intersection_no_key(dfa, dfb, intersection_function):
    t_max = min(dfa.tf.max(), dfb.tf.max())

    out, cache = [], intersection_cache_constructor()
    for event in events(dfa, dfb, intersection_order, weights=True, reference=True):
        intersection_cache(tuple(), cache, event, out, intersection_function)
        if event[1] > t_max:
            break

    return out


def intersection_by_key(dfa, dfb, intersection_function):
    t_max = min(dfa.tf.max(), dfb.tf.max())

    out, cache = [], defaultdict(intersection_cache_constructor)
    for event in events(dfa, dfb, intersection_order, weights=True, reference=True):
        ev, key = event[:5], event[5:]
        intersection_cache(key, cache[key], ev, out, intersection_function)
        if ev[1] > t_max:
            break

    return out


def intersection_on_key(dfa, dfb, intersection_function):
    t_max = min(dfa.tf.max(), dfb.tf.max())

    out, cache, active_weight = [], dict(), None
    for event in events(dfa, dfb, intersection_order, weights=True, reference=True):
        r, t, closed, s, w = event[:5]
        if r:
            key = event[5:]
            if s:
                cache[key] = (t, closed, w)
            else:
                d = cache.pop(key)
                if active_weight is not None:
                    if d[0] != t or (d[1] and closed):
                        w = intersection_function(d[2], active_weight)
                        if w is not None:
                            out.append(key + (d[0], t, d[1], closed, w))
        else:
            if s:
                for key, v in iteritems(cache):
                    cache[key] = (t, closed, v[2])
                active_weight = w
            else:
                for key, v in iteritems(cache):
                    if v[0] != t or (v[1] and closed):
                        wc = intersection_function(v[2], w)
                        if wc is not None:
                            out.append(key + (v[0], t, v[1], closed, wc))
                    cache[key] = (t, not closed, v[2])
                active_weight = None

        if t > t_max:
            break

    return out


# Difference [assumes merged interval-df]
# cache = [w_a, w_b, break, prev]
def difference_cache_constructor():
    return [None, None, None, None]


def difference_cache(key, cache, event, out, difference_function):
    r, t, closed, s, w = event
    if s:
        if r:
            cache[0], cache[2] = w, (t, closed)
        else:
            cache[1] = w
            if cache[0] is not None:
                if (cache[2][0] < t or (cache[2][0] == t and cache[2][1] and not closed)):
                    add_prev(key, cache, out, (cache[2][0], t, cache[2][1], not closed, cache[0]))
                    cache[2] = (t, closed)
    else:
        if cache[0] is not None:
            if cache[1] is not None:
                if cache[2][0] != t or (cache[2][1] and closed):
                    w = difference_function(cache[0], cache[1])
                    if w is not None:
                        out.append(key + (cache[2][0], t, cache[2][1], closed, w))
                cache[2] = (None if r else (t, not closed))
            elif r:
                if cache[2][0] != t or (cache[2][1] and closed):
                    out.append(key + (cache[2][0], t, cache[2][1], closed, cache[0]))
                cache[2] = None
        cache[int(not r)] = None


def difference_no_key(dfa, dfb, difference_function):
    t_max = dfa.tf.max()

    out, cache = [], difference_cache_constructor()
    for ev in events(dfa, dfb, difference_order, reference=True, weights=True):
        difference_cache(tuple(), cache, ev, out, difference_function)
        if ev[1] > t_max:
            break

    return out


def difference_by_key(dfa, dfb, difference_function):
    t_max = dfa.tf.max()

    out, cache = [], defaultdict(difference_cache_constructor)
    for event in events(dfa, dfb, difference_order, reference=True, weights=True):
        ev, key = event[:5], event[5:]
        difference_cache(key, cache[key], ev, out, difference_function)
        if ev[1] > t_max:
            break

    return out


def difference_on_key(dfa, dfb, difference_function):
    t_max = dfa.tf.max()

    out, cache, lookup, active_weight = [], dict(), defaultdict(issuper_cache_constructor), None
    for event in events(dfa, dfb, difference_order, reference=True, weights=True):
        r, t, closed, s, w = event[:5]
        if r:
            key = event[5:]
            if s:
                cache[key] = (t, closed, w)
            else:
                d = cache.pop(key)
                if active_weight is None:
                    if (d[0] < t or (d[0] == t and d[1] and closed)):
                        add_prev(key, lookup[key], out, (d[0], t, d[1], closed, d[2]))
                else:
                    if (d[0] < t or (d[0] == t and d[1] and closed)):
                        wp = difference_function(d[2], active_weight)
                        if wp is not None:
                            add_prev(key, lookup[key], out, (d[0], t, d[1], closed, wp))
        else:
            if s:
                for k, v in iteritems(cache):
                    if (v[0] < t or (v[0] == t and v[1] and not closed)):
                        add_prev(k, lookup[k], out, (v[0], t, v[1], not closed, v[2]))
                    cache[k] = (t, closed, v[2])
                active_weight = w
            else:
                for k, v in iteritems(cache):
                    if (v[0] < t or (v[0] == t and v[1] and closed)):
                        wp = difference_function(v[2], w)
                        if wp is not None:
                            add_prev(k, lookup[k], out, (v[0], t, v[1], closed, wp))
                    cache[k] = (t, not closed, v[2])
                active_weight = None

        if t > t_max:
            break

    return out


# issuper [assumes merged interval-df]
# cache = [event]
def issuper_cache_constructor():
    return [None]


def update_cache_issuper(cache, event, issuper_function):
    r, t, closed, s, w = event
    if r:
        cache[0] = ((t, closed, w) if s else None)
    else:
        if (cache[0] is None or (not s and cache[0][0] == t and cache[0][1] < closed) or not issuper_function(cache[0][2], w)):
            return True
    return False


def issuper_no_key(dfa, dfb, issuper_function):
    t_max = dfb.tf.max()

    cache = issuper_cache_constructor()
    for ev in events(dfa, dfb, issuper_order, reference=True, weights=True):
        if update_cache_issuper(cache, ev, issuper_function):
            return False
        if ev[1] > t_max:
            break

    return True


def issuper_by_key(dfa, dfb, issuper_function):
    t_max = dfb.tf.max()

    cache = defaultdict(issuper_cache_constructor)
    for event in events(dfa, dfb, issuper_order, reference=True, weights=True):
        ev, key = event[:5], event[5:]
        if update_cache_issuper(cache[key], ev, issuper_function):
            return False
        if ev[1] > t_max:
            break

    return True


def issuper_on_key(df, kdf, issuper_function):
    # Extract information and possible keys
    all_keys = get_key_set(df)
    t_max = kdf.tf.max()

    cache = dict()
    for col in events(df, kdf, issuper_order, reference=True, weights=True):
        r, t, closed, s, w = col[:5]
        if r:
            k = col[5:]
            if s:
                cache[k] = (t, closed, w)
            else:
                cache.pop(k, None)
        else:
            if all_keys == viewkeys(cache):
                for k, v in iteritems(cache):
                    if (not s and v[0] == t and v[1] < closed) or not issuper_function(v[2], w):
                        return False
            else:
                return False

        if t > t_max:
            break

    return True


# issuper [assumes merged interval-df]
# cache = [event]
def update_cache_nonempty_intersection(l, event, nonempty_intersection_function):
    r, s, w = event[0], event[3], event[4]
    if r:
        l[0] = (w if s else None)
    else:
        if l[0] is not None and nonempty_intersection_function(l[0], w):
            return True
    return False


def nonempty_intersection_no_key(dfa, dfb, nonempty_intersection_function):
    t_max = dfb.tf.max()

    cache = issuper_cache_constructor()
    for ev in events(dfa, dfb, nei_order, reference=True, weights=True):
        if update_cache_nonempty_intersection(cache, ev, nonempty_intersection_function):
            return True
        if ev[1] > t_max:
            break

    return False


def nonempty_intersection_by_key(dfa, dfb, nonempty_intersection_function):
    t_max = dfb.tf.max()

    cache = defaultdict(issuper_cache_constructor)
    for event in events(dfa, dfb, nei_order, reference=True, weights=True):
        ev, key = event[:5], event[5:]
        if update_cache_nonempty_intersection(cache[key], ev, nonempty_intersection_function):
            return True
        if ev[1] > t_max:
            break

    return False


def nonempty_intersection_on_key(dfa, dfb, nonempty_intersection_function):
    t_max = min(dfa.tf.max(), dfb.tf.max())

    cache = dict()
    for event in events(dfa, dfb, nei_order, reference=True, weights=True):
        r, t, c, s, w = event[:5]
        if r:
            if s:
                cache[event[5:]] = w
            else:
                cache.pop(event[5:], None)
        else:
            for k, wa in iteritems(cache):
                if nonempty_intersection_function(wa, w):
                    return True
        if t > t_max:
            break

    return False


def ci_constructor():
    return (dict(), dict())


def cartesian_intersection(df, base_df, cartesian_intersection_function):
    out, w_current, e, w_area = list(), defaultdict(ci_constructor), dict(), dict()
    for col in events(base_df, df, nei_order, weights=True, reference=True):
        r, t, it, s, w = col[:5]
        if r:
            # base-df
            u = col[5]
            rights, lefts = w_current[u]
            if s:
                w_area[u] = w
                for k in lefts.keys():
                    e[(k, u)] = t, it
                for k in rights.keys():
                    e[(u, k)] = t, it
            else:
                for v, wuv in iteritems(lefts):
                    wv = w_area.get(v, None)
                    to, ito = e[(v, u)]
                    time = (to, t, ito, it)
                    if valid_interval(time):
                        if wv is not None:
                            wt = cartesian_intersection_function(wuv, w, wv)
                            if wt is not None:
                                out.append((v, u) + time + (wt, ))
                    e[(v, u)] = t, not it
                # If an interval-closes
                for v, wuv in iteritems(rights):
                    wv = w_area.get(v, None)
                    to, ito = e[(u, v)]
                    time = (to, t, ito, it)
                    if valid_interval(time):
                        if wv is not None:
                            wt = cartesian_intersection_function(wuv, wv, w)
                            if wt is not None:
                                out.append((u, v) + time + (wt, ))
                    e[(u, v)] = t, not it
                w_area.pop(u, None)

        else:
            # u, v
            u, v = col[5], col[6]
            if s:
                e[(u, v)] = t, it
                w_current[u][0][v] = w
                w_current[v][1][u] = w
            else:
                # If an interval-closes
                wc, itt = w_current[u][0].get(v, None), e.get((u, v), None)
                if wc is not None and itt is not None:
                    time = (itt[0], t, itt[1], it)
                    if valid_interval(time):
                        wu = w_area.get(u, None)
                        if wu is not None:
                            wv = w_area.get(v, None)
                            if wv is not None:
                                wt = cartesian_intersection_function(wc, wu, wv)
                                if wt is not None:
                                    out.append((u, v) + time + (wt, ))
                                e[(u, v)] = t, not it
                w_current[u][0].pop(v, None)
                w_current[v][1].pop(u, None)

    return out


def interval_intersection_cache(cache, event, intersection_function):
    r, t, _, s, w = event
    if cache[2] is not None:
        cache[3] += intersection_function(counter_to_iter(cache[0]), counter_to_iter(cache[1])) * (t - cache[2])

    if s:
        cache[int(r)][w] += 1
        if len(cache[int(not r)]):
            cache[2] = t
    else:
        if cache[int(r)][w] == 1:
            cache[int(r)].pop(w, None)
            cache[2] = None
        else:
            cache[int(r)][w] -= 1
            if len(cache[int(not r)]):
                cache[2] = t


def interval_intersection_size(dfa, dfb, interval_intersection_function):
    t_max = min(dfa.tf.max(), dfb.tf.max())

    cache = [Counter(), Counter(), None, 0]
    for event in events(dfa, dfb, iis_order, reference=True, weights=True):
        interval_intersection_cache(cache, event[:5], interval_intersection_function)
        if event[1] > t_max:
            break
    return cache[3]
