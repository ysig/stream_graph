from __future__ import absolute_import
from six import iteritems, viewkeys
from collections import Counter
from collections import defaultdict
from .utils.misc import counter_to_iter, get_key_set
from .utils.no_bounds import events_uni, events
from .utils.orderings import order_0_n1 as merge_order
from .utils.orderings import r_order_1_0n2_n2 as nonempty_intersection_order
from .utils.orderings import r_order_1_n2 as intersection_order
from .utils.orderings import r_order_1_n2_0n2 as difference_order
from .utils.orderings import r_order_1_n2_0n2 as issuper_order
from .utils.orderings import r_order_1_n0_n2 as cs_order


def add_prev(key, cache, out, value, id=-1):
    # Valid Interval
    prev = cache[id]

    if prev is not None:
        ts_p, tf_p, w_p = out[prev][-3:]
        if value[0] - 1 == tf_p and w_p == value[-1]:
            # What if value[0] == tf_p-1 has another value
            out[prev] = key + (ts_p, value[1], w_p)
            return

    cache[id] = len(out)
    out.append(key + value)


# Merge [assumes merged interval-df]
def merge_cache_constructor():
    return [Counter(), None, None]


def update_cache_merge(key, cache, event, out, merge_function):
    t, s, w = event
    if s:
        if cache[1] is None:
            cache[1] = t
        elif cache[1] <= t - 1:
            add_prev(key, cache, out, (cache[1], t - 1, merge_function(counter_to_iter(cache[0]))))
            cache[1] = t
        cache[0][w] += 1
    else:
        assert cache[1] is not None
        add_prev(key, cache, out, (cache[1], t, merge_function(counter_to_iter(cache[0]))))
        cache[0][w] -= 1
        if cache[0][w] == 0:
            cache[0].pop(w)
        cache[1] = (t + 1 if len(cache[0]) else None)


def merge_no_key(df, merge_function):
    out, cache = [], merge_cache_constructor()

    for event in events_uni(df, merge_order, weights=True):
        update_cache_merge(tuple(), cache, event, out, merge_function)

    return out


def merge_by_key(df, merge_function):
    # Internal
    out, cache = [], defaultdict(merge_cache_constructor)
    for event in events_uni(df, merge_order, weights=True):
        ev, key = event[:3], event[3:]
        update_cache_merge(key, cache[key], ev, out, merge_function)

    return out


# Union [assumes merged interval-df]
# cache = [w_a, w_b, break, prev]
def update_cache_union(key, cache, event, out, union_function):
    t, s, w = event
    if s:
        if cache[0] is None:
            cache[0], cache[2] = w, t
        else:
            cache[1] = w
            if cache[2] <= t - 1:
                add_prev(key, cache, out, (cache[2], t - 1, w))
                cache[2] = t
    elif cache[2] <= t:
        value = (cache[2], t)
        if cache[1] is None:
            add_prev(key, cache, out, value + (cache[0], ))
            cache[0], cache[2] = None, None
        else:
            add_prev(key, cache, out, value + (union_function(cache[0], cache[1]),))
            cache[0], cache[1], cache[2] = cache[1], None, t + 1


def union_cache_constructor():
    return [None, None, None, None]


def union_no_key(dfa, dfb, union_function):
    out, cache = [], union_cache_constructor()
    for event in events(dfa, dfb, merge_order, weights=True):
        update_cache_union(tuple(), cache, event, out, union_function)

    return out


def union_by_key(dfa, dfb, union_function):
    # Internal
    out, cache = [], defaultdict(union_cache_constructor)
    for event in events(dfa, dfb, merge_order, weights=True):
        ev, key = event[:3], event[3:]
        update_cache_union(key, cache[key], ev, out, union_function)

    return out


def union_on_key(df, kdf, union_function):
    # Extract information and possible keys
    all_keys = get_key_set(df)

    out, cache = [], defaultdict(union_cache_constructor)
    for event in events(df, kdf, merge_order, weights=True):
        ev, k = event[:3], event[3:]
        keys = ([k] if len(k) > 0 else all_keys)
        for key in keys:
            update_cache_union(key, cache[key], ev, out, union_function)

    return out


# Intersection [assumes merged interval-df]
# cache = [w_a, w_b, break]
def intersection_cache_constructor():
    return [None, None, None, None]


def update_cache_intersection(key, cache, event, out, intersection_function):
    r, t, s, w = event
    if s:
        cache[int(r)], cache[2] = w, t
    else:
        if not (cache[0] is None or cache[1] is None):
            if cache[2] <= t:
                w = intersection_function(cache[0], cache[1])
                if w is not None:
                    add_prev(key, cache, out, (cache[2], t, w))
        cache[int(r)] = None


def intersection_no_key(dfa, dfb, intersection_function):
    t_max = min(dfa.tf.max(), dfb.tf.max())

    out, cache = [], intersection_cache_constructor()
    for event in events(dfa, dfb, intersection_order, weights=True, reference=True):
        update_cache_intersection(tuple(), cache, event, out, intersection_function)
        if event[1] > t_max:
            break

    return out


def intersection_by_key(dfa, dfb, intersection_function):
    t_max = min(dfa.tf.max(), dfb.tf.max())

    out, cache = [], defaultdict(intersection_cache_constructor)
    for event in events(dfa, dfb, intersection_order, weights=True, reference=True):
        ev, key = event[:4], event[4:]
        update_cache_intersection(key, cache[key], ev, out, intersection_function)
        if ev[1] > t_max:
            break

    return out


def intersection_on_key(dfa, dfb, intersection_function):
    t_max = min(dfa.tf.max(), dfb.tf.max())

    out, cache, active_weight, prev = [], dict(), None, defaultdict(issuper_cache_constructor)
    for event in events(dfa, dfb, difference_order, weights=True, reference=True):
        r, t, s, w, k = event[0], event[1], event[2], event[3], event[4:]
        if r:
            if s:
                cache[k] = [t, w]
            else:
                c = cache.pop(k, None)
                if not (active_weight is None or c is None):
                    e, wa = c
                    wc = intersection_function(wa, active_weight)
                    if wc is not None:
                        add_prev(k, prev[k], out, (e, t, wc))
        else:
            if s:
                for key in cache.keys():
                    cache[key][0] = t
                active_weight = w
            else:
                for key, val in iteritems(cache):
                    wc = intersection_function(val[1], active_weight)
                    if wc is not None:
                        add_prev(key, prev[key], out, (val[0], t, wc))
                active_weight = None
        if t > t_max:
            break
    return out


# Difference [assumes merged interval-df]
# cache = [w_a, w_b, break, prev]
def difference_cache_constructor():
    return [None, None, None, None]


def update_cache_difference(key, cache, event, out, difference_function):
    r, t, s, w = event
    if s:
        if r:
            cache[1], cache[2] = w, t
        else:
            if cache[1] is not None:
                if cache[2] <= t - 1:
                    add_prev(key, cache, out, (cache[2], t - 1, cache[1]))
                    cache[2] = t
            cache[0] = w
    else:
        if cache[1] is not None:
            if cache[0] is not None:
                if cache[2] <= t:
                    w = difference_function(cache[1], cache[0])
                    if w is not None:
                        out.append(key + (cache[2], t, w))
                cache[2] = (None if r else t + 1)
            elif r:
                if cache[2] <= t:
                    out.append(key + (cache[2], t, cache[1]))
                cache[2] = None
        cache[int(r)] = None


def difference_no_key(dfa, dfb, difference_function):
    t_max = dfa.tf.max()

    out, cache = [], difference_cache_constructor()
    for ev in events(dfa, dfb, difference_order, weights=True, reference=True):
        update_cache_difference(tuple(), cache, ev, out, difference_function)
        if ev[1] > t_max:
            break

    return out


def difference_by_key(dfa, dfb, difference_function):
    t_max = dfa.tf.max()

    out, cache = [], defaultdict(difference_cache_constructor)
    for event in events(dfa, dfb, difference_order, weights=True, reference=True):
        ev, key = event[:4], event[4:]
        update_cache_difference(key, cache[key], ev, out, difference_function)
        if ev[1] > t_max:
            break

    return out


def difference_on_key(dfa, dfb, difference_function):
    t_max = dfa.tf.max()

    out, cache, active_weight = [], defaultdict(intersection_cache_constructor), None
    for event in events(dfa, dfb, difference_order, weights=True, reference=True):
        r, t, s, w = event[:4]
        key = event[4:]

        if r:
            d = cache[key]
            if s:
                d[0], d[1] = w, t
            else:
                if active_weight is not None:
                    if d[1] <= t:
                        w = difference_function(d[0], active_weight)
                        if w is not None:
                            add_prev(key, d, out, (d[1], t, w))
                else:
                    add_prev(key, d, out, (d[1], t, d[0]))
                cache.pop(key, None)
        else:
            if s:
                for key, d in iteritems(cache):
                    if d[1] <= t - 1:
                        add_prev(key, d, out, (d[1], t - 1, d[0]))
                        d[1] = t
                active_weight = w
            else:
                for key, d in iteritems(cache):
                    if d[1] <= t:
                        w = difference_function(d[0], active_weight)
                        if w is not None:
                            add_prev(key, d, out, (d[1], t, w))
                        d[1] = t + 1
                active_weight = None
        if t > t_max:
            break

    return out


# issuper [assumes merged interval-df]
# cache = [event]
def issuper_cache_constructor():
    return [None]


def update_cache_issuper(cache, event, issuper_function):
    r, _, s, w = event
    if r:
        cache[0] = (w if s else None)
    else:
        if (cache[0] is None or not issuper_function(cache[0], w)):
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
        ev, key = event[:4], event[4:]
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
        r, t, s, w = col[:4]
        if r:
            k = col[4:]
            if s:
                cache[k] = w
            else:
                cache.pop(k, None)
        else:
            if all_keys == viewkeys(cache):
                for k, wa in iteritems(cache):
                    if not issuper_function(wa, w):
                        return False
            else:
                return False

        if t > t_max:
            break

    return True


# nonempty_intersection [assumes merged interval-df]
def update_cache_nonempty_intersection(l, event, nonempty_intersection_function):
    r, s, w = event[0], event[2], event[3]
    if r:
        l[0] = (w if s else None)
    else:
        if l[0] is not None and nonempty_intersection_function(l[0], w):
            return True
    return False


def nonempty_intersection_no_key(dfa, dfb, nonempty_intersection_function):
    t_max = dfb.tf.max()

    cache = issuper_cache_constructor()
    for ev in events(dfa, dfb, nonempty_intersection_order, reference=True, weights=True):
        if update_cache_nonempty_intersection(cache, ev, nonempty_intersection_function):
            return True
        if ev[1] > t_max:
            break

    return False


def nonempty_intersection_by_key(dfa, dfb, nonempty_intersection_function):
    t_max = dfb.tf.max()

    cache = defaultdict(issuper_cache_constructor)
    for event in events(dfa, dfb, nonempty_intersection_order, reference=True, weights=True):
        ev, key = event[:4], event[4:]
        if update_cache_nonempty_intersection(cache[key], ev, nonempty_intersection_function):
            return True
        if ev[1] > t_max:
            break

    return False


def nonempty_intersection_on_key(dfa, dfb, nonempty_intersection_function):
    t_max = min(dfa.tf.max(), dfb.tf.max())

    cache = dict()
    for event in events(dfa, dfb, nonempty_intersection_order, reference=True, weights=True):
        r, t, s, w = event[:4]
        if r:
            if s:
                cache[event[4:]] = w
            else:
                cache.pop(event[4:], None)
        else:
            for k, wa in iteritems(cache):
                if nonempty_intersection_function(wa, w):
                    return True
        if t > t_max:
            break

    return False


def cartesian_intersection_constructor():
    return (dict(), dict())


def cartesian_intersection(df, base_df, cartesian_intersection_function):
    # Intersect
    out, w_current, e, w_area = list(), defaultdict(cartesian_intersection_constructor), dict(), dict()
    for col in events(base_df, df, nonempty_intersection_order, reference=True, weights=True):
        r, t, f, w = col[:4]
        if r:
            u = col[4]
            left, right = w_current[u]
            if f:
                for v in left:
                    e[(u, v)] = t
                for v in right:
                    e[(v, u)] = t
                w_area[u] = w
            else:
                for v, wuv in iteritems(left):
                    wv = w_area.get(v, None)
                    if wv is not None:
                        wt = cartesian_intersection_function(wuv, w, wv)
                        if wt is not None:
                            out.append((u, v, e[(u, v)], t, wt))
                    e[(u, v)] = t + 1
                for v, wuv in iteritems(right):
                    wv = w_area.get(v, None)
                    if wv is not None:
                        wt = cartesian_intersection_function(wuv, w, wv)
                        if wt is not None:
                            out.append((v, u, e[(v, u)], t, wt))
                    e[(v, u)] = t + 1
                w_area.pop(u, None)
        else:
            u, v = col[4], col[5]
            if f:
                e[(u, v)] = t
                w_current[u][0][v] = w
                w_current[v][1][u] = w
            else:
                wc = w_current[u][0].get(v, None)
                ts = e.get((u, v), None)
                if ts is not None and wc is not None:
                    wu = w_area.get(u, None)
                    if wu is not None:
                        wv = w_area.get(v, None)
                        if wv is not None:
                            wt = cartesian_intersection_function(wc, wu, wv)
                            if wt is not None:
                                out.append((u, v, ts, t, wt))
                            e[(u, v)] = t + 1
                w_current[u][0].pop(v, None)
                w_current[v][1].pop(u, None)

    return out


def intersection_cache(cache, event, intersection_function):
    r, t, s, w = event

    if s:
        if cache[2] is not None:
            if t > cache[2]:
                cache[3] += intersection_function(counter_to_iter(cache[0]), counter_to_iter(cache[1])) * (t - cache[2])

        cache[int(r)][w] += 1
        if len(cache[int(not r)]):
            cache[2] = t
    else:
        if cache[2] is not None:
            cache[3] += intersection_function(counter_to_iter(cache[0]), counter_to_iter(cache[1])) * (t - cache[2] + 1)

        if cache[int(r)][w] == 1:
            cache[int(r)].pop(w, None)
            cache[2] = None
        else:
            cache[int(r)][w] -= 1
            if len(cache[int(not r)]):
                cache[2] = t + 1


def interval_intersection_size(dfa, dfb, intersection_function):
    t_max = min(dfa.tf.max(), dfb.tf.max())

    cache = [Counter(), Counter(), None, 0]
    for event in events(dfa, dfb, intersection_order, reference=True, weights=True):
        intersection_cache(cache, event[:4], intersection_function)
        if event[1] > t_max:
            break
    return cache[3]
