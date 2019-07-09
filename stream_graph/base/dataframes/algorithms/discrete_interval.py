from __future__ import absolute_import
from six import iteritems
from collections import defaultdict

from pandas import DataFrame as DF

from .utils.no_bounds import events
from .utils.no_bounds import events_uni
from .utils.misc import get_key_set
from .utils.orderings import order_0_1 as merge_order
from .utils.orderings import r_order_1_n2 as intersection_order
from .utils.orderings import r_order_1_n2_0n2 as difference_order
from .utils.orderings import r_order_1_n2_0n2 as issuper_order
from .utils.orderings import r_order_1_n0_n2 as cs_order
from .utils.orderings import r_order_1_0n2_n2 as nonempty_intersection_order
from .utils.orderings import r_order_1_n2_0n2 as iis_order


# Merge
def update_cache_merge(d, event):
    t, s = event
    if len(d) < 2:
        if s:
            if True not in d:
                d[True] = t
        else:
            d[False] = t
    else:
        tf = d.pop(False, None)
        if s:
            # start
            if t - tf > 1:
                ts = d.pop(True, None)
                d[True] = t
                return (ts, tf)
        else:
            d[s] = max(t, tf)


def merge_no_key(df):
    out, cache = [], dict()
    for event in events_uni(df, merge_order):
        ret = update_cache_merge(cache, event)
        if ret is not None:
            out.append(ret)

    if len(cache) == 2:
        ts, tf = cache.pop(True, None), cache.pop(False, None)
        out.append((ts, tf))

    return out


def merge_by_key(df):
    # Internal
    out, cache = [], defaultdict(dict)
    for event in events_uni(df, merge_order):
        ev, key = event[:2], event[2:]
        ret = update_cache_merge(cache[key], ev)
        if ret is not None:
            out.append(key + ret)

    for key, rem in iteritems(cache):
        if len(rem) == 2:
            ts, tf = cache[key].pop(True, None), cache[key].pop(False, None)
            out.append(key + (ts, tf))

    return out


def union_no_key(dfa, dfb):
    return merge_no_key(dfa.append(dfb, merge=False))


def union_by_key(dfa, dfb):
    # Internal
    return merge_by_key(dfa.append(dfb, merge=False))


def union_on_key(df, kdf):
    # Extract information and possible keys
    all_keys = get_key_set(df)
    out, cache = [], defaultdict(dict)
    for event in events(df, kdf, merge_order):
        ev, k = event[:2], event[2:]
        keys = ([k] if len(k) > 0 else all_keys)
        for k in keys:
            ret = update_cache_merge(cache[k], ev)
            if ret is not None:
                out.append(k + ret)

    for key, rem in iteritems(cache):
        if len(rem) == 2:
            ts, tf = cache[key].pop(True, None), cache[key].pop(False, None)
            out.append(key + (ts, tf))

    return out


# Intersection [assumes merged interval-df]
def intersection_cache_constructor():
    return [False, False, None]


def update_cache_intersection(l, event):
    r, t, s = event
    if l[int(not r)]:
        # Case one active part
        if s:
            assert l[2] is None
            # Add the start of the other
            l[2] = t
        elif l[2] is not None:
            e, l[2] = l[2], None
            if e <= t:
                return (e, t)
    l[int(r)] = s


def intersection_no_key(dfa, dfb):
    t_max = min(dfa.tf.max(), dfb.tf.max())

    out, cache = [], intersection_cache_constructor()
    for ev in events(dfa, dfb, intersection_order, reference=True):
        ret = update_cache_intersection(cache, ev)
        if ret is not None:
            out.append(ret)
        if ev[1] > t_max:
            break

    return out


def intersection_by_key(dfa, dfb):
    t_max = min(dfa.tf.max(), dfb.tf.max())

    out, cache = [], defaultdict(intersection_cache_constructor)
    for event in events(dfa, dfb, intersection_order, reference=True):
        ev, key = event[:3], event[3:]
        ret = update_cache_intersection(cache[key], ev)
        if ret is not None:
            out.append(key + ret)
        if ev[1] > t_max:
            break

    return out


def intersection_on_key(dfa, dfb):
    t_max = min(dfa.tf.max(), dfb.tf.max())

    out, cache, active_keys, active_envelope = [], dict(), set(), False
    for event in events(dfa, dfb, difference_order, reference=True):
        r, t, s, k = event[0], event[1], event[2], event[3:]
        if r:
            if s:
                cache[k] = t
                active_keys.add(k)
            else:
                e = cache.pop(k, None)
                if active_envelope:
                    out.append(k + (e, t))
                active_keys.remove(k)
        else:
            if s:
                for key in active_keys:
                    cache[key] = t
                active_envelope = True
            else:
                for key in active_keys:
                    e = cache.pop(key)
                    out.append(key + (e, t))
                active_envelope = False
        if t > t_max:
            break
    return out


# Difference order
def difference_constructor():
    return [False, False, None]


# Difference [assumes merged interval-df]
def update_cache_difference(l, event):
    out = None
    r, t, s = event
    if s:
        if r and not l[0]:
            # start and reference and the other is not active
            l[2] = t
        elif not r and l[1]:
            # start and not reference and the reference is active
            assert l[2] is not None
            e, l[2] = l[2], None
            if e <= t - 1:
                out = (e, t-1)
    else:
        if r and not l[0]:
            # finish and reference and the other is not active
            e, l[2] = l[2], None
            if t >= e:
                out = (e, t)
        elif not r and l[1]:
            l[2] = t + 1
    l[int(r)] = s
    return out


def difference_no_key(dfa, dfb):
    t_max = dfa.tf.max()

    out, cache = [], difference_constructor()
    for ev in events(dfa, dfb, difference_order, reference=True):
        ret = update_cache_difference(cache, ev)
        if ret is not None:
            out.append(ret)
        if ev[1] > t_max:
            break

    return out


def difference_by_key(dfa, dfb):
    t_max = dfa.tf.max()

    out, cache = [], defaultdict(difference_constructor)
    for event in events(dfa, dfb, difference_order, reference=True):
        ev, key = event[:3], event[3:]
        ret = update_cache_difference(cache[key], ev)
        if ret is not None:
            out.append(key + ret)
        if ev[1] > t_max:
            break

    return out


def difference_on_key(dfa, dfb):
    t_max = dfa.tf.max()

    out, cache, active_keys = [], defaultdict(difference_constructor), set()
    for event in events(dfa, dfb, difference_order, reference=True):
        ev, k = event[:3], event[3:]
        keys = ([k] if ev[0] else active_keys)
        for key in keys:
            ret = update_cache_difference(cache[key], ev)
            if ret is not None:
                out.append(key + ret)
        if ev[0]:
            if k in active_keys:
                active_keys.remove(k)
            else:
                active_keys.add(k)
        if ev[1] > t_max:
            break

    return out


# issuper
# Assumes merged intervals
def issuper_constructor():
    return [None]


def update_cache_issuper(l, event):
    r, t, s = event
    if r:
        l[0] = (t if s else None)
    else:
        if l[0] is None:
            return True
    return False


def issuper_no_key(dfa, dfb):
    t_max = dfb.tf.max()

    cache = [None]
    for ev in events(dfa, dfb, issuper_order, reference=True):
        if update_cache_issuper(cache, ev[:3]):
            return False
        if ev[1] > t_max:
            break

    return True


def issuper_by_key(dfa, dfb):
    t_max = dfb.tf.max()

    cache = defaultdict(issuper_constructor)
    for event in events(dfa, dfb, issuper_order, reference=True):
        ev, key = event[:3], event[3:]
        if update_cache_issuper(cache[key], ev):
            return False
        if ev[1] > t_max:
            break

    return True


# nonempty_intersection
# Assumes merged intervals
def nonempty_intersection_constructor():
    return [0]


def update_cache_nonempty_intersection(l, ev):
    if ev[0]:
        l[0] += (1 if ev[2] else -1)
    else:
        if l[0] > 0:
            return True
    return False


def nonempty_intersection_no_key(dfa, dfb):
    t_max = dfb.tf.max()

    cache = nonempty_intersection_constructor()
    for ev in events(dfa, dfb, nonempty_intersection_order, reference=True):
        if update_cache_nonempty_intersection(cache, ev):
            return True
        if ev[1] > t_max:
            break

    return False


def nonempty_intersection_by_key(dfa, dfb):
    t_max = dfb.tf.max()

    cache = defaultdict(nonempty_intersection_constructor)
    for event in events(dfa, dfb, nonempty_intersection_order, reference=True):
        ev, key = event[:3], event[3:]
        if update_cache_nonempty_intersection(cache[key], ev):
            return True
        if ev[1] > t_max:
            break

    return False


def cartesian_intersection_constructor():
    return (set(), set())


def cartesian_intersection(df, base_df):
    # Intersect
    out, b_current, e, b_area = [], defaultdict(cartesian_intersection_constructor), {}, set()
    for col in events(base_df, df, cs_order, reference=True):
        r, t, f = col[:3]
        if r:
            u = col[3]
            left, right = b_current[u]
            if f:
                for v in left:
                    e[(u, v)] = t
                for v in right:
                    e[(v, u)] = t
                b_area.add(u)
            else:
                for v in left:
                    if v in b_area:
                        out.append((u, v, e[(u, v)], t))
                    e[(u, v)] = t
                for v in right:
                    if v in b_area:
                        out.append((v, u, e[(v, u)], t))
                    e[(v, u)] = t
                b_area.remove(u)
        else:
            u, v = col[3], col[4]
            if (u, v) in e and t >= e[(u, v)] and v in b_current[u][0] and u in b_area and v in b_area:
                out.append((u, v, e[(u, v)], t))

            if f:
                b_current[u][0].add(v)
                b_current[v][1].add(u)
                e[(u, v)] = t
            else:
                b_current[u][0].remove(v)
                b_current[v][1].remove(u)
                e[(u, v)] = t + 1

    return out


def map_intersection_constructor():
    # count r - count r',
    # ta, tb, interval_type_start (list),  interval_type_finish (list)
    return [None, None, 0]


def map_intersection(df, base_df):
    out, cache, active_links, active_nodes = [], defaultdict(map_intersection_constructor), defaultdict(set), dict()
    for event in events(df, base_df, issuper_order, True):
        r, time, start = event[0:3]
        if r:
            u, v = event[3], event[4]
            if start:
                if cache[v][0] is None:
                    # Keep the first
                    cache[v][0] = time
                if u in active_nodes:
                    if cache[v][1] is None:
                        cache[v][1] = active_nodes[u]
                    cache[v][2] += 1
                active_links[u].add(v)
            else:
                active_links[u].remove(v)
                if cache[v][2] != 0:
                    out.append((v, cache[v][1], time))
                cache[v][0] = None
        else:
            u = event[3]
            if start:
                for v in active_links[u]:
                    if cache[v][1] is None:
                        # Keep the first
                        cache[v][1] = time
                    cache[v][2] += 1
                active_nodes[u] = time
            else:
                for v in active_links[u]:
                    cache[v][2] -= 1
                    if cache[v][0] is not None and cache[v][2] == 0:
                        out.append((v, max(cache[v][0], cache[v][1]), time))
                        cache[v][1] = None
                active_nodes.pop(u, None)
    return out


def interval_intersection_size(a, b):
    wa, wb, ts, count = 0, 0, None, 0
    for r, t, s in events(DF([a.ts, a.tf]).T, DF([b.ts, b.tf]).T, key=iis_order, reference=True):
        if ts is not None:
            if s:
                if t > ts:
                    count += wa*wb*(t - ts)
            else:
                count += wa*wb*(t - ts + 1)
        if r:
            wa += (1 if s else -1)
        else:
            wb += (1 if s else -1)
        ts = (t + int(not s) if wa > 0 and wb > 0 else None)

    return count
