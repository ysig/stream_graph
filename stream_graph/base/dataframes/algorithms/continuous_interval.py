from __future__ import absolute_import
from collections import defaultdict
from six import iteritems
from .utils.bounds import events
from .utils.bounds import events_uni
from .utils.no_bounds import events as events_nb
from .utils.misc import get_key_set
from .utils.orderings import b_order_0_2 as merge_order
from .utils.orderings import b_order_0_1n2_n2 as union_order
from .utils.orderings import rb_order_1_n3_2n3 as intersection_order
from .utils.orderings import rb_order_1_n3_0n3_2n3 as difference_order
from .utils.orderings import rb_order_1_n3_2n3_0n3 as issuper_order
from .utils.orderings import rb_order_1_n2_2e3_0e2 as cs_order
from .utils.orderings import rb_order_1_n2_2e3_0e3 as mi_order
from .utils.orderings import rb_order_nonempty_intersection as nei_order
from .utils.orderings import r_order_1_2_0 as iis_order


# Merge
def update_cache_merge(d, event):
    ev, s = event[:2], event[2]
    if not len(d):
        d[s] = ev
    elif len(d) == 1:
        ev_d = d.get(s, None)
        if ev_d is None:
            d[s] = ev
        elif ev[0] == ev_d[0]:
            d[s] = (ev[0], ev[1] or ev_d[1])
    elif len(d) == 2:
        ev_df = d.pop(False, None)
        if s:
            # start
            if not (ev_df[0] == ev[0] and (ev_df[1] or ev[1])):
                ev_ds = d.pop(True, None)
                d[s] = ev
                return (ev_ds[0], ev_df[0], ev_ds[1], ev_df[1])
        else:
            d[s] = max(ev_df, ev)


def merge_no_key(df):
    out, cache = [], dict()

    for event in events_uni(df, merge_order):
        ret = update_cache_merge(cache, event)
        if ret is not None:
            out.append(ret)

    if len(cache) == 2:
        ev_ds, ev_df = cache.pop(True, None), cache.pop(False, None)
        out.append((ev_ds[0], ev_df[0], ev_ds[1], ev_df[1]))

    return out


def merge_by_key(df):
    # Internal
    out, cache = [], defaultdict(dict)
    for event in events_uni(df, merge_order):
        ev, key = event[:3], event[3:]
        ret = update_cache_merge(cache[key], ev)
        if ret is not None:
            out.append(key + ret)

    for key, rem in iteritems(cache):
        if len(rem) == 2:
            ev_ds, ev_df = cache[key].pop(True, None), cache[key].pop(False, None)
            out.append(key + (ev_ds[0], ev_df[0], ev_ds[1], ev_df[1]))

    return out


# Union [assumes merged interval-df]
def union_cache_constructor():
    return [0, None]


def update_cache_union(d, event):
    ev, s = event[:2], event[2]
    if d[0] == 0 and s:
        d[1] = ev
    elif d[0] == 1 and not s:
        d[0] -= 1
        return (d[1][0], ev[0], d[1][1], ev[1])
    d[0] += (1 if s else -1)


def union_no_key(dfa, dfb):
    out, cache = [], union_cache_constructor()
    for event in events(dfa, dfb, union_order):
        ret = update_cache_union(cache, event)
        if ret is not None:
            out.append(ret)

    return out


def union_by_key(dfa, dfb):
    # Internal
    out, cache = [], defaultdict(union_cache_constructor)
    for event in events(dfa, dfb, union_order):
        ev, key = event[:3], event[3:]
        ret = update_cache_union(cache[key], ev)
        if ret is not None:
            out.append(key + ret)

    return out


def union_on_key(df, kdf):
    all_keys = get_key_set(df)
    out, cache = [], defaultdict(union_cache_constructor)
    for col in events(df, kdf, union_order):
        ev, k = col[:3], col[3:]
        keys = ([k] if len(k) > 0 else all_keys)
        for key in keys:
            ret = update_cache_union(cache[key], ev)
            if ret is not None:
                out.append(key + ret)

    return out


# Intersection [assumes merged interval-df]
def cache_intersection_constructor():
    return [False, False, None]


def update_cache_intersection(l, event):
    r, ev, s, out = event[0], event[1:3], event[3], None
    if l[int(not r)]:
        # Case one active part
        if s:
            # Add the start of the other
            l[2] = ev
        elif l[2] is not None:
            e, f, l[2] = l[2][0], l[2][1], None
            if ev[0] != e or (f and ev[1]):
                out = (e, ev[0], f, ev[1])
    l[int(r)] = s
    return out


def intersection_no_key(dfa, dfb):
    t_max = min(dfa.tf.max(), dfb.tf.max())

    out, cache = [], cache_intersection_constructor()
    for ev in events(dfa, dfb, intersection_order, True):
        ret = update_cache_intersection(cache, ev)
        if ret is not None:
            out.append(ret)
        if ev[1] > t_max:
            break

    return out


def intersection_by_key(dfa, dfb):
    t_max = min(dfa.tf.max(), dfb.tf.max())

    out, cache = [], defaultdict(cache_intersection_constructor)
    for event in events(dfa, dfb, intersection_order, True):
        ev, key = event[:4], event[4:]
        ret = update_cache_intersection(cache[key], ev)
        if ret is not None:
            out.append(key + ret)
        if ev[1] > t_max:
            break

    return out


def intersection_on_key(dfa, dfb):
    t_max = min(dfa.tf.max(), dfb.tf.max())

    out, cache, active_keys, active_envelope = [], dict(), set(), False
    for event in events(dfa, dfb, intersection_order, True):
        r, ev, s, k = event[0], event[1:3], event[3], event[4:]
        if r:
            if s:
                cache[k] = ev
                active_keys.add(k)
            else:
                a = cache.pop(k, None)
                if active_envelope:
                    e, f = a
                    if ev[0] != e or (f and ev[1]):
                        out.append(k + (e, ev[0], f, ev[1]))
                active_keys.remove(k)
        else:
            if s:
                for key in active_keys:
                    cache[key] = ev
                active_envelope = True
            else:
                for key in active_keys:
                    e, f = cache.pop(key)
                    if ev[0] != e or (f and ev[1]):
                        out.append(key + (e, ev[0], f, ev[1]))
                active_envelope = False
        if ev[1] > t_max:
            break

    return out


# Difference [assumes merged interval-df]
def cache_difference_constructor():
    return [False, False, None]


def update_cache_difference(l, event):
    r, ev, s, out = event[0], event[1:3], event[3], None
    if s:
        if r and not l[0]:
            # start and reference and the other is not active
            l[2] = ev
        elif not r and l[1]:
            # start and not reference and the reference is active
            assert l[2] is not None
            e, f, l[2] = l[2][0], l[2][1], None
            if ev[0] == e and f and not ev[1]:
                out = (ev[0], ev[0], True, True)
            elif ev[0] > e:
                out = (e, ev[0], f, not ev[1])
    else:
        if r and not l[0]:
            # finish and reference and the other is not active
            e, f, l[2] = l[2][0], l[2][1], None
            if ev[0] != e or (f and ev[1]):
                out = (e, ev[0], f, ev[1])
        elif not r and l[1]:
            l[2] = (ev[0], not ev[1])
    l[int(r)] = s
    return out


def difference_no_key(dfa, dfb):
    t_max = dfa.tf.max()

    out, cache = [], cache_difference_constructor()
    for ev in events(dfa, dfb, difference_order, True):
        ret = update_cache_difference(cache, ev)
        if ret is not None:
            out.append(ret)
        if ev[1] > t_max:
            break

    return out


def difference_by_key(dfa, dfb):
    t_max = dfa.tf.max()

    out, cache = [], defaultdict(cache_difference_constructor)
    for event in events(dfa, dfb, difference_order, True):
        ev, key = event[:4], event[4:]
        ret = update_cache_difference(cache[key], ev)
        if ret is not None:
            out.append(key + ret)
        if ev[1] > t_max:
            break

    return out


def difference_on_key(dfa, dfb):
    t_max = dfa.tf.max()

    out, cache, active_keys = [], defaultdict(cache_difference_constructor), set()
    for event in events(dfa, dfb, difference_order, True):
        ev, k = event[:4], event[4:]
        not_envelope = len(k) > 0
        keys = ([k] if not_envelope else active_keys)
        for key in keys:
            ret = update_cache_difference(cache[key], ev)
            if ret is not None:
                out.append(key + ret)
        if not_envelope:
            if k in active_keys:
                active_keys.remove(k)
            else:
                active_keys.add(k)
        if ev[1] > t_max:
            break

    return out


# issuper [assumes merged interval-df]
def cache_issuper_constructor():
    return [None]


def update_cache_issuper(l, event):
    r, ev, s = event[0], event[1:3], event[3]
    if r:
        l[0] = (ev if s else None)
    else:
        if (l[0] is None or (not s and l[0][0] == ev[0] and l[0][1] < ev[1])):
            return True
    return False


def issuper_no_key(dfa, dfb):
    t_max = dfb.tf.max()

    cache = cache_issuper_constructor()
    for ev in events(dfa, dfb, issuper_order, reference=True):
        if update_cache_issuper(cache, ev[:4]):
            return False
        if ev[1] > t_max:
            break

    return True


def issuper_by_key(dfa, dfb):
    t_max = dfb.tf.max()

    cache = defaultdict(cache_issuper_constructor)
    for event in events(dfa, dfb, issuper_order, True):
        ev, key = event[:4], event[4:]
        if update_cache_issuper(cache[key], ev):
            return False
        if ev[1] > t_max:
            break

    return True


# nonempty_intersection [assumes merged interval-df]
def cache_nonempty_intersection_constructor():
    return [0]


def update_cache_nonempty_intersection(l, event):
    r, s = event[0], event[3]
    if r:
        l[0] += (1 if s else -1)
    else:
        if l[0] > 0:
            return True
    return False


def nonempty_intersection_no_key(dfa, dfb):
    t_max = dfb.tf.max()

    cache = cache_nonempty_intersection_constructor()
    for ev in events(dfa, dfb, nei_order, reference=True):
        if update_cache_nonempty_intersection(cache, ev):
            return True
        if ev[1] > t_max:
            break

    return False


def nonempty_intersection_by_key(dfa, dfb):
    t_max = dfb.tf.max()

    cache = defaultdict(cache_nonempty_intersection_constructor)
    for event in events(dfa, dfb, nei_order, reference=True):
        ev, key = event[:4], event[4:]
        if update_cache_nonempty_intersection(cache[key], ev):
            return True
        if ev[1] > t_max:
            break

    return False


def ci_constructor():
    return (set(), set())


def cartesian_intersection(df, base_df):
    # Assume merged dfs
    out, cache, active_nodes, active_keys = [], {}, set(), defaultdict(ci_constructor)
    for event in events(base_df, df, nei_order, reference=True):
        r, ev, s, key = event[0], event[1:3], event[3], event[4:]
        if r:
            u = key[0]
            lefts, rights = active_keys[u]

            if s:
                # Activate nodes
                for v in lefts:
                    cache[(u, v)] = ev
                for v in rights:
                    cache[(v, u)] = ev
                active_nodes.add(u)
            else:
                for v in lefts:
                    e = cache.pop((u, v), None)
                    if e is not None and (e[0] != ev[0] or (e[1] and ev[1])):
                        out.append((u, v, e[0], ev[0], e[1], ev[1]))
                for v in rights:
                    e = cache.pop((v, u), None)
                    if e is not None and (e[0] != ev[0] or (e[1] and ev[1])):
                        out.append((v, u, e[0], ev[0], e[1], ev[1]))
                active_nodes.remove(u)
        else:
            u, v = key
            if s:
                if u in active_nodes and v in active_nodes:
                    cache[(u, v)] = ev
                active_keys[u][0].add(v)
                active_keys[v][1].add(u)
            else:
                if u in active_nodes and v in active_nodes:
                    e = cache.pop((u, v), None)
                    if e is not None and (e[0] != ev[0] or (e[1] and ev[1])):
                        out.append((u, v, e[0], ev[0], e[1], ev[1]))
                active_keys[u][0].remove(v)
                active_keys[v][1].remove(u)

    return out


def map_intersection_constructor():
    # count r - count r',
    # start, finish, interval_type_start (list),  interval_type_finish (list)
    return [[None, None], [None, None], 0, 0]


def output_interval(v, cache, out):
    if cache[0][0] is None:
        a, b = cache[1][0]
    elif cache[1][0] is None:
        a, b = cache[0][0]
    else:
        ts_a, its_a = cache[0][0]
        ts_b, its_b = cache[1][0]

        if ts_a > ts_b:
            a, b = ts_a, its_a
        elif ts_a < ts_b:
            a, b = ts_b, its_b
        elif its_a < its_b:
            a, b = ts_a, its_a
        else:
            a, b = ts_a, its_a

    if cache[0][1] is None:
        c, d = cache[1][1]
    elif cache[1][1] is None:
        c, d = cache[0][1]
    else:
        tf_a, itf_a = cache[0][1]
        tf_b, itf_b = cache[1][1]

        if tf_a > tf_b:
            c, d = tf_a, itf_a
        elif tf_a < tf_b:
            c, d = tf_b, itf_b
        elif itf_a < itf_b:
            c, d = tf_a, itf_a
        else:
            c, d = tf_a, itf_a

    if a != b or c < d:
        out.append((v, a, c, b, d))


def map_intersection(df, base_df):
    out, cache, active_links, active_nodes = [], defaultdict(map_intersection_constructor), defaultdict(set), dict()
    for event in events(base_df, df, mi_order, True):
        r, time, interval_type, start = event[0:4]
        key = event[4:]
        if r:
            u = key[0]
            if start:
                for v in active_links[u]:
                    if cache[v][1][0] is None or (cache[v][1][0][0] == time and cache[v][1][0][1] < interval_type):
                        # Keep the first
                        cache[v][1][0] = (time, interval_type)
                    cache[v][3] += 1
                active_nodes[u] = (time, interval_type)
            else:
                for v in active_links[u]:
                    if cache[v][1][1] is None or (cache[v][1][1][0] < time or cache[v][1][1][1] < interval_type):
                        # Keep the last
                        cache[v][1][1] = (time, interval_type)
                    cache[v][3] -= 1
                    if cache[v][3] == 0 and cache[v][2] != 0:
                        output_interval(v, cache[v], out)
                        cache[v][1] = [None, None]
                active_nodes.pop(u, None)
        else:
            u, v = key
            if start:
                if u in active_nodes:
                    if cache[v][1][0] is None:
                        cache[v][1][0] = active_nodes[u]
                    cache[v][3] += 1
                if cache[v][0][0] is None or (cache[v][0][0][0] == time and cache[v][0][0][1] < interval_type):
                    # Keep the first
                    cache[v][0][0] = (time, interval_type)
                cache[v][2] += 1
                active_links[u].add(v)
            else:
                cache[v][2] -= 1
                active_links[u].remove(v)
                if cache[v][0][1] is None or (cache[v][0][1][0] < time or cache[v][0][1][1] < interval_type):
                    # Keep the last
                    cache[v][0][1] = (time, interval_type)
                if cache[v][2] == 0 and cache[v][3] != 0:
                    output_interval(v, cache[v], out)
                    cache[v][0], cache[v][1] = [None, None], [None, None]

    return out


def interval_intersection_size(a, b):
    wa, wb, ts, count = 0, 0, None, 0
    for r, t, s in events_nb(a[['ts', 'tf']], b[['ts', 'tf']], key=iis_order, reference=True):
        if ts is not None:
            count += wa*wb*(t-ts)
        if r:
            wa += (1 if s else -1)
        else:
            wb += (1 if s else -1)
        ts = (t if wa > 0 and wb > 0 else None)

    return count
