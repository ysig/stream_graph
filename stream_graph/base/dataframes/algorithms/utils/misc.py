from six import iteritems, viewkeys
from collections import defaultdict


def dict_tuple():
    return (dict(), dict())


def counter_to_iter(ct):
    return [k for k, v in iteritems(ct) for _ in range(v)]


def get_key_set(a):
    return set(it[:-2] for it in a.itertuples())


def hinge_loss(a, b):
    v = max(a - b, 0)
    if v > 0:
        return v


def first(*args):
    return args[0]


def truer(*args):
    return True


def noner(*args):
    return None


def oner(*args):
    return 1


def min_ignore_zero(a, b):
    m = min(a, b)
    if m > 0:
        return m


def min_sumer(ia, ib):
    return min(sum(ia), sum(ib))


def iter_dmerge_(d_a, d_b, function):
    for t in viewkeys(d_a) | viewkeys(d_b):
        a, b = d_a.get(t, None), d_b.get(t, None)
        if a is None:
            yield t, b
        else:
            if b is None:
                yield t, a
            else:
                yield t, function(a, b)


def set_tuple():
    return (set(), set())


def print_by_key(ks, ds, iter_, map_):
    d = defaultdict(list)
    for it in iter_:
        d[it[ks]].append(it[ds])
    print({k: map_(d[k]) for k in d.keys()})
