from itertools import chain


def events_uni(a, key, weights=False):
    return sorted(events_uni_ns(a, weights=weights), key=key)


def events_uni_ns(a, weights):
    # Assumes that a and b have the same columns)
    if weights:
        return (ev for it in a.itertuples(bounds=True, weights=True) for ev in [(it[-5], it[-3], True, it[-1]) + it[:-5], (it[-4], it[-2], False, it[-1]) + it[:-5]])
    else:
        return (ev for it in a.itertuples(bounds=True) for ev in [(it[-4], it[-2], True) + it[:-4], (it[-3], it[-1], False) + it[:-4]])


def events(a, b, key, reference=False, weights=False):
    return sorted(events_ns(a, b, reference=reference, weights=weights), key=key)


def events_ns(a, b, reference=False, weights=False):
    # Assumes that a and b have the same columns)
    if weights:
        if reference:
            return chain(make_weights_reference(a, True), make_weights_reference(b, False))
        else:
            return chain(make_weights(a), make_weights(b))
    else:
        if reference:
            return (ev for i, df in enumerate([a, b]) for it in df.itertuples(bounds=True) for ev in [(bool(1-i), it[-4], it[-2], True) + it[:-4], (bool(1-i), it[-3], it[-1], False) + it[:-4]])
        else:
            return (ev for it in chain(*[df.itertuples(bounds=True) for df in (a, b)]) for ev in [(it[-4], it[-2], True) + it[:-4], (it[-3], it[-1], False) + it[:-4]])


def make_weights(a):
    try:
        return (ev for it in a.itertuples(bounds=True, weights=True) for ev in [(it[-5], it[-3], True, it[-1]) + it[:-5], (it[-4], it[-2], False, it[-1]) + it[:-5]])
    except TypeError as te:
        if str(te) != 'itertuples() got an unexpected keyword argument \'weights\'':
            raise
        else:
            return (ev for it in a.itertuples(bounds=True) for ev in [(it[-4], it[-2], True, 1) + it[:-4], (it[-3], it[-1], False, 1) + it[:-4]])


def make_weights_reference(a, f):
    try:
        return (ev for it in a.itertuples(bounds=True, weights=True) for ev in [(f, it[-5], it[-3], True, it[-1]) + it[:-5], (f, it[-4], it[-2], False, it[-1]) + it[:-5]])
    except TypeError as te:
        if str(te) != 'itertuples() got an unexpected keyword argument \'weights\'':
            raise
        else:
            return (ev for it in a.itertuples(bounds=True) for ev in [(f, it[-4], it[-2], True, 1) + it[:-4], (f, it[-3], it[-1], False, 1) + it[:-4]])


def valid_interval(value):
    return value[0] != value[1] or (value[2] and value[3])
