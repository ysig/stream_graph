from itertools import chain


def events(a, b, key, reference=False, weights=False):
    return sorted(events_not_sorted(a, b, reference=reference, weights=weights), key=key)


def events_uni(df, key, weights=False):
    if weights:
        return sorted((ev for it in df.itertuples(name=None, index=False, weights=True) for ev in [(it[-3], True, it[-1]) + it[:-3], (it[-2], False, it[-1]) + it[:-3]]), key=key)
    else:
        return sorted((ev for it in df.itertuples(name=None, index=False) for ev in [(it[-2], True) + it[:-2], (it[-1], False) + it[:-2]]), key=key)


def events_not_sorted(a, b, reference=False, weights=False):
    # Assumes that a and b have the same columns)
    if weights:
        if reference:
            return chain(make_weights_reference(a, True), make_weights_reference(b, False))
        else:
            return chain(make_weights(a), make_weights(b))
    else:
        if reference:
            return (ev for i, df in enumerate([a, b]) for it in df.itertuples(name=None, index=False) for ev in [(bool(1 - i), it[-2], True) + it[:-2], (bool(1 - i), it[-1], False) + it[:-2]])
        else:
            return (ev for it in chain(*[df.itertuples(name=None, index=False) for df in (a, b)]) for ev in [(it[-2], True) + it[:-2], (it[-1], False) + it[:-2]])


def make_weights(a):
    try:
        return (ev for it in a.itertuples(weights=True) for ev in [(it[-3], True, it[-1]) + it[:-3], (it[-2], False, it[-1]) + it[:-3]])
    except TypeError as te:
        if str(te) != 'itertuples() got an unexpected keyword argument \'weights\'':
            raise
        else:
            return (ev for it in a.itertuples() for ev in [(it[-2], True, 1) + it[:-2], (it[-1], False, 1) + it[:-2]])


def make_weights_reference(a, f):
    try:
        return (ev for it in a.itertuples(weights=True) for ev in [(f, it[-3], True, it[-1]) + it[:-3], (f, it[-2], False, it[-1]) + it[:-3]])
    except TypeError as te:
        if str(te) != 'itertuples() got an unexpected keyword argument \'weights\'':
            raise
        else:
            return (ev for it in a.itertuples() for ev in [(f, it[-2], True, 1) + it[:-2], (f, it[-1], False, 1) + it[:-2]])
