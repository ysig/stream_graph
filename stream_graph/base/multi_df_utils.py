"""This file contains a set of functions for handling multiple DF class, in a unified manner."""

from itertools import tee
from warnings import warn

import pandas as pd
import numpy as np
from nose.tools import assert_equal

from operator import ne
from six import itervalues, iteritems
from collections import defaultdict
from .dataframes import CIntervalDF
from .dataframes import DIntervalDF
from .dataframes import CIntervalWDF
from .dataframes import DIntervalWDF
from .dataframes import InstantaneousDF
from .dataframes import InstantaneousWDF
from stream_graph.exceptions import UnrecognizedDirection


def load_instantaneous_df(df, no_duplicates, weighted, keys=[], merge_function=None):
    """Transform a bunch of input types to a Instantaneous(W)DF."""
    is_df = isinstance(df, pd.DataFrame)
    if not ((df is None) or (is_df and df.empty) or (not is_df and not bool(df))):
        is_idf, is_iwdf = isinstance(df, InstantaneousDF), isinstance(df, InstantaneousWDF)
        if not (is_idf or is_iwdf):
            # we don't the same input type as the output
            try:
                an = True
                if is_df:
                    # If it already a pandas dataframe
                    weighted = (weighted in df.columns or len(df.columns) == len(keys) + 2 if weighted is None else weighted)
                    ec = (['ts', 'w'] if weighted else ['ts'])
                    try:
                        # get the columns you need
                        df, an = df[keys + ec], False
                    except Exception:
                        pass
                else:
                    # else cast
                    df = pd.DataFrame(list(df))
                    weighted = (len(df.columns) == len(keys) + 2 if weighted is None else weighted)
                    ec = (['ts', 'w'] if weighted else ['ts'])
                if an:
                    # if needed retrieve the desired columns
                    df = df[df.columns[:len(keys) + len(ec)]]
                    # and assign names
                    df.columns = keys + ec
            except Exception:
                raise ValueError('An Iterable input should consist of either 3 or 4 elements')
            return init_instantaneous_df(data=df, weighted=weighted, no_duplicates=no_duplicates, keys=keys, merge_function=merge_function), weighted
        elif weighted is None or (weighted is True and is_iwdf):
            df = df.copy()
        elif weighted is True and is_idf:
            df = init_instantaneous_df(data=df, weighted=True, no_duplicates=no_duplicates, keys=keys, merge_function=merge_function)
        else:
            df = df[keys + ['ts']]
        return df, isinstance(df, InstantaneousWDF)
    else:
        return None, None


def load_interval_wdf(df, discrete, weighted, disjoint_intervals, default_closed, keys=[], merge_function=None):
    """Transform a bunch of input types to a {C,D}Interval(W)DF."""
    if isinstance(df, (CIntervalDF, CIntervalWDF, DIntervalDF, DIntervalWDF)):
        obj = (df if disjoint_intervals else df.merge(inplace=False))
    else:
        weighted = (False if weighted is None else weighted)
        discrete = (True if discrete is None else discrete)
        if isinstance(df, (InstantaneousDF, InstantaneousWDF)):
            obj = init_interval_df(data=df, discrete=discrete, weighted=weighted, keys=keys, disjoint_intervals=disjoint_intervals)
        elif isinstance(df, pd.DataFrame):
            if 'tf' not in df.columns:
                df['tf'] = df['ts']
            if discrete:
                obj = init_interval_df(data=df, discrete=True, weighted=weighted, keys=keys, disjoint_intervals=disjoint_intervals)
            else:
                s, f = ((True, True) if default_closed is None else _closed_to_tuple(default_closed))
                if 'itype' in df.columns:
                    # Check if itype exists and transform it, to 's', 'f' columns
                    s, f = zip(((s, f) if k in [pd.NaT, np.NaN] else _closed_to_tuple(k)) for k in df['itype'])
                    df['s'], df['f'] = list(s), list(f)
                    df = df.drop(columns='itype')
                elif 's' in df.columns:
                    if 'f' not in df.columns:
                        df['f'] = f
                elif 'f' in df.columns:
                    df['s'] = s
                else:
                    df['s'], df['f'] = s, f
                obj = init_interval_df(data=df, discrete=False, weighted=weighted, keys=keys, merge_function=merge_function, disjoint_intervals=disjoint_intervals)
        elif hasattr(df, '__iter__'):
            # Differentiate which case of function we need for parsing intervals
            if discrete:
                if weighted:
                    obj = _load_dinterval_wdf_iterable(df, keys, disjoint_intervals, merge_function)
                else:
                    obj = _load_dinterval_df_iterable(df, keys, disjoint_intervals)
            else:
                if weighted:
                    s, f = ((True, False) if default_closed is None else _closed_to_tuple(default_closed))
                    obj = _load_cinterval_wdf_iterable(df, keys, s, f, disjoint_intervals, merge_function)
                else:
                    s, f = ((True, True) if default_closed is None else _closed_to_tuple(default_closed))
                    obj = _load_cinterval_df_iterable(df, keys, s, f, disjoint_intervals)
        else:
            raise ValueError('unrecognized input')
    return obj, isinstance(obj, (DIntervalDF, DIntervalWDF)), isinstance(obj, (CIntervalWDF, DIntervalWDF))


def _load_dinterval_wdf_iterable(df, keys, disjoint_intervals, merge_function):
    """Store a discrete time-signature iterable with weights to DIntervalWDF."""
    l, lks = list(), len(keys)
    for k in df:
        lk = len(k)
        assert lk >= lks
        if lk == lks + 3:
            l.append(k)
        elif lk == lks + 2:
            # Instants
            l.append(k[:-1] + (k[-2], k[-1]))
        else:
            raise ValueError('Invalid input ')
    return DIntervalWDF(l, disjoint_intervals=disjoint_intervals, columns=keys + ['ts', 'tf', 'w'], merge_function=merge_function)


def _load_cinterval_wdf_iterable(df, keys, s, f, disjoint_intervals, merge_function):
    """Store a continuous time-signature iterable to CIntervalWDF."""
    inp = list()
    len_keys = len(keys)
    for k in df:
        len_key = len(k)
        assert len_key >= len_keys
        if len_key == len_keys + 3:
            # Intervals
            inp.append(k[:-1] + (s, f, k[-1]))
        elif len_key == len_keys + 4:
            # Intervals with bounds
            inp.append(k[:-2] + _closed_to_tuple(k[-2]) + (k[-1],))
        elif len_key == len_keys + 5:
            # Intervals with bounds
            inp.append(k)
        elif len_key == len_keys + 2:
            # Instants
            inp.append(k[:-1] + (k[-2], True, True, k[-1]))
        else:
            raise ValueError('Invalid input ')
    return CIntervalWDF(inp, columns=keys + ['ts', 'tf', 's', 'f', 'w'], disjoint_intervals=disjoint_intervals, merge_function=merge_function)


def weighted_iter(df):
    """Return an iterator of a dataframe that contains also weights if there are."""
    if isinstance(df, (DIntervalDF, CIntervalDF, InstantaneousDF)):
        return df.itertuples()
    else:
        return df.itertuples(weights=True)


def idf_to_df(df, discrete=None):
    """Transforms a instantaneous dataframe to a non-instantaneous."""
    df = pd.DataFrame(df)
    df['tf'] = df['ts']
    discrete = (df.ts.dtype.kind == 'i' if discrete is None else discrete)
    if discrete:
        return DIntervalDF(df)
    else:
        df['s'] = True
        df['f'] = True
        return CIntervalDF(df, no_duplicates=True)


def load_interval_df(df, discrete, default_closed, disjoint_intervals, keys=[]):
    """Transform a bunch of input types to a {C,D}IntervalDF."""
    if isinstance(df, DIntervalDF):
        if discrete is False:
            warn('Ignore \'False\' discrete argument as object seems to be discrete')
        assert_equal(set(df.columns), set(keys) | {'ts', 'tf'})
        return (df if disjoint_intervals else df.merge(inplace=False)), True
    elif isinstance(df, CIntervalDF):
        if discrete is True:
            warn('Ignore \'True\' discrete argument as object seems to be continuous')
        assert_equal(set(df.columns), set(keys) | {'ts', 'tf', 's', 'f'})
        return (df if disjoint_intervals else df.merge(inplace=False)), False
    elif isinstance(df, InstantaneousDF):
        df = idf_to_df(df, discrete=discrete)
        return df, isinstance(df, DIntervalDF)
    elif isinstance(df, pd.DataFrame):
        ci, dcN = all(f not in df.columns for f in ['s', 'f', 'itype']), default_closed is None
        discrete = (ci and dcN and df.ts.dtype.kind == 'i' and df.tf.dtype.kind == 'i' if discrete is None else discrete)
        if 'tf' not in df.columns:
            df['tf'] = df['ts']
        if discrete:
            return DIntervalDF(df[keys + ['ts', 'tf']], disjoint_intervals=disjoint_intervals), True
        else:
            s, f = ((True, True) if default_closed is None else _closed_to_tuple(default_closed))

            if 'itype' in df.columns:
                # Check if itype exists and transform, to 's', 'f' columns
                l = [k[:-1] + ((s, f) if k[-1] in [pd.NaT, np.NaN] else _closed_to_tuple(k[-1])) for k in df[keys + ['ts', 'tf', 'itype']].itertuples(name=None, index=False)]
                return CIntervalDF(l, disjoint_intervals=disjoint_intervals, columns=keys + ['ts', 'tf', 's', 'f']), False
            elif 's' in df.columns:
                if 'f' not in df.columns:
                    df['f'] = f
            elif 'f' in df.columns:
                df['s'] = s
            else:
                df['s'] = s
                df['f'] = f
            return CIntervalDF(df, disjoint_intervals=disjoint_intervals, columns=keys + ['ts', 'tf', 's', 'f']), False
    elif hasattr(df, '__iter__'):
        # Differentiate on interval type
        discrete = (True if discrete is None else discrete)
        if discrete:
            return _load_dinterval_df_iterable(df, keys, disjoint_intervals), True
        else:
            s, f = ((True, True) if default_closed is None else _closed_to_tuple(default_closed))
            return _load_cinterval_df_iterable(df, keys, s, f, disjoint_intervals=disjoint_intervals), False
    else:
        raise ValueError('unrecognized input')
    return (DIntervalDF if discrete else CIntervalDF)


def has_elements(iter):
    """Check if an iterator has elements."""
    iter, any_check = tee(iter)
    try:
        any_check.next()
        return True, iter
    except StopIteration:
        return False, iter


def _load_dinterval_df_iterable(df, keys, disjoint_intervals):
    """Tranform an iterable to a discrete time-signature dataframe."""
    l, lks = list(), len(keys)
    for k in df:
        lk = len(k)
        assert lk >= lks
        if lk == lks + 2:
            l.append(k)
        elif lk == lks + 1:
            # Instants
            l.append(k + (k[-1], ))
        else:
            raise ValueError('Invalid input ')
    return DIntervalDF(l, disjoint_intervals=disjoint_intervals, columns=keys + ['ts', 'tf'])


def _load_cinterval_df_iterable(df, keys, s, f, disjoint_intervals):
    """Transform an iterable to a continuous time-signature dataframe."""
    inp = list()
    len_keys = len(keys)
    for key in df:
        len_key = len(key)
        assert len_key >= len_keys
        if len_key == len_keys + 2:
            # Intervals
            inp.append(key + (s, f))
        elif len_key == len_keys + 3:
            # Intervals with bounds
            inp.append(key[:-1] + _closed_to_tuple(key[-1]))
        elif len_key == len_keys + 1:
            # Instants
            inp.append(key + (key[-1], True, True))
        elif len_key == len_keys + 4:
            inp.append(key)
        else:
            raise ValueError('Invalid input ')
    return CIntervalDF(inp, columns=keys + ['ts', 'tf', 's', 'f'], disjoint_intervals=disjoint_intervals)


def columns_interval_df(discrete, weighted, keys=[]):
    """Obtain the columns of dataframe according with the time-signature, the keys and the weighting."""
    cols = ['ts', 'tf']
    if not discrete:
        cols.extend(['s', 'f'])
    if weighted:
        cols.append('w')
    return keys + cols


def class_interval_df(discrete=False, weighted=False):
    """Obtain the class of interval-dataframe according with the time-signature and the weighting."""
    return ((DIntervalWDF if discrete else CIntervalWDF) if weighted else (DIntervalDF if discrete else CIntervalDF))


def init_interval_df(data=[], discrete=False, weighted=False, disjoint_intervals=True, keys=[], merge_function=None):
    """Initialize a dataframe according with the time-signature and the weighting."""
    cols = columns_interval_df(discrete=discrete, weighted=weighted, keys=keys)
    class_ = class_interval_df(discrete=discrete, weighted=weighted)
    if weighted:
        return class_(data, columns=cols, disjoint_intervals=disjoint_intervals, merge_function=merge_function)
    else:
        return class_(data, columns=cols, disjoint_intervals=disjoint_intervals)


def columns_instantaneous_df(weighted, keys=[]):
    """Obtain the columns of dataframe according with the keys and the weighting."""
    cols = ['ts']
    if weighted:
        cols.append('w')
    return keys + cols


def class_instantaneous_df(weighted=False):
    """Obtain the class of instantaneous-dataframe according with the weighting."""
    return (InstantaneousWDF if weighted else InstantaneousDF)


def init_instantaneous_df(data=[], weighted=False, no_duplicates=True, keys=[], merge_function=None):
    """Obtain the class of interval-dataframe according with the weighting."""
    if weighted:
        return class_instantaneous_df(weighted)(data, columns=columns_instantaneous_df(weighted, keys), no_duplicates=no_duplicates, merge_function=merge_function)
    else:
        return class_instantaneous_df(weighted)(data, columns=columns_instantaneous_df(weighted, keys), no_duplicates=no_duplicates)


def _closed_to_tuple(cl):
    """Transform a closed identifier for bounds to booleans for left and right bound."""
    if cl == 'both':
        return True, True
    elif cl == 'left':
        return True, False
    elif cl == 'right':
        return False, True
    elif cl == 'neither':
        return False, False
    else:
        raise ValueError('Unrecognized interval-input argument')


def _tuple_to_closed(cl):
    """Transform a boolean for left and right bound of an interval to a closed identifier."""
    if cl == (True, True):
        return 'both'
    elif cl == (True, False):
        return 'left'
    elif cl == (False, True):
        return 'right'
    elif cl == (False, False):
        return 'neither'
    else:
        raise ValueError('Unrecognized interval-input argument')


def itertuples_pretty(df, discrete, weighted=False):
    """`df.itertuples` convert boolean for left and right closed bound boolean to a `closed` identifier."""
    if discrete:
        if weighted:
            return df.itertuples(weights=True)
        else:
            return df.itertuples()
    elif weighted:
        return (key[:-3] + (_tuple_to_closed(key[-3:-1]), key[-1]) for key in df.itertuples(bounds=True, weights=True))
    else:
        return (key[:-2] + (_tuple_to_closed(key[-2:]),) for key in df.itertuples(bounds=True))


def itertuples_raw(df, discrete, weighted=False):
    """`df.itertuples` in a raw form left and right bound."""
    kargs = {}
    if not discrete:
        kargs['bounds'] = True
    if weighted:
        kargs['weights'] = True
    return df.itertuples(**kargs)


def apply_direction_on_iter(iter_, direction='out'):
    if direction == 'in':
        iter_ = ((key[1], key[0]) + key[2:] for key in iter_)
    elif direction == 'both':
        iter_ = (it + key[2:] for key in iter_ for it in [(key[0], key[1]), (key[1], key[0])])
    elif direction != 'out':
        raise UnrecognizedDirection()
    return iter_


def itertuples_aw_(df, discrete, weighted, add_weights=True, direction='out'):
    """Iterates a dataframe as `(start, (time), key, w)` tuples."""
    iter_ = apply_direction_on_iter(itertuples_raw(df, discrete, weighted))
    if discrete:
        if weighted:
            return (o for i in iter_ for o in [(True, i[-3], i[:-3], i[-1]), (False, i[-2], i[:-3], i[-1])])
        elif add_weights:
            return (o for i in iter_ for o in [(True, i[-2], i[:-2], 1), (False, i[-1], i[:-2], 1)])
        else:
            return (o for i in iter_ for o in [(True, i[-2], i[:-2]), (False, i[-1], i[:-2])])
    elif weighted:
        return (o for i in iter_ for o in [(True, (i[-5], i[-3]), i[:-5], i[-1]), (False, (i[-4], i[-2]), i[:-5], i[-1])])
    elif add_weights:
        return (o for i in iter_ for o in [(True, (i[-4], i[-2]), i[:-4], 1), (False, (i[-3], i[-1]), i[:-4], 1)])
    else:
        return (o for i in iter_ for o in [(True, (i[-4], i[-2]), i[:-4]), (False, (i[-3], i[-1]), i[:-4])])


def build_time_generator(df, cache_constructor, calculate, value_inequality_condition=None, add_weights=False, get_key=None, direction='out', sparse=False):
    # Make iterator
    discrete, weighted = isinstance(df, (DIntervalDF, DIntervalWDF)), isinstance(df, (DIntervalWDF, CIntervalWDF))
    iter_ = itertuples_aw_(df, discrete, weighted, add_weights, direction)

    # Sort based on discrete and not-discrete
    if discrete:
        def key(k):
            # start < finish
            return (k[1], not k[0])
    else:
        def key(k):
            # [()]
            return (k[1][0], k[1][1], k[0] != k[1][1])

    sorted_iter_ = sorted(iter_, key=key)
    # Build functions
    if discrete:
        def time_continuation(b, e, f):
            return e + int(not f), b < e - 1

        def valid_instant(b, e):
            if b == e:
                return e + 1
    else:
        def time_continuation(b, e, f):
            # []
            return (e[0], f == e[1]), b[0] < e[0] or not (b[1] and e[1])

        def valid_instant(b, e):
            if b[0] == e[0] and b[1] and e[1]:
                return b[0], False

    if value_inequality_condition is None:
        value_inequality_condition = ne

    if get_key is None:
        if sparse:
            return build_time_generator_s(sorted_iter_, cache_constructor, calculate, time_continuation, valid_instant, value_inequality_condition)
        else:
            return build_time_generator_(sorted_iter_, cache_constructor, calculate, time_continuation, valid_instant, value_inequality_condition)
    else:
        if sparse:
            return build_time_generator_skey(sorted_iter_, get_key, cache_constructor, calculate, time_continuation, valid_instant, value_inequality_condition)
        else:
            return build_time_generator_key(sorted_iter_, get_key, cache_constructor, calculate, time_continuation, valid_instant, value_inequality_condition)
        assert callable(get_key)


def generator_step(i, lob, a, time_continuation, valid_instant, value_inequality_condition):
    if i[0]:
        if lob is None or i[1] == lob[0]:
            return False, (i[1], a)
        else:
            t_not, t_not_f = time_continuation(lob[0], i[1], True)
            if t_not_f or value_inequality_condition(lob[1], a):
                return True, (t_not, a)
    else:
        t_not = valid_instant(i[1], lob[0])
        if t_not is not None:
            return True, (t_not, a)
        else:
            t_not, t_not_f = time_continuation(lob[0], i[1], False)
            if t_not_f or value_inequality_condition(lob[1], a):
                return (t_not != lob[0]), (t_not, a)
    return False, lob


def generator_step_sparse(i, lob, a, time_continuation, valid_instant, value_inequality_condition):
    if i[0]:
        if lob is None:
            return False, (i[1], a, i[0])
        elif i[1] == lob[0]:
            return i[0] != lob[2], (i[1], a, i[0])
        else:
            t_not, t_not_f = time_continuation(lob[0], i[1], True)
            if t_not_f or value_inequality_condition(lob[1], a) or i[0] != lob[2]:
                return True, (t_not, a, i[0])
    else:
        t_not = valid_instant(i[1], lob[0])
        if t_not is not None:
            return True, (t_not, a, i[0])
        else:
            t_not, t_not_f = time_continuation(lob[0], i[1], False)
            if t_not_f or value_inequality_condition(lob[1], a) or i[0] != lob[2]:
                return True, (t_not, a, i[0])
    return False, lob


def build_time_generator_(iter_, cache_constructor, calculate, time_continuation, valid_instant, value_inequality_condition):
    # calculate should add an element to cache and return the new-value
    lob, cache, prev = None, cache_constructor(), None
    for i in iter_:
        # i : (start, (time), key, w)
        a = calculate(cache, i)
        ret, new_lob = generator_step(i, lob, a, time_continuation, valid_instant, value_inequality_condition)
        if ret and (prev is None or prev[1] != lob[1]):
            yield lob
            prev = lob

        lob = new_lob
    if lob is not None:
        yield lob


def build_time_generator_s(iter_, cache_constructor, calculate, time_continuation, valid_instant, value_inequality_condition):
    # calculate should add an element to cache and return the new-value
    lob, cache = None, None
    for i in iter_:
        # i : (start, (time), key, w)
        if cache is None or (lob is not None and (lob[0] != i[1] or lob[2] != i[0])):
            cache = cache_constructor()

        a = calculate(cache, i)
        ret, new_lob = generator_step_sparse(i, lob, a, time_continuation, valid_instant, value_inequality_condition)
        if ret:
            yield lob
            cache = None

        lob = new_lob
    if lob is not None:
        yield lob


def lob_constructor():
    return [None, None]


def build_time_generator_key(iter_, get_key, cache_constructor, calculate, time_continuation, valid_instant, value_inequality_condition):
    # calculate should add an element to cache and return the new-value
    lob, cache = defaultdict(lob_constructor), defaultdict(cache_constructor)
    for i in iter_:
        # i : (start, (time), key, w)
        key = get_key(i)
        a = calculate(cache[key], i)
        data_key = lob[key]
        ret, new_lob = generator_step(i, data_key[1], a, time_continuation, valid_instant, value_inequality_condition)
        if ret and (data_key[0] is None or data_key[0][1] != data_key[1][1]):
            yield (key, data_key[1])
            data_key[0] = data_key[1]
        if new_lob is None:
            lob.pop(key, None)
        else:
            lob[key][1] = new_lob

    for k, v in iteritems(lob):
        yield (k, v[1])


def build_time_generator_skey(iter_, get_key, cache_constructor, calculate, time_continuation, valid_instant, value_inequality_condition):
    # calculate should add an element to cache and return the new-value
    lob, cache = dict(), defaultdict(cache_constructor)
    for i in iter_:
        # i : (start, (time), key, w)
        key = get_key(i)
        lob_key = lob.get(key, None)
        if (lob_key is not None and (lob_key[0] != i[1] or lob_key[2] != i[0])):
            cache[key] = cache_constructor()

        a = calculate(cache[key], i)
        ret, new_lob = generator_step_sparse(i, lob_key, a, time_continuation, valid_instant, value_inequality_condition)
        if ret:
            yield (key, lob_key)
            cache.pop(key, None)
        if new_lob is None:
            lob.pop(key, None)
        else:
            lob[key] = new_lob

    for a in iteritems(lob):
        yield a


def set_weighted_links_(cache, i):
    if i[0]:
        cache.add(i[2] + (i[3],))
    else:
        cache.remove(i[2] + (i[3],))
    return set(cache)


def set_unweighted_n(cache, i):
    if i[0]:
        cache.add(i[2][1])
    else:
        cache.remove(i[2][1])
    return set(cache)


def set_unweighted_n_sparse(cache, i):
    cache.add(i[2][1])
    return set(cache)


def set_nodes(cache, i):
    if i[0]:
        cache.add(i[2][0])
    else:
        cache.remove(i[2][0])
    return set(cache)


def set_nodes_sparse(cache, i):
    cache.add(i[2][0])
    return set(cache)


def set_unweighted_links_(cache, i):
    if i[0]:
        cache.add(i[2])
    else:
        cache.remove(i[2])
    return set(cache)


def len_set_(cache, i):
    if i[0]:
        cache.add(i[2])
    else:
        cache.remove(i[2])
    return len(cache)


def len_set_nodes(cache, i):
    if i[0]:
        cache.add(i[2][0])
    else:
        cache.remove(i[2][0])
    return len(cache)


def len_set_n(cache, i):
    if i[0]:
        cache.add(i[2][1])
    else:
        cache.remove(i[2][1])
    return len(cache)


def sum_counter_(cache, i):
    if i[0]:
        cache[i[2]] += i[3]
    else:
        cache[i[2]] -= i[3]
        if cache[i[2]] == 0:
            cache.pop(i[2], None)
    return sum(itervalues(cache))


def sum_counter_n(cache, i):
    if i[0]:
        cache[i[2][1]] += i[3]
    else:
        cache[i[2][1]] -= i[3]
        if cache[i[2][1]] == 0:
            cache.pop(i[2][1], None)
    return sum(itervalues(cache))


def get_key_first(key):
    return key[2][0]
