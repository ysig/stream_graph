import warnings
from datetime import datetime, timedelta
import pandas as pd
from collections import defaultdict
from functools import reduce
from operator import mul
from copy import deepcopy
from six import iteritems
from scipy.stats import poisson

class NodeCollection(object):
    def __init__(self, it={}):
        self.it = dict(it)

    def __str__(self):
        if bool(self):
            return "stream_graph.NodeCollection: " + str(self.it)
        else:
            return "stream_graph.NodeCollection: Empty"

    def __iter__(self):
        return iteritems(self.it)

    def __getitem__(self, u):
        return self.it[u]

    def __len__(self):
        return len(self.it)

    def __bool__(self):
        """Implementation of the :code:`bool` casting of a NodeSet object.
        
        Parameters
        ----------
        None.
        
        Returns
        -------
        out : Bool
            Return True if an object is **both** initialized and contains information.

        """
        return len(self.it) > 0

    # Python2 cross-compatibility
    def __nonzero__(self):
        return self.__bool__()

    def map(self, fun):
        return NodeCollection({x: fun(x, y) for x, y in self})

    @property
    def instants(self):
        return self.instants


class LinkCollection(object):
    def __init__(self, it={}):
        self.it = dict(it)

    def __str__(self):
        if bool(self):
            return "stream_graph.LinkCollection: " + str(self.it)
        else:
            return "stream_graph.LinkCollection: Empty"

    def __iter__(self):
        return iteritems(self.it)

    def __getitem__(self, u):
        return self.it[u]

    def __len__(self):
        return len(self.it)

    def __bool__(self):
        """Implementation of the :code:`bool` casting of a NodeSet object.
        
        Parameters
        ----------
        None.
        
        Returns
        -------
        out : Bool
            Return True if an object is **both** initialized and contains information.

        """
        return len(self.it) > 0

    def map(self, fun):
        return LinkCollection({x: fun(x, y) for x, y in self})

    # Python2 cross-compatibility
    def __nonzero__(self):
        return self.__bool__()

class TimeGenerator(object):
    def __init__(self, it=iter([]), instantaneous=False):
        self.it = it
        self.instantaneous=instantaneous
        
    def __iter__(self):
        return iter(self.it)

    @property
    def instants(self):
        return self.instantaneous

    def merge(self, b, measure, ignore_value=None, missing_value=None):
        assert isinstance(b, (TimeGenerator))
        instants = True
        if b.instants and self.instants:
            assert isinstance(b, (TimeCollection, TimeGenerator))
            def generate(a_iter, b, t_a,  measure, ignore_value, missing_value):
                values = list()
                t_a, v_a = next(a_iter, (None, missing_value))
                for t_b, v_b in b:
                    while t_a is not None and t_a < t_b:
                        if missing_value is not None:
                            m = measure(v_a, missing_value)
                            if m != ignore_value:
                                yield (t_a, m)
                        t_a, v_a = next(a_iter, (None, missing_value))
                    if t_a == t_b:
                        m = measure(v_a, v_b)
                        yield (t_b, m)
                        t_a, v_a = next(a_iter, (None, missing_value))
                    elif missing_value is not None:
                        m = measure(missing_value, v_b)
                        if m != ignore_value:
                            yield (t_b, m)
                if missing_value is not None:
                    while t_a is not None:
                        m = measure(v_a, missing_value)
                        if m != ignore_value:
                            yield (t_a, m)
                        t_a, v_a = next(a_iter, (None, missing_value))

            obj = generate(iter(self), b, -1, measure, ignore_value, missing_value)
        elif (b.instants != self.instants) :
            # Reduce to instants
            def generate(a_interval, b_inst, measure, ignore_value, missing_value):
                values = list()
                t_a_s, v_a_s = next(a_interval, (None, missing_value))
                t_a_f, v_a_f = next(a_interval, (None, missing_value))
                def in_interval(t_a_s, t, t_a_f):
                    if t_a_f is not None:
                        return t_a_s <= t and t < t_a_f
                    else:
                        return t_a_s <= t
                for t_b, v_b in b_inst:
                    while t_a_s is not None and not in_interval(t_a_s, t_b, t_a_f):
                        t_a_s, v_a_s = t_a_f, v_a_f
                        t_a_f, v_a_f = next(a_interval, (None, missing_value))
                    if t_a_s is None:
                        break
                    m = measure(v_a_s, v_b)
                    if m != ignore_value:
                        yield (t_b, m)
            if b.instants:
                obj = generate(iter(self), iter(b), measure, ignore_value, missing_value)                 
            else:
                obj = generate(iter(b), iter(self), measure, ignore_value, missing_value)                 
        else:
            def generate(a_iter, b_iter, measure, ignore_value, missing_value):
                def addq(queue, obj):
                    queue.append(obj)
                    if len(queue) > 1:
                        queue = sorted(queue, key = lambda x: x[0], reverse=True)
                        if queue[0][0] == queue[1][0]:
                            # Always from a different category:
                            if queue[0][2]:
                                queue = [(queue[0][0], (queue[0][1], queue[1][1]), None)]
                            else:
                                queue = [(queue[0][0], (queue[1][1], queue[0][1]), None)]
                    return queue

                def updateq(queue, f):
                    if f is None:
                        return updateq(updateq(queue, True), False)
                    elif f:
                        obj = next(b_iter, None)
                    else:
                        obj = next(a_iter, None)
                    if obj is not None:
                        queue = addq(queue, (obj[0], obj[1], f))
                    return queue

                def ab(a, b, cache):
                    if cache[2] is None:
                        return cache[1]
                    elif cache[2]:
                        return (cache[1], b)
                    else:
                        return (a, cache[1])

                # Initialise queue
                queue = []
                m_old = ignore_value
                a, b = missing_value, missing_value
                queue = updateq(queue, None)

                # Sequence of events
                while len(queue):
                    cache = queue.pop()
                    queue = updateq(queue, cache[2])
                    a, b = ab(a, b, cache)
                    m = measure(a, b)
                    if m != m_old:
                        yield (cache[0], m)
            obj = generate(iter(self), iter(b), measure, ignore_value, missing_value)
        return TimeGenerator(obj, instantaneous=instants)


    def map(self, fun):
        def generator(fun):
            for t, v in self:
                yield t, fun(t, v)
        return TimeGenerator(generator(fun), instantaneous=self.instantaneous)
                

class TimeCollection(TimeGenerator):
    def __init__(self, it=[], instantaneous=False):
        self.it = [(t, v) for t, v in it]
        self.instantaneous=instantaneous

    def __str__(self):
        if bool(self):
            return "stream_graph.TimeCollection: " + str(list(self))
        else:
            return "stream_graph.TimeCollection: Empty"

    def __iter__(self):
        return iter(self.it)

    def __len__(self):
        return len(self.it)

    def __bool__(self):
        """Implementation of the :code:`bool` casting of a NodeSet object.
        
        Parameters
        ----------
        None.
        
        Returns
        -------
        out : Bool
            Return True if an object is **both** initialized and contains information.

        """
        return len(self.it) > 0

    # Python2 cross-compatibility
    def __nonzero__(self):
        return self.__bool__()

    def merge(self, b, measure, ignore_value=None, missing_value=None):
        return TimeCollection(super(TimeCollection, self).merge(b, measure, ignore_value, missing_value),
                              instantaneous=self.instants)

    def map(self, fun):
        return TimeCollection(super(TimeCollection, self).map(fun), instantaneous=self.instantaneous)

class TimeSparseCollection(TimeCollection):
    def __init__(self, it=[], caster=set):
        self.it = [(t, obj, f) for t, obj, f in it]
        self.instantaneous = False
        self.caster = caster

    def __str__(self):
        if bool(self):
            return "stream_graph.TimeSparseCollection: " + str(list(self))
        else:
            return "stream_graph.TimeSparseCollection: Empty"

    @property
    def TimeGenerator(self):
        return TimeGenerator(iter(self), False)

    def __iter__(self):
        holder = set()
        for t, obj, f in (self.it):
            if f:
                holder -= obj
            else:
                holder |= obj
            yield (t, self.caster(iter(holder)))


class DataCube(object):
    def __init__(self, cube=None, columns=None, weight_column_name=None, column_sizes=None):
        if cube is None:
            self.columns = columns
            if columns is None:
                if isinstance(cube, pd.DataFrame):
                    columns = cube.columns
            self.data_ = pd.DataFrame(cube, columns=columns)

            # Weight Handling
            self.weight_column_name = weight_column_name
            if weight_column_name is None:
                self.total_sum = float(self.data_[0])
                self.cols = self.data_.columns
                def weight(row):
                    return 1
                def dump_row(row):
                    return tuple(row[i] for i in range(len(row)))
                self.wcn = 'w'
            else:
                self.cols = [c for c in self.data_.columns if c != weight_column_name]
                self.total_sum = float(self.data_[weight_column_name].sum())
                self.wcn = self.weight_column_name
                wcn_idx = self.data_.columns.get_loc(self.weight_column_name)
                def weight(row):
                    return row[wcn_idx]
                def dump_row(row):
                    return tuple(row[i] for i in range(len(row)) if i!=wcn_idx)
            self.weight = weight
            self.dump_row = dump_row

            # Column-Size
            self.cs_ = defaultdict(dict)
            if column_sizes is None:
                column_sizes = dict()
            elif isinstance(column_sizes, list):
                column_size = dict(zip(column_sizes, self.cols[:min(len(column_sizes), len(self.cols))]))
            elif not isinstance(column_sizes, dict):
                raise ValueError('Column-Sizes must be None, list or dict')

            for c in self.cols:
                val = column_sizes.get(c, None)
                if val is None:
                    val = float(len(set(self.data_[c].values.flat)))
                self.cs_[1][c] = val

            scols = set(self.cols)
            for c in set(column_sizes.keys()) - scols:
                if set(c).issubset(scols):
                    self.cs_[len(c)][c] = column_sizes[c]

    def __bool__(self):
        return hasattr(self, 'data_') and not data.empty

    def __str__(self):
        if bool(self):
            return ("Data Cube\n---------\n" + str(self.data_))
        else:
            return ("Data Cube [Empty]")

    @property
    def data(self):
        if bool(self):
            return self.data_.copy()
        else:
            return None

    def drop(self, columns):
        if bool(self):
            if isinstance(columns, (int, str)):
                col_set = {columns}
            else:
                col_set = set(columns)

            assert set(self.cols).issuperset(col_set)
            rest_columns = [c for c in self.cols if c not in col_set]
            rest_columns_idx = [self.data_.columns.get_loc(c) for c in rest_columns]

            measures = defaultdict(float)
            for row in self.data_.itertuples(index=False, name=None):
                measures[_map(row, rest_columns_idx)] += self.weight(row)

            out = [r + (w,) for r, w in iteritems(measures)]
            return self._weighted_output(out, rest_columns)
        else:
            return self.__class__()

    def poisson(self, op_id, log_overflow=1000.):
        if bool(self):
            const, measures, labeled = self._measure(op_id)        
            out = list()

            with warnings.catch_warnings():
                warnings.simplefilter('error')
                for row in self.data_.itertuples(index=False):
                    # Calculate expectation
                    item = const*measures[labeled[0]][_map(row, labeled[0])]
                    for col in labeled[1:]:
                        item *= measures[col][_map(row, col)]/self.total_sum

                    # Log Fish
                    p, pexp = self.weight(row), item
                    if p < pexp:
                        # Xo < pois(Xexp)
                        try:
                            val = poisson.logcdf(p, pexp) # P(pois(Xexp)<Xo), Xo = pt[v], Xexp = pexp[v]
                        except RuntimeWarning:
                            val = log_overflow
                    else:
                        try:
                            val = -poisson.logsf(p, pexp) # P(pois(Xexp)>=Xo), Xo = pt[v], Xexp = pexp[v]
                        except RuntimeWarning:
                            val = -log_overflow

                    out.append(self.dump_row(row) + (val,))
            
            return self._weighted_output(out)
        else:
            return self.__class__()

    def expectation(self, op_id):
        if bool(self):
            const, measures, labeled = self._measure(op_id)
            out = list()
            for row in self.data_.itertuples(index=False):
                item = const*measures[labeled[0]][_map(row, labeled[0])]
                for col in labeled[1:]:
                    item *= measures[col][_map(row, col)]/self.total_sum
                out.append(self.dump_row(row) + (item,))

            return self._weighted_output(out)
        else:
            return self.__class__()

    def proportion(self, op_id):
        if bool(self):
            const, measures, labeled = self._measure(op_id)
            out = list()
            for row in self.data_.itertuples(index=False):
                item = const
                for col in labeled:
                    item *= measures[col][_map(row, col)]/self.total_sum
                out.append(self.dump_row(row) + (item,))

            return self._weighted_output(out)
        else:
            return self.__class__()

    def _measure(self, op_id):
        ret = _validator(op_id, self.cols, self.data_.columns)
        if ret is None:
            return data_cube(self.data_.copy(), columns=self.columns, weight_column_name=self.weight_column_name)

        labeled, constants = ret

        const = 1
        const_nom = {c: -1 for c in constants}
        for l in range(min(len(constants), max(self.cs_.keys())), 1, -1):
            if l in self.cs_:
                for k in self.cs_[l].keys():
                    if k.issubset(constants):
                        const *= self.cs_[l][k]
                        for c in k:
                            const_nom[c] += 1

        if c in constants:
            const *= (self.cs_[1][c] ** const_nom[c])

        def new():
            return defaultdict(float)

        measures = defaultdict(new)
        for row in self.data_.itertuples(index=False):
            w = self.weight(row)
            for col in labeled:
                measures[col][_map(row, col)] += w

        return const, measures, labeled

    def _weighted_output(self, out, column_names=None):
        if column_names is None:
            column_names = self.cols
        return data_cube(out, columns=column_names+[self.wcn], weight_column_name=self.wcn)

def _validator(op_id, columns, columns_raw):
    out = list()
    cols = set(columns)
    if isinstance(op_id[0], tuple) and set() == cols:
        return None
    
    for i, u in enumerate(op_id):
        if isinstance(u, tuple):
            assert not len(set(u) - cols)
            cols -= set(u)
            out.append(tuple([columns_raw.get_loc(v) for v in u]))
        else:
            assert u in columns
            cols -= {u}
            out.append((columns_raw.get_loc(u),))
    return out, cols

def _map(row, col):
    return tuple(row[c] for c in col)
