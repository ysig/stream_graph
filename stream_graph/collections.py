from __future__ import absolute_import
import warnings
import pandas as pd
import operator
from collections import defaultdict
from six import iteritems


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
    def __init__(self, it=iter([]), discrete=False, instantaneous=False):
        self.it = it
        self.instantaneous = instantaneous
        self.discrete = discrete

    def __iter__(self):
        for i in self.it:
            yield i

    @property
    def instants(self):
        return self.instantaneous

    def merge(self, b, measure, ignore_value=None, missing_value=None):
        """Merge two generator objects."""
        assert isinstance(b, (TimeGenerator))
        assert self.discrete == b.discrete
        instants = b.instants and self.instants
        if b.instants and self.instants:
            assert isinstance(b, (TimeCollection, TimeGenerator))
            def generate(a_iter, b, t_a,  measure, ignore_value, missing_value):
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
        else:
            # queue: element
            # [(t, v, category)] or [(t, (v1, v2), None)]
            def addq(queue, obj):
                queue.append(obj)
                if len(queue) > 1:
                    queue = sorted(queue, key=operator.itemgetter(0), reverse=True)
                    if queue[0][0] == queue[1][0]:
                        # Always from a different category:
                        if queue[0][2]:
                            queue = [(queue[0][0], (queue[0][1], queue[1][1]), None)]
                        else:
                            queue = [(queue[0][0], (queue[1][1], queue[0][1]), None)]
                return queue

            def generate(a_iter, b_iter, measure, ignore_value, missing_value):
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
                fa, fb = False, False
                queue = updateq(queue, None)

                # Sequence of events
                while len(queue):
                    cache = queue.pop()
                    queue = updateq(queue, cache[2])
                    if cache[2] is None:
                        a, b = cache[1]
                        fa, fb = True, True
                    elif cache[2]:
                        a, fa = cache[1], True
                    else:
                        b, fb = cache[1], True
                    if fa and fb:
                        break
                    if missing_value is not None:
                        m = measure(b, a)
                        if m != m_old:
                            yield (cache[0], m)
                            m_old = m

                while True:
                    m = measure(b, a)
                    if m != m_old:
                        yield (cache[0], m)
                        m_old = m
                    if len(queue):
                        cache = queue.pop()
                        queue = updateq(queue, cache[2])
                        if cache[2] is None:
                            a, b = cache[1]
                        elif cache[2]:
                            a = cache[1]
                        else:
                            b = cache[1]
                    else:
                        break

            if (b.instants != self.instants):
                instantize = (instantize_discrete if self.discrete else instantize_continuous)
                if b.instants:
                    iter_a, iter_b = iter(self), instantize(b, ignore_value)
                else:
                    iter_a, iter_b = instantize(self, ignore_value), iter(b)
                obj = generate(iter_a, iter_b, measure, ignore_value, missing_value)
            else:
                obj = generate(iter(self), iter(b), measure, ignore_value, missing_value)
        return TimeGenerator(obj, instantaneous=instants)

    def map(self, fun, unsafe=True):
        if unsafe:
            def generator(fun):
                for t, v in self:
                    yield t, fun(t, v)
        else:
            def generator(fun):
                prev = None
                for t, v in self:
                    val = fun(t, v)
                    if prev is None or prev != val:
                        yield t, val
                        prev = val

        return TimeGenerator(generator(fun), instantaneous=self.instantaneous)


class TimeCollection(TimeGenerator):
    def __init__(self, it=[], discrete=False, instantaneous=False):
        super(TimeCollection, self).__init__(it, discrete, instantaneous)
        self.it = list(it)

    def __str__(self):
        if bool(self):
            return "stream_graph.TimeCollection" + ("[instantaneous]" if self.instantaneous else "") + ": " + str(list(self))
        else:
            return "stream_graph.TimeCollection: Empty"

    def append(self, obj):
        self.it.append(obj)


    def __iter__(self):
        return iter(self.it)

    def __len__(self):
        return len(self.it)

    def __getitem__(self, i):
        return self.it[i]

    def search_time(self, t):
        if self.instantaneous:
            return self._search_time_instantaneous(t)
        elif self.discrete:
            return self._search_time_discrete(t)
        else:
            return self._search_time_continuous(t)

    def _search_time_continuous(self, t):
        if not isinstance(t, tuple):
            t = (t, True)
        L, R = 0, len(self.it)
        while L < R:
            m = int((L + R) / 2)
            if self.it[m][0][0] < t[0] or (self.it[m][0][0] == t[0] and self.it[m][0][1] <= t[1]):
                L = m + 1
            else:
                R = m
        return max(L - 1, 0)

    def _search_time_discrete(self, t):
        L, R = 0, len(self.it)
        while L < R:
            m = int((L + R) / 2)
            if self.it[m][0] <= t:
                L = m + 1
            else:
                R = m
        return max(L - 1, 0)

    def _search_time_instantaneous(self, t):
        L, R = 0, len(self.it) - 1
        while L <= R:
            m = int((L + R) / 2)
            if self.it[m][0] < t:
                L = m + 1
            elif self.it[m][0] > t:
                R = m - 1
            else:
                return m
        return None

    def get_at(self, t, not_found=None):
        if self.instantaneous:
            idx = self._search_time_instantaneous(t)
            if idx is None:
                return not_found
            else:
                return self[idx][1]
        elif self.discrete:
            return self[self._search_time_discrete(t)][1]
        else:
            return self[self._search_time_continuous(t)][1]

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
                              instantaneous=self.instants, discrete=self.discrete)

    def map(self, fun):
        return TimeCollection(super(TimeCollection, self).map(fun), instantaneous=self.instantaneous, discrete=self.discrete)


class TimeSparseCollection(TimeCollection):
    def __init__(self, it=[], caster=set, discrete=False):
        self.it = [(t, obj, f) for t, obj, f in it]
        self.instantaneous = False
        self.discrete = discrete
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
                holder |= obj
            else:
                holder -= obj
            yield (t, self.caster(holder))


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
            from scipy.stats import poisson

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


def instantize_discrete(it, ignore_value):
    t_head = None
    for t, v in it:
        if t_head is None:
            t_head = [t, v, t]
        elif t_head[0] == t-1 and v == t_prev[1]:
            t_head[2] = t
        else:
            yield (t_head[0], t_head[1])
            yield (t_head[2] + 1, ignore_value)
            t_head = [t, v, t+1]
    if t_prev[0] is not None:
        yield (t_head[0], t_head[1])
        yield (t_head[2] + 1, ignore_value)


def instantize_continuous(it, ignore_value):
    for t, v in it:
        yield ((t, True), v)
        yield ((t, False), ignore_value)
