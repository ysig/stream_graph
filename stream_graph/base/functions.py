from collections import deque, defaultdict
from stream_graph.collections import TimeCollection
from stream_graph.exceptions import UnrecognizedDirection

def ego(e, ne, l, both, detailed, discrete):
    # print >> sys.stderr, "Running node : " + str(e)
    u, v, t, index, times = 0, 0, 0, 0, list()

    ce, info = dict(), dict()
    for i in ne:
        info[(e,i)] = -1
        for x in ne-{i}:
            info[(i,x)] = -1
        info[(i,e)] = -1

    index = 0
    time = l[index][2] #starting time.
    ne_x, lines, paths = ne | {e}, dict(), dict()

    if both:
        def add_lines(u, v, t):
            lines[(u,v)] = t
            lines[(v,u)] = t
    else:
        def add_lines(u, v, t):
            lines[(u,v)] = t

    if detailed:
        def take(times, prev, val):
            times.append((time, val))
    else:
        if discrete:
            def take(times, prev, val):
                if prev[0] is None or prev[0] != val:
                    times.append((time, val))
                    prev[0] = val
        else:
            def take(times, prev, val):
                if prev[0] is None or prev[0] != val:
                    times.append(((time, True), val))
                    prev[0] = val

    prev = [None]
    while(index < len(l) -1):
        # get all links of time stamp
        while(index < len(l) -1):
            u,v,t = l[index]
            add_lines(u, v, t)
            index += 1
            if(t != time):
                break

        for u in ne:
            for v in ne-{u}:
                # print u,v
                ce[(u,v)] = 0.0
                Q = set()
                if (u,v) not in lines:
                    # print u,v
                    news = info[(u,v)]
                    for x in ne_x - {u, v}:
                        ux = info[(u, x)]
                        if (x, v) in lines:
                            xv = lines[(x, v)]
                        else:
                            xv = info[(x, v)]
                        if ux != -1 and xv != -1 and ux < xv:
                            if ux == news:
                                Q.add(x)
                            if ux > news:
                                Q = {x}
                                news = ux

                    if (u,v) in paths:
                        old_paths = paths[(u,v)]
                        if old_paths[0] == news:
                            paths[(u, v)] = (news, paths[(u,v)][1] | Q)
                        elif old_paths[0] < news:
                            paths[(u, v)] = (news, Q)
                    else:
                        paths[(u, v)] = (news, Q)

                    if e in paths[(u, v)][1]:
                        ce[(u, v)] = 1.0/len(paths[(u, v)][1])
                else:
                    paths[(u, v)] = (t, {u})


        val = sum(ce.values())
        take(times, prev, val)

        for k in lines:
            info[k] = lines[k]
        time, lines = l[index][2], {}
    return TimeCollection(times, instantaneous=detailed, discrete=discrete)


def ego_at(e, ne, l, at, both, detailed, discrete):
    # print >> sys.stderr, "Running node : " + str(e)
    u, v, t, index, times = 0, 0, 0, 0, list()

    ce, info = dict(), dict()
    for i in ne:
        info[(e,i)] = -1
        for x in ne-{i}:
            info[(i,x)] = -1
        info[(i,e)] = -1

    index = 0
    time = l[index][2] #starting time.
    ne_x, lines, paths = ne | {e}, dict(), dict()

    if both:
        def add_lines(u, v, t):
            lines[(u,v)] = t
            lines[(v,u)] = t
    else:
        def add_lines(u, v, t):
            lines[(u,v)] = t

    prev = None
    while(index < len(l) -1):
        # get all links of time stamp
        while(index < len(l) -1):
            u,v,t = l[index]
            add_lines(u, v, t)
            index += 1
            if(t != time):
                break

        for u in ne:
            for v in ne-{u}:
                # print u,v
                ce[(u,v)] = 0.0
                Q = set()
                if (u,v) not in lines:
                    # print u,v
                    news = info[(u,v)]
                    for x in ne_x - {u, v}:
                        ux = info[(u, x)]
                        if (x, v) in lines:
                            xv = lines[(x, v)]
                        else:
                            xv = info[(x, v)]
                        if ux != -1 and xv != -1 and ux < xv:
                            if ux == news:
                                Q.add(x)
                            if ux > news:
                                Q = {x}
                                news = ux

                    if (u,v) in paths:
                        old_paths = paths[(u,v)]
                        if old_paths[0] == news:
                            paths[(u, v)] = (news, paths[(u,v)][1] | Q)
                        elif old_paths[0] < news:
                            paths[(u, v)] = (news, Q)
                    else:
                        paths[(u, v)] = (news, Q)

                    if e in paths[(u, v)][1]:
                        ce[(u, v)] = 1.0/len(paths[(u, v)][1])
                else:
                    paths[(u, v)] = (t, {u})

        val = sum(ce.values())
        if prev is None and at < time:
            return .0
        elif at == time:
            return val
        elif prev is not None and at > prev[0] and at < time:
            return prev[1]
        prev = (time, val)

        for k in lines:
            info[k] = lines[k]
        time, lines = l[index][2], {}
    return (prev[1] if prev is not None else .0)


def get_maximal_cliques(df, direction='both'):
    S, S_set, R, times, nodes = deque(), set(), set(), dict(), dict()

    if direction == 'out':
        def as_link(u, v):
            return (u, v)

        def add_nodes(u, v):
            nodes[u].add(v)

        def add_element(times, l, ts, tf):
            times[l].append((ts, tf))
            return True
    elif direction == 'in':
        def as_link(u, v):
            return (v, u)

        def add_nodes(u, v):
            nodes[v].add(u)

        def add_element(times, l, ts, tf):
            times[l].append((ts, tf))
            return True
    elif direction == 'both':
        def as_link(u, v):
            return frozenset([u, v])

        def add_nodes(u, v):
            nodes[v].add(u)
            nodes[u].add(v)

        def add_element(times, l, ts, tf):
            if len(times[l]):
                tsp, tfp = times[l][-1]
                assert ts >= tsp
                if ts <= tfp:
                    times[l][-1] = (tsp, max(tf, tfp))
                    return False
            times[l].append((ts, tf))
            return True
    else:
        raise UnrecognizedDirection()

    times, nodes = defaultdict(list), defaultdict(set)
    for u, v, ts, tf, in df[['u', 'v', 'ts', 'tf']].itertuples(index=False, name=None):
        # This a new instance
        add_nodes(u, v)
        if add_element(times, as_link(u, v), ts, tf):
            add_clique((frozenset([u, v]), (ts, ts), set()), S, S_set)

    while len(S) != 0:
        cnds, (ts, tf), can = S.pop()
        is_max = True

        # Grow time on the right side
        td = getTd(cnds, ts, tf, as_link, times)
        if td != tf:
            # nodes, (ts, tf), candidates
            add_clique((cnds, (ts, td), can), S, S_set)
            is_max = False

        # Grow node set
        can = set(can)
        if ts == tf:
            for u in cnds:
                neighbors = nodes[u]
                for n in neighbors:
                    can.add(n)
        can -= cnds

        for node in can:
            if isClique(cnds, node, ts, tf, as_link, times):
                # Is clique!
                Xnew = set(cnds) | set([node])
                add_clique((frozenset(Xnew), (ts, tf), can), S, S_set)
                is_max = False

        if is_max:
            R.add((cnds, (ts, tf)))
    return R


def getTd(can, tb, te, as_link, times):
    min_t = None
    for u in can:
        for v in can:
            link = as_link(u, v)
            if link in times:
                # Find min time x > te s.t. (b,x,u,v) exists in stream
                tlist = times[link]
                first, last = 0, len(tlist)-1
                middle = int((first+last)/2.0)

                while first <= last:
                    if tlist[middle][0] > tb:
                        last = middle - 1
                    elif tlist[middle][1] < tb:
                        first = middle + 1
                    else:
                        # found a link that contains b
                        assert tlist[middle][0] <= tb and tlist[middle][1] >= tb
                        assert tlist[middle][1] >= te
                        if min_t is not None:
                            min_t = min(min_t, tlist[middle][1])
                        else:
                            min_t = tlist[middle][1]
                        break
                    middle = (first + last) // 2

                # We should not be here except for a break in the while loop above
                if not first <= last:
                    assert False
    return min_t


def isClique(cnds, node, tb, te, as_link, times):
    """ returns True if X(c) union node is a clique over tb;te, False otherwise"""
    for i in cnds:
        if as_link(i, node) not in times:
            # Check that (i, node) exists in stream.
            return False
        else:
            link = as_link(i, node)
            # Check there is a link (b, e, i, node) s.t. b <= tb and e >= te, otherwise not a clique.
            #tlist is a list of non-overlapping couples (b,e)
            tlist = times[link]
            # start binary search for b in tlist
            first, last = 0, len(tlist)-1
            middle = (first+last)//2
            while first <= last:
                if tlist[middle][0] > tb:
                    last = middle - 1
                elif tlist[middle][1] < tb:
                    first = middle + 1
                else:
                    # found a link that contains b
                    assert tlist[middle][0] <= tb and tlist[middle][1] >= tb
                    if tlist[middle][1] < te:
                        return False
                    break
                middle = (first + last) // 2
            # if we are here without having found a link that contains self._tb
            if first > last:
                return False
    return True


def add_clique(c, S, S_set):
    if c[0:2] not in S_set:
        S.appendleft(c)
        S_set.add(c[0:2])
