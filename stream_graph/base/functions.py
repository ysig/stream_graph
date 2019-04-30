def ego(e, ne, l, both, detailed):
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
        def take(times, prev, val):
            if prev[0] is None or prev[0] != val:
                times.append((time, val))
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
    return TimeCollection(times, detailed)

def ego_at(e, ne, l, at, both, detailed):
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
