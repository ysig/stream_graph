from stream_graph import ABC
from stream_graph import StreamGraph
from math import tanh, sqrt
from collections import Iterable
import numpy as np


class Visualizer(object):
    """Visualizer object for stream_graph objects.

    Parameters
    ----------
    x_axis_label: str, default=None
        Defines the label of the x-axis. By default is 'time'.

    y_axis_label: str, default=None
        Defines the label of the y-axis. No label by default.

    date_map : dict or callable or True, default=None
        A map that transforms time elements to date-time-objects.
        If time-elements are already unix-epoch-timestamps set to True.
    """
    def __init__(self, x_axis_label=None, y_axis_label=None, date_map=None):
        self.x_axis_label = x_axis_label
        self.y_axis_label = y_axis_label
        self.date_map = date_map
        self._data = dict(temporal_linkset=[], node_set=None, time_set=None, temporal_nodeset=None)

    def fit(self, items):
        if items is not None:
            self.__iadd__(items)
        return self

    def draw(self):
        from bokeh.plotting import figure
        if self._data['time_set'].instantaneous:
            min_time = min(self._data['time_set'])
            max_time = max(self._data['time_set'])
        else:
            min_time = min(k[0] for k in self._data['time_set'])
            max_time = max(k[1] for k in self._data['time_set'])

        # get nodes
        nodes = sorted([str(n) for n in self._data['node_set']])
        # map them to numbers
        ln = dict(enumerate(nodes))
        nl = {n: i for i, n in enumerate(nodes)}
        pallete = self._make_pallete(len(self._data['temporal_linkset']))

        # width height
        w, h = (max_time - min_time) * 70, len(nodes) * 50
        division = (1 if self._data['temporal_nodeset'].discrete else 0.1)
        division = self.map_time(min_time + division) - self.map_time(min_time)
        min_time, max_time = self.map_time(min_time), self.map_time(max_time)

        # plot nodes
        node_points, time_points = [], []
        if self._data['temporal_nodeset'].instantaneous:
            for (u, ts) in self._data['temporal_nodeset']:
                ts = self.map_time(ts)
                if ts >= min_time and ts <= max_time:
                    # add circular points
                    node_points.append(nl[str(u)])
                    time_points.append(ts)
        else:
            for k in self._data['temporal_nodeset']:
                for ts in np.arange(max(self.map_time(k[1]), min_time), min(self.map_time(k[2]), max_time), division):
                    node_points.append(nl[str(k[0])])
                    time_points.append(ts)

        # plot links
        height = 0.5
        data, nan, l, b = self._data['temporal_linkset'], float('nan'), [], []
        if len(data):
            for ls, color in zip(data, pallete):
                lp_x, lp_y = [], []
                x0, y0, x1, y1, cx, cy = [], [], [], [], [], []
                if ls.instantaneous:
                    for (u, v, ts) in ls:
                        u, v = str(u), str(v)
                        ts = self.map_time(ts)
                        yu, yv = sorted((nl[u], nl[v]))
                        cr = curving(yu, yv) * division * 0.7
                        xm = ts + cr
                        ym = (yu + yv) * height
                        if cr > 0.:
                            x0.append(ts)
                            x1.append(ts)
                            y0.append(yu)
                            y1.append(yv)
                            cx.append(xm)
                            cy.append(ym)
                        else:
                            lp_x += [ts, ts, nan]
                            lp_y += [yu, yv, nan]

                else:
                    for k in ls:
                        u, v = str(k[0]), str(k[1])
                        ts, tf = k[2:4]
                        ts = self.map_time(ts)
                        tf = self.map_time(tf)
                        yu, yv = sorted((nl[u], nl[v]))
                        cr = curving(yu, yv) * division * 0.7
                        xm = ts + cr
                        ym = (yu + yv) * height
                        if cr > 0.:
                            x0.append(ts)
                            x1.append(ts)
                            y0.append(yu)
                            y1.append(yv)
                            cx.append(xm)
                            cy.append(ym)
                            if yu + yv == 2 * ym:
                                t = 0.5
                            else:
                                t = (sqrt((yu - ym) * (ym - yv)) + yu - ym) / (yu - 2 * ym + yv)
                            assert 0 <= t <= 1
                            xm = ((1 - t)**2 + t**2) + 2 * t * (1 - t) * (xm + 1)
                        else:
                            lp_x += [ts, ts, nan]
                            lp_y += [yu, yv, nan]
                        lp_x += [xm, tf, nan]
                        lp_y += [ym, ym, nan]
                l.append(((lp_x, lp_y), color))
                b.append(((x0, y0, x1, y1, cx, cy), color))

        aspect_scale = w / float(h)
        if self.date_map is None:
            self.p = figure(aspect_scale=aspect_scale)
            if self._data['time_set'].instantaneous:
                self.p.xaxis.ticker = list(self._data['time_set'])
            elif self._data['time_set'].discrete:
                self.p.xaxis.ticker = [t for ts, tf in self._data['time_set'] for t in range(ts, tf + 1)]
        else:
            assert callable(self.date_map) or isinstance(self.date_map, dict)
            self.p = figure(aspect_scale=aspect_scale, x_axis_type="datetime")
        if self.y_axis_label is not None:
            self.p.yaxis.axis_label = str(self.y_axis_label)
        self.p.xaxis.axis_label = (str(self.x_axis_label) if self.x_axis_label is not None else 'time')
        self.p.yaxis.ticker = list(range(len(nodes)))
        self.p.yaxis.major_label_overrides = ln

        from bokeh.models import ColumnDataSource
        from bokeh.models.glyphs import Quadratic

        self.p.circle(time_points, node_points, size=8, color="black")
        for ((lp_x, lp_y), c) in l:
            self.p.line(lp_x, lp_y, line_width=2, color='black')

        for ((x0, y0, x1, y1, cx, cy), c) in b:
            data = dict(x0=x0, y0=y0, x1=x1, y1=y1, cx=cx, cy=cy)
            self.p.add_glyph(ColumnDataSource(data), Quadratic(x0='x0', y0='y0', x1='x1', y1='y1', cx='cx', cy='cy', line_color=c, line_width=2))

    def show(self):
        self.draw()
        from bokeh.plotting import show
        show(self.p)

    def save(self, filename=None, file_type='html'):
        self.draw()
        if filename is None:
            filename = self.save_address(file_type)

        if file_type == 'html':
            from bokeh.plotting import output_file, save
            output_file(filename)
            save(self.p)
        elif file_type == 'png':
            from bokeh.io import export_png
            export_png(self.p, filename=filename)
        elif file_type == 'svg':
            from bokeh.io import export_svgs
            export_svgs(self.p, filename=filename)
        else:
            raise Exception('Unsupported file_type: ' + str(file_type))

    def map_datetime(self, t):
        if isinstance(self.date_map, dict):
            return self.date_map[t]
        elif callable(self.date_map):
            return self.date_map(t)
        else:
            return t

    def map_time(self, t):
        if isinstance(self.date_map, dict):
            return self.date_map[t].timestamp() * 1000
        elif callable(self.date_map):
            return self.date_map(t).timestamp() * 1000
        else:
            return t

    def save_address(self, ext):
        if hasattr(self, 'save_address_'):
            return self.save_address_
        else:
            import sys
            import os
            if str(sys.argv[0].split('.')[-1]) == 'py':
                heading = '.'.join(sys.argv[0].split('.')[:-1])
            else:
                heading = sys.argv[0]
            name, cnt = heading + "." + ext, 1
            while os.path.exists(name):
                name = heading + "(" + str(cnt) + ")." + ext
                cnt += 1
            return name

    def _add_ls(self, ls):
        self._data['temporal_linkset'].append(ls)

    def _add_ns(self, ns):
        if self._data['node_set'] is None:
            self._data['node_set'] = ns
        else:
            self._data['node_set'] = self._data['node_set'] | ns

    def _add_nsm(self, nsm):
        if self._data['temporal_nodeset'] is None:
            self._data['temporal_nodeset'] = nsm
        else:
            self._data['temporal_nodeset'] = self._data['temporal_nodeset'] | nsm

    def _add_ts(self, ts):
        if self._data['time_set'] is None:
            self._data['time_set'] = ts
        else:
            self._data['time_set'] = self._data['time_set'] | ts

    @property
    def discrete(self):
        return self._discrete

    @discrete.setter
    def discrete(self, discrete):
        if hasattr(self, '_discrete'):
            assert self._discrete == discrete
        else:
            self._discrete = discrete

    def _add(self, item):
        if isinstance(item, StreamGraph):
            self._add_ls(item.temporal_linkset)
            self._add_ns(item.nodeset)
            self._add_nsm(item.temporal_nodeset)
            self._add_ts(item.timeset)
            self.discrete = item.discrete
        elif isinstance(item, ABC.TimeSet):
            self._add_ts(item)
            self.discrete = item.discrete
        elif isinstance(item, ABC.NodeSet):
            self._add_ns(item)
        elif isinstance(item, ABC.TemporalNodeSet):
            self._add_nsm(item)
            self._add_ns(item.nodeset)
            self._add_ts(item.timeset)
            self.discrete = item.discrete
        elif isinstance(item, ABC.TemporalLinkSet):
            self._add_ls(item)
            self._add(item.minimal_temporal_nodeset)
            self.discrete = item.discrete

    def __iadd__(self, item):
        if not any(isinstance(item, x) for x in [StreamGraph, ABC.TimeSet, ABC.NodeSet, ABC.TemporalNodeSet, ABC.TemporalLinkSet]) and isinstance(item, Iterable):
            for i in item:
                self._add(i)
        else:
            self._add(item)

    def _make_pallete(self, n):
        if n == 1:
            return ['black']
        elif n > 256:
            max_value = 16581375  # 255**3
            interval = int(max_value / n)
            return ['#' + hex(I)[2:].zfill(6) for I in range(0, max_value, interval)]
        elif n <= 10:
            from bokeh.palettes import Category10
            return Category10(n)
        elif n <= 20:
            from bokeh.palettes import Category20
            return Category20(n)
        else:
            from bokeh.palettes import Viridis256
            return Viridis256(n)


def curving(a, b):
    return tanh(abs(a - b) - 1)
