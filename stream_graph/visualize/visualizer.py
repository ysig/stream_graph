from stream_graph import ABC
from stream_graph import StreamGraph
from .stream_fig import Drawing

from six import iteritems

class Visualizer(object):
    _data = dict(link_streams=[], node_set=None, time_set=None, node_stream=None)
    _color_cnt = 0
    _ext = 'fig'
    def __init__(self, items=None, filename=None):
        if items is not None:
            self.__iadd__(items)
        if filename is not None:
            self.save_address = filename

    @property
    def save_address(self):
        if hasattr(self, 'save_address_'):
            return self.save_address_
        else:
            import sys
            import os
            if str(sys.argv[0].split('.')[-1]) == 'py':
                heading = '.'.join(sys.argv[0].split('.')[:-1])
            else:
                heading = sys.argv[0]
            name, cnt = heading + "." + self._ext, 1
            while os.path.exists(name):
                name = heading + "(" + str(cnt) + ")." + self._ext
                cnt += 1
            return name

    @save_address.setter
    def save_address(self, val):
        self.save_address_ = val

    def _add_ls(self, ls):
        self._data['link_streams'].append(ls)

    def _add_ns(self, ns):
        if self._data['node_set'] is None:
            self._data['node_set'] = ns
        else:
            self._data['node_set'] = self._data['node_set'] | ns

    def _add_nsm(self, nsm):
        if self._data['node_stream'] is None:
            self._data['node_stream'] = nsm
        else:
            self._data['node_stream'] = self._data['node_stream'] | nsm

    def _add_ts(self, ts):
        if self._data['time_set'] is None:
            self._data['time_set'] = ts
        else:
            self._data['time_set'] = self._data['time_set'] | ts

    def _add(self, item):
        if isinstance(item, StreamGraph):
            self._add_ls(item.linkstream)
            self._add_ns(item.nodeset)
            self._add_nsm(item.nodestream)
            self._add_ts(item.timeset)
        elif isinstance(item, ABC.TimeSet):
            self._add_ts(item)
        elif isinstance(item, ABC.NodeSet):
            self._add_ns(item)
        elif isinstance(item, ABC.NodeStream):
            self._add_nsm(item)
            self._add_ns(item.nodeset)
            self._add_ts(item.timeset) 
        elif isinstance(item, ABC.LinkStream):
            self._add_ls(item)
            self._add(item.basic_nodestream)

    def __iadd__(self, item):
        if not any(isinstance(item, x) for x in [StreamGraph, ABC.TimeSet, ABC.NodeSet, ABC.NodeStream, ABC.LinkStream]) and isinstance(item, Iterable):
            for i in item:
                self._add(i)
        else:
            self._add(item)

    @property
    def _pick_color(self):
        return self._color_cnt
        self._color_cnt += 1

    def _plot_linkstream(self, dwg):
        for ls in self._data['link_streams']:
            color = self._pick_color
            for (u, v, ts, tf) in iter(ls):
                dwg.addLink(u, v, ts, tf, color = color)

    def _plot_nodes(self, dwg, min_time, max_time):
        nodes = dict()
        for n in self._data['node_set']:
            nodes[n] = []
        for (u, ts, tf) in self._data['node_stream']:
            if ts != min_time or tf != max_time:
                nodes[u].append((ts, tf))
        def takez(a):
            return a[0]
        for (u, times) in iteritems(nodes):
            dwg.addNode(u, sorted(times, key=takez))

    def _plot(self, filename):
        min_time = min(i for (i, _) in self._data['time_set'])
        max_time = max(i for (_, i) in self._data['time_set'])
        dwg = Drawing(filename, alpha=min_time, omega=max_time)
        self._plot_nodes(dwg, min_time, max_time)
        self._plot_linkstream(dwg)
        dwg.addTimeLine()

    def produce(self, filename=None):
        if filename is None:
            filename = self.save_address
        self._plot(filename)
