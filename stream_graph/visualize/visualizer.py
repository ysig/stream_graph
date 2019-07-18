from stream_graph import ABC
from stream_graph import StreamGraph
from six import iteritems

class Visualizer(object):
    """Visualization objects for a stream-graph."""
    def __init__(self, items=None, filename=None, image_type='fig'):
        self._data = dict(link_streams=[], node_set=None, time_set=None, node_stream=None)
        if image_type not in ['fig', 'svg']:
            raise ValueError('image_types supported is \'fig\' and \'svg\'')
        else:
            self._ext = image_type
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
        max_value = 16581375 #255**3
        interval = int(max_value / n)
        colors = ['#' + hex(I)[2:].zfill(6) for I in range(0, max_value, interval)]
        if self._ext == 'fig':
            clabels = ['c_' + str(i) for i in range(len(colors))]
            for clabel, color in zip(clabels, colors):
                self.dwg.addColor(clabel, color)
            return clabels
        else:
            return colors

    def _plot_linkstream(self, pallete):
        data = self._data['link_streams']
        if len(data):
            for ls, color in zip(data, pallete):
                if ls.instantaneous:
                    for (u, v, ts) in ls:
                        self.dwg.addLink(u, v, ts, ts, color=color)
                else:
                    for k in ls:
                        self.dwg.addLink(k[0], k[1], k[2], k[3], color=color)


    def _plot_nodes(self, min_time, max_time):
        nodes = dict()
        for n in self._data['node_set']:
            nodes[n] = []
        if self._data['node_stream'].instantaneous:
            for (u, ts) in self._data['node_stream']:
                if ts >= min_time and ts <= max_time:
                    nodes[u].append((ts, ts))
        else:
            for k in self._data['node_stream']:
                if k[1] != min_time or k[2] != max_time:
                    nodes[k[0]].append(k[1:3])
        def takez(a):
            return a[0]
        for (u, times) in iteritems(nodes):
            self.dwg.addNode(u, sorted(times, key=takez))

    def _plot(self, filename, **kargs):
        if self._data['time_set'].instantaneous:
            min_time = min(self._data['time_set'])
            max_time = max(self._data['time_set'])
        else:
            min_time = min(k[0] for k in self._data['time_set'])
            max_time = max(k[1] for k in self._data['time_set'])
        if self._ext == 'fig':
            from .stream_fig import Drawing
        else:
            from .stream_svg import Drawing

        nargs = dict()
        if 'time_width' in nargs:
            nargs['time_width'] = kargs['time_width']
        self.dwg = Drawing(filename, alpha=min_time, omega=max_time, discrete=self.discrete,**nargs)
        pallete = self._make_pallete(len(self._data['link_streams']))
        self._plot_nodes(min_time, max_time)
        self._plot_linkstream(pallete)
        margs = dict()
        if 'ticks' in kargs:
            margs['ticks'] = kargs['ticks']
        self.dwg.addTimeLine(**margs)

    def produce(self, filename=None, **kargs):
        if filename is None:
            filename = self.save_address
        self._plot(filename, **kargs)
