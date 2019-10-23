####################
# Import libraries #
####################

# Here, you import a few functions from the flask library that is used for making the web application,
# and the json library that is used to make json objects.

import json
import tempfile
import functools
import os
import traceback
from pandas import DataFrame as DF
from pandas import read_csv
from datetime import datetime
from flask import Flask
from flask import request, Response
from stream_graph import TemporalLinkSetDF, ITemporalLinkSetDF
from stream_graph import ITemporalNodeSetDF, TemporalNodeSetDF
from stream_graph import TimeSetDF, ITimeSetS
from stream_graph import NodeSetS
from stream_graph import Visualizer
####################
# Name Application #
####################


# Here, you give a name to your component and initialize it.

SIZE_LIMIT = False
service = Flask(__name__)

###############
# Define URLs #
###############

def read_temporal_link_set(request_data):
    discrete = request_data.pop('discrete', None)
    instantaneous = request_data.pop('instantaneous', None)
    merge = not request_data.pop('merge', False)
    return _read_tls_json(request_data, discrete, instantaneous, merge)
        
def _read_tls_json(data, discrete, instantaneous, merge):
    instantaneous = ('tf' not in data if instantaneous is None else instantaneous)
    df = DF(data)
    if SIZE_LIMIT and df.shape[0] > 10000:
        raise Exception("For testing purposes size of the input shouldn't exceed 10K links.")
    if instantaneous:
        return ITemporalLinkSetDF(df, discrete=discrete, no_duplicates=merge)
    else:
        return TemporalLinkSetDF(df, discrete=discrete, disjoint_intervals=merge)

def read_temporal_node_set(request_data):
    discrete = request_data.pop('discrete', None)
    instantaneous = request_data.pop('instantaneous', None)
    merge = not request_data.pop('merge', False)
    return _read_tns_json(request_data, discrete, instantaneous, merge)
        
def _read_tns_json(data, discrete, instantaneous, merge):
    instantaneous = ('tf' not in data if instantaneous is None else instantaneous)
    if instantaneous:
        return ITemporalNodeSetDF(DF(data), discrete=discrete, no_duplicates=merge)
    else:
        return TemporalNodeSetDF(DF(data), discrete=discrete, disjoint_intervals=merge)

def read_time_set(request_data):
    discrete = request_data.pop('discrete', None)
    instantaneous = request_data.pop('instantaneous', None)
    merge = not request_data.pop('merge', False)
    return _read_ts_json(request_data, discrete, instantaneous, merge)
        
def _read_ts_json(data, discrete, instantaneous, merge):
    instantaneous = ('tf' not in data if instantaneous is None else instantaneous)
    if instantaneous:
        return ITimeSetS(data['ts'], discrete=discrete, no_duplicates=merge)
    else:
        return TimeSetDF(DF(data), discrete=discrete, disjoint_intervals=merge)

def read_node_set(request_data):
    return NodeSetS(request_data)

def maximal_cliques_(request_data):
    tls = read_temporal_link_set(request_data['temporal-link-set'])

    if tls.instantaneous:
        delta = request_data.get('delta', 0)
        maximal_cliques = tls.get_maximal_cliques(delta=delta)
    else:
        maximal_cliques = tls.get_maximal_cliques()

    return json.dumps({'maximal_cliques': [(list(clique), (ts, tf)) for (clique, (ts, tf)) in maximal_cliques]})

def closeness_(request_data):
    tls = read_temporal_link_set(request_data['temporal-link-set'])
    
    if tls.instantaneous:
        u = request_data.get('u', None)
        t = request_data.get('t', None)
        closeness = tls.closeness(u=u, t=t)
        if u is None:
            if t is None:
                out = {u: list(tc) for (u, tc) in closeness} 
            else:
                out = dict(closeness)
        else:
            out = list(closeness)    
    else:
        raise Exception('closeness is supported only for instantaneous')

    # Return a JSON object containing the concatenated strings
    return json.dumps({'closeness_centrality': out})

def visualize_(request_data):
    x_axis_label = request_data.get('x_axis_label', None)
    y_axis_label = request_data.get('y_axis_label', None)
    date_map = request_data.get('date_map', None)
    if date_map is not None:
        date_map = {k: datetime.strptime(v, '%Y-%m-%d') for k, v in date_map.items()}
    vz = Visualizer(x_axis_label=x_axis_label,
                    y_axis_label=y_axis_label,
                    date_map=date_map)
    tls = read_temporal_link_set(request_data['temporal-link-set'])
    vz.fit(tls)
    if 'temporal-node-set' in request_data:
        vz.fit(read_temporal_node_set(request_data['temporal-node-set']))
    if 'time-set' in request_data:
        vz.fit(read_time_set(request_data['time-set']))
    if 'node-set' in request_data:
        vz.fit(read_node_set(request_data['node-set']))
    fn = next(tempfile._get_candidate_names()) + '.html'
    vz.save(filename=fn, file_type='html')
    with open(fn, 'r') as f:
        html = f.read()
    os.remove(fn)
    return json.dumps({'html': html})

def merge_tls_(data):
    discrete = data.get('discrete', None)
    instantaneous = data.get('instantaneous', None)
    tls = _read_tls_json(data, discrete, instantaneous, False)    
    df = tls.df
    merge = False
    d = {c: list(df[c]) for c in df.columns}
    d['merge'] = False
    d['discrete'] = tls.discrete
    d['instantaneous'] = tls.instantaneous
    return json.dumps({'temporal-link-set': d})


def stream(func):
    @functools.wraps(func)
    def wrapper_stream(*args, **kwargs):
        try:
            return Response(func(*args, **kwargs), mimetype='json', status=200)
        except Exception as ex:
            return Response(json.dumps({'error': traceback.format_exc()}), mimetype='json', status=400)
    return wrapper_stream


@service.route("/temporal-link-set/merge", methods = ['POST'])
@stream
def merge_tls():
    return merge_tls_(request.get_json(force=True)['temporal-link-set'])

@service.route("/closeness", methods = ['POST'])
@stream
def closeness():
    return closeness_(request.get_json(force=True))

@service.route("/maximal-cliques", methods = ['POST'])
@stream
def maximal_cliques():
    return maximal_cliques_(request.get_json(force=True))

@service.route("/visualize", methods = ['POST'])
@stream
def visualize():
    return visualize_(request.get_json(force=True))

############################
# Starting the application #
############################

# Start the application automatically when running this file using `$ python3 demo_component.py`
# By default, it listens to localhost:5000

if __name__ == '__main__':
    service.run(debug=False)
