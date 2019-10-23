set -x
set -v

# Visualize
curl http://127.0.0.1:5000/visualize -d '{"temporal-link-set": {"u": ["maria", "kostas"], "v": ["panos", "panos"], "ts": [0, 1], "discrete": true, "instantaneous": true}}' -o out.html
python free_html.py out.html

# Maximal-Cliques
curl http://127.0.0.1:5000/maximal-cliques -d '{"temporal-link-set": {"u": ["maria", "kostas"], "v": ["panos", "panos"], "ts": [0, 1], "discrete": true, "instantaneous": true}}'

# Merge
curl http://127.0.0.1:5000/temporal-link-set/merge -d '{"temporal-link-set":{"u": [1, 1, 1, 2], "v": [2, 2, 2, 3], "ts":[0, 1, 2, 3], "tf":[2, 4, 5, 6], "discrete": true}}'

# Closeness
python data_to_json.py
python pre.py -j '{"u": "germany"}' -d 'data.json' | curl  http://127.0.0.1:5000/closeness -d @-


