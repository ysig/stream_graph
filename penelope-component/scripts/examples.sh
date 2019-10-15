set -x
set -v

# Visualize
curl http://127.0.0.1:5000/visualize -d @'dummy.json' -o out.html
python free_html.py out.html

# Maximal-Cliques
curl http://127.0.0.1:5000/maximal-cliques -d @'dummy.json'

# Closeness
python data_to_json.py
python pre.py -j '{"u": "germany"}' -d 'data.json' | curl  http://127.0.0.1:5000/closeness -d @-


