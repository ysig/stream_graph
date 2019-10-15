import json
import sys

with open(str(sys.argv[1]), 'r') as f:
    data = json.load(f)

with open(str(sys.argv[2]) if len(sys.argv) > 2 else str(sys.argv[1]), 'w') as f:
    f.write(data['html'])


