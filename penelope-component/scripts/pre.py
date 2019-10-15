import argparse, json

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-j', '--json', type=str, help='a json')
parser.add_argument('-d', '--data', type=str, help='The data file')
args = parser.parse_args()
d = json.load(open(args.data, 'r'))
d.update(json.loads(args.json))
print(json.dumps(d), end="")

