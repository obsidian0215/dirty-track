#!/usr/bin/env python3
import argparse, requests, time, csv, os, random
from concurrent.futures import ThreadPoolExecutor, as_completed

parser = argparse.ArgumentParser()
parser.add_argument('--url', required=True, help='Endpoint URL (e.g., http://localhost:8080/compress)')
parser.add_argument('--file', default=None, help='File to send as raw body (relative to --dataset if not absolute)')
parser.add_argument('--dataset', default=None, help='Path to datasets directory (default: repo datasets/)')
parser.add_argument('--iters', type=int, default=10)
parser.add_argument('--concurrency', type=int, default=1)
parser.add_argument('--out', default='bench_gzip.csv')
args = parser.parse_args()

# Resolve input file: if --file omitted, pick a random file from dataset
fn = args.file
if fn is None:
    dataset_dir = args.dataset or os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'datasets'))
    if not os.path.isdir(dataset_dir):
        raise SystemExit('dataset dir not found: %s' % dataset_dir)
    candidates = []
    for root, _, files in os.walk(dataset_dir):
        for f in files:
            candidates.append(os.path.join(root, f))
    if not candidates:
        raise SystemExit('no files in dataset dir: %s' % dataset_dir)
    fn = random.choice(candidates)
else:
    if not os.path.isabs(fn):
        candidate = os.path.join(args.dataset or os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'datasets')), fn)
        if os.path.exists(candidate):
            fn = candidate

fn = args.file
if not os.path.exists(fn):
    raise SystemExit('file not found: %s' % fn)
with open(fn, 'rb') as fh:
    data = fh.read()

def do_one(i):
    start = time.time()
    r = requests.post(args.url, data=data, headers={'Content-Type': 'application/octet-stream'}, timeout=60)
    elapsed = time.time() - start
    return (i, r.status_code, elapsed, r.text[:200])

results = []
with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
    futures = [ex.submit(do_one, i) for i in range(args.iters)]
    for f in as_completed(futures):
        results.append(f.result())

with open(args.out, 'w') as csvf:
    writer = csv.writer(csvf)
    writer.writerow(['iter', 'status', 'secs', 'preview'])
    for r in results:
        writer.writerow(r)
print('Wrote', args.out)
