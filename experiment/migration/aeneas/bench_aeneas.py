#!/usr/bin/env python3
import argparse, requests, time, csv, os
from concurrent.futures import ThreadPoolExecutor, as_completed

parser = argparse.ArgumentParser()
parser.add_argument('--url', required=True, help='Endpoint URL (e.g., http://localhost:8080/align)')
parser.add_argument('--audio', default=None, help='Audio file to upload (relative to --dataset if not absolute)')
parser.add_argument('--text', default=None, help='Text file to upload (relative to --dataset if not absolute)')
parser.add_argument('--dataset', default=None, help='Path to datasets directory (default: repo datasets/)')
parser.add_argument('--iters', type=int, default=10)
parser.add_argument('--concurrency', type=int, default=1)
parser.add_argument('--out', default='bench_aeneas.csv')
args = parser.parse_args()

# Resolve audio/text from dataset if not provided
def _resolve_path(p, subdir_hint=None):
    if p:
        if os.path.isabs(p) and os.path.exists(p):
            return p
        # try relative to dataset
        ds = args.dataset or os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'datasets'))
        candidate = os.path.join(ds, p)
        if os.path.exists(candidate):
            return candidate
        # fallback to searching dataset tree for matching filename
        for root, _, files in os.walk(ds):
            if p in files:
                return os.path.join(root, p)
        raise SystemExit(f'file not found: {p} (dataset: {ds})')
    else:
        # pick a sample from dataset
        ds = args.dataset or os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'datasets'))
        if not os.path.isdir(ds):
            raise SystemExit(f'dataset dir not found: {ds}')
        candidates = []
        for root, _, files in os.walk(ds):
            for f in files:
                if subdir_hint and subdir_hint not in f and subdir_hint is not None:
                    continue
                candidates.append(os.path.join(root, f))
        if not candidates:
            raise SystemExit(f'no files found in dataset dir: {ds}')
        return random.choice(candidates)

args.audio = _resolve_path(args.audio, subdir_hint='audio')
args.text = _resolve_path(args.text, subdir_hint='text')


def do_one(i):
    start = time.time()
    with open(args.audio, 'rb') as a, open(args.text, 'rb') as t:
        files = {'audio': (os.path.basename(args.audio), a), 'text': (os.path.basename(args.text), t)}
        r = requests.post(args.url, files=files, timeout=120)
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
