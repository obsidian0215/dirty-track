#!/usr/bin/env python3
import argparse,requests,time,statistics
from concurrent.futures import ThreadPoolExecutor, as_completed

parser = argparse.ArgumentParser()
parser.add_argument('--server', required=True, help='Base URL e.g. http://127.0.0.1:8080/align')
parser.add_argument('--audio', required=True)
parser.add_argument('--text', required=True)
parser.add_argument('--requests', type=int, default=20)
parser.add_argument('--concurrency', type=int, default=2)
args = parser.parse_args()

sess = requests.Session()

latencies = []
success = 0

with open(args.audio, 'rb') as afh, open(args.text, 'rb') as tfh:
    audio_data = afh.read()
    text_data = tfh.read()

def do_one(i):
    start = time.time()
    try:
        files = {'audio': ('audio.mp3', audio_data), 'text': ('text.xhtml', text_data)}
        r = sess.post(args.server, files=files, timeout=120)
        status = r.status_code
    except Exception:
        return None
    return (status, (time.time()-start)*1000.0)

with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
    futures = [ex.submit(do_one, i) for i in range(args.requests)]
    for f in as_completed(futures):
        res = f.result()
        if not res:
            continue
        status, lat = res
        if 200 <= status < 300:
            success += 1
            latencies.append(lat)

ops = len(latencies)
avg = statistics.mean(latencies) if latencies else 0.0
p50 = statistics.median(latencies) if latencies else 0.0
p95 = (sorted(latencies)[int(len(latencies)*0.95)-1] if latencies and len(latencies)>=1 else 0.0)
duration = sum(latencies)/1000.0 if ops else 0.0
ops_per_sec = ops/duration if duration>0 else 0.0

print("METRIC_HEADER\tavg_latency_ms\tp50_ms\tp95_ms\tops_per_sec\ttotal_success")
print(f"METRIC_VALUES\t{avg:.3f}\t{p50:.3f}\t{p95:.3f}\t{ops_per_sec:.3f}\t{success}")
