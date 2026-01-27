#!/usr/bin/env python3
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("--outdir", default="./payloads")
parser.add_argument("--count", type=int, default=200)
parser.add_argument("--size", type=int, default=65536)
args = parser.parse_args()

os.makedirs(args.outdir, exist_ok=True)
for i in range(args.count):
    with open(os.path.join(args.outdir, f"payload_{i:04d}.bin"), "wb") as f:
        f.write(os.urandom(args.size))
print(f"Generated {args.count} payloads of {args.size} bytes in {args.outdir}")
