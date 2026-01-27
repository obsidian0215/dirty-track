#!/usr/bin/env python3
# generate synthetic images with text for OCR testing
import argparse
import os
from PIL import Image, ImageDraw, ImageFont

parser = argparse.ArgumentParser()
parser.add_argument("--outdir", default="./images")
parser.add_argument("--count", type=int, default=200)
parser.add_argument("--width", type=int, default=320)
parser.add_argument("--height", type=int, default=80)
args = parser.parse_args()

os.makedirs(args.outdir, exist_ok=True)
for i in range(args.count):
    img = Image.new("RGB", (args.width, args.height), color=(255,255,255))
    d = ImageDraw.Draw(img)
    try:
        f = ImageFont.load_default()
        d.text((10,10), f"SAMPLE {i}", font=f, fill=(0,0,0))
    except Exception:
        d.text((10,10), f"SAMPLE {i}", fill=(0,0,0))
    img.save(os.path.join(args.outdir, f"img_{i:04d}.png"))
print(f"Generated {args.count} images in {args.outdir}")
