#!/usr/bin/env python3
"""Write the same proportional split points prepare_spanner_db.py uses, one per line, for the Omni CLI.

Usage: gen_splits.py <out_dir> [--scale 1.35] [--total-splits 200]   -> splits-NN.txt files of 20 lines
"""
import argparse, os

TABLES = {'users': ('u', 500_000_000, 1850), 'products': ('p', 125_000_000, 5771), 'orders': ('o', 625_000_000, 2605),
          'events': ('e', 2_333_000_000, 1342), 'follows': ('f', 2_000_000_000, 503),
          'purchased': ('b', 2_000_000_000, 473)}
ap = argparse.ArgumentParser()
ap.add_argument('out')
ap.add_argument('--scale', type=float, default=1.35)
ap.add_argument('--total-splits', type=int, default=200)
ap.add_argument('--type-zoo-splits', type=int, default=0,
                help='extra even split points for type_zoo (keys z + 12 digits); a single hot split otherwise')
a = ap.parse_args()
total_bytes = sum(c * b for _, c, b in TABLES.values())
splits = []
for t, (p, c, b) in TABLES.items():
    n = int(c * a.scale)
    k = max(1, round(a.total_splits * c * b / total_bytes))
    splits += [f"TABLE {t} ('{p}{i * n // (k + 1):012d}')" for i in range(1, k + 1)]
nz = int(10_000_000 * a.scale)
splits += [f"TABLE type_zoo ('z{i * nz // (a.type_zoo_splits + 1):012d}')" for i in range(1, a.type_zoo_splits + 1)]
os.makedirs(a.out, exist_ok=True)
for i in range(0, len(splits), 20):
    with open(os.path.join(a.out, f'splits-{i // 20:02d}.txt'), 'w') as f:
        f.write('\n'.join(splits[i:i + 20]) + '\n')
print(len(splits), 'split points')
