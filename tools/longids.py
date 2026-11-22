#!/usr/bin/env python3
"""Make a copy of a dataset where 1 in 8 distinct user/event/click ids gets a 32-char hex suffix, like the
updated generator (keeps each field's quoting; malformed lines are copied unchanged).
Usage: longids.py SRC DST"""
import hashlib, os, shutil, sys

src, dst = sys.argv[1], sys.argv[2]


def ext(v: str) -> str:
    if not v:
        return v
    h = hashlib.blake2b(v.encode('utf-8', 'surrogateescape'), digest_size=16).digest()
    return v + h.hex() if h[0] < 32 else v  # 32/256 = 1 in 8


def fix(field: str) -> str:
    if len(field) >= 2 and field[0] == '"' and field[-1] == '"':
        return '"' + ext(field[1:-1]) + '"'
    if '"' in field:
        return field
    return ext(field)


def convert(sub, k, cols):
    os.makedirs(os.path.join(dst, sub), exist_ok=True)
    for name in sorted(os.listdir(os.path.join(src, sub))):
        with open(os.path.join(src, sub, name), 'rb') as fi, open(os.path.join(dst, sub, name), 'wb') as fo:
            data = fi.read().decode('utf-8', 'surrogateescape').split('\n')
            out = [data[0]]
            for line in data[1:]:
                f = line.split(',')
                if len(f) == k:
                    for c in cols:
                        f[c] = fix(f[c])
                    line = ','.join(f)
                out.append(line)
            fo.write('\n'.join(out).encode('utf-8', 'surrogateescape'))


os.makedirs(dst, exist_ok=True)
for f in ('products.csv', 'campaigns.csv', 'manifest.json'):
    shutil.copy(os.path.join(src, f), os.path.join(dst, f))
convert('events', 7, (0, 1))
convert('clicks', 4, (0, 1))
convert('links', 2, (0, 1))
print('done', dst)
