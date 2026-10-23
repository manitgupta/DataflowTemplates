#!/usr/bin/env python3
"""Print an Avro container file's metadata (avro.schema, avro.codec) using only the standard library.

Usage: avro_header.py <local.avro> [golden.avsc]   -> prints codec, schema md5, and whether schema == golden
"""
import hashlib, sys


def read_long(f):
    b = f.read(1)[0]
    n = b & 0x7F
    shift = 7
    while b & 0x80:
        b = f.read(1)[0]
        n |= (b & 0x7F) << shift
        shift += 7
    return (n >> 1) ^ -(n & 1)


def header(path):
    meta = {}
    with open(path, 'rb') as f:
        assert f.read(4) == b'Obj\x01', 'not an Avro container file'
        while True:
            count = read_long(f)
            if count == 0:
                break
            if count < 0:
                read_long(f)
                count = -count
            for _ in range(count):
                k = f.read(read_long(f)).decode()
                meta[k] = f.read(read_long(f))
    return meta


m = header(sys.argv[1])
schema = m['avro.schema']
print('codec', m.get('avro.codec', b'null').decode(), 'schema_md5', hashlib.md5(schema).hexdigest())
if len(sys.argv) > 2:
    golden = open(sys.argv[2], 'rb').read()
    print('schema == golden:', schema == golden)
