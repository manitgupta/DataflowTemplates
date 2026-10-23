#!/usr/bin/env python3
"""Write a tiny fake DLQ file (rows whose JSON Spanner's exact mode rejects) for testing replay_dlq.py."""
import json, sys

rows = [
    {'_metadata_table': 'user_profiles', '_metadata_spanner_mutation': True, 'key': 'dlqtest1',
     'id': 'user-profiles/dlqtest1', 'rev': 'r1', 'doc': '{"_key":"dlqtest1","x":0.601049,"y":4.9E-324}'},
    {'_metadata_table': 'knows', 'key': 'dlqtest2', 'id': 'knows/dlqtest2', 'rev': 'r2',
     'from_id': 'user-profiles/a', 'to_id': 'user-profiles/b', 'doc': '{"_key":"dlqtest2","w":0.21313}'},
]
with open(sys.argv[1], 'w') as f:
    for r in rows:
        f.write(json.dumps({'message': r, 'error_message': 'SpannerWriteFailed'}) + '\n')
