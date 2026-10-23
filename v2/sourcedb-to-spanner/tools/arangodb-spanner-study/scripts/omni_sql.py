#!/usr/bin/env python3
"""Run one SQL query against Spanner Omni (plaintext). Usage: omni_sql.py <db> <host:port> "<sql>" """
import os, sys

db_name, host, sql = sys.argv[1:4]
os.environ['SPANNER_EMULATOR_HOST'] = host
from google.cloud import spanner  # noqa: E402
from google.auth.credentials import AnonymousCredentials  # noqa: E402

db = spanner.Client(project='default', credentials=AnonymousCredentials()).instance('default').database(db_name)
with db.snapshot() as snap:
    for row in snap.execute_sql(sql):
        print(*row)
