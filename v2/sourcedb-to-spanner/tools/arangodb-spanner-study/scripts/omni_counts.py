#!/usr/bin/env python3
"""Exact COUNT(*) of every user table in a Spanner Omni (or Cloud Spanner) database.

Usage: omni_counts.py <database> [--omni host:port] [--project P --instance I]
Omni: plaintext endpoint via the emulator code path (SPANNER_EMULATOR_HOST), project/instance 'default'.
"""
import argparse, os, time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('database')
    ap.add_argument('--omni', help='host:port of Spanner Omni')
    ap.add_argument('--project', default='default')
    ap.add_argument('--instance', default='default')
    a = ap.parse_args()
    if a.omni:
        os.environ['SPANNER_EMULATOR_HOST'] = a.omni
    from google.cloud import spanner
    from google.auth.credentials import AnonymousCredentials
    client = (spanner.Client(project='default', credentials=AnonymousCredentials()) if a.omni
              else spanner.Client(project=a.project))
    db = client.instance('default' if a.omni else a.instance).database(a.database)
    with db.snapshot(multi_use=True) as snap:
        tables = [r[0] for r in snap.execute_sql(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '' AND TABLE_TYPE = 'BASE TABLE' "
            "ORDER BY TABLE_NAME")]
        total = 0
        print(time.strftime('%H:%M:%S'))
        for t in tables:
            n = list(snap.execute_sql(f'SELECT COUNT(*) FROM `{t}`'))[0][0]
            total += n
            print(f'{t:13s} {n:>15,}')
        print(f'TOTAL         {total:>15,}')


if __name__ == '__main__':
    main()
