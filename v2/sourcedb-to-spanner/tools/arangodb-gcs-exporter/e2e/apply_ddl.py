#!/usr/bin/env python3
# Copyright (C) 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
"""Applies the exporter's generated Spanner DDL before a template import.

Reads spanner-ddl-googlesql.sql (or -postgresql.sql) in the export root and:
  * database missing            -> creates it with the DDL;
  * database without any of the -> applies the DDL with an update;
    export's tables
  * database with all tables    -> checks every table has exactly the DDL's columns
                                   (fails otherwise) and leaves it unchanged;
  * database with only some     -> fails.

Works for Cloud Spanner (ADC) and Spanner Omni / emulator (--spanner-endpoint host:port,
plaintext, no auth), with the same connection logic as validate.py.

Exit code 0 when the database has the export's schema afterwards.
"""

import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from validate import read_text, spanner_database  # pylint: disable=g-import-not-at-top,wrong-import-position

CREATE_TABLE = re.compile(r'^\s*CREATE\s+TABLE\s+[`"]([^`"]+)[`"]\s*\((.*)\)\s*PRIMARY\s+KEY',
                          re.IGNORECASE | re.DOTALL)
CREATE_TABLE_PG = re.compile(r'^\s*CREATE\s+TABLE\s+"([^"]+)"\s*\((.*)\)\s*$',
                             re.IGNORECASE | re.DOTALL)
COLUMN = re.compile(r'^\s*[`"]([^`"]+)[`"]\s+')


def statements(ddl):
  """Splits the generated DDL (no comments, statements end with ';\\n')."""
  return [s.strip() for s in ddl.split(";\n") if s.strip().rstrip(";").strip()]


def expected_tables(stmts, dialect):
  """Table name -> set of column names, from the CREATE TABLE statements."""
  tables = {}
  for s in stmts:
    m = (CREATE_TABLE_PG if dialect == "postgresql" else CREATE_TABLE).match(s.rstrip(";"))
    if not m:
      continue
    cols = set()
    # Split the column list on commas at nesting depth 0 (generated columns contain commas).
    depth, cur = 0, ""
    for ch in m.group(2):
      if ch == "(":
        depth += 1
      elif ch == ")":
        depth -= 1
      if ch == "," and depth == 0:
        cols.add(cur)
        cur = ""
      else:
        cur += ch
    cols.add(cur)
    names = set()
    for c in cols:
      cm = COLUMN.match(c)
      if cm and not c.strip().upper().startswith("PRIMARY KEY"):
        names.add(cm.group(1))
    tables[m.group(1)] = names
  return tables


def existing_columns(db, dialect):
  """Table name -> set of column names of the user tables in the database."""
  schema = "public" if dialect == "postgresql" else ""
  sql = ("SELECT table_name, column_name FROM information_schema.columns "
         f"WHERE table_schema = '{schema}'")
  out = {}
  with db.snapshot() as snap:
    for table, column in snap.execute_sql(sql):
      out.setdefault(table, set()).add(column)
  return out


def main():
  p = argparse.ArgumentParser(description=__doc__,
                              formatter_class=argparse.RawDescriptionHelpFormatter)
  p.add_argument("--export-dir", required=True, help="Export root with the spanner-ddl-*.sql files (local or gs://)")
  p.add_argument("--spanner-endpoint", default=None, help="host:port of Omni / emulator")
  p.add_argument("--project", required=True)
  p.add_argument("--instance", required=True)
  p.add_argument("--database", required=True)
  p.add_argument("--dialect", choices=["googlesql", "postgresql"], default="googlesql")
  args = p.parse_args()

  export_dir = args.export_dir.rstrip("/")
  ddl_file = f"{export_dir}/spanner-ddl-{args.dialect}.sql"
  stmts = statements(read_text(ddl_file))
  wanted = expected_tables(stmts, args.dialect)
  if not wanted:
    print(f"[FAIL] no CREATE TABLE statements in {ddl_file}")
    return 1
  db = spanner_database(args)

  if not db.exists():
    print(f"creating database {args.database} with {len(stmts)} statements from {ddl_file}")
    if args.dialect == "postgresql":
      from google.cloud.spanner_admin_database_v1 import DatabaseDialect  # pylint: disable=g-import-not-at-top
      db = db._instance.database(args.database,  # pylint: disable=protected-access
                                 database_dialect=DatabaseDialect.POSTGRESQL)
      db.create().result(600)
      db.update_ddl(stmts).result(1800)
    else:
      db = db._instance.database(args.database, ddl_statements=stmts)  # pylint: disable=protected-access
      db.create().result(1800)
    return 0

  have = existing_columns(db, args.dialect)
  present = {t for t in wanted if t in have}
  if not present:
    print(f"database {args.database} exists without the export's tables: applying {ddl_file}")
    db.update_ddl(stmts).result(1800)
    return 0
  if present != set(wanted):
    print(f"[FAIL] database {args.database} has only some of the export's tables "
          f"(present: {sorted(present)}, missing: {sorted(set(wanted) - present)}); "
          "use a fresh database or fix the schema by hand")
    return 1
  diffs = [f"{t}: database {sorted(have[t])} != DDL {sorted(wanted[t])}"
           for t in sorted(wanted) if have[t] != wanted[t]]
  if diffs:
    print("[FAIL] existing tables differ from the generated DDL: " + "; ".join(diffs))
    return 1
  print(f"database {args.database} already has the export's schema ({len(wanted)} tables)")
  return 0


if __name__ == "__main__":
  sys.exit(main())
