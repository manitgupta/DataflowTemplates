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
"""Validates an ArangoDB -> GCS -> Spanner migration end to end.

The export is a SourceDB to Spanner bulk GCS export (<table>/<md5>-NNNNN-of-MMMMM.avro); the
counts come from its summary arangodb-export.json (written last by the exporter).

Checks, per summary table (a table missing in Spanner FAILs unless --allow-missing-tables;
--tables restricts the checked set; at least one table must pass the count check):
  0. layout: files[] are exactly <table>/<md5>-00000-of-MMMMM.avro .. -(MMMMM-1)-of-MMMMM.avro
     with one md5 per table, and the table directory holds nothing else; no _staging left
  1. sum(files[].rows) == rowCount, and rowCount == ArangoDB count (if the source is quiet)
  2. Spanner COUNT(*) + severe DLQ records for the table == summary rowCount
  3. --sample random keys (index-only RAND() sample in ArangoDB): the Spanner `doc`
     equals the ArangoDB document (canonical JSON: sorted keys, numbers as Decimal).
     Known Spanner JSON normalisation differences (-0.0, integers beyond 2^53, float
     rounding) are reported as WARN, not FAIL.
  4. edge tables: generated from_key / to_key (if present) match from_id / to_id

Needs: pip install google-cloud-spanner   (google-cloud-storage for gs:// export dirs)

Spanner Omni / emulator: --spanner-endpoint host:port sets SPANNER_EMULATOR_HOST (plaintext,
no auth), as in the Omni POC. Without it, Application Default Credentials are used.

Exit code 0 when no check FAILed.
"""

import argparse
import base64
import decimal
import glob
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal

decimal.getcontext().prec = 400

RESULTS = {"PASS": 0, "WARN": 0, "FAIL": 0}

SUMMARY_FILE = "arangodb-export.json"
BULK_FILE = re.compile(r"^([0-9a-f]{32})-(\d{5,})-of-(\d{5,})\.avro$")


def report(level, msg):
  RESULTS[level] += 1
  print(f"[{level}] {msg}", flush=True)


# ---------------------------------------------------------------------------- export dir
def read_text(path):
  if path.startswith("gs://"):
    from google.cloud import storage  # pylint: disable=g-import-not-at-top
    bucket, _, blob = path[len("gs://"):].partition("/")
    return storage.Client().bucket(bucket).blob(blob).download_as_text()
  if path.startswith("file://"):
    path = path[len("file://"):]
  with open(path, encoding="utf-8") as f:
    return f.read()


def list_names(path):
  """Names of all objects/files directly below directory `path` (hidden ones included)."""
  path = path.rstrip("/")
  if path.startswith("gs://"):
    from google.cloud import storage  # pylint: disable=g-import-not-at-top
    bucket, _, prefix = path[len("gs://"):].partition("/")
    prefix += "/"
    names = set()
    for blob in storage.Client().list_blobs(bucket, prefix=prefix):
      rest = blob.name[len(prefix):]
      if rest:
        names.add(rest.split("/", 1)[0])
    return sorted(names)
  if path.startswith("file://"):
    path = path[len("file://"):]
  return sorted(os.listdir(path)) if os.path.isdir(path) else []


def check_layout(export_dir, table):
  """Checks the bulk GCS file layout of one summary table; returns True when it is right."""
  name = table["name"]
  files = [f["path"] for f in table.get("files", [])]
  ok = True
  md5s, indexes, totals = set(), [], set()
  for path in files:
    directory, _, base = path.partition("/")
    m = BULK_FILE.match(base)
    if directory != name or not m:
      report("FAIL", f"{name}: file {path} does not follow <table>/<md5>-NNNNN-of-MMMMM.avro")
      ok = False
      continue
    md5s.add(m.group(1))
    indexes.append(int(m.group(2)))
    totals.add(int(m.group(3)))
  if not ok:
    return False
  if len(md5s) > 1:
    report("FAIL", f"{name}: files have {len(md5s)} different schema md5 prefixes")
    ok = False
  if files and (totals != {len(files)} or sorted(indexes) != list(range(len(files)))):
    report("FAIL", f"{name}: file indexes are not 0..{len(files) - 1} of {len(files)}")
    ok = False
  on_disk = list_names(f"{export_dir}/{name}")
  expected = sorted(p.partition("/")[2] for p in files)
  if on_disk != expected:
    extra = sorted(set(on_disk) - set(expected))[:5]
    missing = sorted(set(expected) - set(on_disk))[:5]
    report("FAIL", f"{name}: table directory differs from the summary "
           f"(extra {extra}, missing {missing})")
    ok = False
  if ok:
    report("PASS", f"{name}: {len(files)} bulk files with schema md5 {next(iter(md5s), '-')}")
  return ok


# ---------------------------------------------------------------------------- arangodb
class Arango:

  def __init__(self, url, user, password, database):
    self.url = url.rstrip("/")
    self.database = database
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    self.headers = {"Authorization": f"Basic {token}", "Content-Type": "application/json"}

  def _call(self, method, path, body=None, ok=(200, 201)):
    full = f"{self.url}/_db/{urllib.parse.quote(self.database, safe='')}{path}"
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(full, data=data, method=method, headers=self.headers)
    try:
      with urllib.request.urlopen(req, timeout=600) as resp:
        return resp.status, resp.read()
    except urllib.error.HTTPError as e:
      if e.code in ok or e.code == 404:
        return e.code, e.read()
      raise RuntimeError(f"{method} {path} -> HTTP {e.code}: {e.read()[:300]!r}") from e

  def count(self, collection):
    _, payload = self._call("GET", f"/_api/collection/{urllib.parse.quote(collection)}/count")
    return json.loads(payload)["count"]

  def document_raw(self, collection, key):
    status, payload = self._call(
        "GET",
        f"/_api/document/{urllib.parse.quote(collection, safe='')}/"
        f"{urllib.parse.quote(key, safe='')}")
    return None if status == 404 else payload.decode("utf-8")

  def sample_keys(self, collection, n, count):
    if n <= 0 or count == 0:
      return []
    p = min(1.0, (3.0 * n) / count)
    body = {
        "query": "FOR d IN @@c FILTER RAND() < @p LIMIT @n RETURN d._key",
        "bindVars": {"@c": collection, "p": p, "n": n},
        "batchSize": 10000,
    }
    _, payload = self._call("POST", "/_api/cursor", body)
    res = json.loads(payload)
    keys = list(res["result"])
    while res.get("hasMore"):
      _, payload = self._call("POST", f"/_api/cursor/{res['id']}")
      res = json.loads(payload)
      keys.extend(res["result"])
    return keys


# ---------------------------------------------------------------------------- json compare
def canonical(value, lenient=False):
  """Canonical, comparable form. lenient=True rounds numbers to doubles and folds -0 to 0."""
  if isinstance(value, dict):
    return {k: canonical(v, lenient) for k, v in sorted(value.items())}
  if isinstance(value, list):
    return [canonical(v, lenient) for v in value]
  if isinstance(value, bool) or value is None:
    return value
  if isinstance(value, (Decimal, int, float)):
    d = Decimal(str(value)) if not isinstance(value, Decimal) else value
    if lenient:
      f = float(d)
      return 0.0 if f == 0 else f
    if d == 0:
      return "0"
    return format(d.normalize(), "f") if abs(d.adjusted()) < 50 else str(d.normalize())
  return value


def parse_json(text):
  return json.loads(text, parse_float=Decimal, parse_int=Decimal)


def first_difference(a, b, path="$"):
  if type(a) is not type(b):
    return f"{path}: {a!r} != {b!r}"
  if isinstance(a, dict):
    for k in sorted(set(a) | set(b)):
      if k not in a or k not in b:
        return f"{path}.{k}: present only on one side"
      d = first_difference(a[k], b[k], f"{path}.{k}")
      if d:
        return d
    return None
  if isinstance(a, list):
    if len(a) != len(b):
      return f"{path}: array length {len(a)} != {len(b)}"
    for i, (x, y) in enumerate(zip(a, b)):
      d = first_difference(x, y, f"{path}[{i}]")
      if d:
        return d
    return None
  return None if a == b else f"{path}: {str(a)[:80]!r} != {str(b)[:80]!r}"


# ---------------------------------------------------------------------------- DLQ
def table_of_dlq_record(rec):
  if isinstance(rec, dict):
    for k in ("_metadata_table", "tableName", "table"):
      if isinstance(rec.get(k), str):
        return rec[k]
    for k in ("message", "json", "originalPayload"):
      inner = rec.get(k)
      if isinstance(inner, str):
        try:
          t = table_of_dlq_record(json.loads(inner))
        except ValueError:
          t = None
        if t:
          return t
      elif isinstance(inner, dict):
        t = table_of_dlq_record(inner)
        if t:
          return t
  return None


def severe_dlq_files(dlq_dir):
  """Files with severe DLQ records below dlq_dir (the template's <outputDirectory>/dlq).

  Only files below a 'severe' path component count (or every file when dlq_dir itself is the
  severe directory); retry records and temporary files (a 'tmp' component, or a component
  starting with '.' or '_') are ignored. A missing severe directory means 0 records.
  """
  root = os.path.normpath(dlq_dir)
  in_severe = os.path.basename(root) == "severe"
  files = []
  for f in glob.glob(os.path.join(root, "**", "*"), recursive=True):
    if not os.path.isfile(f):
      continue
    parts = os.path.relpath(f, root).split(os.sep)
    if any(c == "tmp" or c.startswith((".", "_")) for c in parts):
      continue
    if in_severe or "severe" in parts[:-1]:
      files.append(f)
  return files


def severe_dlq_counts(dlq_dir):
  counts = {}
  if not dlq_dir:
    return counts
  for f in severe_dlq_files(dlq_dir):
    with open(f, encoding="utf-8", errors="replace") as fh:
      for line in fh:
        line = line.strip()
        if not line:
          continue
        try:
          t = table_of_dlq_record(json.loads(line))
        except ValueError:
          t = None
        counts[t or "<unknown>"] = counts.get(t or "<unknown>", 0) + 1
  return counts


# ---------------------------------------------------------------------------- spanner
def spanner_database(args):
  if args.spanner_endpoint:
    os.environ["SPANNER_EMULATOR_HOST"] = args.spanner_endpoint.replace(
        "http://", "").replace("https://", "")
  from google.cloud import spanner  # pylint: disable=g-import-not-at-top
  client = spanner.Client(project=args.project)
  return client.instance(args.instance).database(args.database)


def q(dialect, ident):
  return f'"{ident}"' if dialect == "postgresql" else f"`{ident}`"


def main():
  p = argparse.ArgumentParser(description=__doc__,
                              formatter_class=argparse.RawDescriptionHelpFormatter)
  p.add_argument("--export-dir", required=True,
                 help=f"Export root with {SUMMARY_FILE} (local or gs://)")
  p.add_argument("--arango-url", default="http://10.30.1.10:8529")
  p.add_argument("--arango-user", default="root")
  p.add_argument("--arango-password", default=os.environ.get("ARANGO_PASSWORD", ""))
  p.add_argument("--arango-database", default=None, help="Default: summary source.database")
  p.add_argument("--spanner-endpoint", default=None, help="host:port of Omni / emulator")
  p.add_argument("--project", required=True)
  p.add_argument("--instance", required=True)
  p.add_argument("--database", required=True)
  p.add_argument("--dialect", choices=["googlesql", "postgresql"], default="googlesql")
  p.add_argument("--sample", type=int, default=200)
  p.add_argument("--dlq-dir", default=None, help="Template outputDirectory/dlq (local)")
  p.add_argument("--tables", default=None, help="Comma-separated subset of summary tables")
  p.add_argument("--allow-missing-tables", action="store_true",
                 help="WARN instead of FAIL for summary tables not queryable in Spanner")
  args = p.parse_args()

  export_dir = args.export_dir.rstrip("/")
  summary = json.loads(read_text(f"{export_dir}/{SUMMARY_FILE}"))
  if summary.get("formatVersion") != 2:
    report("FAIL", f"unsupported {SUMMARY_FILE} formatVersion {summary.get('formatVersion')}")
    return 1
  if "_staging" in list_names(export_dir):
    report("FAIL", "_staging directory left in the export root")
  source = summary.get("source", {})
  consistent = source.get("consistent", True)
  if not consistent:
    report("WARN", "summary source.consistent=false (source changed during export)")
  arango = Arango(args.arango_url, args.arango_user, args.arango_password,
                  args.arango_database or source.get("database", "_system"))
  db = spanner_database(args)
  dlq = severe_dlq_counts(args.dlq_dir)
  if dlq:
    print(f"severe DLQ records per table: {dlq}")
  wanted = set(args.tables.split(",")) if args.tables else None
  count_passes = 0

  for t in summary["tables"]:
    name, coll = t["name"], t.get("sourceName", t["name"])
    if wanted and name not in wanted:
      continue
    print(f"--- table {name} (collection {coll}, {t.get('collectionType')})")
    check_layout(export_dir, t)
    files_rows = sum(f.get("rows", 0) for f in t.get("files", []))
    row_count = t.get("rowCount")
    if row_count is not None and files_rows != row_count:
      report("FAIL", f"{name}: sum(files.rows)={files_rows} != rowCount={row_count}")
    arango_count = arango.count(coll)
    if row_count is not None:
      if arango_count == row_count:
        report("PASS", f"{name}: ArangoDB count {arango_count} == summary rowCount")
      else:
        report("FAIL" if consistent else "WARN",
               f"{name}: ArangoDB count {arango_count} != summary rowCount {row_count}")

    try:
      with db.snapshot() as snap:
        spanner_count = list(snap.execute_sql(f"SELECT COUNT(*) FROM {q(args.dialect, name)}"))[0][0]
    except Exception as e:  # pylint: disable=broad-except
      # A missing table means the migration silently imported nothing for it.
      report("WARN" if args.allow_missing_tables else "FAIL",
             f"{name}: not queryable in Spanner ({e.__class__.__name__}: {str(e)[:200]})")
      continue
    severe = dlq.get(name, 0)
    expected = row_count if row_count is not None else arango_count
    if spanner_count + severe == expected:
      count_passes += 1
      report("PASS", f"{name}: Spanner {spanner_count} + DLQ {severe} == {expected}")
    else:
      report("FAIL", f"{name}: Spanner {spanner_count} + DLQ {severe} != {expected}")

    keys = arango.sample_keys(coll, args.sample, arango_count)
    edge = t.get("collectionType") == "EDGE"
    doc_expr = ("doc::text" if args.dialect == "postgresql" else "TO_JSON_STRING(`doc`)")
    with db.snapshot(multi_use=True) as snap:
      cols = None
      if edge:
        try:
          list(snap.execute_sql(
              f"SELECT {q(args.dialect, 'from_key')} FROM {q(args.dialect, name)} LIMIT 1"))
          cols = ["from_id", "to_id", "from_key", "to_key"]
        except Exception:  # pylint: disable=broad-except
          report("WARN", f"{name}: no from_key/to_key helper columns; edge check limited")
          cols = ["from_id", "to_id"]
      mismatches = warns = missing = 0
      for key in keys:
        select = doc_expr + "".join(", " + q(args.dialect, c) for c in (cols or []))
        param = "$1" if args.dialect == "postgresql" else "@k"
        params = {"p1": key} if args.dialect == "postgresql" else {"k": key}
        rows = list(snap.execute_sql(
            f"SELECT {select} FROM {q(args.dialect, name)} WHERE {q(args.dialect, 'key')} = {param}",
            params=params,
            param_types={("p1" if args.dialect == "postgresql" else "k"): _string_type()}))
        if not rows:
          if dlq.get(name):
            report("WARN", f"{name}/{key}: not in Spanner (table has DLQ records)")
          else:
            report("FAIL", f"{name}/{key}: missing in Spanner")
          missing += 1
          continue
        raw = arango.document_raw(coll, key)
        if raw is None:
          report("WARN", f"{name}/{key}: deleted from ArangoDB meanwhile")
          continue
        src = parse_json(raw)
        dst = parse_json(rows[0][0])
        if canonical(src) != canonical(dst):
          if canonical(src, True) == canonical(dst, True):
            warns += 1
            report("WARN", f"{name}/{key}: Spanner JSON number normalisation: "
                   f"{first_difference(canonical(src), canonical(dst))}")
          else:
            mismatches += 1
            report("FAIL", f"{name}/{key}: doc differs: "
                   f"{first_difference(canonical(src), canonical(dst))}")
        if edge and cols:
          from_id, to_id = rows[0][1], rows[0][2]
          if from_id != src.get("_from") or to_id != src.get("_to"):
            report("FAIL", f"{name}/{key}: from_id/to_id != _from/_to")
          if len(cols) == 4:
            if rows[0][3] != from_id.split("/", 1)[1] or rows[0][4] != to_id.split("/", 1)[1]:
              report("FAIL", f"{name}/{key}: generated from_key/to_key wrong")
      if keys and not mismatches and not missing:
        report("PASS", f"{name}: {len(keys)} sampled documents match ({warns} with WARN)")

  if count_passes == 0:
    report("FAIL", "no table passed the Spanner count check; nothing was validated")
  print(f"summary: {RESULTS}")
  return 1 if RESULTS["FAIL"] else 0


def _string_type():
  from google.cloud.spanner_v1 import param_types  # pylint: disable=g-import-not-at-top
  return param_types.STRING


if __name__ == "__main__":
  sys.exit(main())
