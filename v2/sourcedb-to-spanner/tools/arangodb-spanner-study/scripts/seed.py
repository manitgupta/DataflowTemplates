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
"""Seeds an ArangoDB database with the exporter's end-to-end fixtures.

Only the Python standard library is used (HTTP API via urllib).

Creates (in --database, created if missing):
  user-profiles  document collection: --docs generated documents plus hostile JSON
                 fixtures (nesting, mixed arrays, {} / [], null vs missing, edge-case
                 numbers, emoji, odd attribute names, a 254-byte _key, a 2 MB document)
  knows          edge collection between user-profiles documents, plus one dangling edge
  hostile_keys   document collection whose _key values use every legal key character
                 class (mixed case, digits, _ - : . @ ( ) + , = ; $ ! * ' %), to
                 exercise RANGE partitioning against the real server

Example (cluster, root with empty password):
  python3 seed.py --arango-url http://10.30.1.10:8529 --database exporter_e2e --docs 10000
"""

import argparse
import base64
import json
import random
import sys
import urllib.error
import urllib.parse
import urllib.request

KEY_ALPHABET = (
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-:.@()+,=;$!*'%"
)


class Arango:
  """Minimal ArangoDB HTTP client."""

  def __init__(self, url, user, password, database):
    self.url = url.rstrip("/")
    self.database = database
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    self.headers = {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json",
    }

  def request(self, method, path, body=None, db=None, ok=(200, 201, 202)):
    db = self.database if db is None else db
    full = f"{self.url}/_db/{urllib.parse.quote(db, safe='')}{path}"
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(full, data=data, method=method, headers=self.headers)
    try:
      with urllib.request.urlopen(req, timeout=300) as resp:
        payload = resp.read()
        status = resp.status
    except urllib.error.HTTPError as e:
      payload = e.read()
      status = e.code
    if status not in ok:
      raise RuntimeError(f"{method} {path} -> HTTP {status}: {payload[:500]!r}")
    return json.loads(payload) if payload else None

  def request_raw(self, method, path, raw_body, ok=(200, 201, 202)):
    full = f"{self.url}/_db/{urllib.parse.quote(self.database, safe='')}{path}"
    req = urllib.request.Request(
        full, data=raw_body.encode(), method=method, headers=self.headers)
    try:
      with urllib.request.urlopen(req, timeout=300) as resp:
        payload, status = resp.read(), resp.status
    except urllib.error.HTTPError as e:
      payload, status = e.read(), e.code
    if status not in ok:
      raise RuntimeError(f"{method} {path} -> HTTP {status}: {payload[:500]!r}")
    return json.loads(payload) if payload else None

  def ensure_database(self):
    if self.database == "_system":
      return
    dbs = self.request("GET", "/_api/database", db="_system")["result"]
    if self.database not in dbs:
      self.request("POST", "/_api/database", {"name": self.database}, db="_system")
      print(f"created database {self.database}")

  def drop_collection(self, name):
    self.request("DELETE", f"/_api/collection/{name}", ok=(200, 404))

  def create_collection(self, name, edge=False, shards=None, replication=None):
    body = {"name": name, "type": 3 if edge else 2}
    if shards:
      body["numberOfShards"] = shards
    if replication:
      body["replicationFactor"] = replication
    self.request("POST", "/_api/collection", body, ok=(200, 201, 409))

  def insert_raw(self, collection, raw_docs, batch=500):
    """Inserts JSON documents given as raw strings (keeps exact number literals)."""
    for i in range(0, len(raw_docs), batch):
      chunk = raw_docs[i:i + batch]
      res = self.request_raw(
          "POST", f"/_api/document/{collection}?silent=false", "[" + ",".join(chunk) + "]")
      errors = [r for r in res if r.get("error")]
      if errors:
        raise RuntimeError(f"insert into {collection} failed: {errors[:3]}")

  def count(self, collection):
    return self.request("GET", f"/_api/collection/{collection}/count")["count"]


def user_profile_fixtures():
  """Hostile JSON documents. Raw strings so number literals reach the server verbatim."""
  long_key = "k" * 254
  big_payload = "0123456789abcdef" * 125_000  # 2,000,000 chars
  return [
      '{"_key":"nested","a":{"b":{"c":[1,{"d":null}],"e":{"f":{"g":"deep"}}}}}',
      '{"_key":"mixed","arr":[1,"two",3.5,true,null,{"x":[]},[],[[1],[2,[3]]]]}',
      '{"_key":"empty","o":{},"a":[],"s":""}',
      '{"_key":"nulls","present":null}',
      '{"_key":"missing"}',
      ('{"_key":"numbers","i":1,"f":1.5,"s":"1","big":9007199254740993,'
       '"max":9223372036854775807,"tenth":0.1,"huge":1e300,"negzero":-0.0,'
       '"neg":-42,"small":1e-300}'),
      '{"_key":"emoji","e":"\\ud83d\\ude00 caf\\u00e9 \\u4e2d\\u6587","ctl":"tab\\tnl\\n"}',
      '{"_key":"names","a-b":1,"a.b":2,"a b":3,"_odd":4,"$dollar":5}',
      '{"_key":"%s","long":true}' % long_key,
      '{"_key":"big","payload":"%s"}' % big_payload,
      '{"_key":"Upper-Case_1","x":1}',
      "{\"_key\":\"punct:.@()+,=;$!*'%\",\"x\":2}",
  ]


def hostile_keys(count, seed=42):
  rnd = random.Random(seed)
  keys = []
  seen = set()

  def add(k):
    if k not in seen:
      seen.add(k)
      keys.append(k)

  for c in KEY_ALPHABET:
    add(c)
    add("k" + c)
    add(c + "k")
    add(c * 3)
  for k in ["a", "B", "Z", "_x", "A", "b", "z", "AbC", "aBc", "abc", "ABC", "%41", "'q'"]:
    add(k)
  while len(keys) < count:
    n = 1 + rnd.randrange(16)
    add("".join(rnd.choice(KEY_ALPHABET) for _ in range(n)))
  return keys


def main():
  p = argparse.ArgumentParser(description=__doc__,
                              formatter_class=argparse.RawDescriptionHelpFormatter)
  p.add_argument("--arango-url", "--url", dest="url", default="http://10.30.1.10:8529")
  p.add_argument("--user", default="root")
  p.add_argument("--password", default="")
  p.add_argument("--database", default="exporter_e2e",
                 help="Created if missing. Use a dedicated database on shared clusters.")
  p.add_argument("--docs", type=int, default=10000)
  p.add_argument("--hostile-keys", type=int, default=5000)
  p.add_argument("--shards", type=int, default=None, help="numberOfShards (cluster)")
  p.add_argument("--replication-factor", type=int, default=None)
  p.add_argument("--keep", action="store_true",
                 help="Do not drop existing fixture collections first")
  args = p.parse_args()

  a = Arango(args.url, args.user, args.password, args.database)
  a.ensure_database()
  for name in ["user-profiles", "knows", "hostile_keys"]:
    if not args.keep:
      a.drop_collection(name)
  a.create_collection("user-profiles", shards=args.shards, replication=args.replication_factor)
  a.create_collection("knows", edge=True, shards=args.shards,
                      replication=args.replication_factor)
  a.create_collection("hostile_keys", shards=args.shards, replication=args.replication_factor)

  docs = user_profile_fixtures()
  for i in range(args.docs):
    docs.append(json.dumps({
        "_key": f"u{i}",
        "n": i,
        "name": f"user {i}",
        "tags": ["t%d" % (i % 7), "t%d" % (i % 11)],
        "score": i / 7.0,
        "active": i % 2 == 0,
        "address": {"zip": f"{i % 100000:05d}", "city": "c%d" % (i % 50)},
    }))
  a.insert_raw("user-profiles", docs)

  edges = []
  for i in range(max(0, args.docs - 1)):
    edges.append(json.dumps({
        "_key": f"e{i}",
        "_from": f"user-profiles/u{i}",
        "_to": f"user-profiles/u{(i * 31 + 7) % args.docs}",
        "since": 2000 + i % 25,
    }))
  edges.append(json.dumps({
      "_key": "dangling",
      "_from": "user-profiles/u0",
      "_to": "missing-collection/nobody",
  }))
  a.insert_raw("knows", edges)

  keys = hostile_keys(args.hostile_keys)
  a.insert_raw("hostile_keys", [json.dumps({"_key": k, "k": k}) for k in keys])

  for name in ["user-profiles", "knows", "hostile_keys"]:
    print(f"{args.database}.{name}: {a.count(name)} documents")
  return 0


if __name__ == "__main__":
  sys.exit(main())
