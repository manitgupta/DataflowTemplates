// Where does the time go for RANGE export queries? Compare variants on disjoint 100k-doc ranges of events.
const db = require('@arangodb').db;
const k = n => 'e' + String(n).padStart(12, '0');
const shard = db.events.shards()[7];
let lo = 3010000000;
function run(name, q, bind, opts) {
  const t0 = Date.now();
  const c = db._query(q, bind, opts);
  let n = 0;
  while (c.hasNext()) { c.next(); n++; }
  const s = (Date.now() - t0) / 1000;
  print(name + ': ' + n + ' docs in ' + s.toFixed(1) + 's = ' + (n / s).toFixed(0) + ' docs/s');
}
const R = `FOR d IN events FILTER d._key >= @lo AND d._key < @hi RETURN {key: d._key, doc: JSON_STRINGIFY(d)}`;
run('range stream bs10k', R, {lo: k(lo), hi: k(lo += 100000)}, {stream: true, batchSize: 10000});
run('range NONstream bs10k', R, {lo: k(lo), hi: k(lo += 100000)}, {stream: false, batchSize: 10000});
run('range RETURN d stream', `FOR d IN events FILTER d._key >= @lo AND d._key < @hi RETURN d`, {lo: k(lo), hi: k(lo += 100000)}, {stream: true, batchSize: 10000});
run('range keys only stream', `FOR d IN events FILTER d._key >= @lo AND d._key < @hi RETURN d._key`, {lo: k(lo), hi: k(lo += 100000)}, {stream: true, batchSize: 10000});
run('range one shard (shardIds) wide', R, {lo: k(0), hi: k(3149550000)}, {stream: true, batchSize: 10000, shardIds: [shard], fullCount: false, maxRuntime: 60});
run('full scan one shard LIMIT 200k', `FOR d IN events LIMIT 200000 RETURN {key: d._key, doc: JSON_STRINGIFY(d)}`, {}, {stream: true, batchSize: 10000, shardIds: [shard]});
