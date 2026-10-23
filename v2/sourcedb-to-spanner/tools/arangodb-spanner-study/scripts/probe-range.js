// arangosh probe: does a _key range filter use the primary index on a sharded collection? AQL string order?
const db = require('@arangodb').db;
if (!db._collection('probe')) db._create('probe', {numberOfShards: 16, replicationFactor: 1});
const c = db._collection('probe');
if (c.count() === 0) {
  db._query(`FOR i IN 1..200000 INSERT {_key: CONCAT('k', i), v: i, nested: {a: [1, 'x', null], b: 1.5e300}} INTO probe`);
}
print('count=' + c.count());
const q = `FOR d IN probe FILTER d._key >= @lo AND d._key < @hi RETURN {key: d._key, doc: JSON_STRINGIFY(d)}`;
const ex = db._createStatement({query: q, bindVars: {lo: 'k1', hi: 'k2'}}).explain();
print(ex.plan.nodes.map(n => n.type + (n.indexes ? '(' + n.indexes.map(i => i.type).join(',') + ')' : '')).join(' -> '));
print('rules: ' + ex.plan.rules.join(','));
const t0 = Date.now();
const r = db._query(q, {lo: 'k1', hi: 'k2'}, {profile: true});
print('rows=' + r.toArray().length + ' scannedIndex=' + r.getExtra().stats.scannedIndex + ' scannedFull=' + r.getExtra().stats.scannedFull + ' ms=' + (Date.now() - t0));
print('order: ' + JSON.stringify(db._query(`RETURN ['B' < 'a', 'a' < 'b', 'Z' < 'a', 'é' > 'z', '10' < '9']`).toArray()));
print('sample: ' + JSON.stringify(db._query(`FOR d IN probe FILTER RAND() < 0.0001 SORT d._key RETURN d._key`, {}, {profile: true}).getExtra().stats));
print(JSON.stringify(db._query(`FOR d IN probe LIMIT 1 RETURN JSON_STRINGIFY(d)`).toArray()));
