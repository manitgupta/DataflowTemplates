// Which ordering does a primary-index _key range use: AQL collation or binary?
const db = require('@arangodb').db;
if (db._collection('probe2')) db._drop('probe2');
db._create('probe2', {numberOfShards: 4, replicationFactor: 1});
const keys = ['A', 'B', 'Z', 'a', 'b', 'z', '0', '9', '_x', '-x', ':x', '.x', '@x', '(x', ')x', '+x', ',x', '=x', ';x', '$x', '!x', '*x', "'x", '%x'];
keys.forEach(k => db.probe2.insert({_key: k}));
const r = (lo, hi) => db._query(`FOR d IN probe2 FILTER d._key >= @lo AND d._key < @hi SORT d._key RETURN d._key`, {lo, hi}).toArray();
print("['B','a') -> " + JSON.stringify(r('B', 'a')));
print("['a','B') -> " + JSON.stringify(r('a', 'B')));
print('AQL SORT order: ' + JSON.stringify(db._query(`FOR d IN probe2 SORT d._key RETURN d._key`).toArray()));
print('AQL SORT on array: ' + JSON.stringify(db._query(`FOR k IN @k SORT k RETURN k`, {k: keys}).toArray()));
// no-index variant (forces AQL comparison semantics)
print("noidx ['B','a') -> " + JSON.stringify(db._query(`FOR d IN probe2 FILTER NOOPT(d._key) >= @lo AND NOOPT(d._key) < @hi RETURN d._key`, {lo: 'B', hi: 'a'}).toArray()));
