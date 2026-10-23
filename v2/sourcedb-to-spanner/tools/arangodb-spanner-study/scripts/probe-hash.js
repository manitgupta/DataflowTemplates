// HASH partition: is the filter evaluated on the primary index before fetching documents?
const db = require('@arangodb').db;
const variants = {
  plain: `FOR d IN probe FILTER HASH(d._key) % 16 == 3 RETURN {key: d._key, doc: JSON_STRINGIFY(d)}`,
  hinted: `FOR d IN probe OPTIONS {indexHint: 'primary', forceIndexHint: true} FILTER HASH(d._key) % 16 == 3 RETURN {key: d._key, doc: JSON_STRINGIFY(d)}`,
  subq: `FOR k IN (FOR x IN probe FILTER HASH(x._key) % 16 == 3 RETURN x._key) FOR d IN probe FILTER d._key == k RETURN {key: d._key, doc: JSON_STRINGIFY(d)}`,
};
for (const [name, q] of Object.entries(variants)) {
  const ex = db._createStatement({query: q}).explain();
  const t0 = Date.now();
  const s = db._query(q, {}, {profile: true}).getExtra().stats;
  print(name + ': ' + ex.plan.nodes.map(n => n.type + (n.indexes ? '(' + n.indexes.map(i => i.type).join(',') + (n.filter ? ',filter' : '') + (n.indexCoversProjections ? ',covers' : '') + ')' : '')).join(' -> ')
    + ' | scannedFull=' + s.scannedFull + ' scannedIndex=' + s.scannedIndex + ' docLookups=' + (s.documentLookups || 'n/a') + ' filtered=' + s.filtered + ' ms=' + (Date.now() - t0));
}
