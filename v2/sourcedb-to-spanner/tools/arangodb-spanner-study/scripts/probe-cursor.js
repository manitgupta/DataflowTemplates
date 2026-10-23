// Streaming cursor throughput of one RANGE export query vs batchSize (200k events docs each, disjoint ranges).
const db = require('@arangodb').db;
let lo = 3000000000;
[1000, 10000, 50000].forEach(bs => {
  const k = n => 'e' + String(n).padStart(12, '0');
  const q = `FOR d IN events FILTER d._key >= @lo AND d._key < @hi RETURN {key: d._key, id: d._id, rev: d._rev, doc: JSON_STRINGIFY(d)}`;
  const t0 = Date.now();
  const c = db._query(q, {lo: k(lo), hi: k(lo + 200000)}, {stream: true, batchSize: bs});
  let n = 0, bytes = 0;
  while (c.hasNext()) { const r = c.next(); n++; bytes += r.doc.length; }
  const s = (Date.now() - t0) / 1000;
  print('batchSize=' + bs + ': ' + n + ' docs in ' + s.toFixed(1) + 's = ' + (n / s).toFixed(0) + ' docs/s, ' + (bytes / s / 1e6).toFixed(1) + ' MB/s');
  lo += 200000;
});
