// Single-query insert throughput: tiny docs vs text-heavy docs; coordinator-generated vs RANDOM_TOKEN.
const db = require('@arangodb').db;
const w = 'lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor'.split(' ');
['pi1', 'pi2', 'pi3', 'pi4'].forEach(c => { if (db._collection(c)) db._drop(c); db._create(c, {numberOfShards: 24}); });
function t(name, q, bind, opts) {
  const t0 = Date.now();
  db._query(q, bind || {}, opts || {});
  const s = (Date.now() - t0) / 1000;
  print(name + ': ' + (200000 / s).toFixed(0) + ' docs/s (' + s.toFixed(1) + 's)');
}
t('tiny', `FOR i IN 1..200000 INSERT {_key: TO_STRING(i), v: i} INTO pi1`);
t('token-1KB', `FOR i IN 1..200000 INSERT {_key: TO_STRING(i), b: RANDOM_TOKEN(1000)} INTO pi2`);
t('words-150', `FOR i IN 1..200000 INSERT {_key: TO_STRING(i), b: CONCAT_SEPARATOR(' ', (FOR j IN 1..150 RETURN @w[FLOOR(RAND() * 12)]))} INTO pi3`, {w});
t('words-150-ic', `FOR i IN 1..200000 INSERT {_key: TO_STRING(i), b: CONCAT_SEPARATOR(' ', (FOR j IN 1..150 RETURN @w[FLOOR(RAND() * 12)]))} INTO pi4 OPTIONS {overwriteMode: 'ignore'}`, {w}, {intermediateCommitCount: 50000});
print('current queries: ' + require('@arangodb/aql/queries').current().length);
