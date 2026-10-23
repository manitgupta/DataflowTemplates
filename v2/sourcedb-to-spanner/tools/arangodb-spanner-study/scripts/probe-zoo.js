// Print the exported JSON text of failing type_zoo / follows documents.
const db = require('@arangodb').db;
['type_zoo/z000003888596', 'type_zoo/z000003888551', 'type_zoo/z000003888597', 'follows/f001162682012'].forEach(id => {
  print(id + ' => ' + db._query(`RETURN JSON_STRINGIFY(DOCUMENT(@id))`, {id}).toArray()[0].substring(0, 400));
});
