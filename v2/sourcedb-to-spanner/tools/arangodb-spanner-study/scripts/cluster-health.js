// Cluster health summary: count of healthy servers per role.
const h = require('@arangodb').arango.GET('/_admin/cluster/health').Health;
const roles = {};
Object.values(h).forEach(s => { const k = s.Role + ':' + s.Status; roles[k] = (roles[k] || 0) + 1; });
print(JSON.stringify(roles));
print('version ' + require('@arangodb').db._version());
