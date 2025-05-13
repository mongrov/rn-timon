const ffi = require('ffi-napi');

// Adjust the path to your compiled .so file
const lib = ffi.Library('./timon/target/debug/libtsdb_timon.so', {
  'nativeInitTimon': [ 'void', [ 'string', 'uint', 'string' ] ],
  'nativeQuery': [ 'string', [ 'string', 'string', 'string' ] ],
});

lib.nativeInitTimon('/home/ahmed/mongrov/rn-timon/timon/tmp', 1440, 'ahmed_test');

function runQuery(i, sql) {
  return new Promise((resolve) => {
    const db = 'zivaring';
    const user = null;
    const start = Date.now();
    const result = lib.nativeQuery(db, sql, user);
    const elapsed = Date.now() - start;
    resolve({i, elapsed, result});
  });
}


(async () => {
  const queries = [
    'SELECT * FROM spo2_readings ORDER BY date DESC LIMIT 25',
    'SELECT * FROM spo2_readings ORDER BY date ASC LIMIT 25',
    'SELECT AVG("automaticSpo2Data") FROM spo2_readings',
    'SELECT MIN("automaticSpo2Data") FROM spo2_readings',
    'SELECT MAX("automaticSpo2Data") FROM spo2_readings',
  ];

  const start = Date.now();
  const promises = queries.map((sql, i) => runQuery(i, sql));
  const results = await Promise.all(promises);
  const totalElapsed = Date.now() - start;

  results.forEach(r => {
    console.log(`Query ${r.i} took ${r.elapsed}ms`, JSON.parse(r.result).status);
  });
  console.log(`Total time for all queries: ${totalElapsed} ms`);
})();
