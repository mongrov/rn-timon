const { parentPort, workerData } = require('worker_threads');
const ffi = require('ffi-napi');

// Adjust the path to your compiled .so file
const lib = ffi.Library('./timon/target/debug/libtsdb_timon.so', {
  'nativeInitTimon': [ 'void', [ 'string', 'uint', 'string' ] ],
  'nativeQuery': [ 'string', [ 'string', 'string', 'string' ] ],
});

// Only initialize once per process (worker)
lib.nativeInitTimon('/home/ahmed/mongrov/rn-timon/timon/tmp', 1440, 'ahmed_test');

const { db, sql, user, i } = workerData;
const start = Date.now();
const result = lib.nativeQuery(db, sql, user);
const elapsed = Date.now() - start;

parentPort.postMessage({ i, elapsed, result });
