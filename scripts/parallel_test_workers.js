const { Worker } = require('worker_threads');

const queries = [
  'SELECT * FROM spo2_readings ORDER BY date DESC LIMIT 25',
  'SELECT * FROM spo2_readings ORDER BY date ASC LIMIT 25',
  'SELECT AVG("automaticSpo2Data") FROM spo2_readings',
  'SELECT MIN("automaticSpo2Data") FROM spo2_readings',
  'SELECT MAX("automaticSpo2Data") FROM spo2_readings',
];

const db = 'zivaring';
const user = null;

function runQuery(i, sql) {
  return new Promise((resolve, reject) => {
    const worker = new Worker('./scripts/native_worker.js', {
      workerData: { db, sql, user, i },
    });
    worker.on('message', resolve);
    worker.on('error', reject);
    worker.on('exit', (code) => {
      if (code !== 0) {
        reject(new Error(`Worker stopped with exit code ${code}`));
      }
    });
  });
}

(async () => {
  const start = Date.now();
  const promises = queries.map((sql, i) => runQuery(i, sql));
  const results = await Promise.all(promises);
  const totalElapsed = Date.now() - start;

  results.forEach(r => {
    console.log(`Query ${r.i} took ${r.elapsed}ms`, JSON.parse(r.result).status);
  });
  console.log(`Total time for all queries: ${totalElapsed} ms`);
})();
