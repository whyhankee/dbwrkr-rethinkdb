/* eslint no-console: 0 */
const DBWrkrMongoDb = require('../dbwrkr-rethinkdb');
const dbWrkrTests = require('dbwrkr').tests;


const testOptions = {
  storage: new DBWrkrMongoDb({
    dbName: 'dbwrkr_tests'
  })
};


dbWrkrTests(testOptions, function (err) {
  if (err) throw err;
});
