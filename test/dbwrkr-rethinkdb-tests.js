/* eslint no-console: 0 */
var DBWrkrMongoDb = require('../dbwrkr-rethinkdb');
var dbWrkrTests = require('dbwrkr').tests;


var testOptions = {
  storage: new DBWrkrMongoDb({
    dbName: 'dbwrkr_tests'
  })
};


dbWrkrTests(testOptions, function (err) {
  if (err) throw err;
});
