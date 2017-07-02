/* eslint no-console: 0 */
const DBWrkrStorage = require('../dbwrkr-rethinkdb');
const dbWrkrTests = require('dbwrkr').tests;


dbWrkrTests({
  storage: DBWrkrStorage({
    dbName: 'dbwrkr_tests'
  })
});
