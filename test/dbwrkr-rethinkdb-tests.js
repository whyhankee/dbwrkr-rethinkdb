/* eslint no-console: 0 */
const DBWrkrStorage = require('../dbwrkr-rethinkdb');
const tests = require('dbwrkr').tests;


tests({
  storage: DBWrkrStorage({
    dbName: 'dbwrkr_tests'
  })
});
