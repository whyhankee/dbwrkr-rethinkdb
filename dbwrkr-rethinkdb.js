const assert = require('assert');
const debug = require('debug')('dbwrkr:rethinkdb');
const flw = require('flw');
const r = require('rethinkdb');

// Document schema
//
// subscripions:
//  eventName   String  (indexed)
//  queues      [String]
//
// qitems:
//  name        String  (indexed)
//  queue       String  (indexed)
//  tid         String
//  payload     Object
//  parent      _ObjectId
//  created     Date
//  when        Date    (sparse indexed)
//  done        Date    (sparse indexed)
//  retryCount  Number


function DbWrkrRethinkDB(opt) {
  debug('DbWrkrRethinkDB - opt', opt);

  this.rOptions = {
    db: opt.dbName,
    host: opt.host || 'localhost',
    port: opt.dbPort || 28015,
    username: opt.username || undefined,
    password: opt.password || undefined,
    timeout: opt.timeout || undefined,
    ssl: opt.ssl || undefined
  };

  assert(this.rOptions.db, 'has database name');
  assert(this.rOptions.port, 'has database port');

  this.db = null;
  this.tSubscriptions = null;
  this.tQitems = null;
}


DbWrkrRethinkDB.prototype.connect = function connect(done) {
  const self = this;
  debug('connecting to Rethinkdb', this.rOptions);

  r.connect(this.rOptions, function (err, conn) {
    if (err) return done(err);

    debug('connected to Rethinkdb');
    self.db = conn;
    self.tSubscriptions = r.table('wrkr_subscriptions');
    self.tQitems = r.table('wrkr_qitems');

    return setupTables(self.db, self.rOptions.db, done);
  });
};


DbWrkrRethinkDB.prototype.disconnect = function disconnect(done) {
  if (!this.db) return done();

  this.tSubscriptions = null;
  this.tQitems = null;
  this.db.close(done);
};



DbWrkrRethinkDB.prototype.subscribe = function subscribe(eventName, queueName, done) {
  debug('subscribe ', {event: eventName, queue: queueName});

  const query = this.tSubscriptions
    .get(eventName)
    .replace({
      id: eventName,
      queues: r.row('queues').default([]).setInsert(queueName)
    }, {
      returnChanges: true
    });

  return query.run(this.db, function (err, result) {
    if (err) return done(err);

    if (result.unchanged === 0) return done(null);
    return done(null);
  });
};


DbWrkrRethinkDB.prototype.unsubscribe = function unsubscribe(eventName, queueName, done) {
  debug('unsubscribe ', {event: eventName, queue: queueName});

  const query = this.tSubscriptions
    .get(eventName)
    .replace({
      id: eventName,
      queues: r.row('queues').default([]).difference([queueName])
    });

  return query.run(this.db, function (err) {
    return done(err || null);
  });
};


DbWrkrRethinkDB.prototype.subscriptions = function subscriptions(eventName, done) {
  const query = this.tSubscriptions.get(eventName);

  return query.run(this.db, function (err, event) {
    if (err) return done(err);

    return done(null, event ? event.queues : []);
  });
};



DbWrkrRethinkDB.prototype.publish = function publish(events, done) {
  const publishEvents = Array.isArray(events) ? events : [events];

  debug('storing ', publishEvents);
  this.tQitems.insert(publishEvents).run(this.db, function (err, results) {
    if (err) return done(err);

    if (publishEvents.length !== results.inserted) {
      return done(new Error('insertErrorNotEnoughEvents'));
    }

    const createdIds = results.generated_keys;
    return done(null, createdIds);
  });
};


DbWrkrRethinkDB.prototype.fetchNext = function fetchNext(queue, done) {
  debug('fetchNext start', queue);

  // Get nextItem based on when, filter on the queue we are receiving
  const min = [queue, r.minval];
  const max = [queue, r.now()];
  const query = this.tQitems.orderBy({index: 'idxQueueWhen'})
    .between(min, max, {index: 'idxQueueWhen'})
    .limit(1)
    .replace(r.row.without('when').merge({done: new Date()}), {
      returnChanges: true,
    });
  return query.run(this.db, function (err, result) {
    if (err) return done(err);
    if (result.replaced ==! 1) return done(null, undefined);

    const newDoc = result.changes[0].new_val;
    debug('fetchNext', newDoc);

    return done(null, newDoc);
  });
};


DbWrkrRethinkDB.prototype.find = function find(criteria, done) {
  const self = this;
  debug('finding ', criteria);

  if (criteria.id) return searchById();
  return searchByFilter();

  function searchById() {
    const searchIds = Array.isArray(criteria.id) ? criteria.id : [criteria.id];
    self.tQitems.getAll(r.args(searchIds)).run(self.db, function (err, cursor) {
      if (err) return done(err);
      return cursor.toArray(done);
    });
  }

  function searchByFilter() {
    self.tQitems.filter(criteria).run(self.db, function (err, cursor) {
      if (err) return done(err);
      return cursor.toArray(done);
    });
  }
};


DbWrkrRethinkDB.prototype.remove = function remove(criteria, done) {
  debug('removing', criteria);
  return this.tQitems.filter(criteria).delete().run(this.db, done);
};



function setupTables(db, dbName, done) {
  debug('Setup tables for db', {db: dbName});

  return flw.series([
    getDbNames,
    createDb,

    listTables,
    createTableSubscriptions,
    createTableEvents,

    listIndexes,
    createIndexQueueWhen,
    waitForIndexes,
  ], done);

  // db
  function getDbNames(c, cb) {
    r.dbList().run(db, c._store('dbNames', cb));
  }

  function createDb(c, cb) {
    if (c.dbNames.indexOf(dbName) !== -1) return cb();

    debug('create database', {db: dbName});
    return r.dbCreate(dbName).run(db, cb);
  }

  // Tables
  function listTables(c, cb) {
    r.db(dbName).tableList().run(db, c._flw_store('tableNames', cb));
  }

  function createTableSubscriptions(c, cb) {
    return createTable(c, 'wrkr_subscriptions', cb);
  }
  function createTableEvents(c, cb) {
    return createTable(c, 'wrkr_qitems', cb);
  }

  function createTable(c, tableName, cb) {
    if (c.tableNames.indexOf(tableName) !== -1) return cb();

    debug('create table', {table: tableName});
    r.db(dbName).tableCreate(tableName).run(db, cb);
  }

  // Indexes (qitem only, subscribtions uses only pk's)
  function listIndexes(c, cb) {
    return r.db(dbName).table('wrkr_qitems')
      .indexList()
      .run(db, c._store('indexNames', cb));
  }
  function createIndexQueueWhen(c, cb) {
    if (c.indexNames.indexOf('idxQueueWhen') !== -1) return cb();

    return r.db(dbName).table('wrkr_qitems')
      .indexCreate('idxQueueWhen', [r.row('queue'), r.row('when')])
      .run(db, cb);
  }

  // wait for indexes to be ready
  function waitForIndexes(c, cb) {
    return r.table('wrkr_qitems').indexWait().run(db, cb);
  }
}


module.exports = DbWrkrRethinkDB;
