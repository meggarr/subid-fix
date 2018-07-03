#!/usr/bin/env node

const program = require('commander');
const req = require('sync-request');
const Mongo = require('mongodb').MongoClient;
const assert = require('assert');
const Rx = require('rxjs/Rx');
const Observable = require('rxjs/Observable').Observable;
const flatMap = require('rxjs/operators').flatMap;
const _ = require('underscore');

program.version('0.10.0', '-v, --version')
  .option('-m, --mongo-url <url>', 'MongoDB connection URL')
  .option('-d, --db-name [value]', 'Database name', 'sxa')
  .option('-f, --fix', 'Fix subscribers in billing')
  .option('-o, --org-id <id>', 'Organization ID')
  .option('--debug', 'Print debug logs')
  .parse(process.argv);

if (typeof program.mongoUrl === 'undefined') {
  console.error('Missing Mongo URL');
  process.exit(1);
}
if (typeof program.orgId === 'undefined') {
  console.error('Missing Organization ID');
  process.exit(1);
}

console.log('Using MongoDB - ' + program.mongoUrl); 
console.log('Database name - ' + program.dbName); 
console.log('Organization ID - ' + program.orgId);
console.log('Fix subscriber - ' + program.fix);
console.log('');

// process.exit(1);

const MongoConnectRx = Observable.bindNodeCallback(Mongo.connect);
MongoConnectRx(program.mongoUrl).subscribe(client => {
  const db = client.db(program.dbName);
  assert.ok(db);

  const subBillingCol = db.collection('sxa-subscriber-billing');
  const subCol = db.collection('sxa-subscribers');
  assert.ok(subBillingCol);
  assert.ok(subCol);

  let quit = function (c) { process.exit(c); client.close(); }
  let debug = (s) => program.debug ? console.debug('DEBUG > ' + s) : '';

  let query = {
    'orgid': Number(program.orgId),
    'subscriberId': {'$exists': false}
  }

  debug('Query sxa-subsciber-billing: ' + JSON.stringify(query, null, 2));

  let findRx = Observable.bindNodeCallback((q, cb) => subBillingCol.find(q).toArray(cb));
  findRx(query).subscribe(subscribers => {
    Observable.from(subscribers)
      .map(s => ({ _id: s._id, name: s.name, csckey: s.csckey }))
      .flatMap(s => {
        let customIdArray = s.csckey.split('||');
        let query = {
          'orgId': program.orgId,
          '$or': _.map(customIdArray, cid => ({'customId': cid}))
        };

        debug('Subscriber Billing, ' + s._id + ', csckey: ' + s.csckey);
        debug('Query CSC Subscriber, q: ' + JSON.stringify(query));

        let findRx = Observable.bindNodeCallback((q, cb) => subCol.find(q).toArray(cb));
        return Observable.zip(Observable.of(s), findRx(query), (b, c) => ({ billing: b, csc: c }));
      })
      .filter(s => s.csc.length > 0)
      .map(s => ({ billing: s.billing, csc: _.map(s.csc, s0 => ({ _id: s0._id, customId: s0.customId, name: s0.name }))[0] }))
      .flatMap(sub => {
        let q = { _id: sub.billing._id };
        let u = { '$set': { subscriberId: sub.csc._id } };

        debug('Update Billing - q: ' + JSON.stringify(q) + ', u: ' + JSON.stringify(u));

        let updateObservable;
        if (!program.fix) updateObservable = Observable.of({ok: 0});
        else {
          let updateRx = Observable.bindNodeCallback((q, u, cb) => subBillingCol.findOneAndUpdate(q, u, cb));
          updateObservable = updateRx(q, u);
        }
        return Observable.zip(Observable.of(sub), updateObservable, (s, r) => ({sub: s, res: r}));
      })
      .concat(Observable.of({end: true}))
      .subscribe(r => {
        debug('Result - ' + JSON.stringify(r, null, 2));

        if (r.end) quit(0);
        console.log('Fixed (update) ' + r.sub.billing._id + ', subscriberId: ' + r.sub.csc._id + ', result -> ' + r.res.ok);
      });
  },
  err => {
    console.error('Failed to query, ' + err);
    quit(2)
  });
},
err => {
  console.error('Failed to connect MongoDB, ' + err);
  process.exit(1);
});

