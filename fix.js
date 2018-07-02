#!/usr/bin/env node
const DEBUG = true;

const program = require('commander');
const req = require('sync-request');
const Mongo = require('mongodb').MongoClient;
const assert = require('assert');
const Rx = require('rxjs/Rx');
const Observable = require('rxjs/Observable').Observable;
const _ = require('underscore');

program.version('0.10.0', '-v, --version')
  .option('-m, --mongo-url <url>', 'MongoDB connection URL')
  .option('-d, --db-name [value]', 'Database name', 'sxa')
  .option('-f, --fix', 'Delete subscribers in billing')
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
const doFix = function (sub, subBillingCol) {
  let billingId = sub.subBilling._id;
  let cscId = sub.subCsc._id;
  let q = { _id: billingId };
  let u = { '$set': { subscriberId: cscId } };

  if (program.debug) console.log('Update with query - q: ' + JSON.stringify(q) + ', u: ' + JSON.stringify(u));
  if (!program.fix) return Observable.of({ok: 0});

  let updateRx = Observable.bindNodeCallback((q, u, cb) => subBillingCol.findOneAndUpdate(q, u, cb));
  return updateRx(q, u);
}

const findCscSubscriber = function (subBilling, subCol) {
  return Observable.create(observer => {
    let customIdArray = subBilling.csckey.split('||');
    let query = {
      'orgId': program.orgId,
      '$or': _.map(customIdArray, cid => { return {'customId': cid} })
    };
    if (program.debug) console.log('Find CSC subscriber with customId, q: ' + JSON.stringify(query));

    let findRx = Observable.bindNodeCallback((q, cb) => subCol.find(q).toArray(cb));
    findRx(query).subscribe(subs => {
      if (subs.length == 0) {
        if (program.debug) console.log('Billing Subscriber has no CSC subscriber, ' + subBilling._id)
      } else {
        if (program.debug) console.log('Billing Subscriber has CSC subscriber, ' + subBilling._id)
        observer.next({
          subBilling: subBilling,
          subCsc: subs[0]
        });
      }
      observer.complete();
    });
  });
}


const MongoConnectRx = Observable.bindNodeCallback(Mongo.connect);

MongoConnectRx(program.mongoUrl).subscribe(client => {
  const db = client.db(program.dbName);
  const subBillingCol = db.collection('sxa-subscriber-billing');
  const subCol = db.collection('sxa-subscribers');
  assert.ok(subBillingCol);
  assert.ok(subCol);

  let quit = function (c) { process.exit(c); client.close(); }

  let query = {
    'orgid': Number(program.orgId),
    'subscriberId': {'$exists': false}
  }

  if (program.debug) console.info('Query sxa-subsciber-billing: ' + JSON.stringify(query, null, 2));

  let findRx = Observable.bindNodeCallback((q, cb) => subBillingCol.find(q).toArray(cb));
  findRx(query).subscribe(subscribers => {
    Observable.create(observer => {
      observer.count = subscribers.length;
      Observable.from(subscribers).subscribe({
        next(s) {
          if (program.debug) console.log('Subscriber, ' + s._id + ', csckey: ' + s.csckey);
          let subBilling = {
            _id: s._id,
            name: s.name,
            csckey: s.csckey
          };

          let fixObserver = {
            sub: null,
            next(s) { this.sub = s; },
            complete() {
              if (this.sub != null) {
                observer.next(this.sub)
              }
              observer.count--;
              if (program.debug) console.log('Subscribers left: ' + observer.count);
              if (observer.count == 0) {
                observer.complete();
              }
            }
          };
          findCscSubscriber(subBilling, subCol).subscribe(fixObserver);
        },
        complete() {
          if (program.debug) {
            console.log('Scanned ' + subscribers.length + ' subscribers');
            console.log('----------------------------------------------');
          }
        }
      });
    }).subscribe({
      sum: 0,
      next(sub) {
        let _self = this;
        _self.sum++;
        let fixer = doFix(sub, subBillingCol)
        if (fixer != null) fixer.subscribe(res => {
          console.log('Fixed (update) ' + sub.subBilling._id + ', subId: ' + sub.subCsc._id + ', result -> ' + res.ok);
        });
      },
      complete() {
        console.log('--------------------------------------------------');
        console.log(this.sum + ' subscribers processed');
        //quit(0);
      }
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

