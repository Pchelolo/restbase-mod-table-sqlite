"use strict";

var P = require('bluebird');
var mysql      = require('mysql');

function expandDBName(options) {
    var dbName = options.conf.dbname || 'sqlite.db';
    return dbName.replace(/^~/, process.env.HOME || process.env.USERPROFILE);
}

function Wrapper(options) {
    var delay = options.conf.retry_delay || 100;

    this.conf = options.conf;
    this.log = options.log;
    this.retryLimit = options.conf.retry_limit || 5;
    this.randomDelay = function() {
        return Math.ceil(Math.random() * delay);
    };

    this.connectionPool = mysql.createPool({
        connectionLimit : 10,
        host            : '127.0.0.1',
        user            : 'root',
        password        : 'password',
        database        : 'test'
    });
    P.promisifyAll(this.connectionPool, { suffix: '_p' });

    this.readerConnection = P.promisifyAll(mysql.createConnection({
        host            : '127.0.0.1',
        user            : 'root',
        password        : 'password',
        database        : 'test'
    }), { suffix: '_p' });
}

/**
 * Run a set of queries within a transaction.
 *
 * @param queries an array of query objects, containing sql field with SQL
 *        and params array with query parameters.
 * @returns {*} operation promise
 */
Wrapper.prototype.run = function(queries) {
    var self = this;
    var retryCount = 0;

    var beginTransaction = function(connection) {
        connection = P.promisifyAll(connection, {suffix: '_p'});
        return connection.query_p('begin')
        .then(function() {
            return connection;
        });
    };

    return self.connectionPool.getConnection_p()
    .then(beginTransaction)
    .then(function(client) {
        queries = queries.filter(function(query) {
            return query && query.sql;
        });
        return P.each(queries, function(query) {
            return client.query_p(query.sql, query.params);
        })
        .then(function(res) {
            return client;
        })
        .catch(function(err) {
            if (self.conf.show_sql) {
                self.log('rollback');
            }
            return client.query_p('rollback')
            .finally(function() {
                client.release();
                throw err;
            });
        });
    })
    .then(function(client) {
        self.log('commit');
        client.query('commit');
        client.release();
    });
};

/**
 * Run read query and return a result promise
 *
 * @param query SQL query to execute
 * @param params query parameters
 * @returns {*} query result promise
 */
Wrapper.prototype.all = function(query, params) {
    console.log(query, params);
    return this.readerConnection.query_p(query, params);
};

module.exports = Wrapper;
