//var async = require('async');
var cassandraDriver = require('cassandra-driver');
var CassandraClient = cassandraDriver.Client;
var CassandraPlainTextAuthProvider = cassandraDriver.auth.PlainTextAuthProvider;


/**
 * Manage a connection to Apache Cassandra
 *
 * @param {Object} config
 * @return {Object}
 * @api private
 */

var Connection = module.exports = function(config, cb) {

  if (!(this instanceof Connection)) {
    throw new Error("Connection is not instantiated (forgot new() perhaps?).");
  }

  // check for missing password
  if (('user' in config) && !('password' in config)) {
    config.password = null;
  }

  if (config.database) {
    config.keyspace = config.database;
  }

  // create authProvider
  if (('user' in config) && ('password' in config)) {
    if (config.authProvider) {
      throw new Error('config.user and config.password are not allowed if config.authProvider is specified.');
    }
    else {
      config.authProvider = new CassandraPlainTextAuthProvider(config.user, config.password);
    }
  }

  var client = this.cassandraClient = new CassandraClient(config);

  client.connect(function(err) {
    if (err) return cb(err);
    cb(null, client);
  });
};


Connection.prototype.createCollection = function(name, collection, cb) {
  cb();
};


Connection.prototype.dropCollection = function(name, cb) {
  cb();
};
