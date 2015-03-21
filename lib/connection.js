var async = require('async');
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

var Connection = module.exports = function Connection(config, cb) {

  // check for missing password
  if (('user' in config) && !('password' in config)) {
    config.password = '';
    //throw new Error('config.password is required when config.user is provided.');
    console.log("Connecting as user '" + config.user + "' with an empty password.");
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

  this.cassandraClient = new CassandraClient(config).connect(cb);
};
