const cassandra = require('cassandra-driver');

/**
 * Manage a connection to Apache Cassandra
 *
 * @param {Object} config
 * @return {Object}
 * @api private
 */

var Connection = function(config, cb) {

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
      config.authProvider = new cassandra.auth.PlainTextAuthProvider(config.user, config.password);
    }
  }

  var client = new cassandra.Client(config);

  client.connect(function(err) {
    if (err) return cb(err);
    cb(null, client);
  });
};

module.exports = Connection;
