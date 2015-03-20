var async = require('async');
var CassandraClient = null;

/**
 * Manage a connection to Apache Cassandra
 *
 * @param {Object} config
 * @return {Object}
 * @api private
 */

var Connection = module.exports = function Connection(config, cb) {

  var self = this;

  // Hold the config object
  this.config = config || {};

  // Build Database connection
  this._buildConnection(function(err, db) {

    if (err) {
      return cb(err);
    }

    // Store the DB object
    self.db = db;

    // Return the connection
    cb(null, self);
  });
};
