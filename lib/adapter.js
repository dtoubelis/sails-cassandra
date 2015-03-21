var Connection = require('./connection');
var Errors = require('waterline-errors').adapter;

module.exports = (function() {

  // Keep track of all the connections used by the app
  var _connections = {};


  var adapter = {

    identity: 'sails-cassandra',

    // Which type of primary key is used by default
    pkFormat: 'string',

    // Set to true if this adapter supports (or requires) things like data types, validations, keys, etc.
    // If true, the schema for models using this adapter will be automatically synced when the server starts.
    // Not terribly relevant if your data store is not SQL/schemaful.
    syncable: true,

    // Default configuration for collections
    // (same effect as if these properties were included at the top level of the model definitions)
    defaults: {

      contactPoints: [ '127.0.0.1' ],

      // If setting syncable, you should consider the migrate option, 
      // which allows you to set how the sync will be performed.
      // It can be overridden globally in an app (config/adapters.js)
      // and on a per-model basis.
      // 
      // IMPORTANT:
      // `migrate` is not a production data migration solution!
      // In production, always use `migrate: safe`
      //
      // drop   => Drop schema and data, then recreate it
      // alter  => Drop/add columns as necessary.
      // safe   => Don't change anything (good for production DBs)
      migrate: 'alter'
    },


    /**
     * Register a connection and the collections assigned to it.
     *
     * @param  {Connection} connection
     * @param  {Object} collections
     * @param  {Function} cb
     */
    registerConnection: function(connection, collections, cb) {

      var self = this;

      if(!connection.identity) return cb(Errors.IdentityMissing);
      if(_connections[connection.identity]) return cb(Errors.IdentityDuplicate);

      // Store the connection
      _connections[connection.identity] = {
        config: connection,
        collections: collections
      };

      // connect to database
      _connections[connection.identity].client = new Connection(connection, cb);
    },


    /**
     * Teardown
     *
     * Removes the connection object from the registry.
     *
     * @param {String} connectionName
     * @param {Function} callback
     */
    teardown: function (connectionName, cb) {

      if (typeof connectionName == 'function') {
        cb = connectionName;
        connectionName = null;
      }

      if (connectionName == null) {
        // remove all connections
        _connections = {};
      }
      else {
        // remove named connection
        delete _connections[connectionName];
      }

      return cb();
    }

  };

  return adapter;

})();
