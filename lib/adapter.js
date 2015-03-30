var Connection = require('./connection');
var Collection = require('./collection');
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
      migrate: 'safe'
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
        collections: collections,
        connection: {}
      };


      // connect to database
      new Connection(connection, function(_err, client) {

        if (_err) return cb(_err);

        _connections[connection.identity].client = client;

        // Build up a registry of collections
        Object.keys(collections).forEach(function(key) {
          _connections[connection.identity].collections[key] = new Collection(collections[key], client);
        });

        // execute callback
        cb();
      });

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

      function shutdownConnection(id) {
        if (_connections[id].client) {
          _connections[id].client.shutdown();
        }
        delete _connections[id];
      }

      if (typeof connectionName == 'function') {
        cb = connectionName;
        connectionName = null;
      }

      // shutdown connections
      if (!connectionName) {
        Object.keys(_connections).forEach(function(conn) {
          shutdownConnection(conn);
        });
      }
      else {
        shutdownConnection(connectionName);
      }

      cb();
    },


    /*
     * SEMANTIC:
     *
     * Mandatory methods:
     *   .create()
     *   .find()
     *   .update()
     *   .destroy()
     *
     * Optimizations:
     *   .findOrCreate()
     *   .createEach()
     */


    /**
     * Create
     *
     * Insert a single document into a collection.
     *
     * @param {String} connectionName
     * @param {String} collectionName
     * @param {Object} data
     * @param {Function} callback
     */
    create: function(connectionName, collectionName, data, cb) {

      var connectionObject = _connections[connectionName];
      var collection = connectionObject.collections[collectionName];

      // Insert a new document into the collection
      collection.insert(data, function(err, results) {
        if(err) return cb(err);
        cb(null, results[0]);
      });
    },


    /**
     * Create Each
     *
     * Insert an array of documents into a collection.
     *
     * @param {String} connectionName
     * @param {String} collectionName
     * @param {Object} data
     * @param {Function} callback
     */

    createEach: function(connectionName, collectionName, data, cb) {

      if (data.length === 0) {return cb(null, []);}

      var connectionObject = _connections[connectionName];
      var collection = connectionObject.collections[collectionName];

      // Insert a new document into the collection
      collection.insert(data, function(err, results) {
        if(err) return cb(err);
        cb(null, results);
      });
    },


    /**
     * Find
     *
     * Find all matching documents in a colletion.
     *
     * @param {String} connectionName
     * @param {String} collectionName
     * @param {Object} options
     * @param {Function} callback
     */
    find: function(connectionName, collectionName, options, cb) {

      options = options || {};
      var connectionObject = _connections[connectionName];
      var collection = connectionObject.collections[collectionName];

      // Find all matching documents
      collection.find(options, function(err, results) {
        if(err) return cb(err);
        cb(null, results);
      });
    },


    /**
     * Update
     *
     * Update all documents matching a criteria object in a collection.
     *
     * @param {String} connectionName
     * @param {String} collectionName
     * @param {Object} options
     * @param {Object} values
     * @param {Function} callback
     */
    update: function(connectionName, collectionName, options, values, cb) {

      options = options || {};
      var connectionObject = _connections[connectionName];
      var collection = connectionObject.collections[collectionName];

      // Update matching documents
      collection.update(options, values, function(err, results) {
        if (err) return cb(err);
        cb(null, results);
      });
    },


    /**
     * Destroy
     *
     * Destroy all documents matching a criteria object in a collection.
     *
     * @param {String} connectionName
     * @param {String} collectionName
     * @param {Object} options
     * @param {Function} callback
     */
    destroy: function(connectionName, collectionName, options, cb) {

      options = options || {};
      var connectionObject = _connections[connectionName];
      var collection = connectionObject.collections[collectionName];

      // Find matching documents
      collection.delete(options, function(err, result) {
        if (err) return cb(err);
        cb(null, result);
      });
    },


    /*
     * End of SEMANTIC
     */

  };

  return adapter;

})();
