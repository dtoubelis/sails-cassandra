var async = require('async'),
  Connection = require('./connection'),
  Collection = require('./collection'),
  Errors = require('waterline-errors').adapter;


module.exports = (function () {

  // Keep track of all the datastores used by the app
  var datastores = {};

  var adapter = {

    identity: 'sails-cassandra',

    // Waterline Adapter API Version
    adapterApiVersion: 1,

    // Which type of primary key is used by default
    pkFormat: 'string',

    // Set to true if this adapter supports (or requires) things like data types, validations, keys, etc.
    // If true, the schema for models using this adapter will be automatically synced when the server starts.
    // Not terribly relevant if your data store is not SQL/schemaful.
    syncable: true,

    // Default configuration for models
    // (same effect as if these properties were included at the top level of the model definitions)
    defaults: {

      contactPoints: ['127.0.0.1'],

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

    // This allows outside access to the connection manager.
    datastores: datastores,


    /**
     * Register a connection and the models assigned to it.
     *
     * @param  {Connection} connection
     * @param  {Object} models
     * @param  {Function} cb
     */
    registerDatastore: function (connection, models, cb) {

      if (!connection.identity) return cb(Errors.IdentityMissing);
      if (datastores[connection.identity]) return cb(Errors.IdentityDuplicate);

      // Store the connection
      datastores[connection.identity] = {
        config: connection,
        models: models,
        client: null,
        datastores: datastores,
      };


      // connect to database
      new Connection(connection, function (err, client) {

        if (err) return cb(err);

        datastores[connection.identity].client = client;

        // Build up a registry of models
        Object.keys(models).forEach(function (key) {
          datastores[connection.identity].models[key] = new Collection(models[key], client);
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
     * @param {Function} cb
     */
    teardown: function (connectionName, cb) {

      function shutdownConnection(id) {
        if (datastores[id].client) {
          datastores[id].client.shutdown();
        }
        delete datastores[id];
      }

      if (typeof connectionName == 'function') {
        cb = connectionName;
        connectionName = null;
      }

      // shutdown connections
      if (connectionName) {
        shutdownConnection(connectionName);
      }
      else {
        Object.keys(datastores).forEach(function (conn) {
          shutdownConnection(conn);
        });
      }

      cb();
    },


    /*
     * MIGRATABLE:
     *
     * .describe()
     * .define()
     * .drop()
     *
     */


    /**
     * Describe
     *
     * Return the Schema of a collection after first creating the collection
     * and indexes if they don't exist.
     *
     * @param {String} connectionName
     * @param {String} collectionName
     * @param {Function} cb
     */
    describe: function (connectionName, collectionName, cb) {

      var connectionObject = datastores[connectionName];
      if (!connectionObject) return cb(Errors.InvalidConnection);

      var collection = connectionObject.models[collectionName];
      if (!collection) return cb(Errors.CollectionNotRegistered);

      collection.describeTable(cb);
    },


    /**
     * Define
     *
     * Create a new Mongo Collection and set Index Values
     *
     * @param {String} connectionName
     * @param {String} collectionName
     * @param {Object} definition
     * @param {Function} cb
     */
    define: function (connectionName, collectionName, definition, cb) {

      var connectionObject = datastores[connectionName];
      if (!connectionObject) return cb(Errors.InvalidConnection);

      var collection = connectionObject.models[collectionName];
      if (!collection) return cb(Errors.CollectionNotRegistered);

      collection.createTable(cb);
    },


    /**
     * Drop
     *
     * Drop a table
     *
     * @param {String} connectionName
     * @param {String} collectionName
     * @param {Array} relations
     * @param {Function} cb
     */
    drop: function (connectionName, collectionName, relations, cb) {

      var connectionObject = datastores[connectionName];
      if (!connectionObject) return cb(Errors.InvalidConnection);

      async.series(
        [
          // drop the main table
          function(cb) {

            var collection = connectionObject.models[collectionName];
            if (!collection) return cb(Errors.CollectionNotRegistered);

            collection.dropTable(function(err) {
              // ignore "table does not exist" error
              if (err && err.code === 8704) return cb();
              if (err) return cb(err);
              cb();
            });
          },

          // drop relations
          function(cb) {
            // do it in parallel
            async.eachSeries(relations,
              function(item, cb) {

                var collection = connectionObject.models[item];
                if (!collection) return cb(Errors.CollectionNotRegistered);

                collection.dropTable(function(err) {
                  // ignore "table does not exist" error
                  if (err && err.code === 8704) return cb();
                  if (err) return cb(err);
                  cb();
                });
              },
              function(err, results) {
                if (err) return cb(err);
                cb();
              }
            );
          }
        ],
        function(err, results) {
          if (err) return cb(err);
          cb();
        }
      );
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
     * @param {Function} cb
     */
    create: function (connectionName, collectionName, data, cb) {

      var connectionObject = datastores[connectionName];
      var collection = connectionObject.models[collectionName];

      // Insert a new document into the collection
      collection.insert(data, function (err, results) {
        if (err) return cb(err);
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
     * @param {Function} cb
     */

    createEach: function (connectionName, collectionName, data, cb) {

      if (data.length === 0) {
        return cb(null, []);
      }

      var connectionObject = datastores[connectionName];
      var collection = connectionObject.models[collectionName];

      // Insert a new document into the collection
      collection.insert(data, function (err, results) {
        if (err) return cb(err);
        cb(null, results);
      });
    },


    /**
     * Find
     *
     * Find all records matching provided criteria.
     *
     * @param {String} connectionName
     * @param {String} collectionName
     * @param {Object} options
     * @param {Function} cb
     */
    find: function (connectionName, options, cb) {

      options = options || {};
      var connectionObject = datastores[connectionName];
      var collection = connectionObject.models[options.using];

      // Find all matching documents
      collection.find(options.criteria, function (err, results) {
        if (err) return cb(err);
        cb(null, results);
      });
    },


    /**
     * Count
     *
     * Count records matching provided criteria. This function
     * is memory optimized version of find.
     *
     * @param {String} connectionName
     * @param {String} collectionName
     * @param {Object} options
     * @param {Function} cb
     */
    count: function (connectionName, options, cb) {

      options = options || {};
      var connectionObject = datastores[connectionName];
      var collection = connectionObject.models[options.using];

      // Find all matching documents
      collection.count(options.criteria, function (err, recordCount) {
        if (err) return cb(err);
        cb(null, recordCount);
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
     * @param {Function} cb
     */
    update: function (connectionName, collectionName, options, values, cb) {

      options = options || {};
      var connectionObject = datastores[connectionName];
      var collection = connectionObject.models[collectionName];

      // Update matching documents
      collection.update(options, values, function (err, results) {
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
     * @param {Function} cb
     */
    destroy: function (connectionName, collectionName, options, cb) {

      options = options || {};
      var connectionObject = datastores[connectionName];
      var collection = connectionObject.models[collectionName];

      // Find matching documents
      collection.delete(options, function (err, result) {
        if (err) return cb(err);
        cb(null, result);
      });
    },


    /*
     * End of SEMANTIC
     */


    /**
     * Stream
     *
     * Sream rows from the database as soon as they are received.
     *
     * @param {String} connectionName
     * @param {String} collectionName
     * @param {Object} options
     * @param {WritableStream} stream
     */
    stream: function(connectionName, collectionName, options, stream) {

      options = options || {};

      var connectionObject = datastores[connectionName];
      if (!connectionObject) return stream.end(Errors.InvalidConnection);

      var collection = connectionObject.models[collectionName];
      if (!collection) return stream.end(Errors.CollectionNotRegistered);

      collection.stream(options, stream);
    },



    /**
     * Query
     *
     * Direct access to cql query. If `query` is a string then .execute() method
     * of cassandra drivel will be used to perform a query. If `query` is an array
     * then queries will be executed using .batch() and `param` must be an array
     * containing array of parameters for each query.
     *
     * @param {String} connectionName
     * @param {String} collectionName
     * @param {String|Array} query
     * @param {Object|Array} params
     * @param {WritableStream} cb
     */
    query: function(connectionName, collectionName, query, params, consistency, cb) {

      if (_.isFunction(params)) {
        cb = params;
        params = null;
      }

      var connectionObject = datastores[connectionName];
      var collection = connectionObject.models[collectionName];

      // Do raw query using collection's query method
      collection.query(query, params, consistency, function (err, result) {
        if (err) return cb(err);
        cb(null, result);
      });
    }

  };

  return adapter;

})();

