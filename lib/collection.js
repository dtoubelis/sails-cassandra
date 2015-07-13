var _ = require('lodash'),
  async = require('async'),
  through2 = require('through2'),
  myUtil = require('./util'),
  TimeUuid = require('cassandra-driver').types.TimeUuid;

/**
 * Collection
 *
 * Constructor of collection
 * @param {Object} definition
 * @param {Client} cassandra client
 */
var Collection = function(definition, client) {

  this.client = client;

  this.schema = null;

  this.ddlHelper = {
    tableName: null,
    pkAttrName: null,
    pkAutoIncrement: false,
    attrNamesToColNames: {},
    attrTypes: {},
    colNamesToAttrNames: {},
    colTypes: {},
    defaultValues: {},
    cqlCreateTable: null,
    indexes: {}
  };

  this._parseDefinition(definition);
};


/////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS
/////////////////////////////////////////////////////////////////////////////////

/**
 * Raw cql query
 *
 * @param {Object|Array} queries
 * @param {Object|Array} params
 * @param {Function} callback
 * @api public
 */
Collection.prototype.query = function(queries, params, cb) {

  // validate cb
  if (!cb || !_.isFunction(cb)) {
    throw new Error("Callback is missing or not a function.");
  }

  if (!_.isArray(params)) {
    return cb(new Error("params is not an array."));
  }

  // validate params
  if (_.isString(queries)) {
    // execute query
    this.client.execute(queries, params, { prepare: true }, function(err, result) {
      if (err) return cb(err);
      cb(null, result.rows);
    });
  }
  else if (!_.isArray(queries)) {
    return cb(new Error("query param is not an array nor string."));
  }
  else {
    var batch_queries = [];

    for(var i=0;i<queries.length;i++) {
      batch_queries.push({query: queries[i], params: params[i]});
    }

    // execute batch
    this.client.batch(batch_queries, { prepare: true }, function(err, result) {
      if (err) return cb(err);
      cb(null, result.rows);
    });
  }

};


/**
 * Create collection table and indices
 *
 * @param {String} tableName
 * @param {Function} callback
 * @api public
 */
Collection.prototype.createTable = function(cb) {

  var self = this;

  async.series(
    [
      // create table
      function(cb) {
        self.client.execute(self.ddlHelper.cqlCreateTable, function(err, result) {
          if (err) return cb(err);
          cb();
        });
      },
      // create indexes
      function(cb) {
        async.eachSeries(_.values(self.ddlHelper.indexes),
          function(item, cb) {
            self.client.execute(item, function(err, result) {
              if (err) return cb(err);
              cb();
            });
          },
          function (err, results) {
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
};


/**
 * Describe table constructing collection definition
 * from the underlying database. Only attributes that
 * are found in the oridinal definition are included.
 *
 * @param {String} tableName
 * @param {Function} callback
 * @api public
 */
Collection.prototype.describeTable = function(cb) {

  var self = this;

  var pkAttr;
  var collection =  {};

  var cql = "SELECT * FROM system.schema_columns WHERE keyspace_name='" + self.client.keyspace + "'";
  cql +=  " AND columnfamily_name='" + self.ddlHelper.tableName + "';";

  self.client.eachRow(cql,
    function(n, row) {
      var attrName = self.ddlHelper.colNamesToAttrNames[row.column_name];
      if (attrName) {
        collection[attrName] = {};
        collection[attrName].columnName = row.column_name;
      }
      if (row.type === 'partition_key') {
        if (!pkAttr) {
          pkAttr = attrName;
        }
        else {
          console.warn("Compound partition key detected. Schema will not be updated.");
          pkAttr = undefined;
        }
      }
      else if (row.type === 'regular') {
        if (row.index_name) {
          collection[attrName].index = true;
        }
        else {
          collection[attrName].index = false;
        }
      }
    },
    function(err, result) {
      if (err) return cb(err);
      if (result.rowLength > 0) {
        if (pkAttr) {
          collection[pkAttr].primaryKey = true;
        }
        // merge discovered attributes with the schema
        return cb(null, _.merge({}, self.schema, collection));
      }
      cb();
    }
  );
};



/**
 * Drop collection structures
 *
 * @param {Function} callback
 * @api public
 */
Collection.prototype.dropTable = function(cb) {

  // drop table
  this.client.execute("DROP TABLE " + this.ddlHelper.tableName + ";", function(err, result) {
    if (err) return cb(err);
    cb();
  });
};


/**
 * Insert a new record
 *
 * @param {Object|Array} values
 * @param {Function} callback
 * @api public
 */
Collection.prototype.insert = function(items, cb) {

  var queries = [];

  // validate cb
  if (!cb || !_.isFunction(cb)) {
    throw new Error("Callback is missing or not a function.");
  }

  // validate params
  if (_.isPlainObject(items)) {
    items = [ items ];
  }
  else if (!_.isArray(items)) {
    return cb(new Error("Param is not an array nor object."));
  }

  // make sure no references here (TODO: douleckeck if this is necessary)
  items.forEach(function(item) {

    // validate primary key
    if (this.ddlHelper.pkAttrName in item) {
      if (this.ddlHelper.pkAutoIncrement) {
        return cb(new Error("Overriding autoincrement fields is not allowed."));
      }
    }
    else {
      if (this.ddlHelper.pkAutoIncrement) {
        item[this.ddlHelper.pkAttrName] = new TimeUuid().toString();
      }
      else {
        return cb(new Error("Missing primary key in the insert request."));
      }
    }

    // construct insert statement and param list
    var cqlInsert = "INSERT INTO " + this.ddlHelper.tableName + " (";
    var cqlValues = "VALUES (";
    var cqlParams = [];
    Object.keys(item).forEach(function(attrName) {
      cqlInsert += attrName + ",";
      cqlValues += "?,";
      cqlParams.push(item[attrName]);
    }, this);

    // execute INSERT on the server
    var cql = cqlInsert.replace(/,$/, ') ') + cqlValues.replace(/,$/, ");");

    // add query with corresponding params to the batch
    queries.push({query: cql, params: cqlParams});

  }, this);


  // execute batch
  this.client.batch(queries, { prepare: true }, function(err, result) {
    if (err) return cb(err);
    cb(null, items);
  });

};


/**
 * Find
 *
 * @param {Object} criteria
 * @param {Function} callback
 * @api public
 */
Collection.prototype.find = function(criteria, cb) {

  var self = this;
  var arr = self._constructSelectQuery(criteria);

  // get results
  var results = [];
  this.client.eachRow(arr[0], arr[1], { prepare: true },
    function(n, row) {
      var rs = {};
      Object.keys(self.ddlHelper.colNamesToAttrNames).forEach(function(colName) {
        rs[self.ddlHelper.colNamesToAttrNames[colName]] = myUtil.castFromCassandraToWaterline(row[colName]);
      });
      results.push(rs);
    },
    function(err, totalCount) {
      if (err) return cb(err);
      cb(null, results);
    }
  );
};



/**
 * Count
 *
 * @param {Object} criteria
 * @param {Function} callback
 * @api public
 */
Collection.prototype.count = function(criteria, cb) {

  var self = this;
  var where, skip, limit, sort;
  var idCol, idVal, arr;

  var cql = "SELECT COUNT(*) FROM " + this.ddlHelper.tableName;
  var cqlParams = [];

  // validate where clause
  if ('where' in criteria) {
    where = criteria.where;
    limit = criteria.limit;
  }
  else {
    where = criteria;
  }


  if (where) {
    // As we are supporting compound primary key with partition key and
    // clustering columns now, so there is no need to find pkAttribute or
    // it's column name to ignore other criteria. Support for clustering
    // columns in compound primary key will also enable sorting functionality.
    // So just parse criteria would handle everything in this case.
    arr = self._parseCriteria(where);
    cql += " WHERE " + arr[0];
    cqlParams = arr[1];
  }

  // add limit
  if (typeof limit !== 'undefined') {
    cql += " LIMIT " + limit;
  }
  // close the statement
  cql += " ALLOW FILTERING;";

  // get results
  this.client.execute(cql, cqlParams, { prepare: true },
    function(err, result) {
      if (err) return cb(err);
      cb(null, myUtil.castFromCassandraToWaterline(result.rows[0].count));
    }
  );
};



/**
 * Update single or multiple records
 *
 * @param {Object/Array} criteria
 * @param {Object/Array} values
 * @param {Function} callback
 * @api public
 */
Collection.prototype.update = function(criteria, values, cb) {

  var self = this;

  // validate cb
  if (!cb || !_.isFunction(cb)) {
    throw new Error("Callback is missing or not a function.");
  }

  // validate params
  if (_.isPlainObject(criteria)) {
    criteria = [ criteria ];
  }
  else if (!_.isArray(criteria)) {
    return cb(new Error("Param is not an array nor object."));
  }

  if (_.isPlainObject(values)) {
    values = [ values ];
  }
  else if (!_.isArray(values)) {
    return cb(new Error("Param is not an array nor object."));
  }

  var updates = [];

  for(var i=0;i<criteria.length;i++) {
    var where, skip, limit, sort;
    var queries = [];

    var cqlUpdate = "UPDATE " + self.ddlHelper.tableName + " SET";
    var cqlUpdateParams;
    var idCol, idVal, arr;

    // build update cql
    cqlUpdateParams = [];
    Object.keys(values[i]).forEach(function(attrName) {
      cqlUpdate += " " + self.ddlHelper.attrNamesToColNames[attrName] + " = ?,";
      cqlUpdateParams.push(values[i][attrName]);
    });
    cqlUpdate = cqlUpdate.replace(/,$/, " WHERE");

    // parse criteria
    if ('where' in criteria[i]) {
      where = criteria[i].where;
    }
    else {
      where = criteria[i];
    }


    if (where) {
      // As we are supporting compound primary key with partition key and
      // clustering columns now, so there is no need to find pkAttribute or
      // it's column name to ignore other criteria. Support for clustering
      // columns in compound primary key will also enable sorting functionality.
      // So just parse criteria would handle everything in this case.
      arr = self._parseCriteria(where);
      cqlUpdate += " " + arr[0];
      cqlUpdateParams = cqlUpdateParams.concat(arr[1]);

    }
    // close the statement
    cqlUpdate += ";";

    // add query to a batch
    queries.push({query: cqlUpdate, params: cqlUpdateParams});
    Object.keys(where).forEach(function(key) {
      values[i][key] = where[key];
    });
    updates.push(values[i]);
  }

  // execute batch update
  self.client.batch(queries, { prepare: true }, function(err, result) {
    if(err) return cb(err);
    cb(null, updates);
  });

};


/**
 * Delete a record
 *
 * @param {Object} criteria
 * @param {Function} callback
 * @api public
 */
Collection.prototype.delete = function(criteria, cb) {

  var self = this;

  // validate cb
  if (!cb || !_.isFunction(cb)) {
    throw new Error("Callback is missing or not a function.");
  }

  // validate params
  if (_.isPlainObject(criteria)) {
    criteria = [ criteria ];
  }
  else if (!_.isArray(criteria)) {
    return cb(new Error("Param is not an array nor object."));
  }

  var updates = [];

  for(var i=0;i<criteria.length;i++) {
    var cqlDelete = "DELETE FROM " + self.ddlHelper.tableName + " WHERE";
    var cqlDeleteParams;
    var queries = [];
    var where, idCol, idVal, arr;

    // validate the criteia
    if ('where' in criteria[i]) {
      where = criteria[i].where;
    }
    else {
      where = criteria[i];
    }

    if (where) {
      // As we are supporting compound primary key with partition key and
      // clustering columns now, so there is no need to find pkAttribute or
      // it's column name to ignore other criteria. Support for clustering
      // columns in compound primary key will also enable sorting functionality.
      // So just parse criteria would handle everything in this case.
      arr = self._parseCriteria(where);
      cqlDelete += " " + arr[0];
      cqlDeleteParams = arr[1];
    }
    // close the statement
    cqlDelete += ";";

    // add query to a batch
    queries.push({query: cqlDelete, params: cqlDeleteParams});

    updates.push(where);
  }

  // execute batch delete
  self.client.batch(queries, { prepare: true }, function(err, result) {
    if(err) return cb(err);
    cb(null, updates);
  });

};


/**
 * Stream rows directly from the database
 *
 * @param {Object} criteria
 * @param {WritableStream} stream
 * @api public
 */
Collection.prototype.stream = function(criteria, stream) {

  var self = this;

  var arr = self._constructSelectQuery(criteria);

  this.client.stream(arr[0], arr[1], { prepare: true })
    .pipe(through2({ objectMode: true },
      function(chunk, encoding, cb) {
        var rs = {};
        Object.keys(self.ddlHelper.colNamesToAttrNames).forEach(function(colName) {
          rs[self.ddlHelper.colNamesToAttrNames[colName]] = myUtil.castFromCassandraToWaterline(chunk[colName]);
        });
        // push the result
        cb (null, rs);
      }
    ))
    .pipe(stream);
};


/**
 * Raw cql query
 *
 * @param {String|Array} queries
 * @param {Object|Array} params
 * @param {Function} callback
 * @api public
 */
Collection.prototype.query = function(queries, params, cb) {

  // validate cb
  if (!cb || !_.isFunction(cb)) {
    throw new Error("Callback is missing or not a function.");
  }

  if (!_.isArray(params)) {
    return cb(new Error("params is not an array."));
  }

  // validate params
  if (_.isString(queries)) {
    // execute query
    this.client.execute(queries, params, { prepare: true }, function(err, result) {
      if (err) return cb(err);
      cb(null, result.rows);
    });
  }
  else if (!_.isArray(queries)) {
    return cb(new Error("query param is not an array nor string."));
  }
  else {
    var batch_queries = [];

    for(var i=0; i<queries.length; i++) {
      batch_queries.push({query: queries[i], params: params[i]});
    }

    // execute batch
    this.client.batch(batch_queries, { prepare: true }, function(err, result) {
      if (err) return cb(err);
      cb(null, result.rows);
    });
  }

};


/////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS
/////////////////////////////////////////////////////////////////////////////////

Collection.prototype._parseDefinition = function(definition) {

  // derrive table name
  if (!definition.tableName) {
    definition.tableName = definition.identity.toLowerCase();
  }
  this.ddlHelper.tableName = definition.tableName;

  // set schema
  this.schema = _.cloneDeep(definition.definition);

  var uniqColNames = [];
  var indexColNames = [];
  var clusterColNames = [];

  Object.keys(this.schema).forEach(function(attrName) {

    var attribute = this.schema[attrName];

    // map attribute name to column name
    // Note, that database col names are case insensitive, so we convert
    // everything to lower case to have some consistency
    if (!attribute.columnName) {
      attribute.columnName = attrName.toLowerCase();
    }
    var colName = attribute.columnName;
    this.ddlHelper.attrNamesToColNames[attrName] = colName;
    this.ddlHelper.colNamesToAttrNames[colName] = attrName;

    // resolve sails/waterline's type into cassandra type
    var colType = myUtil.mapWaterlineTypeToCassandra(attribute.type);
    if (attribute.autoIncrement) {
      colType = 'timeuuid';
    }
    this.ddlHelper.attrTypes[attrName] = attribute.type;
    this.ddlHelper.colTypes[colName] = colType;

    // handle default value
    if ('defaultsTo' in attribute) {
      if (attrName === "createdAt" || attrName === "updatedAt") {
        console.log("Default value for attribute '%s' is not supported.");
      }
      else {
        this.ddlHelper.defaultValues[colName] = attribute.defaultsTo;
      }
    }

    // handle primary key and unique
    if (attribute.primaryKey) {
      if (this.ddlHelper.pkAttrName) {
        throw new Error("Duplicate primary key definition.");
      }
      else {
        this.ddlHelper.pkAttrName = attrName;
      }
      //handle clustering columns in primary key definition
      if(attribute.on) {
        clusterColNames = attribute.on;
      }
    }
    else if (attribute.unique) {
      uniqColNames.push(colName);
    }

    // handle index
    if (attribute.index) {
      indexColNames.push(colName);
    }

    // handle autoincrement flag
    if (attribute.autoIncrement) {
      if (!attribute.primaryKey) {
        throw new Error("Autoincrement attribute on non primary key.");
      }
      else {
        this.ddlHelper.pkAutoIncrement = true;
      }
    }

  }, this);

  // create table SQL
  var cql = "CREATE TABLE " + this.ddlHelper.tableName + " (";
  Object.keys(this.ddlHelper.colTypes).forEach(function(colName) {
    cql += "" + colName + " " + this.ddlHelper.colTypes[colName] + ", ";
  }, this);
  cql += "PRIMARY KEY (" + this.ddlHelper.attrNamesToColNames[this.ddlHelper.pkAttrName];
  if(clusterColNames.length > 0) {
    for(var i=0;i<clusterColNames.length;i++) {
      //cluster columns can be specified as column name or attribute name
      //so we make sure we handle both cases
      if (!(clusterColNames[i] in this.ddlHelper.colNamesToAttrNames)) {
        var candidate_col_name = this.ddlHelper.attrNamesToColNames[clusterColNames[i]];
        if (!candidate_col_name) {
          throw new Error("Unknown clustering field '" + clusterColNames[i] + "' in compound primary key definition");
        }
        clusterColNames[i] = candidate_col_name;
      }
    }
    cql += ","+clusterColNames.toString();
  }
  cql +=  "));";

  this.ddlHelper.cqlCreateTable = cql;

  // add indices
  _.uniq(uniqColNames.concat(indexColNames)).forEach(function(colName) {
    var indexName = "idx__" + this.ddlHelper.tableName + "__" + colName;
    cql = "CREATE INDEX " + indexName + " ON " + this.ddlHelper.tableName + "(" + colName + ");";
    this.ddlHelper.indexes[indexName] = cql;
  }, this);

};


/**
 * Parse criteria and create CQL 'WHERE' clause.
 * This method returns array with first element being CQL dtring,
 * and the socond - is array of query parameters.
 *
 * Note: this method expects criteria to be validated and attribute
 * names resolved into column names.
 *
 * @param {Object} criteria
 * @param {Function} callback
 * @api public
 */
Collection.prototype._parseCriteria = function(value, key, parentKey) {

  var cql = "";
  var params = [];
  var a, ops;

  if (!key) {
    if (_.isPlainObject(value)) {
      a = [];
      Object.keys(value).forEach(function(k) {
        var arr = this._parseCriteria(value[k], k, null);
        a = a.concat(arr[0]);
        params = params.concat(arr[1]);
      }, this);
      cql = a.join(" AND ");
    }
    else {
      throw new Error("The value must be an object.");
    }
  }
  else if (parentKey) {
    // handle operators
    switch (key) {
      case '<':
      case '>':
      case '<=':
      case '>=':
        if (_.isString(value) || _.isNumber(value) || _.isDate(value)) {
          cql = parentKey + " " + key + " ?";
          params = [ value ];
        }
        else {
          throw new Error("Invalid operand type '" + typeof value + "' .");
        }
        break;
      default:
        throw new Error("Unsupported operation '" + key + "'.");
    }
  }
  else {

    // make sure key exists
    if (!(key in this.ddlHelper.colNamesToAttrNames)) {
      a = this.ddlHelper.attrNamesToColNames[key];
      if (!a) {
        throw new Error("Unknown field '" + key + "'");
      }
      key = a;
    }

    // pair
    if (_.isString(value) || _.isNumber(value) || _.isDate(value)) {
      cql = key + " = ?";
      params = params.concat(value);
    }
    // in pair
    else if (_.isArray(value)) {
      cql = key + " IN (?)";
      params = [ value ];
    }
    // modified pair
    else if (_.isPlainObject(value)) {
      a = [];
      ops = {};
      Object.keys(value).forEach(function(k) {
        // normalize operators
        var op;
        switch (k) {
          case 'lessThan':
            op = '<';
            break;
          case 'lessThanOrEqual':
            op = '<=';
            break;
          case 'greaterThan':
            op = '>';
            break;
          case 'greaterThanOrEqual':
            op = '>=';
            break;
          default:
            op = k;
        }
        // validate
        if (op === '<' && '<=' in ops || op === '<=' && '<' in ops) {
          throw new Error("Mutually exclusive operations '<' and '<=' on '" + key + "' attribute.");
        }
        else if (op === '>' && '>=' in ops || op === '>=' && '>' in ops) {
          throw new Error("Mutually exclusive operations '<' and '<=' on '" + key + "' attribute.");
        }
        else {
          ops[op] = true;
        }
        // process
        var arr = this._parseCriteria(value[k], op, key);
        a = a.concat(arr[0]);
        params = params.concat(arr[1]);
      }, this);
      cql = a.join(" AND ");
    }
    else {
      throw new Error("Value for attribute '" + key + "' must be string or array.");
    }
  }

  return [ cql, params ];
};


Collection.prototype._constructSelectQuery = function(criteria) {

  var self = this;
  var where, skip, limit, sort;
  var idCol, idVal, arr;

  var cql = "SELECT ";
  Object.keys(this.ddlHelper.colTypes).forEach(function(colName) {
    cql += colName + ",";
  }, this);
  cql = cql.replace(/,$/, " FROM " + this.ddlHelper.tableName);
  var cqlParams = [];

  // validate where clause
  if ('where' in criteria) {
    where = criteria.where;
    skip = criteria.skip;
    limit = criteria.limit;
    sort = criteria.sort;
  }
  else {
    where = criteria;
  }

  if (where) {
    // As we are supporting compound primary key with partition key and
    // clustering columns now, so there is no need to find pkAttribute or
    // it's column name to ignore other criteria. Support for clustering
    // columns in compound primary key will also enable sorting functionality.
    // So just parse criteria would handle everything in this case.
    arr = self._parseCriteria(where);
    cql += " WHERE " + arr[0];
    cqlParams = arr[1];
  }

  // add sort
  if (typeof sort !== 'undefined') {
    var sort_key = Object.keys(sort)[0];
    var sort_direction = (sort[sort_key] == 1 ? " ASC" : " DESC");

    //sort_key can be specified as column name or attribute name
    //so we make sure we handle both cases
    if (!(sort_key in self.ddlHelper.colNamesToAttrNames)) {
      var candidate_sort_key = self.ddlHelper.attrNamesToColNames[sort_key];
      if (!candidate_sort_key) {
        throw new Error("Unknown sort field '" + sort_key + "'");
      }
      sort_key = candidate_sort_key;
    }

    cql += " ORDER BY " + sort_key + sort_direction;
  }
  // add limit
  if (typeof limit !== 'undefined') {
    cql += " LIMIT " + limit;
  }
  // close the statement
  cql += " ALLOW FILTERING;";

  return [ cql, cqlParams ];
};

module.exports = Collection;
