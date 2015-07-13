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
      cqlInsert += '"' + this.ddlHelper.attrNamesToColNames[attrName] + '",';
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
    skip = criteria.skip;
    limit = criteria.limit;
    sort = criteria.sort;
  }
  else {
    where = criteria;
  }


  if (where) {
    // criteria key can be specified as column name or attribute name
    // so, we make sure we handle both cases
    if (self.ddlHelper.attrNamesToColNames[self.ddlHelper.pkAttrName] in where) {
      idCol = self.ddlHelper.attrNamesToColNames[self.ddlHelper.pkAttrName];
      idVal = where[idCol];
    }
    else if (self.ddlHelper.pkAttrName in where) {
      idCol = self.ddlHelper.attrNamesToColNames[self.ddlHelper.pkAttrName];
      idVal = where[self.ddlHelper.pkAttrName];
    }

    if (idCol) {
      // if PK is specified then ignore all other criteria
      cql += ' WHERE "' + idCol + '" = ?';
      cqlParams = [ idVal ];
    }
    else {
      // parse criteria
      arr = self._parseCriteria(where);
      cql += " WHERE " + arr[0];
      cqlParams = arr[1];
      // add limit 
      if (typeof limit !== 'undefined') {
        cql += " LIMIT " + limit;
      }
      // close the statement
      cql += " ALLOW FILTERING";
     }
  }
  // close the statement
  cql += ";";

  // get results
  this.client.execute(cql, cqlParams, { prepare: true },
    function(err, result) {
      if (err) return cb(err);
      cb(null, myUtil.castFromCassandraToWaterline(result.rows[0].count));
    }
  );
};



/**
 * Update multiple records by executing select first
 *
 * @param {Object} criteria
 * @param {Object} values
 * @param {Function} callback
 * @api public
 */
Collection.prototype.update = function(criteria, values, cb) {

  var self = this;
  var where, skip, limit, sort;
  var queries = [];
  var updates = [];

  var pkColName = self.ddlHelper.attrNamesToColNames[self.ddlHelper.pkAttrName];
  var cqlSelect = "SELECT " + pkColName + " FROM " + self.ddlHelper.tableName + " WHERE";
  var cqlUpdate = "UPDATE " + self.ddlHelper.tableName + " SET";
  var cqlSelectParams, cqlUpdateParams;
  var idCol, idVal, arr;

  // build update cql
  cqlUpdateParams = [];
  Object.keys(values).forEach(function(attrName) {
    cqlUpdate += ' "' + self.ddlHelper.attrNamesToColNames[attrName] + '" = ?,';
    cqlUpdateParams.push(values[attrName]);
  });
  cqlUpdate = cqlUpdate.replace(/,$/, ' WHERE "' + pkColName + '" = ?;');

  // parse criteria
  if ('where' in criteria) {
    where = criteria.where;
  }
  else {
    where = criteria;
  }

  // criteria key can be specified as column name or attribute name
  // so, we make sure we handle both cases
  if (self.ddlHelper.attrNamesToColNames[self.ddlHelper.pkAttrName] in where) {
    idCol = self.ddlHelper.attrNamesToColNames[self.ddlHelper.pkAttrName];
    idVal = where[idCol];
  }
  else if (self.ddlHelper.pkAttrName in where) {
    idCol = self.ddlHelper.attrNamesToColNames[self.ddlHelper.pkAttrName];
    idVal = where[self.ddlHelper.pkAttrName];
  }

  if (idCol) {

    // build update CQL
    cqlUpdateParams.push(idVal);
    updates = [ values ];
    updates[0][self.ddlHelper.pkAttrName] = idVal;

    // execute a single query
    self.client.execute(cqlUpdate, cqlUpdateParams, { prepare: true }, function(err, result) {
      if (err) return cb(err);
      cb(null, updates);
    });

  }
  else {

    // parse criteria
    arr = self._parseCriteria(where);
    cqlSelect += " " + arr[0] + " ALLOW FILTERING;";
    cqlSelectParams = arr[1];

    // build update array
    self.client.eachRow(cqlSelect, cqlSelectParams, { prepare: true },
      function(n, row) {
        // construct return value
        values[self.ddlHelper.pkAttrName] = myUtil.castFromCassandraToWaterline(row[pkColName]);
        updates.push(values);
        // add query to a batch
        queries.push({query: cqlUpdate, params: cqlUpdateParams.concat(row[pkColName])});
      },
      function(err, totalCount) {
        if (err) return cb(err);

        // execute batch update
        self.client.batch(queries, { prepare: true }, function(err, result) {
          if (err) return cb(err);
          cb(null, updates);
        });
      }
    );
  }

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
  var pkColName = self.ddlHelper.attrNamesToColNames[self.ddlHelper.pkAttrName];
  var cqlSelect = 'SELECT ' + pkColName + ' FROM ' + self.ddlHelper.tableName + ' WHERE';
  var cqlDelete = 'DELETE FROM ' + self.ddlHelper.tableName + ' WHERE "' + pkColName + '" = ?;';
  var cqlDeleteParams, cqlSelectParams;
  var where, idCol, idVal, arr;

  // validate the criteia
  if ('where' in criteria) {
    where = criteria.where;
  }
  else {
    where = criteria;
  }

  // criteria key can be specified as column name or attribute name
  // so, we make sure we handle both cases
  if (self.ddlHelper.attrNamesToColNames[self.ddlHelper.pkAttrName] in where) {
    idCol = self.ddlHelper.attrNamesToColNames[self.ddlHelper.pkAttrName];
    idVal = where[idCol];
  }
  else if (self.ddlHelper.pkAttrName in where) {
    idCol = self.ddlHelper.attrNamesToColNames[self.ddlHelper.pkAttrName];
    idVal = where[self.ddlHelper.pkAttrName];
  }

  // prepare where clause
  if (idCol) {

    cqlDeleteParams = [ idVal ];

    // execute delete by PK
    self.client.execute(cqlDelete, cqlDeleteParams, { prepare: true }, function(err) {
      if (err) return cb(err);
      cb();
    });

  }
  else {

    arr = self._parseCriteria(where);
    cqlSelect += " " + arr[0] + " ALLOW FILTERING;";
    cqlSelectParams = arr[1];

    // build a delete array
    arr = [];
    self.client.eachRow(cqlSelect, cqlSelectParams, { prepare: true },
      function(n, row) {
        // add query to a batch
        arr.push({ query: cqlDelete, params: [ row[pkColName] ] });
      },
      function(err, totalCount) {
        if (err) return cb(err);

        // execute batch update
        self.client.batch(arr, { prepare: true }, function(err, result) {
          if (err) return cb(err);
          cb();
        });
      }
    );
  }

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
  var cql = 'CREATE TABLE ' + this.ddlHelper.tableName + ' (';
  Object.keys(this.ddlHelper.colTypes).forEach(function(colName) {
    cql += '"' + colName + '" ' + this.ddlHelper.colTypes[colName] + ', ';
  }, this);
  cql += 'PRIMARY KEY ("' + this.ddlHelper.attrNamesToColNames[this.ddlHelper.pkAttrName] + '"));';
  this.ddlHelper.cqlCreateTable = cql;

  // add indices
  _.uniq(uniqColNames.concat(indexColNames)).forEach(function(colName) {
    var indexName = "idx__" + this.ddlHelper.tableName + "__" + colName;
    cql = "CREATE INDEX " + indexName + " ON " + this.ddlHelper.tableName + '("' + colName + '");';
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
          cql = '"' + parentKey + '" ' + key + ' ?';
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
        throw new Error("Unknown field '" + key + '"');
      }
      key = a;
    }

    // pair
    if (_.isString(value) || _.isNumber(value) || _.isDate(value)) {
      cql = '"' + key + '" = ?';
      params = params.concat(value);
    }
    // in pair
    else if (_.isArray(value)) {
      cql = '"' + key + '" IN (?)';
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
    cql += '"' + colName + '",';
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
    // criteria key can be specified as column name or attribute name
    // so, we make sure we handle both cases
    if (self.ddlHelper.attrNamesToColNames[self.ddlHelper.pkAttrName] in where) {
      idCol = self.ddlHelper.attrNamesToColNames[self.ddlHelper.pkAttrName];
      idVal = where[idCol];
    }
    else if (self.ddlHelper.pkAttrName in where) {
      idCol = self.ddlHelper.attrNamesToColNames[self.ddlHelper.pkAttrName];
      idVal = where[self.ddlHelper.pkAttrName];
    }

    if (idCol) {
      // if PK is specified then ignore all other criteria
      cql += ' WHERE "' +  idCol + '" = ?';
      cqlParams = [ idVal ];
    }
    else {
      // parse criteria
      arr = self._parseCriteria(where);
      cql += " WHERE " + arr[0];
      cqlParams = arr[1];
      // add limit 
      if (typeof limit !== 'undefined') {
        cql += " LIMIT " + limit;
      }
      // close the statement
      cql += " ALLOW FILTERING";
    }
  }
  // close the statement
  cql += ";";

  return [ cql, cqlParams ];
};

module.exports = Collection;
