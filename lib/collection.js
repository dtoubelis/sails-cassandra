var _ = require('lodash');
var cqlTypeCast = require('./util').cqlTypeCast;
var types = require('cassandra-driver').types;
var Uuid = types.Uuid;
var TimeUuid = types.TimeUuid;
var Integer = types.Integer;
var BigDecimal = types.BigDecimal;
var InetAddress = types.InetAddress;
var Long = require('long');


var Collection = module.exports = function(definition, client) {

  if (!(this instanceof Collection)) {
    throw new Error("Collection is not instantiated (forgot new() perhaps?).");
  }

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
 * Create collection structures
 *
 * @param {Function} callback
 * @api public
 */
Collection.prototype.create = function(cb) {

  // validate params
  if (!cb || !_.isFunction(cb)) {
    throw new Error("Callback is missing or not a function.");
  }

  this.client.batch([ this.ddlHelper.cqlCreateTable ].concat(Object.values(this.ddlHelper.indexes)), function(err) {
    if (err) return cb(err);
    cb();
  });
};


/**
 * Drop collection structures
 *
 * @param {Function} callback
 * @api public
 */
Collection.prototype.drop = function(cb) {

  // validate params
  if (!cb || !_.isFunction(cb)) {
    throw new Error("Callback is missing or not a function.");
  }

  var cqlBatch = [];
  Object.keys(this.ddlHelper.indexes).forEach(function(indexName) {
    cqlBatch.push("DROP INDEX " + indexName + ";");
  }, this);

  cqlBatch.push("DROP TABLE " + this.ddlHelper.cqlCreateTable + ";");

  this.client.batch(cqlBatch, function(err) {
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
Collection.prototype.insert = function(val, cb) {

  // validate cb
  if (!cb || !_.isFunction(cb)) {
    throw new Error("Callback is missing or not a function.");
  }

  var values = _.cloneDeep(val);

  // validate params
  if (_.isPlainObject(values)) {
    values = [ values ];
  }
  else if (!_.isArray(values)) {
    return cb(new Error("Param is not an array nor object."));
  }

  // make sure no references here (TODO: douleckeck if this is necessary)
  values.forEach(function(item) {

    // validate primary key
    if (this.ddlHelper.pkAttrName in item) {
      if (this.ddlHelper.pkAutoIncrement) {
        return cb(new Error("Overriding autoincrement fields is not allowed."));
      }
    }
    else {
      if (this.ddlHelper.pkAutoIncrement) {
        item[this.ddlHelper.pkAttrName] = new TimeUuid();
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
      cqlInsert += this.ddlHelper.attrNamesToColNames[attrName] + ",";
      cqlValues += "?,";
      cqlParams.push(item[attrName]);
    }, this);

    // execute INSERT on the server
    var cql = cqlInsert.replace(/,$/, ') ') + cqlValues.replace(/,$/, ");");
    this.client.execute(cql, cqlParams, { prepare: true }, function(err) {
      if (err) return cb(err);
      return cb(null, values);
    });

  }, this);
};


/**
 * Find 
 *
 * @param {Object} criteria
 * @param {Function} callback
 * @api public
 */
Collection.prototype.find = function(criteria, cb) {

  // validate cb
  if (!cb || !_.isFunction(cb)) {
    throw new Error("Callback is missing or not a function.");
  }

console.log();
console.log("+++ Collection.find() +++");
console.log(criteria);

  // only request attributes we know about
  var cql = "SELECT ";
  Object.keys(this.ddlHelper.colTypes).forEach(function(colName) {
    cql += colName + ",";
  }, this);
  cql = cql.replace(/,$/, " FROM " + this.ddlHelper.tableName);

  var cqlParams = [];

  var where, skip, limit, sort;;
  if ('where' in criteria) {
    where = criteria.where;
    skip = criteria.skip;
    limit = criteria.limit;
    sort = criteria.sort;
  }
  else {
    where = criteria;
  }
  
  if (!_.isPlainObject(where)) {
    return cb(new Error("criteria 'where' is not an object."));
  }

  // parse criteria
  var arr = this._criteriaToCql(where);
  cql += " WHERE " + arr[0] + ";";
  cqlParams = arr[1];

console.log("+++ CQL +++");
console.log(cql);
console.log("+++ CQL params +++");
console.log(cqlParams);
console.log("+++");


  var result = [];
  var self = this;
  this.client.eachRow(cql, cqlParams, { prepare: true },
    function(n, row) {
      var rs = {};
      Object.keys(self.ddlHelper.colNamesToAttrNames).forEach(function(colName) {
        var value = row[colName];
        if (value instanceof Uuid) {
          value = value.toString();
        }
        else if (value instanceof InetAddress) {
          value = value.toString();
        }
        else if (value instanceof Integer) {
          value = value.toNumber();
        }
        else if (value instanceof BigDecimal) {
          value = value.toNumber();
        }
        else if (value instanceof Long) {
          value = value.toNumber();
        }

        rs[self.ddlHelper.colNamesToAttrNames[colName]] = value;
      });
      result.push(rs);
    },
    function(err, totalCount) {
      if (err) return cb(err);
      cb(null, result);
    }
  );
};


/**
 * Update a record
 *
 * @param {Object} criteria
 * @param {Object} values
 * @param {Function} callback
 * @api public
 */
Collection.prototype.update = function(criteria, values, cb) {

  // validate cb
  if (!cb || !_.isFunction(cb)) {
    throw new Error("Callback is missing or not a function.");
  }

console.log();
console.log("+++ Collection.update() criteria and values +++");
console.log(criteria);
console.log(values);

  // prepare attribute names
  var cql = "UPDATE " + this.ddlHelper.tableName + " SET ";
  var cqlParams = [];
  Object.keys(this.values).forEach(function(attrName) {
    cql += this.sslHelper.attrNamesToColNames[attrName] + " = ?,";
    cqlParams.push(values.colName);
  }, this);
  cql = cql.replace(/,$/, "");

  // prepare where clause
  if (criteria) {
    var arr = this._criteriaToCql(criteria);
    cql += " WHERE " + arr[0];
    cqlParams.concat(arr[1]);
  }

  cql += ";";

console.log("+++ CQL +++");
console.log(cql);
console.log("+++ CQL params +++");
console.log(cqlParams);
console.log("+++");

  // execute CQL
  this.client.execute(cql, cqlParams, { prepare: true }, function(err) {
    if (err) return cb(err);
    cb();
  });
};


/**
 * Drop a record
 *
 * @param {Object} criteria
 * @param {Function} callback
 * @api public
 */
Collection.prototype.drop = function(criteria, cb) {

  // validate cb
  if (!cb || !_.isFunction(cb)) {
    throw new Error("Callback is missing or not a function.");
  }

console.log();
console.log("+++ Collection.drop() criteria +++");
console.log(criteria);

  // prepare cql
  var cql = "DELETE FROM " + this.ddlHelper.tableName;
  var cqlParams = [];

  // prepare where clause
  if (criteria) {
    var arr = this._criteriaToCql(criteria);
    cql += " WHERE " + arr[0];
    cqlParams.concat(arr[1]);
  }

  cql += ";";

console.log("+++ CQL +++");
console.log(cql);
console.log("+++ CQL params +++");
console.log(cqlParams);
console.log("+++");

  // execute CQL
  this.client.execute(cql, cqlParams, { prepare: true }, function(err) {
    if (err) return cb(err);
    cb();
  });

};


/////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS
/////////////////////////////////////////////////////////////////////////////////

Collection.prototype._parseDefinition = function(definition) {

  // derrive table name
  this.ddlHelper.tableName = (definition.tableName ? definition.tableName : definition.identity).toLowerCase();

  // set schema
  this.schema = _.cloneDeep(definition.definition);

console.log();
console.log("+++ schema +++");
console.log(this.schema);

  var uniqColNames = [];
  var indexColNames = [];

  Object.keys(this.schema).forEach(function(attrName) {

    var attribute = this.schema[attrName];

    // map attribute name to column name
    // Note, that database col names are case insensitive, so we convert
    // everything to lower case to have some consistency
    var colName = (attribute.columnName ? attribute.columnName : attrName).toLowerCase();
    this.ddlHelper.attrNamesToColNames[attrName] = colName;
    this.ddlHelper.colNamesToAttrNames[colName] = attrName;

    // resolve sails.js type into cassandra DB type and
    var colType = cqlTypeCast(attribute.type);
    if (attribute.autoIncrement) {
      if (attribute.type !== 'string') {
        console.error("Data type of the primary key is expected to be 'string' when autoincrement is enabled.");
      }
      colType = 'timeuuid';
    }
    this.ddlHelper.attrTypes[attrName] = attribute.type;
    this.ddlHelper.colTypes[colName] = colType;

    // handle default value
    if ('defaultsTo' in attribute) {
      if (attrName == "createdAt" || attrName == "createdAt") {
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
  var cql = "CREATE TABLE " + this.ddlHelper.tableName + " (";
  Object.keys(this.ddlHelper.colTypes).forEach(function(colName) {
    cql += "" + colName + " " + this.ddlHelper.colTypes[colName] + ", ";
  }, this);
  cql += "PRIMARY KEY (" + this.ddlHelper.attrNamesToColNames[this.ddlHelper.pkAttrName] + "));";
  this.ddlHelper.cqlCreateTable = cql;

  // add indices
  _.uniq(uniqColNames.concat(indexColNames)).forEach(function(colName) {
    var indexName = "idx__" + this.ddlHelper.tableName + "__" + colName;
    cql = "CREATE INDEX " + indexName + " ON " + this.ddlHelper.tableName + "(" + colName + ");";
    this.ddlHelper.indexes[indexName] = cql;
  }, this);

console.log("+++");
console.log(this.ddlHelper.cqlCreateTable);
console.log(this.ddlHelper.indexes);
console.log("+++");

};


/**
 *
 * @param {Object} criteria
 * @param {Function} callback
 * @api public
 */
Collection.prototype._criteriaToCql = function(value, key, parentKey) {

  var cql = "";
  var params = [];

  if (!key) {
    if (_.isPlainObject(value)) {
      var a = [];
      Object.keys(value).forEach(function(k) {
        if (k in this.ddlHelper.attrNamesToColNames) {
          var arr = this._criteriaToCql(value[k], k, null);
          a = a.concat(arr[0]);
          params = params.concat(arr[1]);
        }
        else {
          throw new Error();
        }
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
          cql = this.ddlHelper.attrNamesToColNames[parentKey] + " " + key + " ?";
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
  else if (!(key in this.ddlHelper.attrNamesToColNames)) {
    throw new Error("Unknown field '" + key + '"');
  }
  else {
    // pair
    if (_.isString(value) || _.isNumber(value) || _.isDate(value)) {
      cql = this.ddlHelper.attrNamesToColNames[key] + " = ?";
      params = params.concat(value);
    } 
    // in pair
    else if (_.isArray(value)) {
      cql = this.ddlHelper.attrNamesToColNames[key] + " IN (?)";
      params = [ value ];
    }
    // modified pair
    else if (_.isPlainObject(value)) {
      var a = [];
      var ops = {};
      Object.keys(value).forEach(function(k) {
        // normalize operators
        var op;
        switch (k) {
          case 'lessThen':
            op = '<';
            break;
          case 'lessThenOrEqual':
            op = '<=';
            break;
          case 'greaterThen':
            op = '>';
            break;
          case 'greaterThenOrEqual':
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
        var arr = this._criteriaToCql(value[k], op, key);
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
}
