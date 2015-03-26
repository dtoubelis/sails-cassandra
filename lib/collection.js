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

  this.client = client;

  this.tableName = null;

  this.schema = null;

  this.ddlHelper = {
    pkAttrName: null,
    pkAutoIncrement: false,
    attrNamesToColNames: {},
    attrTypes: {},
    colNamesToAttrNames: {},
    colTypes: {},
    defaultValues: {},
    uniqueColNames: [],
    indexColNames: []
  };

  this._parseDefinition(definition);
};


/////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS
/////////////////////////////////////////////////////////////////////////////////


/**
 * Create table
 *
 * @param {Function} callback
 * @api public
 */
Collection.prototype.create = function(cb) {

  // validate params
  if (!cb || !_.isFunction(cb)) {
    throw new Error("Callback is missing or not a function.");
  }

  this.client.batch(this.ddlHelper.cqlCreateTable, function(err) {
    if (err) return cb(err);
    return cb();
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
    var cqlInsert = "INSERT INTO " + this.tableName + " (";
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
  console.log("+++");
  console.log(criteria);
  console.log("+++");

  // only request attributes we know about
  var cql = "SELECT ";
  Object.keys(this.ddlHelper.colTypes).forEach(function(colName) {
    cql += colName + ",";
  }, this);
  cql = cql.replace(/,$/, " FROM " + this.tableName);

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

  console.log("Find CQL: '%s'", cql);
  console.log(cqlParams);

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
      console.log(">>>");
      console.log(result);
      console.log(">>>");
      cb(null, result);
    }
  );
};


/////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS
/////////////////////////////////////////////////////////////////////////////////

Collection.prototype._parseDefinition = function(definition) {

  // derrive table name
  this.tableName = (definition.tableName ? _.clone(definition.tableName) : definition.identity).toLowerCase();

  // set schema
  this.schema = _.cloneDeep(definition.definition);

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
      this.ddlHelper.uniqueColNames.push(colName);
    }

    // handle index
    if (attribute.index) {
      this.ddlHelper.indexColNames.push(colName);
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
  var cql = "CREATE TABLE " + this.tableName + " (";
  Object.keys(this.ddlHelper.colTypes).forEach(function(colName) {
    cql += "" + colName + " " + this.ddlHelper.colTypes[colName] + ", ";
  }, this);
  cql += "PRIMARY KEY (" + this.ddlHelper.attrNamesToColNames[this.ddlHelper.pkAttrName] + "));";
  this.ddlHelper.cqlCreateTable = [ cql ];

  // add indices
  var indices = _.uniq(this.ddlHelper.uniqueColNames.concat(this.ddlHelper.indexColNames));
  indices.forEach(function(colName) {
    cql = "CREATE INDEX " + this.tableName + "_" + colName + " ON " + this.tableName + "(" + colName + ");";
    this.ddlHelper.cqlCreateTable.push(cql);
  }, this);

  console.log(this.ddlHelper.cqlCreateTable);

};

////////////////////////////////////////////////////////
// Functions
////////////////////////////////////////////////////////

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
