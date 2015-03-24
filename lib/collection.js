var _ = require('lodash');
var cqlTypeCast = require('./util').cqlTypeCast;
//var async = require('async');

var Collection = module.exports = function(definition, cl) {

  this.client = cl;

  this.tableName = null;

  this.schema = null;

  this.ddlHelper = {
    pkAttrName: null,
    pkAutoIncrement: false,
    colNames: {},
    dataTypes: {},
    defaultValues: {},
    unique: []
  };

  this._parseDefinition(definition);
};


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

  this.client.execute(this.ddlHelper.cqlCreateTable, cb);
}


/**
 * Insert a new record
 *
 * @param {Object} values
 * @param {Function} callback
 * @api public
 */
Collection.prototype.insert = function(values, cb) {

  var self = this;

  // validate params
  if (!_.isPlainObject(values)) {
    throw new Error("Param is not an object.");
  }
  if (!cb || !_.isFunction(cb)) {
    throw new Error("Callback is missing or not a function.");
  }

  // make sure no references here (TODO: douleckeck if this is necessary)
  values = _.cloneDeep(values);

  // validate primary key
  if (this.ddlHelper.pkAttrName in values) {
    if (this.ddlHelper.pkAutoIncrement) {
      return cb(new Error("Overriding autoincrement fields is not allowed."));
    }
  }
  else {
    if (this.ddlHelper.pkAutoIncrement) {
      values[this.ddlHelper.pkAttrName] = null;
    }
    else {
      return cb(new Error("Missing primary key in the insert request."));
    }
  }

  // construct insert statement and param list
  var cqlInsert = "INSERT INTO " + this.tableName + " (";
  var cqlValues = "VALUES (";
  var cqlParams = [];
  Object.keys(values).forEach(function(attrName) {
    cqlInsert += self.ddlHelper.colNames[attrName] + ",";
    if (attrName == self.ddlHelper.pkAttrName && self.ddlHelper.pkAutoIncrement) {
      cqlValues += "now(),"; 
    }
    else {
      cqlValues += "?,";
      cqlParams.push(_.clone(values[attrName]));
    }
  });

  // execute INSERT on the server
  var cql = cqlInsert.replace(/,$/, ') ') + cqlValues.replace(/,$/, ");");
  this.client.execute(cql, cqlParams, {prepare: true}, function(_err) {
    if (_err) {
      return cb(err);
    }
    return cb(null, values);
  });
};


/////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS
/////////////////////////////////////////////////////////////////////////////////

Collection.prototype._parseDefinition = function(definition) {

  var self = this;

  // derrive table name
  this.tableName = definition.tableName ? _.clone(definition.tableName) : definition.identity.toLowerCase();

  // set schema
  this.schema = _.cloneDeep(definition.definition);

  Object.keys(this.schema).forEach(function(attributeName) {

    var attribute = self.schema[attributeName];

    // map attribute name to column name
    var colName = attribute.columnName ? _.clone(attribute.columnName) : attributeName;
    self.ddlHelper.colNames[attributeName] = colName;

    // resolve sails.js type into cassandra DB type
    var colType = cqlTypeCast(attribute.type);
    if (attribute.autoIncrement) {
      colType = 'timeuuid';
    }
    self.ddlHelper.dataTypes[colName] = colType;

    // handle default value
    if ('defaultsTo' in attribute) {
      if (attributeName == "createdAt" || attributeName == "createdAt") {
        console.log("Default value for attribute '%s' is not supported.");
      }
      else {
        self.ddlHelper.defaultValues[colName] = attribute.defaultsTo;
      }
    }

    // handle primary key and unique
    if (attribute.primaryKey) {
      if (self.ddlHelper.pkAttrName) {
        throw new Error("Duplicate primary key definition.");
      }
      else {
        self.ddlHelper.pkAttrName = attributeName;
      }
    }
    else if (attribute.unique) {
      self.ddlHelper.unique.push(attributeName);
    }

    // handle autoincrement flag
    if (attribute.autoIncrement) {
      if (!attribute.primaryKey) {
        throw new Error("Autoincrement attribute on non primary key.");
      }
      else {
        self.ddlHelper.pkAutoIncrement = true;
      }
    }

  });

  // create table SQL
  var cqlCreateTable = "CREATE TABLE " + this.tableName + " (";
  Object.keys(this.ddlHelper.dataTypes).forEach(function(colName) {
    cqlCreateTable += "" + colName + " " + self.ddlHelper.dataTypes[colName] + ", ";
  });
  cqlCreateTable += "PRIMARY KEY (" + this.ddlHelper.colNames[this.ddlHelper.pkAttrName] + ")";
  cqlCreateTable += ");";
  this.ddlHelper.cqlCreateTable = cqlCreateTable;
};
