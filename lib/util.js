var types = require('cassandra-driver').types,
  Uuid = types.Uuid,
  TimeUuid = types.TimeUuid,
  Integer = types.Integer,
  BigDecimal = types.BigDecimal,
  InetAddress = types.InetAddress,
  Long = require('long');


/**
 * Cast value from Cassandra data type to Waterline type
 *
 * @param value
 * @returns {*}
 */
exports.castFromCassandraToWaterline = function(value) {
  var cast = function (value) {
    var ret;
    if (value instanceof Uuid) {
      ret = value.toString();
    }
    else if (value instanceof InetAddress) {
      ret = value.toString();
    }
    else if (value instanceof Integer) {
      ret = value.toNumber();
    }
    else if (value instanceof BigDecimal) {
      ret = value.toNumber();
    }
    else if (value instanceof Long) {
      ret = value.toNumber();
    }
    else {
      ret = value;
    }
    return ret;
  };
  if (value instanceof Array) {
    return value.map(cast);
  } else {
    return cast(value);
  }
};


/**
 * Map Waterline data type string to Cassandra data type string.
 *
 * @param waterlineDataType
 * @returns {String} cassandra data type
 * @private
 */
exports.mapWaterlineTypeToCassandra = function(waterlineDataType) {

  switch(waterlineDataType) {

    case 'string':
    case 'text':
    case 'json':
      return 'text';

    case 'email':
      return 'ascii';

    case 'integer':
      return 'bigint';

    case 'float':
      return 'double';

    case 'boolean':
      return 'boolean';

    case 'date':
    case 'datetime':
      return 'timestamp';

    case 'binary':
      return 'blob';

    case 'array':
      return 'list<text>';

    default:
      console.error("Unregistered type '%s'. Treating as 'text'.", type);
      return 'text';
  }

};
