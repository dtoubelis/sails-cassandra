//var _ = require('lodash');

// map of modelDataType -> cassandraDataType
exports.cqlTypeCast = function(waterlineDataType) {

  var type = waterlineDataType && waterlineDataType.toLowerCase();

  switch(type) {

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
