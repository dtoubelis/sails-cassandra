/**
 * Dependencies
 */

var Waterline = require('waterline');

module.exports = Waterline.Collection.extend({

  identity: 'users',
  tableName: 'userTable',
  connection: 'semantic',
  autoPK: true,
  autoCreatedAt: false,

  attributes: {
    id: {
      type: 'string',
      primaryKey: true,
      autoIncrement: true
    },
    firstName: {
      type: 'string',
      columnName: 'first_name',
      index: true
    },
    lastName: {
      type: 'string',
      columnName: 'last_name',
      index: true
    },
    email: {
      type: 'string',
      columnName: 'email_address',
      index: true
    },
    avatar: {
      type: 'binary'
    },
    title: 'string',
    phone: 'string',
    type: 'string',
    favoriteFruit: {
      type: 'string',
      defaultsTo: 'cherry'
    },
    age: { 
      type: 'integer',
      index: true
    },
    dob: 'datetime',
    status: {
      type: 'boolean',
      defaultsTo: false
    },
    percent: 'float',
    list: {
      type: 'array',
      columnName: 'arrList'
    },
    obj: 'json',
    fullName: function() {
      return this.first_name + ' ' + this.last_name;
    }
  }
});
