var Collection = require('../../lib/collection'),
    assert = require('assert'),
    async = require('async');

describe('Query type', function() {

  var collection = new Collection({
      identity: 'user_table',
      definition: {
        id: {
          type: 'string',
          autoIncrement: true,
          primaryKey: true,
        },
        title: { type: 'string', index: true },
        firstName: { type: 'string', columnName: 'firstname', index: true },
        lastName: { type: 'string', index: true },
        age: { type: 'integer', index: true },
        emailAddress: { type: 'string', columnName: 'email_address' },
        createdAt: { type: 'datetime', columnName: 'created_ts' },
        updatedAt: { type: 'datetime', columnName: 'updated_ts' }
      }
    }, null);

  describe('"pair"', function() {
    var query = {firstName: 'Joe'};
    var cql = '"firstname" = ?';
    var params = [ 'Joe' ];
    it("should return valid CQL", function() {
      var arr = collection._parseCriteria(query);
      assert.equal(arr[0], cql);
      assert.equal(arr[1][0], params[0]);
    });
  });


  describe('"multi-pair"', function() {
    var query = {firstName: 'Joe',lastName: 'Doe'};
    var cql = '"firstname" = ? AND "lastname" = ?';
    var params = [ 'Joe', 'Doe' ];
    it ("should return valid CQL", function() {
      var arr = collection._parseCriteria(query);
      assert.equal(arr[0], cql);
      assert.equal(arr[1][0], params[0]);
      assert.equal(arr[1][1], params[1]);
    });
  });


  describe('"in"', function() {
    var query = {firstName: [ 'Joe', 'Peter', 'Greg' ]};
    var cql = '"firstname" IN (?)';
    var params = [ ['Joe', 'Peter', 'Greg'] ];
    it ("should return valid CQL", function() {
      var arr = collection._parseCriteria(query);
      assert.equal(arr[0], cql);
      assert.equal(arr[1][0][0], params[0][0]);
      assert.equal(arr[1][0][1], params[0][1]);
      assert.equal(arr[1][0][2], params[0][2]);
    });
  });


  describe('"modified pair"', function() {
    var query = {firstName: {'<': 'Joe'}};
    var cql = '"firstname" < ?';
    var params = [ 'Joe' ];
    it ("should return valid CQL", function() {
      var arr = collection._parseCriteria(query);
      assert.equal(arr[0], cql);
      assert.equal(arr[1][0], params[0]);
    });
  });


  describe('"modified multi-pair"', function() {
    var query = {age: {'>=': 40, 'lessThan': 50}};
    var cql = '"age" >= ? AND "age" < ?';
    var params = [ 40, 50 ];
    it ("should return valid CQL", function() {
      var arr = collection._parseCriteria(query);
      assert.equal(arr[0], cql);
      assert.equal(arr[1][0], params[0]);
      assert.equal(arr[1][1], params[1]);
    });
  });


  describe('"mixed"', function() {
    var query = {
      title: [ 'Mr', 'Mrs' ],
      lastName: 'Doe',
      age: {'greaterThanOrEqual': 25, '<': 50}
    };
    var cql = '"title" IN (?) AND "lastname" = ? AND "age" >= ? AND "age" < ?';
    var params = [ [ 'Mr', 'Mrs' ], 'Doe', 25, 50 ];
    it ("should return valid CQL", function() {
      var arr = collection._parseCriteria(query);
      assert.equal(arr[0], cql);
      assert.equal(arr[1][0][0], params[0][0]);
      assert.equal(arr[1][0][1], params[0][1]);
      assert.equal(arr[1][1], params[1]);
      assert.equal(arr[1][2], params[2]);
      assert.equal(arr[1][3], params[3]);
    });
  });

});

