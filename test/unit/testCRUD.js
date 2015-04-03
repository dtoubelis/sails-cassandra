var adapter = require('../../lib/adapter'),
    Waterline = require('waterline'),
    Users = require('../setup/collectionUsers'),
    assert = require('assert'),
    _ = require('lodash');

var connectionName = 'semantic';
var collectionName = 'testUsers';

describe('Collection', function() {

  var model;

  before(function(done) {

    var waterline = new Waterline();

    var config = {

      adapters: {
        cassandra: adapter
      },

      connections: {
        semantic: {
          adapter: 'cassandra',
          contactPoints: [ process.env.WATERLINE_ADAPTER_TESTS_HOST || '127.0.0.1' ],
          user: process.env.WATERLINE_ADAPTER_TESTS_USER || 'root',
          password: process.env.WATERLINE_ADAPTER_TESTS_PASSWORD || '',
          keyspace: process.env.WATERLINE_ADAPTER_TESTS_DATABASE || 'test',
        }
      },

      defaults: {
        migrate: 'drop'
      }

    };

    waterline.loadCollection(Users);

    waterline.initialize(config, function(err, models) {
      if(err) return done(err);
      model = models.collections.users;
      done();
    });
  });


  describe('create() and destroy() a single user', function() {

    var id;

    it('should create a new user', function(done) {
      var userNew = {
        firstName: 'Joe',
        lastName: 'Doe',
        dob: new Date('1900-01-15 EST')
      };
      model.create(userNew, function(err, result) {
        assert.ifError(err);
        id = result.id;
        done();
      });
    });

    it('should verify the new user', function(done) {
      model.find(id, function(err, user) {
        assert.ifError(err);
        assert(_.isArray(user));
        assert.equal(user.length, 1);
        done();
      });
    });

    it('should return user count of 1', function(done) {
      model.count(id, function(err, userCount) {
        assert.ifError(err);
        assert.equal(userCount, 1);
        done();
      });
    });

    it('should destroy the new user', function(done) {
      model.destroy(id, function(err, result) {
        assert.ifError(err);
        done ();
      });
    });


    it('should confirm that user is gone', function(done) {
      model.find(id, function(err, user) {
        assert.ifError(err);
        assert(_.isArray(user));
        assert.equal(user.length, 0);
        done();
      });
    });

  });


  describe('create() and destroy() multiple users', function(done) {

    var users = [];
    var lastName;
    var emailAddress;

    before(function(done) {
      lastName = 'Doe_' + _.random(1000000, 9999999);
      emailAddress = 'joe@' + lastName.toLowerCase() + '.com';
      for (var i=0; i<3; i++) {
        users.push({
          firstName: 'Joe_' + i,
          lastName: lastName,
          emailAddress: emailAddress
        });
      }
      done();
    });

    it('should create 3 new users', function(done) {
      model.create(users, function(err, result) {
        assert.ifError(err);
        assert(_.isArray(result));
        assert.equal(result.length, 3);
        done();
      });
    });

    it('should verify that users exist', function(done) {
      model.find({lastName: lastName}, function(err, users) {
        assert.ifError(err);
        assert(_.isArray(users));
        assert.equal(users.length, 3);
        done();
      });
    });

    it('should return user count of 0', function(done) {
      model.count({lastName: 'Nemo'}, function(err, userCount) {
        assert.ifError(err);
        assert.equal(userCount, 0);
        done();
      });
    });

    it('should return user count of 3', function(done) {
      model.count({lastName: lastName}, function(err, userCount) {
        assert.ifError(err);
        assert.equal(userCount, 3);
        done();
      });
    });

    it('should destroy users using search criteria', function(done) {
      model.destroy({lastName: lastName, emailAddress: emailAddress}, function(err) {
        assert.ifError(err);
        done();
      });
    });

    it('should verify that users are gone', function(done) {
      model.find({lastName: lastName}, function(err, users) {
        assert.ifError(err);
        assert(_.isArray(users));
        assert.equal(users.length, 0);
        done();
      });
    });

  });


  describe('.update()', function() {

    var users = [];
    var lastName;

    before(function(done) {
      lastName = 'Carter' + _.random(1000000, 9999999);
      users.push({firstName: 'John', lastName: lastName});
      for (var i=0; i<3; i++) {
        users.push({
          firstName: 'John_' + i,
          lastName: lastName,
        });
      }
      model.create(users, function(err, result) {
        if (err) return done(err);
        users = result;
        done();
      });
    });


    it('update one user by PK', function(done) {
      model.update(users[0].id, {age: 99}, function(err, result) {
        assert.ifError(err);
        assert(_.isArray(result));
        assert.equal(result.length, 1);
        assert.equal(result[0].id, users[0].id);
        assert.equal(result[0].age, 99);
        done();
      });
    });


    it('update multiple users', function(done) {
      var i;
      model.update({lastName: lastName}, {age: 44}, function(err, result) {
        assert.ifError(err);
        assert(_.isArray(result));
        assert.equal(result.length, users.length);
        for (i=0; i<result.length; i++) {
          assert.equal(result[i].age, 44);
        }
        done();
      });
    });


    after(function(done) {
      model.destroy({lastName: lastName}, done);
    });

  });


});
