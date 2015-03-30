var Waterline = require('waterline'),
    adapter = require('../../lib/adapter'),
    Users = require('../setup/usersCollection'),
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
        migrate: 'safe'
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
        if (err) return done(err);
        id = result.id;
        done();
      });
    });

    it.skip('should verify the new user', function(done) {
      model.find(id, function(err, user) {
        if (err) return done(err);
        assert(_.isArray(user));
        assert.equal(user.length, 1);
        done();
      });
    });

    it('should destroy the new user', function(done) {
      model.destroy(id, function(err, result) {
        if (err) return done(err);
        done ();
      });
    });


    it.skip('should confirm that user is gone', function(done) {
      done();
    });

  });


  describe('create() and destroy() multiple users', function(done) {

    var users = [];
    var lastName;

    before(function(done) {
      lastName = 'Doe_' + _.random(1000000, 9999999);
      for (var i=0; i<3; i++) {
        users.push({
          firstName: 'Joe_' + i,
          lastName: lastName,
        });
      }
      done();
    });

    it('should create 3 new users', function(done) {
      model.create(users, function(err, result) {
        if (err) return done(err);
        assert(_.isArray(result));
        assert.equal(result.length, 3);
        done();
      });
    });

    it.skip('should verify that users exist', function(done) {
      done();
    });

    it('should destroy users using search criteria', function(done) {
      model.destroy({lastName: lastName}, function(err) {
        if (err) return done(err);
        done();
      });
    });

    it.skip('should verify that users are gone', function(done) {
      done();
    });

  });


  describe('.update()', function() {

    var users = [];

    before(function(done) {
      var u = [{firstName: 'Dima', lastName: 'Two'}];
      for (var i=0; i<3; i++) {
        u.push({
          firstName: 'Joe_' + i,
          lastName: 'Katsman',
        });
      }
      model.create(u, function(err, res) {
        if (err) return done(err);
        users = res;
        done();
      });
    });


    it('update one user by PK', function(done) {
      model.update(users[0].id, {age: 99}, function(err, result) {
        if (err) return done(err);
        assert.equal(users[0].id, result[0].id);
        done();
      });
    });


    it('update multiple users', function(done) {
      model.update({lastName: 'Katsman'}, {age: 44}, function(err, result) {
        if (err) return done(err);
        done();
      });
    });

  });


});
