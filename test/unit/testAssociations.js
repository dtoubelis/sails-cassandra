// This use case provided by zaheershaik

var _ = require('lodash'),
    async = require("async"),
    assert = require('assert'),
    adapter = require('../../lib/adapter'),
    Waterline = require('waterline');

var connectionName = 'associations';

var ProductPrice = Waterline.Collection.extend({
  identity: 'productPrice',
  connection: connectionName,
  autoPK: true,
  autoCreatedAt: false,
  autoUpdatedAt: false,
  attributes: {
    id: {type: 'string', primaryKey: true, autoIncrement: true},
    price: {type: 'float', required: true, defaultsTo: 0.0, index: true}
  }
});

var ProductInventory = Waterline.Collection.extend({
  identity: 'productInventory',
  connection: connectionName,
  autoPK: true,
  autoCreatedAt: false,
  autoUpdatedAt: false,
  attributes: {
    id: {type: 'string', primaryKey: true, autoIncrement: true},
    availableQty: {type: 'integer', required: true, defaultsTo: 0, index: true}
  }
});

var Product = Waterline.Collection.extend({
  identity: 'product',
  connection: connectionName,
  autoPK: true,
  autoCreatedAt: false,
  autoUpdatedAt: false,
  attributes: {
    id: {type: 'string', primaryKey: true, autoIncrement: true},
    name: {type: 'string', required: true},
    price: {model: 'productPrice'},
    inventory: {model: 'productInventory'},
    categories: {collection: 'productByCategory', via: 'product'}
  }
});

var Category = Waterline.Collection.extend({
  identity: 'category',
  connection: connectionName,
  autoPK: true,
  autoCreatedAt: false,
  autoUpdatedAt: false,
  attributes: {
    id: {type: 'string', primaryKey: true, autoIncrement: true},
    name: {type: 'string', required: 'true'}
  }
});

var ProductByCategory = Waterline.Collection.extend({
  identity: 'productByCategory',
  connection: connectionName,
  autoPK: true,
  autoCreatedAt: false,
  autoUpdatedAt: false,
  attributes: {
    id: {type: 'string', primaryKey: true, autoIncrement: true},
    product: {model: 'product', index: true},
    category: {model: 'category', index: true}  
  }
});


describe('Associations', function() {

  var models;
  var products = [];

  before(function(done) {

    this.timeout(10000);

    var waterline = new Waterline();

    var config = {
      adapters: {
        cassandra: adapter
      },
      connections: {
      },
      defaults: {
        migrate: 'drop'
      }
    };

    config.connections[connectionName] = {
      adapter: 'cassandra',
      contactPoints: [ process.env.WATERLINE_ADAPTER_TESTS_HOST || '127.0.0.1' ],
      user: process.env.WATERLINE_ADAPTER_TESTS_USER || 'root',
      password: process.env.WATERLINE_ADAPTER_TESTS_PASSWORD || '',
      keyspace: process.env.WATERLINE_ADAPTER_TESTS_DATABASE || 'test'
    };

    waterline.loadCollection(Product);
    waterline.loadCollection(ProductPrice);
    waterline.loadCollection(ProductInventory);
    waterline.loadCollection(Category);
    waterline.loadCollection(ProductByCategory);

    waterline.initialize(config, function(err, model) {
      if(err) return done(err);
      models = model.collections;
      done();
    });
  });


  describe('.create()', function() {

    var categories;

    it('should create a product', function(done) {

      var price;
      var inventory;

      async.series([
          function(cb) {
            models.productprice.create({price: 99.99}, function(err, result) {
              if (err) return cb(err);
              price = result;
              cb();
            });
          },
          function(cb) {
            models.productinventory.create({inventory: 1}, function(err, result) {
              if (err) return cb(err);
              inventory = result;
              cb();
            });
          },
          function(cb) {
            models.product.create({name:"Awesome Rig",inventory:inventory,price:price}, function(err, result) {
              if (err) return cb(err);
              products = products.concat(result);
              cb();
            });
          },
        ],
        function(err, results) {
          if (err) return done(err);
          done();
        }
      );
    });

    it('should create another product', function(done) {
      models.product.create({name:"Jelly Goo",inventory:{inventory:1},price:{price:19.95}}, function(err, result) {
        if (err) return done(err);
        products = products.concat(result);
        done();
      });
    });

    it('should create categories', function(done) {
      models.category.create([
          {name:"Category 1"},
          {name:"Category 2"}
        ],
        function(err, result) {
          if (err) return done(err);
          assert(_.isArray(result));
          categories = result;
          done();
        }
      );
    });

    it('should create productsByCategories', function(done) {
      var productByCategory = [];
      products.forEach(function(product) {
        categories.forEach(function(category) {
          productByCategory.push({product:product,category:category});
        });
      });
      models.productbycategory.create(productByCategory, function(err, result) {
        if (err) return done(err);
        done();
      });
    });

  });


  describe('.update()', function() {

    it('should update product name', function(done) {
      models.product.update(products[0].id, {name:"Jiffy Glue"}, function(err, result) {
        if (err) return done(err);
        done();
      });
    });

    it.skip('should update product price', function(done) {
      models.product.update(products[0].id, {price: {price: 0.99}}, function(err, result) {
        if (err) return done(err);
        done();
      });
    });

  });

});
