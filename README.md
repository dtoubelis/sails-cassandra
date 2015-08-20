[![Build Status][buildImg]][buildURL] [![Dependency Status][depImg]][depURL] [![npmImg][npmImg]][npmURL]

[buildImg]: https://travis-ci.org/dtoubelis/sails-cassandra.svg?branch=master
[buildURL]: https://travis-ci.org/dtoubelis/sails-cassandra

[depImg]: https://gemnasium.com/dtoubelis/sails-cassandra.svg
[depURL]: https://gemnasium.com/dtoubelis/sails-cassandra

[npmImg]: https://badge.fury.io/js/sails-cassandra.svg
[npmURL]: http://badge.fury.io/js/sails-cassandra


# sails-cassandra
Apache Cassanda 2.+ database adapter for Sails/Waterline

> Implements:
> - [Semantic](https://github.com/balderdashy/sails-docs/blob/master/contributing/adapter-specification.md#semantic-interface)
>   - .create()
>   - .createEach()
>   - .find()
>   - .count()
>   - .update()
>   - .destroy()
> - [Migratable](https://github.com/balderdashy/sails-docs/blob/master/contributing/adapter-specification.md#migratable-interface)
>   - .define()
>   - .describe()
>   - .drop()
> - [Iterable](https://github.com/balderdashy/sails-docs/blob/master/contributing/adapter-specification.md#iterable-interface)
>   - .stream()


## 1. Installation
Install from NPM.

```bash
# In your app:
$ npm install sails-cassandra
```

## 2. Configuring Sails
Add the `cassandra` configuration to the `config/connections.js` file. The basic
options are as follows:

```javascript
module.exports.connections = {

  my-cassandra-connection: {

    module        : 'sails-cassandra',

    // typical sails/waterline options (see comment below)
    user          : 'username',
    password      : 'password',

    // cassandra driver options
    contactPoints : [ '127.0.0.1' ],
    keyspace      : 'keyspace name',
    ...
  }
};
```

And then change default model configuration in the `config/models.js`:

```javascript
module.exports.models = {
  connection: 'my-cassandra-connection'
};
```

Adapter configuration may contain any of [Cassandra client options].
However, you probably will only need `contactPoints` and `keyspace` to get
started and the adapter will provide reasonable defaults for the rest.

[Cassandra client options]: http://www.datastax.com/documentation/developer/nodejs-driver/2.0/common/drivers/reference/clientOptions.html

Authentication information for `cassandra-driver` is typically supplied in
`authProvider` option, however `sails-cassandra` adapter will also recognize
`user` and `password` options and convert them into `authProvider` overriding
an existing value. This also means that if you wish to use your own `authProvider`
you will need to remove `user` and `password` from the configuration.  


## 3. Running Tests
You can set environment variables to override the default database configuration
for running tests as follows:

```sh
$ WATERLINE_ADAPTER_TESTS_PASSWORD=yourpass npm test
```


Default settings are:

```javascript
{
  contactPoints: [ process.env.WATERLINE_ADAPTER_TESTS_HOST || '127.0.0.1' ],
  user: process.env.WATERLINE_ADAPTER_TESTS_USER || 'root',
  password: process.env.WATERLINE_ADAPTER_TESTS_PASSWORD || '',
  keyspace: process.env.WATERLINE_ADAPTER_TESTS_DATABASE || 'test'
}
```

> **Note:** Default name of the keyspace for running tests is `test`. Make
> sure you created it in your database before executing `npm test`.


## 4. Implementation Notes
This section describes behaviour of Apache Cassandra adapter that is distinct
from other database types.


### 4.1. Naming of tables and columns
Column and table names in Cassandra are case insensitive and this ambiguity
makes it difficult to map between attribute names that are case sensitive and
column names that are not. There are two possible workarounds for this:

1. There is a mechanism in Apache Cassandra to make table/column names case
   sensitive by including them in double quotes. This may seem as a good idea on
   the surface but it does not cover a use case when tables are not created by
   sails/waterline but by an external process.
 
2. Converting table/column names to lower case is an another approach. This way
   we always convert table/column names to lower case before mapping them
   to/from attribute names. This is not very elegant but it works and this is
   the current preferred approach.


### 4.2. Autoincrement
The autoincrement feature was plaguing ORM frameworks right from their inseption
as it requires 1-2 extra queries in order to retrieve new record identifier from
underlying database into the framework. It also does not work very well with
sharding and replication.

Cassandra database does not support autoincrement, however it achieves the same
functionality in a much more efficient way by using time based UUIDs (a.k.a.
Type 1 UUID) for primary keys.

Sails/Waterline supports autoincrement and its implementation is heavily
influenced by MySQL database. The `sails-cassandra` adapter makes an attempt to
achieve the same functionality using the following rules:

1. Model attribute that represents primary key may have `autoIncrement`
   property set to `true`.

2. This automatically forces attribute type to `string` and supersedes any other
   declarations. The adapter will give a warning message is there is a
   discrepancy.

3. The value of the primary key cannot be overridden by `create()` or `update()`
   calls once `autoIncrement` property is enabled. You will see a (non-lethal)
   warning message if such attempt is made.

> **Note**: This logic is inconsistent with the current Sails/Waterline
> specifications as it requires `autoIncrement` field to be of type `integer`.
> Please use discretion. Also, see [this issue].

[this issue]: https://github.com/balderdashy/waterline/issues/909


### 4.3. Type conversion between Cassandra and Sails/Waterline
The following table represents mappings between Sails/Waterline model data types
and Apache Cassandra data types:

| Sails/Waterline Type | JS Type  | Cassandra Type                    |
|:---------------------|:---------|:----------------------------------|
| string               | String   | text (UTF-8 text)                 |
| text                 | String   | text (UTF-8 text)                 |
| integer              | Number   | bigint (64-bit signed integer)    |
| float                | Number   | double (64-bit float)             |
| date                 | Date     | timestamp                         |
| datetime             | Date     | timestamp                         |
| boolean              | Boolean  | boolean                           |
| binary               | [Buffer] | blob                              |
| array                | Array    | list<text>                        |
| json                 | ???      | text (UTF-8 text)                 |
| email                | String   | ascii (US-ASCII character string) |
| autoIncrement=true   | String   | timeuuid                          |


The following table may be used as a guideline when creating Sails/Waterline
models for existing tables:

| Cassandra Type | Type Id | Driver JS type | Waterline JS Type | Waterline Type       |
|:---------------|:-------:|:---------------|:------------------|:---------------------|
| ascii          | 1       | String         | String            | string               |
| bigint         | 2       | [Long]         | Number or NaN     | integer              |
| blob           | 3       | [Buffer]       | [Buffer]          | binary               |
| boolean        | 4       | Boolean        | Boolean           | boolean              |
| counter        | 5       | [Long]         | Number or NaN     | integer              |
| decimal        | 6       | [BigDecimal]   | Number or NaN     | float                |
| double         | 7       | Number         | Number            | float                |
| float          | 8       | Number         | Number            | float                |
| inet           | 16      | [InetAddress]  | String            | string               |
| int            | 9       | Number         | Number            | integer              |
| list           | 32      | Array          | Array             | array                |
| map            | 33      | Object         | Null              | not supported (null) |
| set            | 34      | Array          | Null              | not supported (null) |
| text           | 10      | String         | String            | text                 |
| timestamp      | 11      | Date           | Date              | datetime or date     |
| timeuuid       | 15      | [TimeUuid]     | String            | string               |
| uuid           | 12      | [Uuid]         | String            | string               |
| varchar        | 13      | String         | String            | text                 |
| varint         | 14      | [Integer]      | Number or NaN     | integer              |

[Buffer]: https://nodejs.org/api/buffer.html

[Long]: http://www.datastax.com/drivers/nodejs/2.0/module-types-Long.html

[BigDecimal]: http://www.datastax.com/drivers/nodejs/2.0/module-types-BigDecimal.html

[InetAddress]: http://www.datastax.com/drivers/nodejs/2.0/module-types-InetAddress.html

[TimeUuid]: http://www.datastax.com/drivers/nodejs/2.0/module-types-TimeUuid.html

[Uuid]: http://www.datastax.com/drivers/nodejs/2.0/module-types-Uuid.html

[Integer]: http://www.datastax.com/drivers/nodejs/2.0/module-types-Integer.html


### 4.4. Use of indexes
Apache Cassandra require index on a column that is used in `where` clause of
`select` statement and unlike other database it will produce and exception if
the index is missing.

Sails/Waterline allows to set `index` or `unique` properties on model
attributes. The `sails-cassanda` adapter will respect these attributes and it
will create indexes for attributes with `index` or `unique` attributes set to
`true`.

> **Note**: that Apache Cassandra have no notion of `unique` constraint and
> the uniqueness has to be enforced either by Sails/Waterline core or in your
> own code. The `unique` attribute property is considered an alias for `index`
> and both are treated in the exactly same way.

### 4.5. Search criteria
Apache Cassandra only supports subset of operation in selection criteria in
comparison to relational databases and this section describes what is currently
supported.


#### 4.5.1. Key Pairs
This is an exact match criteria and it is declared as follows:

```javascript
Model.find({firstName: 'Joe', lastName: 'Doe'});
```

It is supported and it will be executed as follows:

```
SELECT id, first_name, last_name
  FROM users
  WHERE first_name = 'Joe' AND last_name = 'Doe'
  ALLOW FILTERING;
```
Please also refer to [Use of Indexes](#44-use-of-indexes) above.


#### 4.5.2. Modified Pair
This criteria:

```javascript
Model.find({age: {'>': 18, 'lessThanOrEqual': 65});
```

will be converted to CQL query that may look like this:

```
SELECT id,first_name,last_name
  FROM users
  WHERE age > 18 AND age <= 65
  ALLOW FILTERING;
```

and supported operations are as follows:

| Operation              | Shorthand | Supported |
|:-----------------------|:---------:|:---------:|
| `'lessThan'`           |  `'<'`    |    Yes    |
| `'lessThanOrEqual'`    |  `'<='`   |    Yes    |
| `'greaterThan'`        |  `'>'`    |    Yes    |
| `'greaterThanOrEqual'` |  `'>='`   |    Yes    |
| `'not'`                |  `'!'`    |  **No**   |
| `'like'`               |  `none`   |  **No**   |
| `'contains'`           |  `none`   |  **No**   |
| `'startsWith'`         |  `none`   |  **No**   |
| `'endsWith'`           |  `none`   |  **No**   |

    
#### 4.5.3. In Pairs
This criteria:

```javascript
Model.find({title: ['Mr', 'Mrs']});
```

will be rendered into the following CQL statement:

```
SELECT id, first_name, last_name
  FROM users
  WHERE title IN ( 'Mr', 'Mrs' )
  ALLOW FILTERING;
```
> **Note:** that `IN` criterion works differently in Apache Cassandra. It is
> subject of [certain limitations] and is considered a pattern to be avoided.

[certain limitations]: http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/select_r.html?scroll=reference_ds_d35_v2q_xj__selectIN


#### 4.5.4. Not-In Pair
**Not supported** since Apache Cassandra does not support `NOT IN` criterion,
so this construct:

```javascript
Model.find({name: {'!': ['Walter', 'Skyler']}});
```

will cause adapter to throw an exception.


#### 4.5.5. Or Pairs
**Not supported** since Apache Cassandra has no `OR` criterion, so this construct:

```javascript
Model.find({
  or : [
    {name: 'walter'},
    {occupation: 'teacher'}
  ]
});
```

will cause the adapter to throw an exception.

#### 4.5.6. Limit, Sort, Skip
Only `limit` is curently implemented and works as expected. `sort` and `skip` are
not supported and silently ignored if provided.


## 5. License
See [LICENSE.md](./LICENSE.md) file for details.

