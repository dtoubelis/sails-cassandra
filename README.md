# sails-cassandra
Apache Cassanda database adapter for Sails/Waterline


## Installation
Install from NPM.

```bash
# In your app:
$ npm install sails-cassandra
```

## Configuring Sail
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


## Running Tests
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

## Implementation Notes
This section describes behaviour of Apache Cassandra adapter distinct from other
database types.


### Naming of tables and columns
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


### Use of indexes
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


### Autoincrement
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
> Please use this on your own discretion. 


### Type conversion between Cassandra and Sails/Waterline
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
| binary               | ?        | blob                              |
| array                | Array    | list<text>                        |
| json                 | Object(?)| text (UTF-8 text)                 |
| email                | String   | ascii (US-ASCII character string) |
| autoIncrement=true   | String   | timeuuid                          |


The following table may be used as a guideline when creating Sails/Waterline
models for existing tables:

| Cassandra Type | Type Id | Driver JS type | Sails/Waterline Type |
|:---------------|:-------:|:---------------|:---------------------|
| ascii          | 1       | String         | string               |
| bigint         | 2       | [Long]         | integer              |
| blob           | 3       | Buffer         | binary               |
| boolean        | 4       | Boolean        | boolean              |
| counter        | 5       | [Long]         | integer              |
| decimal        | 6       | [BigDecimal]   | float                |
| double         | 7       | Number         | float                |
| float          | 8       | Number         | float                |
| inet           | 16      | [InetAddress]  | string               |
| int            | 9       | Number         | integer              |
| list           | 32      | Array          | array                |
| map            | 33      | Object         | not supported (null) |
| set            | 34      | Array          | not supported (null) |
| text           | 10      | String         | text                 |
| timestamp      | 11      | Date           | datetime or date     |
| timeuuid       | 15      | [TimeUuid]     | string               |
| uuid           | 12      | [Uuid]         | string               |
| varchar        | 13      | String         | text                 |
| varint         | 14      | [Integer]      | integer              |

[Long]: http://www.datastax.com/drivers/nodejs/2.0/module-types-Long.html

[BigDecimal]: http://www.datastax.com/drivers/nodejs/2.0/module-types-BigDecimal.html

[InetAddress]: http://www.datastax.com/drivers/nodejs/2.0/module-types-InetAddress.html

[TimeUuid]: http://www.datastax.com/drivers/nodejs/2.0/module-types-TimeUuid.html

[Uuid]: http://www.datastax.com/drivers/nodejs/2.0/module-types-Uuid.html

[Integer]: http://www.datastax.com/drivers/nodejs/2.0/module-types-Integer.html

> **Note:** The `sails-cassandra` adapter maintains mappings between model and
> database data types at all times and it will perform an additional conversion
> between data type returned by the driver into types prefered by
> Sails/Waterline. For instance, cassandra type `timeuuid` will be converted to
> `string` when reading from the database even though `cassandra-driver` returns
> `TimeUuid` type.


## License
See [LICENSE.md](./LICENSE.md) file for details.

