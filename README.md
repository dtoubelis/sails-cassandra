# sails-cassandra
Cassanda database adapter for Sails/Waterline

## Implementetion notes
This section describes behaviour of Apache Cassandra adapter distinct from other database types;

### Mapping of column and table names
Column and table names in Cassandra are case insensitive and this ambiguity makes it difficult to map between attribute names that are case sensitive and column names that are not. There are two possible workarounds for this:
1. There is a mechanism in Apache Cassandra to make table/column names case sensitive by including them in double quotes. This may seem as a good idea on the surface but it does not cover a use case when tables are not created by sails/waterline but by an external process.
2. Converting table/column names to lower case is an another approach. This way we always convert table/column names to lower case before mapping them to/from attribute names. This is not very elegant but it works and this is the current preferred approach.

### Mandatory use of indexes for lookups
Apache Cassandra require index on a column that is used in `where` clause of `select` statement and unlike other database it will produce and exception if the index is missing.

Sails/Waterline allows to set `index` or `unique` properties on model attributes. `sails-cassanda` adapter will respect these attributes and it will create indexes for attributes with `index` or `unique` attributes set to `true`.

Please note, that Apache Cassandra have no notion of `unique` constraint and the uniqueness has to be enforced either by Sails/Waterline core or in your own code. The `unique` attribute property is considered an alias for `index` and both are treated in the exactly same way.

### Autoincrement field
The autoincrement feature was plaguing ORM frameworks right from their inseption as it requires 1-2 extra queries in order to retrieve new record identifier from underlying database into the framework. It also does not work very well with sharding and replication.

Cassandra database does not support autoincrement, however it achieves the same functionality in a much more efficient way by using time based (A.K.A. Type 1) UUIDs for primary keys.

Sails/Waterline supports autoincrement and its implementation is heavily influenced by MySQL database. `sails-cassandra` adapter makes an attempt to achieve the same functionality without breaking any existing logic. and this is how:
1. Model attribute that represents primary key may have `autoIncrement` property set to `true`.
2. This automatically forces attribute type to `string` and supersedes any other declarations. The adapter will give a warning message is there is a discrepancy.
3. The value of the primary key cannot be overridden by `create()` or `update()` calls once `autoIncrement` property is enabled. You will see a (non-lethal) warning message if such attempt is made.

### Type conversion between Cassandra and Sails/Waterline

Direct mappings inside of `sails-cassandra`:

| Sails/Waterline Type | Cassandra Type                    |
|:---------------------|:----------------------------------|
| string               | text (UTF-8 text)                 |
| text                 | text (UTF-8 text)                 |
| integer              | bigint (64-bit signed integer)    |
| float                | double (64-bit float)             |
| date                 | timestamp                         |
| datetime             | timestamp                         |
| boolean              | boolean                           |
| binary               | blob                              |
| array                | list<text>                        |
| json                 | text (UTF-8 text)                 |
| email                | ascii (US-ASCII character string) |
| autoIncrement=true   | timeuuid                          |

> **Note:** The `cassanda-driver` module will try to find best match for types not represented in this table. For example, if a database was created by a different application and has column of type `uuid` then we can declare the corresponding model attribute as `string` and the driver will convert between these two automatically types. Please check `cassandra-driver` documentation for more details on this subject.

And this is reverse conversion:

| Cassandra Type | Internal Type Id | Sails/Waterline Type |
|:---------------|:----------------:|:---------------------|
| ascii          | x                | string               |
| bigint         | x                | integer              |
| blob           | x                | binary               |
| boolean        | x                | boolean              |
| counter        | x                | integer              |
| decimal        | x                | float                |
| double         | x                | float                |
| inet           | x                | string               |
| int            | x                | integer              |
| list           | x                | array of strings     |
| map            | x                | not supported (null) |
| set            | x                | not supported (null) |
| text           | x                | text                 |
| timestamp      | x                | datetime or date     |
| timeuuid       | x                | string               |
| tuple (v2.1+)  | x                | not supported (null) |
| uuid           | x                | string               |
| varchar        | x                | text                 |
| varint         | x                | integer              |

> **Note:** The `sails-cassandra` adapter maintains mappings between model and database data types at all times and it will perform an additional logic in order to get best possible match. For instance, cassandra type `timestamp` will be resolved either to `date` or `datatime` depending on attribute type definition in the model.
