# The InterSystems IRIS Spark Connector

Copyright © 2016-2018, InterSystems. All rights reserved.

## Introduction

The InterSystems IRIS Spark Connector enables an InterSystems IRIS database to 
function as an [Apache Spark data source](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources).
It implements a plug-compatible replacement for the [Spark JDBC data source](https://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases)
that ships with Spark itself, allowing the results of a complex SQL query 
executed within the database to be retrieved by a Spark application as a [Spark 
dataset](https://spark.apache.org/docs/latest/sql-programming-guide.html#datasets-and-dataframes),
and, conversely, for a dataset to be written back into the database as a SQL 
table.

Its intimate knowledge of - and tight integration with - the underlying database 
provides several advantages over the Spark JDBC data source, however:

- **Predicate Push Down**

  The connector recognizes a richer set of operators that can be 'pushed down' 
  into the underlying database for execution than the Spark JDBC data source is
  aware of.

- **Implicit Parallelism**

  The connector exploits the server's innate ability to automatically parallelize
  certain queries and so allow large result sets to be returned efficiently to a
  Spark application through multiple concurrent network connections. By contrast,
  the Spark JDBC data source requires the user to explicitly specify how the result
  set is to be partitioned, which in practice is often very difficult to do well.

- **Shard Aware**

  If the database is 'sharded' - that is, its tables are transparently 
  partitioned across multiple servers running on different host computers - 
  the connector arranges that compute tasks are executed on Spark executors
  that are co-located with the servers from which they draw their data. This 
  not only reduces the motion of data across the network, but - and more 
  importantly - allows the performance of a Spark performance to scale linearly
  with the number of shards in the cluster, and so with size of the data set
  on which it operates.

## Requirements

The connector requires Spark 2.0+ and JVM 1.8. It has been primarily tested 
against Spark version 2.1.1 under JVM 1.8 u144, using the Spark Standalone cluster manager.

## Interfaces

The connector currently offers two main interfaces:

- an implementation of the generic [Spark data source API](https://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources) 
  that uses string key-value pairs to pass parameters, and that closely models
  the [Spark JDBC data source](https://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases);

- a custom Scala interface that adds extension methods to the various Spark
  contexts to provide a convenient and type-safe interface for clients written
  in the Scala programming language.

### Generic Interface

The connector implements a Spark SQL data source format named "com.intersystems.spark"
- also aliased as "iris" - whose usage is similar to that of the built in "jdbc" 
format: 

    scala> spark.read.format("iris").option("dbtable","Owls").load()

All options supported by the "jdbc" format, as well as those supported by the
InterSystems JDBC driver, are recognized by the connector and retain their same 
meanings, with the following exceptions:

- **query** *string*:

  This option is just a synonym for the **dbtable** option.

- **driver** *string*:

  This option is ignored - the connector always uses the InterSystems JDBC 
  driver, a copy of which is embedded within the connector itself.

- **url** *string*, **user** *string*, and **password** *string*:

  These are are now optional; if omitted, the connection details of the default
  cluster specified in the SparkConf configuration are used instead. See the
  section titled 'Configuration' below.

- **mfpi** *int*: 

  The **m**aximum number of **p**artitions **p**er **i**nstance to include in
  any implicit query factorization performed by the server.

  When reading data, the connector attempts to negotiate with the server as to
  how best to partition the resulting dataset; depending on how the cluster is
  configured,  each partition can potentially run in parallel within its own 
  Spark executor and establish its own independent connection into the shard
  from which it draws its data.

  Ignored when writing, or when **numPartions** is specified. 
  Default = "1".

- **description** *string*: 

  An optional description for the newly created table.

  Ignored when reading, or appending to a table that already exists.
  Default = "".

- **publicRowId** *boolean*:

  Specifies whether or not the master row ID column for the newly created 
  table is to be made publicly visible.

  Ignored when reading, or appending to a table that already exists.
  Default = "false".

- **shard**:

  Indicates that the records of the table are to be distributed across the
  cluster using a system assigned shard key.
 
  Ignored when reading, or when writing to a table that already exists. 
  Default = "false".

- **shard** *boolean*:

  Indicates that the records of the table are to be distributed across the
  cluster using a system assigned sharding key ("true"), or instead should be
  stored locally on the shard master itself ("false").

  Ignored when reading, or when writing to a table that already exists. 
  Default = "false".
 
- **shard** *field* [, *field*]\*:

  Indicates that the records of the  table are to be distributed across the
  cluster using the given sequence of fields to compute the shard key.

  Ignored when reading, or when writing to a table that already exists. 
  Default = "false".

- **autobalance** *boolean*

  When writing a dataset to a table that is sharded on a system assigned shard
  key, the value "true" specifies that the inserted records are to be evenly 
  distributed amongst the available shards, while the value "false" specifies 
  that they be sent to whichever shard is 'closest' to where the dataset partitions
  reside.

  In a properly configured cluster, a Spark slaves runs on each shard server.
  Writing a dataset with this option disabled can be faster, since records no
  longer need to travel across the network to reach their destination shard; 
  it now becomes the Spark application's responsibility however to ensure that
  roughly the same number of records is written to each shard.

  Ignored when reading, when writing to a table that is not sharded, or when
  writing to a table sharded on a user defined shard key.
  Default = "true".

See [JDBC To Other Database](https://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases)
for the full set of options recognized by the Spark JDBC Data Source. 

See [IRIS JDBC Connection Properties](http://docs.intersystems.com/latest/csp/docbook/DocBook.UI.Page.cls?KEY=BGJD_connecting#BGJD_connecting_connprops)
for the full set of options recognized by the InterSystems JDBC driver.

### Custom Scala Interface

The connector also provides an alternate Scala interface in the form of a set 
of extension methods for the various Spark contexts. This offers a convenient
and type-safe interface for clients written in the Scala programming language.
It is accessed by importing the contents of the package "com.intersystems.spark" 
into the current context:

    scala> import com.intersystems.spark._

    scala> spark.read.iris("SELECT name FROM Owls").show()
    +---------+
    |    name |
    +---------+
    |    Barn |
    |  Horned |
        ....

    scala> (1 to 100).toDF("i").write.shard().iris("Integers")

#### SparkContext Extension Methods

    def rdd[α: ClassTag](text: String,mfpi: ℕ,format: Format[α]): RDD[α]
    def rdd[α: ClassTag](text: String,column: String,lo: Long,hi: Long,partitions: ℕ,format: Format[α]): RDD[α]

#### SparkSession Extension Methods

    def dataframe(text: String,mfpi: ℕ = 1): DataFrame
    def dataframe(text: String,column: String,lo: Long,hi: Long,partitions: ℕ): DataFrame
    def dataset[α: Encoder](text: String,mfpi: ℕ = 1): Dataset[α]
    def dataset[α: Encoder](text: String,column: String,lo: Long,hi: Long,partitions: ℕ): Dataset[α]

#### DataFrameReader Extension Methods

    def iris(text: String,mfpi: ℕ = 1): DataFrame
    def iris(text: String,column: String,lo: Long,hi: Long,partitions: ℕ): DataFrame
    def address(address: Address): DataFrameReader
    def address(url: String,user: String = "",password: String = ""): DataFrameReader

#### DataFrameWriter Extension Methods

    def iris(table: String): Unit
    def description(value: String): DataFrameWriter[α]
    def publicRowID(value: Boolean): DataFrameWriter[α]
    def shard(value: Boolean): DataFrameWriter[α]
    def shard(fields: String*): DataFrameWriter[α]
    def autobalance(value: Boolean): DataFrameWriter[α]
    def address(address: Address): DataFrameWriter[α]
    def address(url: String,user: String = "",password: String = ""): DataFrameWriter[α]

See the source code and generated Scala documentation for further details.
 
## Configuration

The connector recognizes a number of configuration settings that parameterize
its operation. These are parsed from the `SparkConf` configuration structure
at startup and are specified in the usual way; that is, via:

- the file `spark-defaults.conf` associated with the Spark cluster;

- values for the `--conf` option passed on the command line;

- arguments to the `SparkConf()` constructor or its `set()` member functions,
  called from within the driver application itself.

See class [SparkConf](https://spark.apache.org/docs/2.1.1/api/java/org/apache/spark/SparkConf.html)
for more information on specifying configuration settings.

### Configuration Settings

- **spark.iris.master.url**

  A string of the form `"IRIS://host:port/namespace"` that specifies the address 
  of the default cluster to connect to if none is explicitly specified in a 
  read or write operation.

- **spark.iris.master.user**

  The user account with which to connect to the default cluster.

- **spark.iris.master.password**

  The password for the user account with which to connect to the default 
  cluster.

- **spark.iris.master.expires**
  
  A positive integral number of seconds after which a server connection is 
  judged to have expired and is automatically closed and garbage collected. 
  Default = 60.

- **spark.iris.worker.host**

  An optional string of the form `pattern ⇒ rewrite` where:
 
  - `pattern` is a regular expression (which may contain parenthesized groups)
  - `rewrite` is a replacement string (which may contain $ references to those
    groups)
  - `⇒` is the Unicode RIGHT DOUBLE ARROW character #8658 (U21+d2). The ASCII
  character pair `=>` ('equals' followed by 'greater-than') may also be used 
  instead if you prefer.

 If specified, this setting describes how to convert the host name of an IRIS
 server into the host name of the preferred Spark worker that should handle
 requests to read and write dataset partitions to it. 
 
 The most common cluster configuration is to arrange that a Spark worker runs
 on each machine that hosts an IRIS server,  for then records need not travel 
 across the network.  It can happen, however, that the host name by which the 
 master IRIS server knows its shard server differs from the host name by which
 the Spark master knows its worker, even though they are running on the very
 same machine; their host names could be aliased by a DNS server, for example,
 or could be running in separate Docker containers.
 
 This setting offers a means of defining at install time a function that maps
 between the two host names.

## Predicate Pushdown

The connector currently recognizes the following Spark Catalyst operators as 
having direct counterparts within the underlying database:

- EqualTo           
- LessThan          
- GreaterThan       
- LessThanOrEqual   
- GreaterThanOrEqual
- StringContains    
- StringStartsWith  
- StringEndsWith    
- In                
- IsNull            
- IsNotNull         
- Or                
- And               
- Not               

Spark SQL queries and dataframe methods that introduce occurrences of these 
operators are compiled by the connector into SQL queries that are executed
remotely within the database.

## Data Types

Internally, the connector uses the InterSystems JDBC driver to read and write
values to and from the server.  This constrains the data types that can be
serialized in and out of database tables via Spark. The server exposes the
following JDBC data types as available projections for InterSystems data types:

    BIGINT, BIT, DATE, DOUBLE, GUID, INTEGER, LONGVARBINARY, LONGVARCHAR, 
    NUMERIC, SMALLINT, TIME, TIMESTAMP, TINYINT, VARBINARY, VARCHAR.

When reading columns from a table, the connector maps their types as follows: 

    + ----------------------+--------------------+
    |      JDBC Type        ⇒   Spark Type       |
    + ----------------------+--------------------+
    | -11  UNIQUEIDENTIFIER |   StringType       |
    |  -7  BIT              |   BooleanType      |
    |  -6  TINYINT          |   ByteType         |
    |  -5  BIGINT           |   LongType         |
    |  -4  LONGVARBINARY    |   BinaryType       |
    |  -3  VARBINARY        |   BinaryType       |
    |  -1  LONGVARCHAR      |   StringType       |
    |   2  NUMERIC(p,s)     |   DecimalType(p,s) |
    |   4  INTEGER          |   IntegerType      |
    |   5  SMALLINT         |   ShortType        |
    |   8  DOUBLE           |   DoubleType       |
    |  12  VARCHAR          |   StringType       |
    |  91  DATE             |   DateType         |
    |  92  TIME             |   TimestampType    |
    |  93  TIMESTAMP        |   TimestampType    |
    +-----------------------+--------------------+

When writing columns to a table, the connector maps their types as follows: 

    + ----------------------+--------------------+
    |   Spark Type          ⇒      JDBC Type     |
    + ----------------------+--------------------+
    |   BinaryType          |  -3  VARBINARY     |
    |   BooleanType         |  -7  BIT           |
    |   ByteType            |  -6  TINYINT       |
    |   DateType            |  91  DATE          |
    |   DecimalType(p,s)    |   2  NUMERIC(p,s)  |
    |   DoubleType          |   8  DOUBLE        |
    |   FloatType           |   8  DOUBLE        |
    |   IntegerType         |   4  INTEGER       |
    |   LongType            |  -5  BIGINT        |
    |   ShortType           |   5  SMALLINT      |
    |   StringType          |  12  VARCHAR       |
    |   TimestampType       |  93  TIMESTAMP     |
    +-----------------------+--------------------+

Notes:

- the integers *p* > 0 and *s* >= 0 respectively denote the precision and scale 
  of the numeric representation.
  
- the types `...Type` are members of the `org.apache.spark.sql.types` package.

- there is no Spark SQL encoder currently available for the type `java.sql.Time`.

- the InterSystems class `%Library.Float` is deprecated. InterSystems now
  projects columns with this type into JDBC as type `DOUBLE`.

- value types (Boolean, Byte, Double, Float, Integer, Long, and Short) are
  all qualified as being NOT NULL when saved to a table, while object types
  (Binary, Date, Decimal, String, and Timestamp) are not.

This mapping between Spark Catalyst and JDBC data types differs subtly from 
that used by the Spark JDBC data source itself. In particular:

- it recognizes the UNIQUEIDENTIFIER data type, which is not widely supported 
  by all JDBC vendors;
- it distinguishes between the different sizes of integer TINYINT, SMALLINT,
  INTEGER, BIGINT, whereas the Spark JDBC data source does not;
- it maps Spark's FloatType to DOUBLE; the FLOAT type is deprecated by IRIS. 

See [IRIS SQL Data Types](http://docs.intersystems.com/latest/csp/docbook/DocBook.UI.Page.cls?KEY=RSQL_datatype)
for details on how values are represented within IRIS and projected to xDBC.

## Known Issues

### Java 9

Java 9, and the JVM 1.9 on which it runs, became available for general release
in September 2017.

The connector does not currently run on this version of the JVM. Nor, indeed,
does Spark itself.

We hope to address this issue in a subsequent release.

### Pruning Columns with Synthetic Names

Consider the following query Spark session:

    scala> spark.read.iris("select a, min(a) from A")

where A is some table that presumably has a column named `a`. 

Notice that no alias is provided for the selection expression `min(a)`. The
server synthesizes names for such columns, and in this case might describe the
schema for the resulting dataframe as having two columns, named 'a' and 'Aggregate_2' 
respectively. 

No actual field named 'Aggregate_2' exists in the table however, so an attempt
to reference it in an enclosing selection would fail:

    scala> val df = spark.read.iris("select Aggregate_2 from (select a, min(a) from A)")
    > SQL ERROR: No such field SQLUSER.A.Aggregate_2 ....

This is to be expected in a standard SQL implementation.

The connector uses just such enclosing projection expressions as these however
when attempting to prune the columns of a dataframe to those that are actually
referenced in subsquent code:

    scala> spark.read.iris("select a, min(a) from A").select("a")...

internally generates the query `select a from(select a, min(a) from A)` to be
executed on the server in order to minimize the motion of data into the spark
cluster. 

As a result, the connector cannot efficiently prune columns with synthetic names 
and instead resorts to fetching the entire result set:

    scala> spark.read.iris("select a, min(a) from A").select("Aggregate_2")

internally generates the query `select * from(select a, min(a) from A)`.

For this reason, you should consider modifying the original query by attaching
aliases to columns that would otherwise receive server synthesized names.
    
We hope to address this issue in a subsequent release.

### Handling of TINYINT

The mapping between Spark Catalyst and JDBC datatypes (described above in the
section Data Types) differs subtly from that used by the Spark JDBC data source
itself. The connector achieves this mapping by automatically installing its own
subclass of class `org.apache.spark.sql.jdbc.JdbcDialect' but this also has the
side effect of changing the mapping used by Spark JDBC itself. 

By and large this is a good thing, but one problem that has been identified 
recently is that due to a bug in Spark 2.1.1, which neglects to implement a
a low level reader function for the ByteType, attempting to read an IRIS table 
with a column of type TINYINT using the Spark JDBC data source will fail once
the connecter has been loaded.

For now, it is probably best to avoid reading and writing dataframes using the
Spark JDBC data source directly once the connector has been loaded.

We hope to address this issue in a subsequent release.

### JDBC Isolation Levels

The IRIS server does not currently support the writing of a dataset to a SQL table using JDBC isolation levels other than NONE and READ_UNCOMITTED. 

We hope to address this issue in a subsequent release.

## Getting Started

    scala> import com.intersystems.spark._
    import com.intersystems.spark._
                      
    scala> (1 to 5).toDF("i").write.iris("I")
                      
    scala> spark.read.iris("I").show
    +---+
    |  i|
    +---+
    |  3|
    |  2|
    |  1|
    |  5|
    |  4|
    +---+

    scala> spark.read.iris("SELECT square(i - avg(i)) FROM I").show
    +-------------------+
    |       Expression_1|
    +-------------------+
    |0.00000000000000000|
    |1.00000000000000000|
    |4.00000000000000000|
    |4.00000000000000000|
    |1.00000000000000000|
    +-------------------+

## Source Code Organization

The implementation resides in the following packages:

- **com.intersystems.spark** implements the public interface to the connector. 

  Implemented in Scala. Resides in the intersystems-spark JAR file.

- **com.intersystems.spark.core** provides the underlying implementation for the 
  connector.

  Implemented in Scala. Resides in the intersystems-spark JAR file.

  These classes should be considered private: their APIs may undergo breaking
  changes in a future release and so should not be referenced directly.

- **com.intersystems.sqf** consists of a number of supporting classes concerned 
  with factorizing SQL queries. 

  Implemented in the Java language and also used by the the InterSystems JDBC 
  driver, whose JAR file they reside in. 

  These classes should be considered private: their APIs may undergo breaking
  changes in a future release, so should not be referenced directly.

## Logging

The connector logs various events of interest via the same infrastructure as
used by Spark itself, namely [Log4J](https://logging.apache.org/log4j/1.2/download.html).

The content, format, and destination of the system as a whole is configured by 
the file `${SPARK_HOME}/conf/log4j-defaults`. 

The connector is implemented in classes that reside in a package named `com.intersystems.spark` and so can easily
be configured by specifying keys of the form:

     log4j.logger.com.intersystems.spark                      = INFO
    #log4j.logger.com.intersystems.spark.core                 = DEBUG
    #log4j.logger.com.intersystems.spark.core.DataSource = ALL
     ...

## Contributors

- Jonathon Bell
- Benjamin De Boe
- Aleks Djakovic
- Chris Gardner
- Stuart Stinson
- Christian Witte
