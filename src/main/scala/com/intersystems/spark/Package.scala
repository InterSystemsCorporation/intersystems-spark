//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Package object for the InterSystems IRIS Spark Connector.
//*
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems

import scala.reflect.ClassTag

import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import com.intersystems.spark.core._
import com.intersystems.spark.core.{DataFrameWriter}

/**
 * Package object for the InterSystems IRIS Spark Connector.
 *
 * Defines a custom interface for the InterSystems IRIS Spark Connector, a set
 * of classes and types that together offer  a more convenient type safe Scala
 * interface for the connector than that of the generic string-based interface
 * that comes built into Spark itself.
 *
 * @example
 * {{{
 *    scala> import com.intersystems.spark._
 * }}}
 * imports the custom interface into the current scope, allowing one to write,
 * for example:
 * {{{
 *    scala> spark.read.iris("SELECT name FROM Owls").show(3)
 *    +---------+
 *    |    name |
 *    +---------+
 *    |    Barn |
 *    |  Horned |
 *    | Screech |
 *    +---------+
 * }}}
 * to read and display the first few records of a table called 'Owls' from the
 * default cluster, while:
 * {{{
 *    scala> owls.write.mode("append").iris("Owls")
 * }}}
 * appends the contents of the dataframe `owls` to this very same table within
 * the cluster.
 *
 * @author Jonathon Bell
 */
package object spark
{
  /**
   * Describes the connection details of an IRIS cluster.
   *
   * Implemented as a Java class within the InterSystems JDBC driver jar file.
   */
  type Address = com.intersystems.sqf.Address

  /**
   * Augments the Java class Address with additional functionality.
   */
  val Address = core.Address

  /**
   * A function that formats the current row of a JDBC ResultSet as an element
   * of an RDD.
   *
   * The function ''pair'', for example:
   * {{{
   *    val pair: Format[(String,String)]  =  r ⇒ (r.getString(1),r.getString(2))
   * }}}
   * extracts a pair of  strings from the first two columns of the current row
   * of a result set, so can be used to construct an RDD[(String,String)] from
   * the result of any query of the cluster that includes at least two strings
   * per record.
   *
   * Format functions should normally restrict themselves to calling only pure
   * (that is, non-side effecting) member functions of the result set, such as
   * `getInt`, get`Double`, `getDate` and the like, since they will be invoked
   * for each and every record requested by the client.
   */
  type Format[α] = java.sql.ResultSet ⇒ α

  /**
   * Extends the given context with IRIS specific methods.
   *
   * @param sc  The active Spark context.
   *
   * @author Jonathon Bell
   */
  implicit class SparkContextEx(sc: SparkContext)
  {
    /**
     * Executes a query on the default cluster to yield a suitably partitioned
     * RDD whose elements are formatted by applying the given function to each
     * row of the resulting JDBC result set.
     *
     * @param text        The text of a query to be executed on the cluster or
     *                    the name of an existing table in the cluster to load.
     * @param mfpi        The maximum number of  factors per distinct instance
     *                    to include in the factorization implicitly performed
     *                    by the server, or 0 if no limit is necessary.
     * @param format      Formats an element from a single row of a result set.
     *
     * @return The results of the query in the form of a suitably partitioned
     *         RDD.
     * @throws SQLException if a database access error occurs.
     */
    def rdd[α: ClassTag](text: String,mfpi: ℕ,format: Format[α]): RDD[α] =
    {
      val m = Master()                                   // The default master
      val q = Query(text,mfpi)                           // Repackage as query

      ShardedRDD(sc,q.factorize(m),format)               // Create sharded RDD
    }

    /**
     * Executes a query on the default cluster to yield a suitably partitioned
     * RDD whose elements are formatted by applying the given function to each
     * row of the resulting JDBC result set.
     *
     * @param text        The text of a query to be executed on the cluster or
     *                    the name of an existing table in the cluster to load.
     * @param column      The name of the integral valued column in the result
     *                    set with which to further partition the query.
     * @param lo          The lower bound of the partitioning column.
     * @param hi          The upper bound of the partitioning column.
     * @param partitions  The number of partitions per instance to create.
     * @param format      Formats an element from a single row of a result set.
     *
     * @return The results of the query in the form of a suitably partitioned
     *         RDD.
     * @throws SQLException if a database access error occurs.
     * @see    [[https://spark.apache.org/docs/2.1.0/sql-programming-guide.html#jdbc-to-other-databases JDBC to Other Databases]]
     *         for more on the semantics of the `column`, `lo`, `hi`, and `partitions`
     *         parameters.
     */
    def rdd[α: ClassTag](text: String,column: String,lo: Long,hi: Long,partitions: ℕ,format: Format[α]): RDD[α] =
    {
      val m = Master()                                   // The default master
      val q = Query(text,column,lo,hi,partitions)        // Repackage as query

      ShardedRDD(sc,q.factorize(m),format)               // Create sharded RDD
    }
  }

  /**
   * Extends the given session with IRIS specific methods.
   *
   * @param ss  The active Spark session.
   *
   * @author Jonathon Bell
   */
  implicit class SparkSessionEx(ss: SparkSession)
  {
    /**
     * Executes a query on the default cluster to yield a suitably partitioned
     * DataFrame.
     *
     * @param text        The text of a query to be executed on the cluster or
     *                    the name of an existing table in the cluster to load.
     * @param mfpi        The maximum number of  factors per distinct instance
     *                    to include in the factorization implicitly performed
     *                    by the server, or 0 if no limit is necessary.
     *
     * @return The results of the query in the form of a suitably partitioned
     *         RDD.
     * @throws SQLException if a database access error occurs.
     */
    def dataframe(text: String,mfpi: ℕ = 1): DataFrame =
    {
      val a = Address()                                  // Use default master
      val q = Query(text,mfpi)                           // Repackage as query
      val r = Relation(ss.sqlContext,a,q)                // Wrap in a relation

      ss.baseRelationToDataFrame(r)                      // Create DataFrame
    }

    /**
     * Executes a query on the default cluster to yield a suitably partitioned
     * DataFrame.
     *
     * @param text        The text of a query to be executed on the cluster or
     *                    the name of an existing table in the cluster to load.
     * @param column      The name of the integral valued column in the result
     *                    set with which to further partition the query.
     * @param lo          The lower bound of the partitioning column.
     * @param hi          The upper bound of the partitioning column.
     * @param partitions  The number of partitions per instance to create.
     *
     * @return The results of the query in the form of a suitably partitioned
     *         RDD.
     * @throws SQLException if a database access error occurs.
     * @see    [[https://spark.apache.org/docs/2.1.0/sql-programming-guide.html#jdbc-to-other-databases JDBC to Other Databases]]
     *         for more on the semantics of the `column`, `lo`, `hi`, and `partitions`
     *         parameters.
     */
    def dataframe(text: String,column: String,lo: Long,hi: Long,partitions: ℕ): DataFrame =
    {
      val a = Address()                                  // Use default master
      val q = Query(text,column,lo,hi,partitions)        // Repackage as query
      val r = Relation(ss.sqlContext,a,q)                // Wrap in a relation

      ss.baseRelationToDataFrame(r)                      // Create DataFrame
    }

    /**
     * Executes a query on the default cluster to yield a suitably partitioned
     * Dataset of elements of type α.
     *
     * Typically α will be a user defined case class. For example:
     * {{{
     *    case class Person(name: String,age: Int)
     *
     *    spark.dataset[Person]("SELECT name,age from Person")
     * }}}
     * returns a result set of type Dataset[Person] whose arguments `name` and
     * `age` are matched against the meta data for the underlying DataFrame by
     * their respective names.
     *
     * The only real requirement, however, is that the type be a member of the
     * `Encoder` type class.  This includes classes implementing the `Product`
     * trait, and so, in particular, all small case classes.
     *
     * @tparam α          Any type for which an Encoder is available; includes
     *                    small case classes and extensions of trait Product.
     * @param text        The text of a query to be executed on the cluster or
     *                    the name of an existing table in the cluster to load.
     * @param mfpi        The maximum number of  factors per distinct instance
     *                    to include in the factorization implicitly performed
     *                    by the server, or 0 if no limit is necessary.
     *
     * @return The results of the query in the form of a suitably partitioned
     *         Dataset[α].
     * @throws SQLException if a database access error occurs.
     */
    def dataset[α: Encoder](text: String,mfpi: ℕ = 1): Dataset[α] =
    {
      dataframe(text,mfpi).as[α]                         // Reformat DataFrame
    }

    /**
     * Executes a query on the default cluster to yield a suitably partitioned
     * Dataset of elements of type α.
     *
     * Typically α will be a user defined case class. For example:
     * {{{
     *    case class Person(name: String,age: Int)
     *
     *    spark.dataset[Person]("SELECT name,age from Person","column",0,10000,2)
     * }}}
     * returns a result set of type Dataset[Person] whose arguments `name` and
     * `age` are matched against the meta data for the underlying DataFrame by
     * their respective names.
     *
     * The only real requirement, however, is that the type be a member of the
     * `Encoder` type class.  This includes classes implementing the `Product`
     * trait, and so, in particular, all small case classes.
     *
     * @tparam α          Any type for which an Encoder is available; includes
     *                    small case classes and extensions of trait Product.
     * @param text        The text of a query to be executed on the cluster or
     *                    the name of an existing table in the cluster to load.
     * @param column      The name of the integral valued column in the result
     *                    set with which to further partition the query.
     * @param lo          The lower bound of the partitioning column.
     * @param hi          The upper bound of the partitioning column.
     * @param partitions  The number of partitions per instance to create.
     *
     * @return The results of the query in the form of a suitably partitioned
     *         Dataset[α].
     * @throws SQLException if a database access error occurs.
     * @see    [[https://spark.apache.org/docs/2.1.0/sql-programming-guide.html#jdbc-to-other-databases JDBC to Other Databases]]
     *         for more on the semantics of the `column`, `lo`, `hi`, and `partitions`
     *         parameters.
     */
    def dataset[α: Encoder](text: String,column: String,lo: Long,hi: Long,partitions: ℕ): Dataset[α] =
    {
      dataframe(text,column,lo,hi,partitions).as[α]      // Reformat DataFrame
    }
  }

  /**
   * Extends the given reader with IRIS specific methods.
   *
   * @param reader  A DataFrame reader.
   *
   * @author Jonathon Bell
   */
  implicit class DataFrameReaderEx(reader: DataFrameReader)
  {
    /**
     * Executes a query on the given cluster to compute a suitably partitioned
     * DataFrame.
     *
     * This enables one to write, for example:
     * {{{
     *    spark.read.iris("SELECT * FROM table",2)
     * }}}
     * as a convenient shorthand for the more explicit:
     * {{{
     *    spark.read
     *         .format("com.intersystems.spark")
     *         .option("query","SELECT * FROM table")
     *         .option("mfpi",2)
     *         .load()
     * }}}
     *
     * The following options affect how the operation is performed:
     *
     *  - `url`:          A string of  the form  "IRIS://host:port/namespace"
     *                    that specifies the cluster from which the data is to
     *                    be read.  If omitted,  the default cluster specified
     *                    via the "spark.iris.master.url" configuration setting
     *                    is used instead.
     *
     *  - `user`:         The account with which to make the connection to the
     *                    cluster named in the "url" option above.
     *
     *  - `password`:     The password for the given user account.
     *
     *  - `fetchsize`:    The number of rows to fetch per server round trip.
     *
     *                    Default = 1000.
     *
     * @param text        The text of a query to be executed on the cluster or
     *                    the name of an existing table in the cluster to load.
     * @param mfpi        The maximum number of  factors per distinct instance
     *                    to include in the factorization implicitly performed
     *                    by the server, or 0 if no limit is necessary.
     *
     * @return The results of the query in the form of a suitably partitioned
     *         DataFrame.
     * @throws SQLException if a database access error occurs.
     */
    def iris(text: String,mfpi: ℕ = 1): DataFrame =
    {
      reader.format("com.intersystems.spark")            // Our Spark format
            .option("query",text)                        // The query text
            .option("mfpi", mfpi)                        // The mfpi throttle
            .load()                                      // Load data frame
    }

    /**
     * Executes a query on the given cluster to compute a suitably partitioned
     * DataFrame.
     *
     * This enables one to write, for example:
     * {{{
     *    spark.read.iris("SELECT * FROM Owls","column",0,10000,2)
     * }}}
     * as a convenient shorthand for the more explicit:
     * {{{
     *    spark.read
     *         .format("com.intersystems.spark")
     *         .option("query","SELECT * FROM Owls")
     *         .option("paftitionCol","column")
     *         .option("lowerBound",0)
     *         .option("upperBound",10000)
     *         .option("numPartitions",2)
     *         .load()
     * }}}
     *
     * The following options affect how the operation is performed:
     *
     *  - `url`:          A string of  the form  "IRIS://host:port/namespace"
     *                    that specifies the cluster from which the data is to
     *                    be read.  If omitted,  the default cluster specified
     *                    via the "spark.iris.master.url" configuration setting
     *                    is used instead.
     *
     *  - `user`:         The account with which to make the connection to the
     *                    cluster named in the "url" option above.
     *
     *  - `password`:     The password for the given user account.
     *
     *  - `fetchsize`:    The number of rows to fetch per server round trip.
     *
     *                    Default = 1000.
     *
     * @param text        The text of a query to be executed on the cluster or
     *                    the name of an existing table in the cluster to load.
     * @param column      The name of the integral valued column in the result
     *                    set with which to further partition the query.
     * @param lo          The lower bound of the partitioning column.
     * @param hi          The upper bound of the partitioning column.
     * @param partitions  The number of partitions per instance to create.
     *
     * @return The results of the query in the form of a suitably partitioned
     *         DataFrame.
     * @throws SQLException if a database access error occurs.
     * @see    [[https://spark.apache.org/docs/2.1.0/sql-programming-guide.html#jdbc-to-other-databases JDBC to Other Databases]]
     *         for more on the semantics of the `column`, `lo`, `hi`, and `partitions`
     *         parameters.
     */
    def iris(text: String,column: String,lo: Long,hi: Long,partitions: ℕ): DataFrame =
    {
      reader.format("com.intersystems.spark")            // Our Spark format
            .option("query",          text)              // The query text
            .option("partitionColumn",column)            // The partition col
            .option("lowerBound",     lo)                // Its lower bound
            .option("upperBound",     hi)                // Its upper bound
            .option("numPartitions",  partitions)        // The partition count
            .load()                                      // Load data frame
    }

    /**
     * Specifies the connection details of the cluster to read from.
     *
     * Overrides the default cluster specified in the Spark configuration for
     * the duration of this read operation.
     *
     * @param address  The connection details of the cluster to read from.
     *
     * @return The same DataFrameReader on which this method was invoked.
     */
    def address(address: Address): DataFrameReader =
    {
      reader.option("url",     address.url)              // The master's url
            .option("user",    address.user)             // The user account
            .option("password",address.password)         // The user password
    }

    /**
     * Specifies the connection details of the cluster to read from.
     *
     * Overrides the default cluster specified in the Spark configuration for
     * the duration of this read operation.
     *
     * @param url       A string of the form "IRIS://host:port/namespace" that
     *                  specifies the cluster to read from.
     * @param user      The user account with which to make the connection to
     *                  the cluster named in the "url" option above.
     * @param password  The password for the given user account.
     *
     * @return The same DataFrameReader on which this method was invoked.
     */
    def address(url: String,user: String = "",password: String = ""): DataFrameReader =
    {
      reader.option("url",     url)                      // The master's url
            .option("user",    user)                     // The user account
            .option("password",password)                 // The user password
    }
  }

  /**
   * Extends the given writer with IRIS specific methods.
   *
   * @param writer  A DataFrame writer.
   *
   * @author Jonathon Bell
   */
  implicit class DataFrameWriterEx[α](writer: DataFrameWriter[α])
  {
    /**
     * Saves a DataFrame to the given table within the cluster.
     *
     * This enables one to write, for example:
     * {{{
     *    df.write.iris("Owls")
     * }}}
     * as a convenient shorthand for the more explicit:
     * {{{
     *    df.write.format("com.intersystems.spark")
     *            .option("dbtable","Owls")
     *            .save()
     * }}}
     *
     * The following options affect how the operation is performed:
     *
     *  - `url`:          A string of  the form  "IRIS://host:port/namespace"
     *                    that specifies the cluster to which the data will be
     *                    written.  If omitted,  the default cluster specified
     *                    via the "spark.iris.master.url" configuration setting
     *                    is used instead.
     *
     *  - `user`:         The account with which to make the connection to the
     *                    cluster named in the "url" option above.
     *
     *  - `password`:     The password for the given user account.
     *
     *  - `mode`:         Describes how to behave if  the target table already
     *                    exists.
     *
     *                    Can be either OVERWRITE, APPEND, IGNORE, or ERROR.
     *
     *                    Default = ERROR.
     *
     *  - `batchsize`:    The number of rows to insert per server round trip.
     *
     *                    Default = 1000.
     *
     *  - `isolationlevel`:
     *                    The transaction isolation level.
     *
     *                    Can be either NONE, REPEATABLE_READ, READ_COMMITTED,
     *                    READ_UNCOMMITTED, or SERIALIZABLE.
     *
     *                    Corresponds to the standard transaction isolation
     *                    levels specified by the JDBC Connection object.
     *
     *                    Default = READ_UNCOMMITTED.
     *
     *  - `description`:  An optional description for the newly created table.
     *
     *                    Default = "".
     *
     *  - `publicrowid`:  Specifies that the master RowID field for the newly
     *                    created table be publicly visible.
     *
     *                    Default = false.
     *
     *  - `shard`:        Specifies the shard key for the newly created table.
     *
     *                    Can be either true, false, or else a comma separated
     *                    set of field names.
     *
     *                    Default = false.
     *
     * @param table  The name of the table to write to.
     *
     * @throws SQLException if a database access error occurs.
     * @see    [[http://docs.InterSystems.com/latest/csp/docbook/DocBook.UI.Page.cls?KEY=RSQL_createtable
     *         InterSystems IRIS SQL Reference]]  for more information on the
     *         options supported by the CREATE TABLE statement.
     * @see    [[https://docs.oracle.com/cd/E19830-01/819-4721/beamv/index.html
     *         Using JDBC Transaction Isolation Levels]]
     */
    def iris(table: String): Unit =
    {
      writer.format("com.intersystems.spark")            // Our Spark format
            .option("dbtable",table)                     // The target table
            .save()                                      // Save data frame
    }

    /**
     * Specifies a description for the newly created table.
     *
     * Has no effect if the table already exists and the save mode is anything
     * other than OVERWRITE.
     *
     * @param value  An arbitrary description for the newly created table.
     *
     * @return The same DataFrameWriter on which this method was invoked.
     * @see    [[http://docs.InterSystems.com/latest/csp/docbook/DocBook.UI.Page.cls?KEY=RSQL_createtable
     *         InterSystems IRIS SQL Reference]]  for more information on the
     *         options supported by the CREATE TABLE statement.
     */
    def description(value: String): DataFrameWriter[α] =
    {
      writer.option("description",value)                 // The description
    }

    /**
     * Specifies whether or not the master row ID column of the newly created
     * table is to be made publicly visible.
     *
     * Has no effect if the table already exists and the save mode is anything
     * other than OVERWRITE.
     *
     * @param value  true for a publicly visible row ID, false otherwise.
     *
     * @return The same DataFrameWriter on which this method was invoked.
     * @see    [[http://docs.InterSystems.com/latest/csp/docbook/DocBook.UI.Page.cls?KEY=RSQL_createtable
     *         InterSystems IRIS SQL Reference]]  for more information on the
     *         options supported by the CREATE TABLE statement.
     */
    def publicRowID(value: Boolean): DataFrameWriter[α] =
    {
      writer.option("publicrowid",value)                 // Public row id flag
    }

    /**
     * Specifies whether or not the newly created table is to be sharded.
     *
     * Has no effect if the table already exists and the save mode is anything
     * other than OVERWRITE.
     *
     * @param value  true to create a sharded table using a shard key assigned
     *                by the system, false to create a non sharded table.
     *
     * @return The same DataFrameWriter on which this method was invoked.
     * @see    [[http://docs.InterSystems.com/latest/csp/docbook/DocBook.UI.Page.cls?KEY=RSQL_createtable
     *         InterSystems IRIS SQL Reference]]  for more information on the
     *         options supported by the CREATE TABLE statement.
     */
    def shard(value: Boolean): DataFrameWriter[α] =
    {
      writer.option("shard",value.toString)              // Is table sharded?
    }

    /**
     * Specifies the shard key for the newly created table.
     *
     * Has no effect if the table already exists and the save mode is anything
     * other than OVERWRITE.
     *
     * @param fields  A (possibly empty) sequence of field names to be used as
     *                the user defied shard key. If the sequence is empty then
     *                the table will be sharded on the system assigned key.
     *
     * @return The same DataFrameWriter on which this method was invoked.
     * @see    [[http://docs.InterSystems.com/latest/csp/docbook/DocBook.UI.Page.cls?KEY=RSQL_createtable
     *         InterSystems IRIS SQL Reference]]  for more information on the
     *         options supported by the CREATE TABLE statement.
     */
    def shard(fields: String*): DataFrameWriter[α] =
    {
      if (fields.isEmpty)                                // Empty field list?
        writer.option("shard","true")                    // ...system defined
      else
        writer.option("shard",fields.mkString(","))      // ...user defined
    }

    /**
     * Specifies whether or not inserted  records should be evenly distributed
     * amongst the available shards of the cluster when saving to a table that
     * is sharded on a system assigned shard key.
     *
     * When explicitly disabled, every dataset partition attempts to write its
     * records directly into the shard on which  its Spark executor also runs.
     * This is often faster, since records no longer need to travel across the
     * network to reach their destination shard, but shifts the responsibility
     * of saving roughly the same number of records to each shard to the user,
     * who will now wish to partition the dataset appropriately before writing
     * it to the cluster.
     *
     * Has no effect if the table is not sharded, or is sharded using a custom
     * shard key.
     *
     * @param value  true to evenly distribute records amongst the available
     *               shards of the cluster, false to save records into shards
     *               that are 'closest' to where the partitions of the dataset
     *               reside.
     *
     * @return The same DataFrameWriter on which this method was invoked.
     * @see    [[http://docs.InterSystems.com/latest/csp/docbook/DocBook.UI.Page.cls?KEY=RSQL_createtable
     *         InterSystems IRIS SQL Reference]]  for more information on the
     *         options supported by the CREATE TABLE statement.
     */
    def autobalance(value: Boolean = true): DataFrameWriter[α] =
    {
      writer.option("autobalance",value.toString)        // Evenly distribute?
    }

    /**
     * Specifies the connection details of the cluster to write to.
     *
     * Overrides the default cluster specified in the Spark configuration for
     * the duration of this write operation.
     *
     * @param address  The connection details of the cluster to write to.
     *
     * @return The same DataFrameWriter on which this method was invoked.
     */
    def address(address: Address): DataFrameWriter[α] =
    {
      writer.option("url",     address.url)               // The master's url
            .option("user",    address.user)              // The user account
            .option("password",address.password)          // The user password
    }

    /**
     * Specifies the connection details of the cluster to write to.
     *
     * Overrides the default cluster specified in the Spark configuration for
     * the duration of this write operation.
     *
     * @param url       A string of the form "IRIS://host:port/namespace" that
     *                  specifies the cluster to read from.
     * @param user      The user account with which to make the connection to
     *                  the cluster named in the "url" option above.
     * @param password  The password for the given user account.
     *
     * @return The same DataFrameWriter on which this method was invoked.
     */
    def address(url: String,user: String = "",password: String = ""): DataFrameWriter[α] =
    {
      writer.option("url",     url)                      // The master's url
            .option("user",    user)                     // The user account
            .option("password",password)                 // The user password
    }
  }
}

//****************************************************************************
