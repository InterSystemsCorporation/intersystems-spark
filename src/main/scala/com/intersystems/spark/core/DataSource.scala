//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Implementation for class DataSource.
//*
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark
package core

import scala.util.Try

import org.apache.spark.sql.{DataFrame,SQLContext,SaveMode}
import org.apache.spark.sql.sources.{BaseRelation,CreatableRelationProvider,DataSourceRegister,RelationProvider}
import org.apache.spark.sql.types.{StructField}

/**
 * Implements the various interfaces that enable the database to function as a
 * Spark SQL data source.
 *
 * @author Jonathon Bell
 */
class DataSource extends DataSourceRegister
                         with CreatableRelationProvider
                         with RelationProvider
                         with Logging
{
  /**
   * A short user-friendly name for the data source.
   */
  val shortName: String = "iris"

  /**
   * Constructs a BaseRelation from the given arguments. These are supplied as
   * a collection of (key,value) pairs, and must include a value for either:
   *
   *  - `query`:     The text of a query to be executed on the cluster, or
   *
   *  - `dbtable`:   The name of a database table within the cluster, in which
   *                 case the entire table is loaded.
   *
   * Other optional arguments include:
   *
   *  - `url`:       A string of  the  form  "IRIS://host:port/namespace" that
   *                 specifies the cluster from  which the data is to be read.
   *                 If omitted, the default cluster specified via the "spark.
   *                 iris.master.url" configuration setting is used instead.
   *
   *  - `user`:      The user account with which to make the connection to the
   *                 cluster named in the "url" option above.
   *
   *  - `password`:  The password for the given user account.
   *
   *  - `mfpi`:      The maximum number of  partitions per server instance  to
   *                 include in any implicit query  factorization performed by
   *                 the server.
   *
   *                 Default = 1.
   *
   *  - `fetchsize`: The number of rows to fetch per server round trip.
   *
   *                 Default = 1000.
   *
   *  - `partitionColumn`, `lowerBound`, `upperBound`, and `numPartitions`:
   *                 An explicit description  of how to  partition the queries
   *                 sent to each  distinct instance;  these have an identical
   *                 semantics to the similarly  named arguments  for the JDBC
   *                 data source that is built into Spark.
   *
   * If both `mfpi` and `partionColumn` arguments are given, then the explicit
   * partitioning specification takes precedence.
   *
   * @param sc       The context in which to create the new relation.
   * @param options  The arguments with which to initialize the new relation.
   *
   * @return A new Relation that can be called upon to execute the given
   *         query on the cluster as needed.
   * @throws IllegalArgumentException if passed an invalid parameter value.
   * @see    [[https://spark.apache.org/docs/2.1.0/sql-programming-guide.html#jdbc-to-other-databases JDBC to Other Databases]]
   *         for more on the semantics of the `column`, `lo`, `hi`, and `partitions`
   *         parameters.
   */
  def createRelation(sc: SQLContext,options: Map[String,String]): BaseRelation =
  {
    log.debug("createRelation({})",options)              // Trace our progress

    val ma: Address = Address.parse(options)             // The master address

    val qs: String =                                     // The query string
    {
      if (options.isDefinedAt("query"))                  // ...got "query"?
      {
        options("query")                                 // ....then use it
      }
      else
      if (options.isDefinedAt("dbtable"))                // ...got "dbtable"?
      {
        options("dbtable")                               // ....then use it
      }
      else                                               // ...sigh, give up
      {
        argex("Neither options 'query' nor 'dbtable' specified")
      }
    }

    val cq: Query =                                      // Query wrapper
    {
      if (options.isDefinedAt("partitionColumn"))        // ...explicit spec?
      {
        def get(o: String): String =                     // ...required args
        {
          options.getOrElse(o,argex("Partitioning incompletely specified: " +
                                    "missing value for required option %s",o))
        }

        Query(qs,                                        // ....query string
              options("partitionColumn"),                // ....column name
              get("lowerBound")   .toLong,               // ....lower bound
              get("upperBound")   .toLong,               // ....upper bound
              get("numPartitions").toInt)                // ....partitions
      }
      else
      if (options.isDefinedAt("mfpi"))                   // ...implicit spec?
      {
        Query(qs,options("mfpi").toInt)                  // ....then use it
      }
      else                                               // ...none specified
      {
        Query(qs)                                        // ....accept default
      }
    }

    Relation(sc,ma,cq,options)                           // Construct relation
  }

  /**
   * Write the given DataFrame to the database and return a base relation with
   * which it can subsequently be read.
   *
   * Arguments are supplied as a collection of (key,value) pairs. They include
   * all of the options described above, as for reading; in particular:
   *
   *  - `dbtable`    The name of the target table to write to.
   *
   * Other optional arguments include:
   *
   *  - `url`:       A string of  the  form  "IRIS://host:port/namespace" that
   *                 specifies the cluster to which the data is to be written.
   *                 If omitted, the default cluster specified via the "spark.
   *                 iris.master.url" configuration setting is used instead.
   *
   *  - `user`:      The user account with which to make the connection to the
   *                 cluster named in the "url" option above.
   *
   *  - `password`:  The password for the given user account.
   *
   *  - `batchsize`: The number of rows to insert per server round trip.
   *
   *                 Default = 1000.
   *
   *  - `isolationlevel`: The transaction isolation level.
   *
   *                 Can be either NONE, REPEATABLE_READ, READ_COMMITTED,
   *                 READ_UNCOMMITTED, or SERIALIZABLE.
   *
   *                 Corresponds to the standard transaction isolation levels
   *                 specified by the JDBC Connection object.
   *
   *                 Default = READ_UNCOMMITTED.
   *
   *  - `description`: An optional description for the newly created table.
   *
   *  - `publicrowid`: Specifies whether or not the master row ID column for
   *                 the newly created table is to be made publicly visible.
   *
   *  - `shard`:     Indicates the records of the  table are to be distributed
   *                 across the  instances of the cluster;  the optional value
   *                 specifies the shard  key to use - see IRIS documentation
   *                 for CREATE TABLE for more details.
   *
   *  - `autobalance`: When writing a dataset to  a table that is sharded on a
   *                 system assigned shard key,  the value true specifies that
   *                 the inserted records are to be evenly distributed amongst
   *                 the available shards,while the value false specifies that
   *                 that they be sent to whichever shard is  closest to where
   *                 the partitions of the dataset reside.
   *
   *                 Default = true.
   *
   * @param sc       The context in which to create the new relation.
   * @param mode     Describes the behavior if the target table already exists.
   * @param options  The arguments with which to initialize the new relation.
   * @param df       The source data frame that is to be written.
   *
   * @return A new Relation that can be called upon to execute the given
   *         query on the cluster as needed.
   * @throws IllegalArgumentException if passed an invalid parameter value.
   * @throws Exception if the proposed write operation would certainly fail.
   * @see    [[http://docs.InterSystems.com/latest/csp/docbook/DocBook.UI.Page.cls?KEY=RSQL_createtable
   *         InterSystems IRIS SQL Reference]]  for more information on the
   *         options supported by the CREATE TABLE statement.
   * @see    [[https://docs.oracle.com/cd/E19830-01/819-4721/beamv/index.html
   *         Using JDBC Transaction Isolation Levels]]
   */
  def createRelation(sc: SQLContext,mode: SaveMode,options: Map[String,String],df: DataFrame): BaseRelation =
  {
    log.debug("createRelation({},{})",mode,options,"")   // Trace our progress

    val cm: Master = Master(options)                     // Fetch the master

    /**
     * Drop the given table from the IRIS database.
     */
    def drop(table: String): Unit =
    {
      log.info("Dropping table {}",table)                // Trace our progress

      cm.execute(s"DROP TABLE $table")                   // Execute DROP table
    }

    /**
     * Create the given table in the database.
     */
    def create(table: String): Unit =
    {
      log.info(s"Creating table $table")                 // Trace our progress

      val s = new StringBuffer(s"CREATE TABLE $table (") // The initial prefix

      s.append(schemaString(df,"jdbc:IRIS://"))          // Append the columns

      options.get("description").map(_.trim) match       // Match option value
      {
        case None         ⇒                              // ...nothing to do
        case Some(value)  ⇒ s.append(s", %DESCRIPTION '${value.replace("'","''")}'")
      }

      options.get("publicrowid").map(_.trim.toLowerCase) match
      {
        case None                                        // ...nothing to do
           | Some("false")⇒                              // ...nothing to do
        case Some("true") ⇒ s.append(s", %PUBLICROWID")  // ...public row id
        case Some(v)      ⇒ argex("Bad value '%s' for boolean option 'publicrowid'",v)
      }

      options.get("shard").map(_.trim.toLowerCase) match // Match option value
      {
        case None                                        // ...nothing to do
           | Some("false")⇒                              // ...nothing to do
        case Some("")                                    // ...system assigned
           | Some("true") ⇒ s.append(s", SHARD")         // ...system assigned
        case Some(k)      ⇒ s.append(s", SHARD KEY($k)") // ...user defined
      }

      options.get("autobalance").map(_.trim.toLowerCase) match
      {
        case None                                        // ...nothing to do
           | Some("true")                                // ...pass on to JDBC
           | Some("false")⇒                              // ...pass on to JDBC
        case Some(v)      ⇒ argex("Bad value '%s' for boolean option 'autobalance'",v)
      }

      s.append(')')                                      // Add trailing paren

      cm.execute(s.toString)                             // Execute statement
    }

    /**
     * When saving a dataframe partition in anything other than isolationlevel
     * 'NONE', Spark places a transaction around the entire insertion. Now the
     * default size of a partition can be large -  1M records or more -  which
     * triggers lock escalation on the server. This can lead to a deadlock  in
     * which the multiple JDBC connections created by the Spark slaves are all
     * stuck waiting for another to commit, an event which never happens.
     *
     * For now, we cannot support isolation levels other than READ_UNCOMMITTED
     * and NONE.
     */
    def PL147781: Map[String,String] =
    {
      options.getOrElse("isolationLevel","NONE") match
      {
        case "NONE"                                      // Fine, no problemo
           | "READ_UNCOMMITTED" ⇒                        // Effectively, NONE
        case  otherwise        ⇒                         // Not yet supported
          argex("Bad isolation level %s; only NONE and READ_UNCOMMITTED currently supported.",otherwise)
      }

      options + "isolationlevel" → "NONE"                // = AutoCommit(true)
    }

    val table: String =                                  // Target table name
    {
      if (options.isDefinedAt("dbtable"))                // ...got 'dbtable'?
      {
        options("dbtable")                               // ....then use it
      }
      else                                               // ...sigh, give up
      {
        argex("Option 'dbtable' not specified")          // ....complain
      }
    }

    val source = df.schema                               // The source schema
    val result = createRelation(sc,options)              // The query result
    val target = Try(result.schema)                      // The target schema

    if (target.isSuccess)                                // The target exists?
    {
      log.info("Table {} already exists",table)          // ...trace progress

      mode match                                         // Which save mode?
      {
        case SaveMode.Ignore ⇒                           // ignore altogether
        {
          return result                                  // ...skip the write
        }
        case SaveMode.ErrorIfExists ⇒                    // complain to user
        {
          argex(s"Table $table already exists")          // ...throw an error
        }
        case SaveMode.Overwrite ⇒                        // overwrite existing
        {
          drop  (table)                                  // ...drop the table
          create(table)                                  // ...then re-create
        }
        case SaveMode.Append  ⇒                          // append to existing
        {
          validate(table,source,target.get)              // ...validate schema
        }
      }
    }
    else
    {
      log.info("Table {} does not yet exist",table)      // ...trace progress

      create(table)                                      // ...and create now
    }

    saveTable(df,cm.address,table,PL147781)              // Delegate to Spark

    result                                               // Return the result
  }

  /**
   * Compares the schema of a source data frame with that of a target database
   * table to determine whether the two are compatible with one another before
   * actually writing to the table, when errors will be correspondingly harder
   * to diagnose and explain.
   *
   * Checks for the following basic problems:
   *
   *  - the source includes multiple columns with the same name
   *  - the source includes a column that is absent in the target table
   *  - the target includes a non-nullable column that is absent in the source
   *  - the source and target disagree on the type of a column -  no provision
   *    is yet made for the possibility of introducing a coercion
   *
   * @param table   The name of the target database table (for error messages).
   * @param source  The schema of the data frame being written to the database.
   * @param target  The schema of the target database table.
   *
   * @throws Exception if the proposed write operation would certainly fail.
   */
  private[core]
  def validate(table: String,source: Schema,target: Schema): Unit =
  {
    def fail(reason: String): Nothing =
    {
      core.fail("Can't write to table $table: $reason",table,reason)
    }

    /**
     * Pairs a field of the target table with a mutable flag to record whether
     * or not we have seen a matching field of the same type in the source.
     */
    class info(target: StructField,var seen: Boolean = false)
    {
      def mark(source: StructField) =
      {
        if (seen)
        {
          fail(s"the source dataframe contains more than one column named ${source.name}")
        }

        if (source.dataType != target.dataType)
        {
          fail(s"column ${source.name} has type ${source.dataType} in the source dataframe but type ${target.dataType}' in the target table $table")
        }

        seen = true
      }

      def unwritten = !seen && !target.nullable
    }

    val map = target.map(t ⇒ t.name.toUpperCase → new info(t)).toMap

    for (s ← source)
    {
      map.getOrElse(s.name.toUpperCase,fail(s"table $table has no column named ${s.name}"))
         .mark(s)
    }

    map.foreach{case(n,t) ⇒ t.unwritten && fail(s"the source dataframe has no column named $n")}
  }
}

//****************************************************************************
