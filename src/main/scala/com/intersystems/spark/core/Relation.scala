//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Represents a collection of tuples obtained by executing a query
//*            on an IRIS cluster.
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark
package core

import org.apache.spark.rdd.{RDD}
import org.apache.spark.sql.{Row,SQLContext}
import org.apache.spark.sql.sources.{BaseRelation,Filter,PrunedFilteredScan}

/**
 * Represents a collection of tuples obtained by evaluating a query on an IRIS
 * cluster.
 *
 * @param sqlContext  The context in which to create the relation.
 * @param address     The address of the master instance of the cluster.
 * @param query       The text of the query to evaluate on the cluster.
 * @param options     Additional options with which to make each connection to
 *                    the individual shards of the cluster.
 *
 * @author Jonathon Bell
 */
case class Relation
(
  sqlContext: SQLContext,
  address   : Address,
  query     : Query,
  options   : Map[String,String] = Map()
)
extends BaseRelation with PrunedFilteredScan with Logging
{
  /**
   * Connects to and returns the master instance of the cluster upon which our
   * query is to be evaluated.
   *
   * @return The master instance of the cluster that will evaluate this query.
   * @throws SQLException if a database access error occurs.
   */
  def master: Master =
  {
    Master(address)                                      // Connect to master
  }

  /**
   * Describes the number and type of tuple components the relation will later
   * compute, formatted as a Spark schema.
   *
   * @return A schema describing the number and type of tuple components to be
   *         computed.
   * @throws SQLException if the argument string does not describe a valid SQL
   *         query.
   */
  lazy val schema: Schema =
  {
    Query.validate(master.schema(query.sql))             // Delegate to master
  }

  /**
   * Returns true to indicate that the relation delivers rows in the Spark SQL
   * InternalRow format, which represents native types like Int, Long, Double,
   * and so on in a compact form that obviates the need for boxing.
   */
  override
  def needConversion: Boolean = false                    // Uses internal rows

  /**
   * Returns the tuples resulting  from the execution of our query on the IRIS
   * cluster in the form of a suitably partitioned RDD.
   *
   * @param columns  The subset of columns to include in the resulting RDD.
   * @param filters  A set of filters that restrict the rows to include in the
   *                 resulting RDD.
   *
   * @return The results of the query in the form of a suitably partitioned RDD.
   */
  def buildScan(columns: Array[String],filters: Array[Filter]): RDD[Row] =
  {
    log.debug("buildScan({},{})",columns,filters,"")     // Trace our progress

    val(s,c) = Query.prune(schema,columns)               // Prune the columns

    ShardedRDD(sqlContext.sparkContext,                  // The spark context
               query.factorize(master,c,filters),        // The query factors
               Relation.asInternalRow(s),                // The row formatter
               options)                                  // The JDBC options
              .asInstanceOf[RDD[Row]]                    // Forget 'internal'
  }

  /**
   * Remove from the given list of filters those that have direct translations
   * into InterSystems' dialect of the SQL language.
   *
   * The remaining filters will be computed on the worker by Spark itself.
   *
   * @param filters  An array of filters.
   *
   * @return Those filters containing an occurrence of an operator that is not
   *         directly expressible in InterSystems SQL.
   */
  override
  def unhandledFilters(filters: Array[Filter]): Array[Filter] =
  {
    Query.unhandledFilters(filters)                      // Delegate to query
  }

  /**
   * Returns a string representation of this relation for inclusion within the
   * output of the Spark explain() operator.
   */
  override
  def toString: String =
  {
    s"Relation(${query.sql})"                            // Include query text
  }
}

/**
 * Companion object for class Relation.
 *
 * Houses the coercion machinery needed to format a row of the result set as a
 * Spark SQL InternalRow.
 *
 * @author Jonathon Bell
 */
object Relation extends Logging
{
  import java.sql.{ResultSet,Date,Timestamp}
  import org.apache.spark.unsafe.types.UTF8String.{fromString}
  import org.apache.spark.sql.catalyst.util.DateTimeUtils.{fromJavaDate,fromJavaTimestamp}
  import org.apache.spark.sql.types._

  /**
   * Extends class MutableRow with helper functions for writing compound types
   * into a mutable row.
   */
  private implicit
  class MutableRowEx(val row: MutableRow) extends AnyVal
  {
    @inline
    def setString(i: ℕ,v: String) =
    {
      row.update(i,fromString(v))
    }

    @inline
    def setDecimal2(i: ℕ,v: BigDecimal,d: DecimalType) = v match
    {
      case null ⇒ row.setNullAt(i)
      case  _   ⇒ row.update(i,Decimal(v,d.precision,d.scale))
    }

    @inline
    def setDate(i: ℕ,v: Date) = v match
    {
      case null ⇒ row.setNullAt(i)
      case  _   ⇒ row.setInt(i,fromJavaDate(v))
    }

    @inline
    def setTimestamp(i: ℕ,v: Timestamp) = v match
    {
      case null ⇒ row.setNullAt(i)
      case  _   ⇒ row.setLong(i,fromJavaTimestamp(v))
    }
  }

  /**
   * Return a function that formats each record of a result set as an internal
   * row.
   *
   * An internal row is a compact data structure that represents native atomic
   * types like Date, Long, Double, and so on, in a compact form that obviates
   * the need for boxing and unboxing.
   *
   * Furthermore, ''the same'' mutable internal row is returned from each call
   * to the returned format function, thus avoiding the cost of allocating and
   * garbage collecting each row; apparently this is safe, because Spark's own
   * JDBC data source uses this very same technique.
   *
   * @param schema  The description of the number and type of tuple components
   *                to be computed for each row.
   *
   * @return A function that formats the current row of a JDBC result set as a
   *         mutable internal row whose shape is described by `schema`.
   */
  def asInternalRow(schema: Schema): Format[MutableRow] =
  {
    log.debug("asInternalRow({})",schema.asString())     // Trace our progress

    /**
     * A function that reads a given value from a given result set, and stores
     * it in the same slot of the given mutable row, coercing it if the source
     * and target have different types.
     */
    type Coercion = (MutableRow,ℕ,ResultSet) ⇒ Unit

    /**
     * Returns an appropriate function for coercing the values of a result set
     * column into the target field of a mutable internal row.
     *
     * @param field  The type of the target field in the row being written to.
     *
     * @return A coercion from the values of a column of a result set into the
     *         target field of the row being written.
     */
    def coercion(field: StructField): Coercion = field.dataType match
    {
      case BinaryType    ⇒ (m,i,r) ⇒ m.update            (i,r.getBytes(i+1))
      case BooleanType   ⇒ (m,i,r) ⇒ m.setBoolean        (i,r.getBoolean(i+1))
      case ByteType      ⇒ (m,i,r) ⇒ m.setByte           (i,r.getByte(i+1))
      case DateType      ⇒ (m,i,r) ⇒ m.setDate           (i,r.getDate(i+1))
      case d:DecimalType ⇒ (m,i,r) ⇒ m.setDecimal2       (i,r.getBigDecimal(i+1),d)
      case DoubleType    ⇒ (m,i,r) ⇒ m.setDouble         (i,r.getDouble(i+1))
      case FloatType     ⇒ (m,i,r) ⇒ m.setFloat          (i,r.getFloat(i+1))
      case IntegerType   ⇒ (m,i,r) ⇒ m.setInt            (i,r.getInt(i+1))
      case LongType      ⇒ (m,i,r) ⇒ m.setLong           (i,r.getLong(i+1))
      case ShortType     ⇒ (m,i,r) ⇒ m.setShort          (i,r.getShort(i+1))
      case StringType    ⇒ (m,i,r) ⇒ m.setString         (i,r.getString(i+1))
      case TimestampType ⇒ (m,i,r) ⇒ m.setTimestamp      (i,r.getTimestamp(i+1))
      case _             ⇒ (m,i,r) ⇒ m.update            (i,r.getObject(i+1))
    }

    val m = new MutableRow(schema.fields.map(_.dataType))// Mutable record
    val c = schema.fields.map(coercion)                  // Coercion array

    /**
     * Return a lambda that 'closes over' the constants 'm' and 'c', thus
     * avoiding their reconstruction each time the function is applied.
     */
    (r: ResultSet) ⇒                                     // Given a result set
    {
      var i = 0                                          // ...current field

      while (i < c.size)                                 // ...for each field
      {
        c(i)(m,i,r)                                      // ....call coercion
        i += 1                                           // ....next field
      } ; m                                              // The resulting row
    }
  }
}

//****************************************************************************
