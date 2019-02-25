//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Implements a shard-aware Resilient Distributed Dataset (RDD).
//*
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark
package core

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Try

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition,SparkContext,TaskContext}

/**
 * Implements a shard-aware Resilient Distributed Dataset (RDD).
 *
 * Class ShardedRDD makes a separate partition for each query factor specified
 * at construction, and tries to arrange for each to execute in a Spark worker
 * that runs on the same host as the shard instance to which it connects, thus
 * minimizing the transfer of data across the network.
 *
 * Partitions connect to their shard workers via JDBC.
 *
 * @param sc       The context in which to create the new RDD.
 * @param factors  The factors of the original query.
 * @param format   Formats an element from a single row of a result set.
 * @param optins   Additional options with which to make the connection into
 *                 each shard.
 *
 * @author Jonathon Bell
 */
class ShardedRDD[α: ClassTag] private
(
  sc     : SparkContext,
  factors: Seq[Factor],
  format : Format[α],
  options: Map[String,String]
)
extends RDD[α](sc,Nil)
{
  /**
   * Represents a single partition of the parent RDD.
   *
   * The partition index also uniquely identifies the corresponding factor of
   * the original query.
   *
   * @param index  The index by which Spark uniquely identifies this partition.
   */
  case class partition (index: ℕ) extends Partition

  /**
   * Return the factor corresponding to the given partition.
   *
   * @param p  A single partition of the parent RDD.
   *
   * @return The factor corresponding to the given partition.
   */
  private implicit
  def toPartition(p: Partition): Factor =
  {
    factors(p.index)                                     // Subscript by index
  }

  /**
   * Construct a separate partition for each factor of the original query.
   *
   * @return An array of partitions, one per query factor.
   */
  def getPartitions: Array[Partition] =
  {
    log.info("Creating {} partitions",factors.size)      // Log the dimensions

    Array.tabulate(factors.size)(partition(_))           // Create partitions
  }

  /**
   * Return the host name of the shard instance in which the rows of the given
   * partition are to be materialized.
   *
   * @param p  A single partition of the parent RDD.
   *
   * @return The host name of the instance that is to materialize the rows for
   *         the given partition.
   */
  override
  def getPreferredLocations(p: Partition): Seq[String] =
  {
    val h = Config.WORKER_HOST(p.address)                // Apply host mapping
    val a = Try(p.address.ipAddress).getOrElse("")       // Append raw address

    log.info("{} prefers to run on host {} [{}]",p,h,a)  // Log preferred host

    Seq(h,a)                                             // Colocate with host
  }

  /**
   * Materialize the elements of the given partition.
   *
   * We open a JDBC connection to the host shard, evaluate a query against it,
   * and return an iterator that applies the formatting function passed to our
   * constructor to each row of the result set, so computing the corresponding
   * elements of the partition. If all was set up right, this method is called
   * from within a closure that is sent across the network to the same node of
   * the cluster on which the data resides.
   *
   * @param p  The partition whose elements we are to materialize.
   * @param k  The context in which the task associated with this method call
   *           is executing.
   */
  def compute(p: Partition,k: TaskContext): Iterator[α] = new Iterator[α]
  {
    val c = connect(p.address,options)                   // Connect to shard
    val s = c.createStatement                            // Create a statement
    val r = s.executeQuery(p.query)                      // Execute the query
    var n = false                                        // Is next row ready?

    k.addTaskCompletionListener(_ ⇒ close())             // Remember to close

    def close() =                                        // Close connections
    {
      def close(ac: AutoCloseable,s: String) =           // ...close an object
      {
        if (ac != null) try ac.close() catch             // ....try to close
        {
          case e: Exception ⇒ log.warn("Exception closing " + s,e)
        }
      }

      close(r,"result set")                              // ...close result set
      close(s,"statement")                               // ...close statement
      close(c,"connection")                              // ...close connection
    }

    @inline
    def hasNext: Boolean =                               // Rows yet to visit?
    {
      n ||= r.next() ; n                                 // Look ahead one row
    }

    def next: α =                                        // Format another row
    {
      if (hasNext)                                       // Rows yet to visit?
      {
        n = false                                        // ...reset lookahead
        format(r)                                        // ...format next row
      }
      else                                               // Seen the final row
      {
        throw new NoSuchElementException("End of result set")
      }
    }
  }
}

/**
 * Companion object for class ShardedRDD.
 *
 * @author Jonathon Bell
 */
object ShardedRDD
{
  /**
   * Construct an RDD that includes a separate partition for each query factor
   * and that tries to arrange for each to execute in a Spark worker that runs
   * on the same host as the shard worker to which it connects.
   *
   * Non-trivial reductions are not currently supported.
   *
   * @param sc       The context in which to create the new RDD.
   * @param factors  The factorization of a query.
   * @param format   Formats an element from a single row of a result set.
   * @param options  Additional options with which to make the connection into
   *                 each shard.
   *
   * @return The results of the query in the form of a suitably partitioned RDD.
   */
  def apply[α: ClassTag](sc: SparkContext,query: Factorization,format: Format[α],options: Map[String,String] = Map()): RDD[α] =
  {
    require(query.reduction.isEmpty,"only trivial reductions currently supported")

    new ShardedRDD(sc,query.factors,format,options)      // A union of factors
  }
}

//****************************************************************************
