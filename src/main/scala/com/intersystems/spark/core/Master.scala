//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Represents the master instance of an IRIS cluster.
//*
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark
package core

import org.apache.spark.sql.types.{DataType,StructField,StructType}
import com.intersystems.jdbc.IRISConnection
import com.intersystems.jdbc.IRIS
import Dialect.{getCatalystType,getSchemaQuery}

/**
 * Represents the master instance of an IRIS cluster.
 *
 * Class Master provides a concrete implementation of the Factorizer interface
 * It serves as a proxy for the master IRIS instance with, which it negotiates
 * to obtain connections into the shard instances themselves.
 *
 * @param address  The connection details of the master instance with which to
 *                 negotiate for connections into the shards of the cluster.
 *
 * @author Jonathon Bell
 */
class Master private
(
  val address: Address
)
extends Factorizer
{
  /**
   * Returns a string representation of the master instance for which we serve
   * as a proxy.
   */
  override
  def toString: String =
  {
    s"Master($address)"                                  // Format our address
  }

  /**
   * Return a Spark SQL schema that describes the shape of the result set that
   * would be retrieved were we to execute  the given query on the master IRIS
   * instance.
   *
   * @param query  The text of a query to be executed on the IRIS cluster.
   *
   * @return A schema that describes the shape of the result set obtained when
   *         executing the given query.
   * @throws SQLException if the argument string does not describe a valid SQL
   *         query.
   * @see    The file 'readme.md' for details of the type mapping we implement
   *         here.
   * @see    [[http://docs.intersystems.com/latest/csp/docbook/DocBook.UI.Page.cls?KEY=RSQL_datatype
   *         IRIS SQL Data Types]] for details on how IRIS projects types into
   *         JDBC.
   */
  def schema(query: String): Schema =
  {
    /**
     * Returns the field types that best describe the types of the columns for
     * the given result set meta data.
     *
     * @param md  The meta data describing the result of evaluating `query`.
     *
     * @return The field types that describe the corresponding columns of the
     *         given meta data.
     */
    def fields(md: java.sql.ResultSetMetaData): Seq[StructField] =
    {
      for {c ← 1 to md.getColumnCount} yield             // For each column
      {
        val n = md.getColumnName(c)                      // ...its name
        val t = md.getColumnType(c)                      // ...its type code
        val z = md.isNullable(c) != 0                    // ...its nullability
        val d = getCatalystType(t,                       // ...ask the dialect
                                md.getPrecision(c),      // ....only if needed
                                md.getScale(c))          // ....only if needed

        if (d.isEmpty)                                   // ...not recognized?
        {
          argex(s"Unsupported type $t:${md.getColumnTypeName(c)} for column $n")
        }

        StructField(n,d.get,z)                           // ...make field type
      }
    }

    /**
     * Connect to the shard master, ask it for meta data describing the shape
     * of the anticipated result set, then format this as an equivalent Spark
     * SQL schema.
     */
    evaluate(getSchemaQuery(s"($query)"),r ⇒ StructType(fields(r.getMetaData)))
  }

  /**
   * Returns an equivalent factorization of the given sequence of queries that
   * includes at most `mfpi`  factors per distinct instance within the cluster
   * to which we are connected.
   *
   * Here ''factorization'' refers to  the act of decomposing a query as a set
   * of `(query, instance)` pairs in such a way that when each query factor is
   * executed in parallel on its respective  instance and the results are then
   * combined, the resulting combined result set satisfies the original query.
   * These factors later give rise to the underlying partitions of an RDD.
   *
   * @param queries  A non empty sequence of queries to factorize.
   * @param mfpi     The maximum acceptable number of factors per instance, or
   *                 0 if no limit is necessary.
   *
   * @return An equivalent factorization of the query, as computed by IRIS.
   * @throws SQLException if a database access error occurs.
   */
  def factorize(queries: Seq[String],mfpi: ℕ): Factorization =
  {
    require(!queries.isEmpty,"empty query sequence")     // Validate arguments

    import scala.collection.JavaConverters._             // For Java coercions

    val f = connection.getMaster.getFactors(queries.asJava,mfpi).asScala

    log.debug(s"Server factorized queries:\n\n$queries\n\n as:\n\n$f\n")

    Factorization(f,None)                                // The factorization
  }

  /**
   * Executes the given statement on the master instance of the IRIS cluster.
   *
   * @param sql  An SQL Data Manipulation Language (DML) statement, or an SQL
   *             statement that returns nothing, such as a DDL statement.
   *
   * @return Either (1) the row count for SQL Data Manipulation Language (DML) statements
   *         or (2) 0 for SQL statements that return nothing.
   * @throws SQLException if the argument string does not describe a valid SQL
   *         command.
   */
  def execute(sql: String): ℤ =
  {
    log.debug("Executing on {}:\n\n {}\n",address,sql,"")// Log the statement

    autoclose                                            // Close when leaving
    {
      for {s ← mgd(connection.createStatement())}        // ...make statement
        yield s.executeUpdate(sql)                       // ....and execute it
    }
  }

  /**
   * Evaluates the given query on the master instance of the IRIS cluster.
   *
   * @param query   The text of a query to be evaluated on the master instance
   *                of the IRIS cluster.
   *
   * @param format  Formats the result set computed by evaluating the query on
   *                the master instance of the IRIS cluster.
   *
   * @return The result of applying the given format function to the result
   *         set computed by the master instance of the IRIS cluster.
   * @throws SQLException if the argument string does not describe a valid SQL
   *         query.
   */
  def evaluate[α](query: String,format: Format[α]): α =
  {
    log.debug("Evaluating on {}:\n\n {}\n",address,query,"")// Log the query

    autoclose                                            // Close when leaving
    {
      for {s ← mgd(connection.prepareStatement(query))   // ...prepare query
           r ← mgd(s.executeQuery())}                    // ....execute query
        yield format(s.executeQuery())                   // ....format result
    }
  }

  /**
   * Invokes the given class method with the given parameters on the master
   * instance of the IRIS cluster.
   *
   * @param klass   The name of the IRIS class whose method is to be invoked.
   *
   * @param method  The name of the IRIS ObjectScript class method to be invoked.
   *
   * @param params  The parameters to the IRIS ObjectScript class method being invoked.
   *
   * @return The result of applying the given class method to its parameters.
   *
   * @throws Any exception that the class method being invoked might throw.
   */
  def invoke(klass: String,method: String,params: AnyRef*): AnyRef =
  {
    log.debug("Invoking {}.{}({})",klass,method,params.mkString(","))

    connection.getMaster.getIRIS.classMethodBytes(klass,method,params:_*)
  }

  /**
   * Invokes the given class method with the given parameters on the master
   * instance of the IRIS cluster.
   *
   * @param klass   The name of the IRIS class whose method is to be invoked.
   *
   * @param method  The name of the IRIS ObjectScript class method to be invoked.
   *
   * @param params  The parameters to the IRIS ObjectScript class method being invoked.
   *
   * @throws Any exception that the class method being invoked might throw.
   */
  def invokeWithStatus(klass: String,method: String,params: AnyRef*): Unit =
  {
    log.debug("Invoking {}.{}({})",klass,method,params.mkString(","))

    connection.getMaster.getIRIS.classMethodStatusCode(klass,method,params:_*)
  }

  /**
   * Returns an open connection to the master instance for which we serve as a
   * proxy.
   *
   * @return An open connection into the master IRIS instance.
   * @throws SQLException if a database access error occurs.
   */
  private
  def connection: IRISConnection =
  {
    Master.connections.connect(address)                  // Defer to the cache
  }
}

/**
 * Controls access to one or more [[Master]] objects.
 *
 * Manages a cache of open JDBC connections that are indexed by their address,
 * and sets up a background thread to reap those that look like they have been
 * abandoned by the user.
 *
 * @author Jonathon Bell
 */
object Master extends Logging
{
  import Config._                                        // Config parameters

  Dialect.register                                       // Register dialect

  /**
   * Manages a cache of leased connections that are automatically discarded if
   * abandoned by the user.
   */
  private
  object connections extends Thread("Master.reaper")
  {
    /**
     * Pairs an open server connection with a mutable timestamp that indicates
     * when the lease on the connection expires. The lease is renewed whenever
     * the connection is accessed.
     */
    class Lease (connection: IRISConnection,var expires: Long = 0)
    {
      assert(!connection.isClosed)                       // Validate argument

      /**
       * Returns true if the connection appears to have been abandoned.
       */
      def expired: Boolean =
      {
        System.currentTimeMillis >= expires              // Abandoned by user?
      }

      /**
       * Renews the lease on the connection by updating its expiration time.
       */
      def renew(): IRISConnection =
      {
        assert(!connection.isClosed)                     // Check it is open

        expires = System.currentTimeMillis + EXPIRES_MS  // Update expiration

        connection                                       // Return connection
      }

      /**
       * Closes the connection and returns true if successful.
       */
      def close(): Boolean =
      {
        log.info("Closing connection to {} from {}",connection.getMaster.address,hostname,"")

        connection.close()                               // Close connection
        connection.isClosed                              // Did we close it?
      }
    }

    /**
     * Returns an open connection to the given master IRIS instance.
     *
     * @param to  The connection details of the master to connect to.
     *
     * @return An open connection to the given master.
     * @throws SQLException if a database access error occurs.
     */
    def connect(to: Address): IRISConnection = synchronized
    {
      map.getOrElseUpdate(to,new Lease(core.connect(to)))
         .renew()
    }

    /**
     * Periodically awaken to run through the cache of open master connections
     * and purge those that have expired.
     */
    override
    def run() =
    {
      while (true)                                       // Until session ends
      {
        Thread.sleep(REAP_PERIOD_MS)                     // ...sleep for a bit

        synchronized
        {
       /* Close and remove connections that have not been used recently...*/

          map.retain((_,l) ⇒ !l.expired || !l.close())   // ...close + remove
        }
      }
    }

    val map=scala.collection.mutable.Map[Address,Lease]()// Connection cache

    log.info("Starting thread {}",getName)               // Trace our progress

    scala.sys.addShutdownHook                            // Before terminating
    {
      map.values.foreach(_.close())                      // ...close them all
    }

    setDaemon(true)                                      // Killed on shutdown
    start()                                              // Set reaper running
  }

  /**
   * Returns a string representation of the contents of the connection cache.
   */
  override
  def toString: String =
  {
    connections.map.keys.mkString("[",", ","]")          // List the addresses
  }

  /**
   * Connects to the master instance of the default IRIS cluster specified in
   * the "spark.iris.master.url" configuration parameter.
   *
   * @return A proxy for the master instance of the default IRIS cluster.
   */
  def apply(): Master =
  {
    apply(DEFAULT_MASTER_ADDRESS)                        // The default master
  }

  /**
   * Connects to the master instance of the given IRIS cluster.
   *
   * @param address  The connection details of the master instance of an IRIS
   *                 cluster.
   *
   * @return A proxy for the master instance of the given IRIS cluster.
   */
  def apply(address: Address): Master =
  {
 /* Use the normalized address to index the master, since this increases the
    chance of being able to reuse an existing connection...*/

    new Master(address.normalize)                        // Normalize and wrap
  }

  /**
   * Connects to the master instance of the given IRIS cluster or else to the
   * default cluster specified in the Spark configuration.
   *
   * The connection details are specified via the following (key,value) pairs:
   *
   *  - `url`:       a string of the form "IRIS://host:port/namespace"
   *  - `user`:      the user account with which to make the connection
   *  - `password`:  the password for the given user account
   *
   * If the given map contains no such "url" key, then return a proxy for the
   * master instance of the default IRIS cluster instead.
   *
   * @param map  A map that may or may not contain the connection details for
   *             the master instance of an IRIS cluster.
   *
   * @return A proxy for the master instance of the given IRIS cluster or for
   *         the default cluster if the map is not defined at the key "url".
   */
  def apply(map: Map[String,String]): Master =
  {
    apply(Address.parse(map))                            // Parse from the map
  }
}

//****************************************************************************
