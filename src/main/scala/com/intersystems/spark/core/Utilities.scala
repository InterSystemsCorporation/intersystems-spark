//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Defines a number of general purpose utility functions.
//*
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark
package core

import java.util.{Map ⇒ jMap,Properties}

import scala.collection.JavaConverters._

import resource.{Resource,ManagedResource}

import com.intersystems.jdbc.{IRISConnection}

/**
 * Defines a number of general purpose utility functions.
 *
 * @author Jonathon Bell
 */
trait Utilities extends Logging
{
  /**
   * Opens a JDBC connection into the given server.
   *
   * @param address     The connection details of the server to connect to.
   * @param properties  Additional properties needed to make the connection.
   *
   * @return An open connection into the given instance.
   * @throws SQLException if a database access error occurs.
   * @see    [[http://docs.intersystems.com/latest/csp/docbook/DocBook.UI.Page.cls?KEY=BGJD_connecting#BGJD_connecting_drivermgr IRIS JDBC Connection Properties]]
   *         for the full list of connection properties supported by the IRIS
   *         JDBC driver.
   */
  def connect(address: Address,properties: (String,AnyRef)*): IRISConnection =
  {
    val p = new Properties()                             // New property map

    properties.foreach{case (k,v) ⇒ p.put(k,v)}          // Add key-val pairs

    connect(address,p)                                   // Connect to server
  }

  /**
   * Opens a JDBC connection into the given server.
   *
   * @param address     The connection details of the server to connect to.
   * @param properties  Additional properties needed to make the connection.
   *
   * @return An open connection into the given instance.
   * @throws SQLException if a database access error occurs.
   * @see    [[http://docs.intersystems.com/latest/csp/docbook/DocBook.UI.Page.cls?KEY=BGJD_connecting#BGJD_connecting_drivermgr IRIS JDBC Connection Properties]]
   *         for the full list of connection properties supported by the IRIS
   *         JDBC driver.
   */
  def connect(address: Address,properties: Map[String,AnyRef]): IRISConnection =
  {
    val p = new Properties()                             // New property map

    properties.foreach{case (k,v) ⇒ p.put(k,v)}          // Add key-val pairs

    connect(address,p)                                   // Connect to server
  }

  /**
   * Opens a JDBC connection into the given server.
   *
   * @param address     The connection details of the server to connect to.
   * @param properties  Additional properties needed to make the connection.
   *
   * @return An open connection into the given instance.
   * @throws SQLException if a database access error occurs.
   * @see    [[http://docs.intersystems.com/latest/csp/docbook/DocBook.UI.Page.cls?KEY=BGJD_connecting#BGJD_connecting_drivermgr IRIS JDBC Connection Properties]]
   *         for the full list of connection properties supported by the IRIS
   *         JDBC driver.
   */
  def connect(address: Address,properties: jMap[String,AnyRef]): IRISConnection =
  {
    connect(address,properties.asScala.toMap)            // Connect to server
  }

  /**
   * Opens a JDBC connection into the given server.
   *
   * @param address     The connection details of the server to connect to.
   * @param properties  Additional properties needed to make the connection.
   *
   * @return An open connection into the given instance.
   * @throws SQLException if a database access error occurs.
   * @see    [[http://docs.intersystems.com/latest/csp/docbook/DocBook.UI.Page.cls?KEY=BGJD_connecting#BGJD_connecting_drivermgr IRIS JDBC Connection Properties]]
   *         for the full list of connection properties supported by the IRIS
   *         JDBC driver.
   */
  def connect(address: Address,properties: Properties): IRISConnection =
  {
    log.info("Connecting to {} from {} with {}",address,hostname,properties)

    var a = address                                      // The target address

 /* Our JDBC driver is optimized to  transfer data via shared memory rather
    than TCPIP when the requested host string is exactly "127.0.0.1", so if
    'address' is local, then we rewrite its host name now to take advantage
    of the optimization...*/

    if (a.isLocal)                                       // Same as our own?
    {
      a = new Address("127.0.0.1",                       // ...make explicit
                      a.port,                            // ...same port
                      a.namespace,                       // ...same namespace
                      a.user,                            // ...same user
                      a.password)                        // ...same password
    }

    new IRISConnection(a,properties)                     // Connect to server
  }

  /**
   * Manage the lifetime of a given resource by wrapping it within an instance
   * of the monadic ManagedResource type.  The caller may no longer access the
   * resource directly, but can, however, evaluate functions against it Within
   * the monad, as well as close it while retrieving a final value with a call
   * to the helper function `autoclose()`.
   *
   * @param open  A lazily evaluated expression that 'open's the resource.
   *
   * @see   [[http://jsuereth.com/scala-arm Scala ARM]] for full documentation
   *        of the Scala Automatic Resource Management library.
   */
  def mgd[α: Resource: Manifest](open: ⇒ α): ManagedResource[α] =
  {
    resource.managed(open)                               // Inject into monad
  }

  /**
   * Close the underlying resource being tracked by the given managed wrapper,
   * and return the value that it carries.
   *
   * @param managed  A managed resource wrapper.
   *
   * @see   [[http://jsuereth.com/scala-arm Scala ARM]] for full documentation
   *        of the Scala Automatic Resource Management library.
   */
  def autoclose[α](managed: ManagedResource[α]): α =
  {
    managed.acquireAndGet(identity)                      // And close resource
  }

  /**
   * Clamp the given value to lie within the closed interval [''lo'', ''hi''].
   *
   * @param lo  The lower bound of the range to which ''i'' is clamped.
   * @param i   The value to be clamped.
   * @param hi  The upper bound of the range to which ''i'' is clamped.
   *
   * @return The value ''i'', clamped to lie within the closed interval
   *         [''lo'', ''hi''].
   */
  def clamp[α: Ordering](lo: α,i: α,hi: α): α =
  {
    import Ordering.Implicits._                          // For comparison ops

    assert(lo <= hi);                                    // Validate arguments

    if (i < lo)                                          // Less than 'lo'?
      lo                                                 // ...clamp to 'lo'
    else
    if (i > hi)                                          // Greater than 'hi'?
      hi                                                 // ...clamp to 'hi'
    else                                                 // Within interval
      i                                                  // ...nothing to do
  }

  /**
   * Formats a printf-style message string and throws it as an Exception.
   *
   * @param format  A printf-style message template string.
   * @param args    Optional arguments to be spliced into the format string.
   *
   * @throws Exception - always
   */
  def fail(format: String,args: Any*): Nothing =
  {
    throw new Exception(format.format(args:_*))
  }

  /**
   * Formats a printf-style message string and throws it as an Exception.
   *
   * @param format  A printf-style message template string.
   * @param cause   The (possibly null) cause of the exception.
   * @param args    Optional arguments to be spliced into the format string.
   *
   * @throws Exception - always
   */
  def fail(format: String,cause: Throwable,args: Any*): Nothing =
  {
    throw new Exception(format.format(args:_*),cause)
  }

  /**
   * Formats a printf-style message string and throws it as an `IllegalArgumentException`.
   *
   * @param format  A printf-style message template string.
   * @param args    Optional arguments to be spliced into the format string.
   *
   * @throws IllegalArgumentException - always
   */
  def argex(format: String,args: Any*): Nothing =
  {
    throw new IllegalArgumentException(format.format(args:_*));
  }

  /**
   * Return the name of the host computer on which this instance of the JVM is
   * running, or an empty string if something goes wrong.
   *
   * @return The name of the host computer on which we are running.
   */
  def hostname: String =
  {
    com.intersystems.sqf.Utilities.hostname              // Delegate to SQF
  }

  /**
   * Extends the given traversable collection with additional methods.
   */
  implicit class TraversableEx (traversable: Traversable[_])
  {
    /**
     * Returns a string representation of the given collection, formatted as a
     * list of its elements.
     *
     * Overloads the library function of the same name to clamp the number of
     * elements included in the resulting string to a given maximum.
     *
     * @param max  The maximum number of elements to include in the result.
     *
     * @return A (possibly truncated) string representation of the collection,
     *         formatted as a list of its elements.
     */
    def mkString(max: ℕ): String =
    {
      mkString(max,"")
    }

    /**
     * Returns a string representation of the given collection, formatted as a
     * list of its elements.
     *
     * Overloads the library function of the same name to clamp the number of
     * elements included in the resulting string to a given maximum.
     *
     * @param max  The maximum number of elements to include in the result.
     * @param sep  A string with which to delimit the individual elements.
     *
     * @return A (possibly truncated) string representation of the collection,
     *         formatted as a list of its elements.
     */
    def mkString(max: ℕ,sep: String): String =
    {
      mkString(max,"",sep,"")
    }

    /**
     * Returns a string representation of the given collection, formatted as a
     * list of its elements.
     *
     * Overloads the library function of the same name to clamp the number of
     * elements included in the resulting string to a given maximum.
     *
     * @param max  The maximum number of elements to include in the result.
     * @param beg  An initial prefix to begin the resulting string with.
     * @param sep  A string with which to delimit the individual elements.
     * @param end  A final suffix to terminate the resulting string with.
     *
     * @return A (possibly truncated) string representation of the collection,
     *         formatted as a list of its elements.
     */
    def mkString(max: ℕ,beg: String,sep: String,end: String): String =
    {
      require(max >= 0)                                  // Validate arguments

      val n = traversable.size                           // Number of elements

      if (n < max + 2)                                   // Fits within limit?
      {
        traversable.mkString(beg,sep,end)                // ...ok, include them
      }
      else                                               // Exceeds the maximum
      {
        traversable.take(max)                            // ...so drop the rest
                   .mkString(beg,sep,s" ... ${n - max} more items$end")
      }
    }
  }

  /**
   * Extends the given schema with additional methods.
   */
  implicit class SchemaEx (schema: Schema)
  {
    /**
     * Returns a more readable string representation of the given schema.
     *
     * @param max  The maximum number of fields to include in the result.
     *
     * @return A (possibly truncated) string representation of the schema.
     */
    def asString(max: ℕ = 5): String =
    {
      schema.fields.take(max + 1)                        // For initial fields
            .map(f ⇒ f.name+": "+f.dataType.simpleString)// Format as a string
            .toSeq                                       // Coerce to sequence
            .mkString(max,"[",", ","]")                  // Collapse to string
    }
  }
}

//****************************************************************************
