//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Augments the Java class Address with additional functionality.
//*
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark
package core

import java.net.URI

/**
 * Augments the Java class Address with additional functionality.
 *
 * Implements a 'companion' object for class Address, though technically not a
 * companion in the sense that it has no special access to the class's private
 * members (of which, however, there are none).
 *
 * @author Jonathon Bell
 */
object Address
{
  /**
   * Returns the address of the default cluster to use when reading or writing
   * a dataset or RDD in the "com.intersystems.spark" format.
   *
   * This address is specified in the Spark configuration as the values of the
   * keys "spark.iris.master.{url,user,password}."
   */
  @inline
  def apply(): Address =
  {
    Config.DEFAULT_MASTER_ADDRESS                   // The default master
  }

  /**
   * Parses a connection description from a universal resource locator string.
   *
   * The credentials will be supplied later when the connection is made.
   *
   * @param url  A string of the form "IRIS://host:port/namespace".
   *
   * @return The connection details for the specified server.
   * @throws Exception if the URL is ill-formed.
   */
  @inline
  def apply(url: String): Address =
  {
    new Address(url)                                     // Create the address
  }

  /**
   * Creates a connection description from a universal resource identifier.
   *
   * The credentials will be supplied later when the connection is made.
   *
   * @param uri  Specifies the server to connect to.
   *
   * @return The connection details for the specified server.
   * @throws Exception if the URI is ill-formed.
   */
  @inline
  def apply(uri: URI): Address =
  {
    new Address(uri)                                     // Create the address
  }

  /**
   * Creates a connection description from its constituent details.
   *
   * The credentials will be supplied later when the connection is made.
   *
   * @param host       The host on which the server is running.
   * @param port       The port on which the server listens for connections.
   * @param namespace  The namespace in which the server is running.
   *
   * @return The connection details for the specified server.
   * @throws Exception if the arguments are ill-formed.
   */
  @inline
  def apply(host: String,port: ℕ,namespace: String): Address =
  {
    new Address(host,port,namespace)                     // Create the address
  }

  /**
   * Parses a connection description from a universal resource locator string.
   *
   * @param url       A string of the form "IRIS://host:port/namespace".
   * @param user      The user account with which to make the connection.
   * @param password  The password for the given user account.
   *
   * @return The connection details for the specified server.
   * @throws Exception if the URL is ill-formed.
   */
  @inline
  def apply(url: String,user: String,password: String): Address =
  {
    new Address(url,user,password)                       // Create the address
  }

  /**
   * Creates a connection description from a universal resource identifier.
   *
   * @param uri       Specifies the server to connect to.
   * @param user      The user account with which to make the connection.
   * @param password  The password for the given user account.
   *
   * @return The connection details for the specified server.
   * @throws Exception if the URI is ill-formed.
   */
  @inline
  def apply(uri: URI,user: String,password: String): Address =
  {
    new Address(uri,user,password)                       // Create the address
  }

  /**
   * Creates a connection description from its constituent details.
   *
   * @param h  The host on which the server is running.
   * @param p  The port on which the server listens for connections.
   * @param n  The namespace in which the server is running.
   * @param u  The user account with which to make the connection.
   * @param z  The password for the given user account.
   *
   * @return The connection details for the specified server.
   * @throws Exception if the arguments are ill-formed.
   */
  @inline
  def apply(host: String,port: ℕ,namespace: String,user: String,password: String): Address =
  {
    new Address(host,port,namespace,user,password)       // Create the address
  }

  /**
   * Parses the address of a server by searching the keys of a property map.
   *
   * The connection details are specified via the following (key,value) pairs:
   *
   *  - `url`:       a string of the form "IRIS://host:port/namespace"
   *
   *  - `user`:      the user account with which to make the connection
   *
   *  - `password`:  the password for the given user account
   *
   * Returns the default cluster address specified via the Spark configuration
   * settings "spark.iris.master.{url,user,password}" if the map is not defined
   * at the key "url".
   *
   * @param map  A map that may or may not contain the connection details of a
   *             server.
   *
   * @return The connection details of the specified instance,  or the address
   *         of the default cluster if the given map is not defined at the key
   *         "url".
   */
  def parse(map: Map[String,String]): Address =
  {
    val o = map.get("url")                               // Fetch the key "url"

    if (o.isEmpty)                                       // No such key found?
    {
      Config.DEFAULT_MASTER_ADDRESS                      // ...use the default
    }
    else                                                 // Found the key "url"
    {
      val u = map.getOrElse("user","")                   // ...fetch user
      val p = map.getOrElse("password","")               // ...fetch password

      new Address(o.get,u,p)                             // ...create address
    }
  }
}

//****************************************************************************
