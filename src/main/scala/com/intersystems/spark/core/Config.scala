//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Manages session wide configuration settings.
//*
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark
package core

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Manages session wide configuration settings.
 *
 * Centralizes the parsing of configuration parameters so that the rest of the
 * code is largely relieved of the burden of detecting such errors as the user
 * neglecting to specify a required setting and so on.
 *
 * @author Jonathon Bell
 */
object Config extends Logging
{
  /**
   * The session wide configuration settings.
   */
  private
  //val conf: SparkConf = new SparkConf()                  // Parse the settings
  val conf: SparkConf =  SparkSession.builder().getOrCreate().sparkContext.getConf

  /**
   * Parses the configuration settings for an integral value of the given key,
   * clamping it to the closed interval [''lo'', ''hi''], or returns the given
   * default value if no such configuration setting was specified.
   *
   * The connector's own settings live under the "spark.iris" group of keys.
   *
   * @param key      The name of the setting to parse, but without its initial
   *                 "spark.iris." prefix.
   * @param default  The default value to return if no such setting was found.
   * @param lo       A  lower bound for the value returned.
   * @param hi       An upper bound for the value returned.
   *
   * @return The integral value of the setting ''key'', clamped to lie within
   *         the closed interval [''lo'', ''hi''],  or ''default'' if no such
   *         setting was specified.
   */
  private
  def int(key: String,default: ℤ,lo: ℤ = Int.MinValue,hi: ℤ = Int.MaxValue) =
  {
    assert(lo <= default && default <= hi)               // Validate arguments

    clamp(lo,conf.getInt(s"spark.iris.$key",default),hi) // Parse and clamp it
  }

  /**
   * Parses the configuration settings for a string value of the given key, or
   * returns the given default if no such configuration setting was specified.
   *
   * The connector's own settings live under the "spark.iris" group of keys.
   *
   * @param key      The name of the setting to parse, but without its initial
   *                 "spark.iris." prefix.
   * @param default  The default value to return if no such setting was found.
   *
   * @return The value of the given key, or the given default value if no such
   *         setting was specified.
   */
  private
  def str(key: String,default: String = ""): String =
  {
    conf.get(s"spark.iris.$key",default)                 // Get value for key
  }

  /**
   * The default cluster to connect to when reading or writing datasets in the
   * "com.intersystems.spark" format.
   *
   * This value can be overridden by the "url", "user", and "password" options
   * of the DataFrameReader/Writer structures.
   *
   * @throw  Exception if the Spark configuration fails to specify a value for
   *         the "spark.iris.master.url" configuration setting.
   */
  def DEFAULT_MASTER_ADDRESS: Address =
  {
    val url = str("master.url")                          // Get value for key

    if (url.isEmpty)                                     // Setting not found?
    {
      fail("No default cluster specified for configuration setting 'spark.iris.master.url'")
    }

    new Address(url,                                     // The instance URL
                str("master.user"),                      // The user account
                str("master.password"))                  // The user password
  }

  /**
   * The period (in milliseconds) between successive cycles of the Master
   * reaper thread that closes and recycles abandoned server connections.
   */
  val REAP_PERIOD_MS: ℕ = 30 * 1000                      // Every 30 seconds

  /**
   * The time (in milliseconds) idle after which we consider a Master to
   * have been abandoned by the user.
   *
   * Abandoned Masters are closed and collected by the Master daemon
   * reaper thread.
   */
  val EXPIRES_MS: ℕ = int("master.expires",60,5) * 1000  // Duration in millis

  /**
   * Returns the host name of the Spark worker on which queries for a specific
   * instance prefer to run.
   *
   * In general,  Spark clusters run at most one Spark worker instance on each
   * host, while an IRIS cluster may run multiple instances on the hosts. Thus
   * Spark workers are identified by their host names while IRIS instances are
   * identified by Address objects.
   *
   * Ideally, each shard shares a host with its own dedicated Spark worker; in
   * this case the query results are consumed where they are generated, and no
   * records need move across the network.
   *
   * It can happen however that the host name by which the  Spark master knows
   * its worker differs from the host name by which the shard master knows its
   * shard, even though they are in fact running on the same host; each may be
   * running in its own separate Docker container, for example.
   *
   * The WORKER_HOST setting enables the configuration to override the default
   * identity mapping of shard address to Spark worker host name by specifying
   * a string of the form `pattern => rewrite` where:
   *
   *  - ''pattern'' is a regular expression (which can contain paren groups)
   *
   *  - ''rewrite'' is a replacement string (which can contain $ references to
   *                those groups),
   *
   * The ''pattern'' and ''rewrite''  strings are used to construct a function
   * that transforms the address of a shard instance into the host name of the
   * Spark worker on which queries for that shard prefer to run. This function
   * simply replaces all occurrences of ''pattern'' within the host name of an
   * address with ''rewrite''.
   *
   * @param shard  The address of a server instance.
   *
   * @return The host name of the Spark worker on which queries for the given
   *         instance prefer to run.
   */
  val WORKER_HOST: Address ⇒ String =
  {
    var m = (shard: Address) ⇒ shard.host               // The default mapping
    val v = str("worker.host")                          // Get value for key

    if (!v.isEmpty)                                     // User specified it?
    {
      try                                               // ...in case its bad
      {
        val Array(p,r) = v.split("⇒|=>")                // ...split the value
                          .map(_.trim)                  // ...trim whitespace

        log.info("Compiling host map '{} => {}'",p,r,"")// ...trace the pieces

        val pattern = p.r                               // ...compile pattern

        m = shard ⇒ pattern.replaceAllIn(shard.host,r)  // ...and update map
      }
      catch
      {
        case e: MatchError ⇒ log.error("Bad value for configuration setting spark.cache.worker.host\nWanted 'pattern ⇒ rewrite' but got '{}'",v)
        case e: Exception  ⇒ log.error("Bad value for configuration setting spark.cache.worker.host\n{}",e.getMessage)
      }
    }

    m                                                   // Compiled host map
  }
}

//****************************************************************************
