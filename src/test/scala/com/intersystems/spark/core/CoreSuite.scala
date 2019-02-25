//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Defines a common trait for the core unit test suites.
//*
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark
package core

import org.apache.spark.{SparkConf,SparkContext}
import org.scalatest.FunSuite

/**
 * Defines a common trait for the core unit test suites.
 *
 * @author Jonathon Bell
 */
trait CoreSuite extends FunSuite with CoreTestUtilities
{
  /**
   * The shared SparkContext.
   */
  val sc: SparkContext = CoreSuite.sc

  /**
   * The shared query factorizer.
   */
  val factorizer = CoreSuite.factorizer
}

/**
 * Companion object for trait CoreSuite.
 *
 * @author Jonathon Bell
 */
object CoreSuite extends Logging
{
  val c: SparkConf = new SparkConf()

  /* Provide sensible default values for the various keys that parameterize
     the test suite...*/

  c.set("spark.master",           "local")
  c.set("spark.log.level",        "WARN")
  c.set("spark.app.name",         "intersystems-spark unit tests")
  c.set("spark.iris.master.url",   "IRIS://M:0/M")
  c.set("spark.iris.master.shards","IRIS://A:0/A")

  /* Now overlay any similarly named values inherited from the environment...*/

  for ((k,v) ← sys.env if k.startsWith("spark."))
  {
    c.set(k,v)
  }

  val sc = new SparkContext(c)

  /* If the IRIS master URL was not overwritten, install a MockFactorizer in
     its place...*/

  val factorizer: Factorizer =
  {
    if (c.get("spark.iris.master.url") == "IRIS://M:0/M")
    {
      new MockFactorizer(c.get("spark.iris.master.shards")
                          .trim
                          .split(",")
                          .map(new Address(_)))
    }
    else
    {
      Master(c.getAllWithPrefix("spark.iris.master.").toMap)
    }
  }

  log.info("Testing with {}",factorizer)

  /* And finally, adjust the logging level ...*/

  sc.setLogLevel(c.get("spark.log.level"))
}

//****************************************************************************
