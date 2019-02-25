//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Registers class DataSource as the default provider for the
//*            format "com.intersystems.spark".
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark

/**
 * Registers the InterSystems IRIS Spark Connector  as a Spark SQL data source
 * provider for the format "com.intersystems.spark", also known by its shorter
 * alias "iris".
 *
 * This allows clients to execute queries against a cluster by calling Spark's
 * generic load and save functions. For example:
 * {{{
 *    spark.read
 *         .format("com.intersystems.spark")
 *         .option("query","SELECT * FROM Owls")
 *         .load()
 * }}}
 * executes the query `"SELECT * FROM Owls"` on the default cluster, and hands
 * its rows back in the form of an appropriately partitioned DataFrame.
 *
 * Here `read` means 'execute a SELECT statement against the database',  while
 * `write` means 'execute batch INSERT statements against a database table'.
 *
 * @see    [[http://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources
 *         Apache Spark Documentation]] for more on how to use the generic load
 *         and save functions.
 *
 * @author Jonathon Bell
 */
final class DefaultSource extends com.intersystems.spark.core.DataSource

//****************************************************************************
