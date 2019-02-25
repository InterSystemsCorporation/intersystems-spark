//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Represents the ability to factorize a query.
//*
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark
package core

/**
 * Represents the ability to factorize query.
 *
 * Abstracts the conversation between a client program and the master instance
 * of an IRIS cluster,  thus enabling multiple implementations to coexist with
 * one another.
 *
 * @see    [[Factorization]]
 * @author Jonathon Bell
 */
trait Factorizer extends Serializable
                    with Logging
{
  /**
   * Return a Spark SQL schema that describes the shape of the result set that
   * would be retrieved were we to execute the given query on the master IRIS
   * instance.
   *
   * @param query  The text of a query to be executed on the cluster.
   *
   * @return A schema that describes the shape of the result set obtained when
   *         executing the given query.
   * @throws SQLException if the argument string does not describe a valid SQL
   *         query.
   */
  def schema(query: String): Schema

  /**
   * Returns an equivalent factorization of the given sequence of queries that
   * includes at most `mfpi`  factors per distinct instance within the cluster
   * to which we are connected.
   *
   * The given queries are assumed to ''partition'' the desired result set, in
   * the sense that the sets of tuples they each select are pairwise disjoint,
   * yet their collective union spans the entire desired result set.
   *
   * @param queries  A non empty sequence of queries to factorize.
   * @param mfpi     The maximum acceptable number of factors per instance, or
   *                 0 if no limit is necessary.
   *
   * @return An equivalent factorization of the query, as computed by IRIS.
   */
  def factorize(queries: Seq[String],mfpi: ℕ): Factorization
}

//****************************************************************************
