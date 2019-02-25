//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : A mock implementation of the Factorizer interface.
//*
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark
package core

/**
 * A mock implementation of the Factorizer interface.
 *
 * Simulates the master instance of a cluster that consists of the given shard
 * instances, all of whose tables share the same schema and are sharded evenly
 * across the instances of the cluster.
 *
 * @param shards  The addresses of our shard instances.
 *
 * @author Jonathon Bell
 */
class MockFactorizer (shards: Seq[Address]) extends Factorizer
                                               with CoreTestUtilities
{
  def schema(query: String): Schema =
  {
    asSchema("i,r,s")
  }

  def factorize(queries: Seq[String],mfpi: ℕ): Factorization =
  {
    require(!queries.isEmpty,             "empty query sequence")
    require(mfpi>=queries.size || mfpi==0,"mfpi too small")

    val factors = for {query ← queries; shard ← shards}
                    yield new Factor(query,shard)

    Factorization(factors,None)
  }

  override
  def toString: String = s"MockFactorizer($shards)"
}

//****************************************************************************
