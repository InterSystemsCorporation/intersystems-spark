//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Represents the decomposition of a query as a set of factors and
//*            a combining reduction query.
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark
package core

/**
 * Represents the decomposition of a SQL query into a set of (query, instance)
 * pairs - its ''factors'' - together with a description of how to combine the
 * results of evaluating the factors into a single set of result tuples -  the
 * ''reduction''. Think ''map-reduce''.
 *
 * === Factorization ===
 *
 * '''Definition''': Let Θ(Q, I) denote the set of tuples that result from the
 * evaluation of query Q on server instance I.
 *
 * '''Definition''': Let Φ(Q, Σ₁, .. , Σ,,n,,)  denote the set  of tuples that
 * result from the evaluation of query Q in an environment in which the system
 * identifiers Ω₁, .. , Ω,,n,, are bound to the sets of tuples Σ₁, .. , Σ,,n,,
 * respectively.
 *
 * '''Definition''':  We say that the factors (Q₁, I₁) .. (Q,,n,,, I,,n,,)
 * ''factorize'' query Q on instance M with ''reduction'' ρ whenever Θ(Q, M) =
 * Φ(ρ, Θ(Q₁, I₁), .. , Θ(Q,,n,,, I,,n,,))
 *
 * Intuitively,  we mean that the result of  evaluating Q on M  may instead be
 * obtained by first evaluating its factors in parallel and then combining the
 * intermediate results by evaluating the reduction ρ against their respective
 * result sets.
 *
 * This factorization is performed by the IRIS server, and can, in principle,
 * encompass both intra-instance and inter-instance partitioning of the query,
 * also referred to as ''parallelization'' and ''sharding'' respectively;  the
 * point being that the instances referenced in a factorization need not cover
 * the entire set of instances in the cluster,  nor even need be distinct from
 * one another.
 *
 * === Reduction ===
 *
 * The reduction query, if present, is expressed as a selection that refers to
 * the special reserved identifiers Ω₁ ..  Ω,,n,, that nominally represent the
 * intermediate results of evaluating each factor in parallel; that is Θ(Q₁, I₁),
 *  .. , Θ(Q,,n,,, I,,n,,). For example, the query:
 * {{{
 *    Q  := SELECT max(f) FROM T
 * }}}
 * might factorize as:
 * {{{
 *    Φ₁ := (SELECT max(f) FROM T, A)
 *    Φ₂ := (SELECT max(f) FROM T, B)
 *    ρ  :=  SELECT max(*) FROM
 *          (
 *            SELECT * FROM Ω₁ UNION
 *            SELECT * FROM Ω₂
 *          )
 * }}}
 * and expresses the idea that Q may be evaluated by evaluating the factors Φ₁
 * and Φ₂, forming the union of their resulting tuples, and finally selecting
 * the maximum value from this union.
 *
 * The absence of a reduction query is semantically equivalent to applying the
 * trivial reduction:
 * {{{
 *    ρ := SELECT * FROM Ω₁ UNION
 *                  ..
 *         SELECT * FROM Ωₙ
 * }}}
 * where ''n'' is the number of factors.
 *
 * === Example ===
 *
 * Suppose for example that the master IRIS instance M controls the two slave
 * instances A and B, and consider the query Q:
 * {{{
 *    Q := SELECT * FROM T
 * }}}
 * Then the following are all valid factorizations of (Q, M):
 * {{{
 *  1)  Φ₁ := (SELECT * FROM T,                   M)
 *
 *  2)  Φ₁ := (SELECT * FROM T  WHERE ID <= 1000, M)
 *      Φ₂ := (SELECT * FROM T  WHERE ID  > 1000, M)
 *
 *  3)  Φ₁ := (SELECT * FROM T,                   A)
 *      Φ₂ := (SELECT * FROM T,                   B)
 *
 *  4)  Φ₁ := (SELECT * FROM T  WHERE ID <= 1000, A)
 *      Φ₂ := (SELECT * FROM T  WHERE ID  > 1000, A)
 *      Φ₃ := (SELECT * FROM T  WHERE ID <= 1000, B)
 *      Φ₄ := (SELECT * FROM T  WHERE ID  > 1000, B)
 * }}}
 *
 * Notice that:
 *
 *  1. The first is simply (Q, M) itself; this trivial factorization is always
 *     available,  and reflects the decision to have the master M evaluate the
 *     query Q as per usual.
 *
 *  1. The second factors Q as two distinct queries that are then evaluated on
 *     the same instance. This form of ''intra-instance'' partitioning is like
 *     the effect of having added the %PARALLEL  keyword to the original query
 *     Q, but the results of each factor are now available as distinct streams
 *     to be returned to the client for subsequent processing in parallel.
 *
 *  1. The third factors Q as two  identical queries that are then executed on
 *     each of the two slaves A and B; presumably the  table T is ''sharded''.
 *     This represents a form of ''inter-instance''  partitioning,  or what we
 *     call ''sharding''. Again both result streams are returned to the client
 *     via distinct connections and so may be consumed in parallel.
 *
 *  1. The fourth factors Q as two distinct queries, each evaluated on both of
 *     the slaves, with the client fetching the results  through four distinct
 *     connections in parallel.
 *
 * It should be clear then that the two forms of partitioning - intra-instance
 * and inter-instance - are complimentary; it is therefore up to the server to
 * determine an optimal factorization.
 *
 * @param factors    A non-empty sequence of factors, as described above.
 * @param reduction  An optional reduction query, as described above.
 *
 * @author Jonathon Bell
 */
case class Factorization (factors: Seq[Factor],reduction: Option[String])
{
  assert(factors.nonEmpty)                               // At least 1 factor
}

//****************************************************************************
