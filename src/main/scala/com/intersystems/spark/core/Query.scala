//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Describes a request to evaluate a query across the cluster.
//*
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark
package core

import org.apache.spark.sql.sources._

import Query._

/**
 * Describes a request to evaluate a query in parallel across the cluster.
 *
 * Collects together the various bits and pieces needed to specify how a query
 * is to be factorized and distributed across the cluster for evaluation.
 *
 * A first factorization is performed by the server, which introduces at least
 * one factor for each shard that contributes rows to the result,  but more if
 * it is able to detect further opportunities for parallelization.
 *
 * In addition, the user may request an explicit repartitioning of the factors
 * returned by the server by supplying a set of Spark filters to be applied to
 * each factor prior to distribution across the cluster.
 *
 * @param text        The text of a query to be evaluated across the cluster,
 *                    or else the name of an existing table within the cluster
 *                    to load.
 * @param mfpi        The maximum number of factors per instance to include in
 *                    the factorization implicitly performed by the server.
 * @param partitions  An optional set of filters that explicitly partition the
 *                    factorization computed by the server, thus providing yet
 *                    further control over the parallelization of the query.
 *
 * @author Jonathon Bell
 */
class Query
(
  text      : String,
  mfpi      : ℕ,
  partitions: Seq[Filter]
)
extends Logging
{
  /**
   * The text of the SQL query to be evaluated across the cluster prior to its
   * factorization by the shard master.
   */
  val sql =
  {
    if (!text.contains(' ') || text.startsWith("\""))    // Looks like an ID?
    {
      s"SELECT * FROM ($text)"                           // ...load the table
    }
    else                                                 // Looks like a query
    {
      text                                               // ...leave it alone
    }
  }

  /**
   * Factorize the query for distribution across the shards of the cluster.
   *
   * In addition,  first push down the given column projections and predicates
   * so that they execute within the database itself, as opposed to within the
   * Spark workers after the fact.
   *
   * @param factorizer  The factorizer with which to decompose the query prior
   *                    to evaluation across the cluster.
   * @param columns     An optional subset of columns to include in the result
   *                    set.
   * @param filters     A set of filters that restrict the rows to include in
   *                    the result set.
   *
   * @return An equivalent factorization of the query, as computed by IRIS.
   */
  def factorize(factorizer: Factorizer,
                columns:    Option[Seq[String]] = None,
                filters:    Seq[Filter]         = Seq()): Factorization =
  {
    log.debug("factorize({},{},{})",factorizer,columns,filters)

 /* Format a query based upon our own that selects only the specified columns
    and restricts the matching rows with the given filter expressions...*/

    def rewrite(filters: Seq[Filter]): String = columns match
    {
      case None if filters.isEmpty ⇒                     // No modifications?
      {
        sql                                              // ...so use original
      }
      case None ⇒                                        // No column pruning?
      {
        s"SELECT * FROM ($text)${format(filters)}"       // ...take all fields
      }
      case Some(columns) if columns.isEmpty ⇒            // Prune All columns?
      {
        s"SELECT 1 FROM ($text)${format(filters)}"       // ...take constant 1
      }
      case Some(columns) ⇒                               // Prune some columns
      {
        val c = columns.mkString(",")                    // ...format columns

        s"SELECT $c FROM ($text)${format(filters)}"      // ...project columns
      }
    }

    /**
     * Delegates to the factorizer to factorize the given queries.
     */
    def factorize(queries: String*): Factorization =
    {
      factorizer.factorize(queries,mfpi)                 // Call on factorizer
    }

    /**
     * Rewrite each partition of the original query to incorporate the filters
     * Spark is asking us to push down and project out the referenced columns,
     * then factorize the sequence of rewritten queries.
     */
    if (partitions.isEmpty)                              // Single partition?
    {
      factorize(rewrite(filters))                        // ...factorize one
    }
    else                                                 // Factorize queries
    {
      factorize(partitions.map(p ⇒ rewrite(filters :+ p)) :_*)
    }
  }
}

/**
 * Companion object for class Query.
 */
object Query extends Logging
{
  /**
   * Construct a query from the description of how to factorize and distribute
   * its evaluation across the shards of the cluster.
   *
   * @param text        The text of a query to be evaluated on the cluster, or
   *                    else the name of an  existing table within the cluster
   *                    to load.
   * @param mfpi        The maximum number of factors per cluster instance, or
   *                    0 if no limit is necessary.
   */
  def apply(text: String,mfpi: ℕ = 1): Query =
  {
    new Query(text,mfpi,Seq())
  }

  /**
   * Construct a query from the description of how to factorize and distribute
   * its evaluation across the shards of the cluster.
   *
   * The filters should collectively describe a ''partitioning'' of the result
   * set; that is, they should select pairwise disjoint subsets of tuples that
   * nevertheless span the entire result space.
   *
   * Each filter gives rise to a corresponding partition of the ShardedRDD to be
   * created when the query is eventually evaluated.
   *
   * @param text        The text of a query to be evaluated on the cluster, or
   *                    else the name of an  existing table within the cluster
   *                    to load.
   * @param partitions  A set of filter predicates that further partition the
   *                    factorization of the query computed by the server.
   */
  def apply(text: String,partitions: Seq[Filter]): Query =
  {
    new Query(text,partitions.size,partitions)
  }

  /**
   * Construct a query from the description of how to factorize and distribute
   * its evaluation across the shards of the cluster.
   *
   * This constructor variant accepts a partitioning scheme that is similar to
   * that used by the JDBC data source built into Spark itself.
   *
   * Each partition gives rise to a corresponding partition of the ShardedRDD to
   * be created when the query is eventually executed.
   *
   * @param text        The text of a query to be evaluated on the cluster, or
   *                    else the name of an  existing table within the cluster
   *                    to load.
   * @param column      The name of an integer valued column in the result set
   *                    on which to further partition the query.
   * @param lo          The lower bound of the partitioning column.
   * @param hi          The upper bound of the partitioning column.
   * @param partitions  The number of partitions to create.
   */
  def apply(text: String,column: String,lo: Long,hi: Long,partitions: ℕ): Query =
  {
    new Query(text,partitions,partition(column,lo,hi,partitions))
  }

  /**
   * Creates a set of filters from a partitioning scheme that is specified in
   * terms of the values of an integer valued column within the result set.
   *
   * @param column      The name of an integer valued column in the result set
   *                    on which to further partition the query.
   * @param lo          The lower bound of the partitioning column.
   * @param hi          The upper bound of the partitioning column.
   * @param partitions  The number of partitions to create.
   *
   * @return An equivalent set of filters.
   * @see    Documentation for the JDBC data source built into Spark for more
   *         details.
   */
  private[core]
  def partition(column: String,lo: Long,hi: Long,partitions: ℕ): Seq[Filter] =
  {
    require(partitions > 0,s"too few partions ($partitions)")
    require(lo <= hi      ,s"bad partitioning bounds [$lo,$hi]")

    var n: ℕ = partitions                                // Partitions desired

    if (n > hi-lo)                                       // Too big for range?
    {
      n = (hi-lo).toInt                                  // ...shrink to fit

      log.warn(s"Reducing partitions from $partitions to $n to fit bounds [$lo,$hi]")
    }

    /**
     * Creates a filter to select the 'i'th block of values of length 'step'.
     */
    def partition(step: Long)(i: ℕ): Filter =
    {
      val bound = lo + i * step
      def above = GreaterThanOrEqual(column,bound)
      def below = LessThan          (column,bound+step)

      i match                                            // Which predicate?
      {
        case 0          ⇒ Or (below,IsNull(column))      // ...below or null
        case i if i<n-1 ⇒ And(above,below)               // ...above and below
        case _          ⇒ above                          // ...just above
      }
    }

    if (n>1 && lo<hi)                                    // Something to do?
    {
      Array.tabulate(n)(partition(hi/n - lo/n))          // ...create filters
    }
    else                                                 // Just one partition
    {
      Seq()                                              // ...nothing to do
    }
  }

  /**
   * Return true if the given column name could possibly have been synthesized
   * by the server to describe a compound expression that lacks an alias. Such
   * columns cannot directly be projected out of the result without the use of
   * an alias.
   *
   * In the query `SELECT a, min(a) from A`, for example, the server describes
   * the schema as being `["a": int, "Aggregate_2": int]`,  yet any subsequent
   * attempt to select the 'field' `Aggregate_2` will cause the server to moan
   * that it has never heard of such a field; ironic,  since it was the server
   * that synthesized the name.
   *
   * @param name  An identifier that may or may not have been synthesized by
   *              by the server.
   *
   * @return False if the given name was definitely not synthesized by server.
   */
  private[core]
  val isSynthetic: String ⇒ Boolean =
  {
    import scala.collection.Searching._                  // For binary search

    val regex = "([ABCEFHLS].+)_[0-9]+".r                // Matches the prefix
    val array = Array("AS_Alias",                        // The entire list of
                      "Aggregate",                       //  strings that can
                      "Built_In_Function",               //  prefix a column
                      "COSExpr",                         //  name synthesized
                      "Correlated_Field",                //  by the server
                      "Expression",
                      "ExtrFun",
                      "Field",
                      "HostVar",
                      "Literal",
                      "Subquery");
    /**
     * Return a lambda that 'closes over' the constants 'regex' and 'array',
     * thus avoiding their reconstruction each time the function is applied.
     */
    {case regex(prefix) ⇒ array.search(prefix)           // Search for prefix
                               .isInstanceOf[Found]      // ...did we find it?
     case _             ⇒ false}                         // No, bona fide name
  }

  /**
   * Return false if the argument contains one or more synthetic column names,
   * or one or more column names are duplicated, and true otherwise.
   *
   * Log an appropriate warning if the sequence of names is not prunable.
   *
   * @param columns  The subset of columns to include in the result set.
   *
   * @return True if none of the given names were synthesized by the server,
   *         and no name occurs more than once.
   */
  private[core]
  def arePrunable(columns: Seq[String]): Boolean =
  {
    val s = columns.filter(isSynthetic)                  // Synthesized names
    val d = columns.groupBy(identity)                    // Group all by name
                   .filter {case (_,c) ⇒ c.size > 1}     // Select duplicates
                   .keys                                 // Duplicated names

    if (s.nonEmpty)                                      // Synthesized names?
    {
      log.warn("Performance Warning: Cannot prune selection because columns "+
      "[{}] have synthetic names, so cannot be selected unless aliased. "  +
      "Consider modifying the original query to alias these expressions.",
      s.mkString(5,", "))                                // ...format the list
      false                                              // ...no, can't prune
    }
    else
    if (d.nonEmpty)                                      // Duplicated names?
    {
      log.warn("Performance Warning: Cannot prune selection because columns "+
      "[{}] occur more than once, so cannot be selected unless aliased. "  +
      "Consider modifying the original query to alias these expressions.",
      d.mkString(5,", "))                                // ...format the list
      false                                              // ...no, can't prune
    }
    else
    {
      true                                               // ...names look good
    }
  }

  /**
   * Check the field names in the given schema and log a warning if any appear
   * to have been synthesized by the server.
   *
   * @param schema  The result set schema returned by the database server.
   *
   * @return The argument schema, unmodified.
   */
  private[core]
  def validate(schema: Schema): Schema =
  {
    arePrunable(schema.fields.map(_.name))               // Check field names
    schema                                               // But use it anyway
  }

  /**
   * Returns a schema describing the shape of the result set if we project out
   * only those columns named in the given collection.
   *
   * Fails if any of the columns have synthetic names, in which case we return
   * the original schema and mark it is unpruned.
   *
   * @param schema   A schema describing the shape of the unpruned result set.
   * @param columns  The subset of columns to include in the pruned result set.
   *
   * @return The pruned schema, paired with column names, or the orignal schema
   *          paired with the missing option flag `None`.
   */
  private[core]
  def prune(schema: Schema,columns: Array[String]): (Schema,Option[Seq[String]]) =
  {
    log.debug("prune({},{})",schema.asString(),columns,"")// Trace our progress

    if (arePrunable(columns))                            // Are they prunable?
    {
      val map = schema.fields.map(f ⇒ f.name → f).toMap  // ...index of fields

      (new Schema(columns.map(map(_))),Some(columns))    // ...select columns
    }
    else                                                 // Has synthetic names
    {
      (schema,None)                                      // ...can't be pruned
    }
  }

  /**
   * Format the list of filters that restrict the rows of the data frame.  The
   * result is appended to the WHERE clause of the SELECT statement with which
   * we wrap the user's original query.
   *
   * @param filters  The filters `''f'',,i,,` with which to format the filter
   *                 clause.
   *
   * @return A string of the form `" WHERE ''f'',,1,, & .. & f'',,n,,"`.
   */
  private[core]
  def format(filters: Seq[Filter]): String =
  {
    def compile(filter: Filter): String =
    {
      val b = new StringBuilder()

      def infix(a: String,o: String,v: Any = null): Unit =
      {
        b += '('
        b++= a
        b++= o
        if (v!=null)
          b++= value(v)
        b += ')'
      }

      def logical(l: Filter,o: String,r: Filter = null): Unit =
      {
        b += '('
        if (l != null)
          visit(l)
        b ++= o
        if (r !=null)
          visit(r)
        b += ')'
      }

      def value(a: Any): String = a match
      {
        case s: String               ⇒ s"'${s.replace("'","''")}'"
        case d: java.sql.Date        ⇒ s"'${d}'"
        case t: java.sql.Timestamp   ⇒ s"'${t}'"
        case a: Array[_]             ⇒ a.map(value).mkString("(",",",")")
        case _                       ⇒ a.toString
      }

      def visit(filter: Filter): Unit = filter match
      {
        case EqualTo           (a,v) ⇒ infix(a,"=" ,v)
        case LessThan          (a,v) ⇒ infix(a,"<" ,v)
        case GreaterThan       (a,v) ⇒ infix(a,">" ,v)
        case LessThanOrEqual   (a,v) ⇒ infix(a,"<=",v)
        case GreaterThanOrEqual(a,v) ⇒ infix(a,">=",v)
        case StringContains    (a,v) ⇒ infix(a,"[" ,v)
        case StringStartsWith  (a,v) ⇒ infix(a," LIKE ",v+'%')
        case StringEndsWith    (a,v) ⇒ infix(a," LIKE ",'%'+v)
        case In                (a,v) ⇒ infix(a," IN ",v)
        case IsNull            (a)   ⇒ infix(a," IS NULL")
        case IsNotNull         (a)   ⇒ infix(a," IS NOT NULL")
        case Or                (l,r) ⇒ logical(l," ! ",r)
        case And               (l,r) ⇒ logical(l," & ",r)
        case Not               (f)   ⇒ logical(null,"NOT",f)
        case _                       ⇒ b.append("1=1")
      }

      visit(filter)
      b.toString
    }

    if (filters.isEmpty)
      ""
    else
      filters.map(compile).mkString(" WHERE "," & ","")
  }

  /**
   * Remove from the given list of filters those that have direct translations
   * into InterSystems dialect of the SQL language.
   *
   * @param filters  An collection of filters.
   *
   * @return Those filters containing an occurrence of an operator that is not
   *         directly expressible in InterSystems SQL.
   */
  private[core]
  def unhandledFilters(filters: Array[Filter]): Array[Filter] =
  {
    def unhandled(filter: Filter): Boolean = filter match
    {
      case EqualTo           (_,_)
         | LessThan          (_,_)
         | GreaterThan       (_,_)
         | LessThanOrEqual   (_,_)
         | GreaterThanOrEqual(_,_)
         | StringContains    (_,_)
         | In                (_,_)
         | IsNull            (_)
         | IsNotNull         (_)
         | StringStartsWith  (_,_) ⇒ false
      case Or                (l,r) ⇒ unhandled(l) || unhandled(r)
      case And               (l,r) ⇒ unhandled(l) || unhandled(r)
      case Not               (f)   ⇒ unhandled(f)
      case _                       ⇒ true
    }

    filters.filter(unhandled)
  }
}

//****************************************************************************
