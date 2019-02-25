//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Unit tests for class Query.
//*
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark
package core

import org.apache.spark.sql.sources.{Filter,IsNull}
import Query._

//****************************************************************************

class QueryTest extends CoreSuite
{
  test("partition(+)")
  {
    def partitions(column: String,lo: Long,hi: Long,partitions: ℕ): Seq[String] =
    {
      for (f ← partition(column,lo,hi,partitions)) yield
      {
        format(Array(f))
      }
    }

    for ((lo,hi,n,where) ← Seq(
         (0,9,1,Seq()),
         (0,9,1,Seq()),
         (0,1,2,Seq()),
         (0,6,2,Seq(" WHERE ((c<3) ! (c IS NULL))"," WHERE (c>=3)")),
         (0,9,2,Seq(" WHERE ((c<4) ! (c IS NULL))"," WHERE (c>=4)")),
         (0,6,3,Seq(" WHERE ((c<2) ! (c IS NULL))"," WHERE ((c>=2) & (c<4))"," WHERE (c>=4)")),
         (0,9,3,Seq(" WHERE ((c<3) ! (c IS NULL))"," WHERE ((c>=3) & (c<6))"," WHERE (c>=6)"))))
    {
      assert(partitions("c",lo,hi,n) == where)
    }
  }

  test("partition(-)")
  {
    assertThrows[IllegalArgumentException](partition("c",0,1,0))
    assertThrows[IllegalArgumentException](partition("c",9,0,1))
  }

  test("isSynthetic(+)")
  {
    assert{isSynthetic("AS_Alias_1")}
    assert{isSynthetic("Aggregate_2")}
    assert{isSynthetic("Built_In_Function_3")}
    assert{isSynthetic("COSExpr_4")}
    assert{isSynthetic("Correlated_Field_5")}
    assert{isSynthetic("Expression_6")}
    assert{isSynthetic("ExtrFun_7")}
    assert{isSynthetic("Field_8")}
    assert{isSynthetic("HostVar_9")}
    assert{isSynthetic("Literal_10")}
    assert{isSynthetic("Subquery_11")}
  }

  test("isSynthetic(-)")
  {
    assert{!isSynthetic("aS_Alias_1")}
    assert{!isSynthetic("0Aggregate_2")}
    assert{!isSynthetic("Built_in_Function_3")}
    assert{!isSynthetic("COSExpr_")}
    assert{!isSynthetic("Correlated")}
    assert{!isSynthetic("Expression6")}
  }

  test("arePrunable(+)")
  {
    assert{arePrunable(Seq("A","B","C","D"))}
    assert{arePrunable(Seq("A","hostvar_3"))}
  }

  test("arePrunable(-)")
  {
    assert{!arePrunable(Seq("A","B","C","C"))}
    assert{!arePrunable(Seq("A","C","C","C"))}
    assert{!arePrunable(Seq("A","B","B","A"))}
    assert{!arePrunable(Seq("A","HostVar_3"))}
  }

  test("schema")
  {
    def check(query: String,fields: String) =
    {
      assert(factorizer.schema(Query(query).sql) == asSchema(fields))
    }

    check("Spark.A",              "i,r,s")
    check("Spark.B",              "i,r,s")
    check("SELECT * FROM Spark.A","i,r,s")
    check("SELECT * FROM Spark.B","i,r,s")
  }

  test("factorize")
  {
    def none   = Seq[Filter]()
    def isnull = Seq(new IsNull("i"))

    def check(query: Query,columns: String,filters: Seq[Filter],factors: String*) =
    {
      val c: Option[Seq[String]] = columns match
      {
        case "*" ⇒ None
        case ""  ⇒ Some(Seq())
        case  _  ⇒ Some(columns.split(",").map(_.trim))
      }

      val w = factors.sorted
      val g = query.factorize(factorizer,c,filters)
                   .factors
                   .map(_.query)
                   .sorted

      assert(w.sameElements(g),s"\nWANTED:\n  $w\nGOT:\n  $g")
    }

    // ***

    check(Query("A"),"",none,

      "SELECT 1 FROM (A)")

    check(Query("A"),"*",none,

      "SELECT * FROM (A)")

    check(Query("A"),"i",none,

      "SELECT i FROM (A)")

    check(Query("A"),"i,r",none,

      "SELECT i,r FROM (A)")

    // ***

    check(Query("A","i",0,10,2),"",none,

      "SELECT 1 FROM (A) WHERE (i>=5)",
      "SELECT 1 FROM (A) WHERE ((i<5) ! (i IS NULL))")

    check(Query("A","i",0,10,2),"*",none,

      "SELECT * FROM (A) WHERE (i>=5)",
      "SELECT * FROM (A) WHERE ((i<5) ! (i IS NULL))")

    check(Query("A","i",0,10,2),"i",none,

      "SELECT i FROM (A) WHERE (i>=5)",
      "SELECT i FROM (A) WHERE ((i<5) ! (i IS NULL))")

    check(Query("A","i",0,10,2),"i,r",none,

      "SELECT i,r FROM (A) WHERE (i>=5)",
      "SELECT i,r FROM (A) WHERE ((i<5) ! (i IS NULL))")
}
}

//****************************************************************************
