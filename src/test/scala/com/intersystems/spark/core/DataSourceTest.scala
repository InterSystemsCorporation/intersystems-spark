//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Unit tests for class DataSource.
//*
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark
package core

//****************************************************************************

class DataSourceTest extends CoreSuite
{
  val ds = new DataSource()

  test("validate(+)")
  {
    ds.validate("test",asSchema("i,r,s"),asSchema("i,r,s"))
  }

  fail("validate(-) missing source column 'S'")
  {
    ds.validate("test",asSchema("i,r"),asSchema("i,r,S"))
  }

  fail("validate(-) missing target col 'x'")
  {
    ds.validate("test",asSchema("i,r,x,s"),asSchema("i,r,s"))
  }

  fail("validate(-) repeated source col 'r'")
  {
    ds.validate("test",asSchema("i,r,r,s"),asSchema("i,r,s"))
  }

  def fail(tag: String)(body: ⇒ Unit): Unit =
  {
    test(tag)(assertThrows[Exception](body))
  }
}

//****************************************************************************
