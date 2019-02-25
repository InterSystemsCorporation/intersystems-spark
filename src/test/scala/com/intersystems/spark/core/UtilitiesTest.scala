//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Unit tests for object utilities.
//*
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark
package core

//****************************************************************************

class UtilitiesTest extends CoreSuite
{
  test("clamp")
  {
    assert{clamp(-1,-2, 1) == -1}
    assert{clamp(-1,-1, 1) == -1}
    assert{clamp(-1, 0, 1) ==  0}
    assert{clamp(-1, 1, 1) ==  1}
    assert{clamp(-1, 2, 1) ==  1}
  }

  test("fail")
  {
    assertThrows[Exception]{core.fail("text")}
    assertThrows[Exception]{core.fail("text %s","1")}
    assertThrows[Exception]{core.fail("text %s %d","1",2)}
    assertThrows[Exception]{core.fail("text %s %d %f","1",2,3.4)}
  }

  test("argex")
  {
    assertThrows[IllegalArgumentException]{argex("text")}
    assertThrows[IllegalArgumentException]{argex("text %s","1")}
    assertThrows[IllegalArgumentException]{argex("text %s %d","1",2)}
    assertThrows[IllegalArgumentException]{argex("text %s %d %f","1",2,3.4)}
  }
}

//****************************************************************************
