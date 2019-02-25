//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Defines functions useful for writing unit tests.
//*
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark
package core

import org.apache.spark.sql.types._

/**
 * Defines functions useful for writing unit tests.
 *
 * @author Jonathon Bell
 */
trait CoreTestUtilities
{
  /**
   * Constructs a Spark SQL schema for a table whose field names correspond to
   * the comma separated elements of the given string,  the first character of
   * each abbreviating the corresponding field's data type.
   *
   * @param names  A comma separated  list of field names,  each whose initial
   *               character abbreviates the data type, and whose casity shows
   *               whether or not the field is nullable.
   *
   * @example  `asSchema("int,real,String")` creates a schema for a table with
   *           fields named "int", "real", and "String", each with the obvious
   *           type, the last of which is also non-nullable.
   *
   * @return A Spark SQL schema.
   */
  def asSchema(names: String) =
  {
    def field(name: String): StructField =
    {
      val t = name.charAt(0).toLower match
      {
        case 'i' ⇒ IntegerType
        case 'r' ⇒ DoubleType
        case 's' ⇒ StringType
        case 'b' ⇒ BooleanType
        case 'f' ⇒ FloatType
        case 'l' ⇒ LongType
        case 'd' ⇒ DateType
        case 't' ⇒ TimestampType
        case  _  ⇒ BinaryType
      }

      StructField(name,t,name.charAt(0).isLower)
    }

    StructType(names.split(",").map(n ⇒ field(n.trim)))
  }
}

//****************************************************************************
