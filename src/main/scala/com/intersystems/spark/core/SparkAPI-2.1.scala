//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Isolates inconsistencies between versions of the Spark API.
//*
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark
package core

import scala.Boolean.box
import scala.reflect.ClassTag
import scala.util.Try

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions,JdbcUtils}

/**
 * Isolates inconsistencies between the various versions of the Spark API that
 * we wish to support.
 *
 * Mixing this trait into the core package object enables the rest of the code
 * to remain independent of these discrepancies.
 *
 * @author Jonathon Bell
 */
trait SparkAPI
{
  /**
   * Spark 2.0 introduced the polymorphic `DataFrameWriter[α]` helper class.
   */
  type DataFrameWriter[α] = org.apache.spark.sql.DataFrameWriter[α]

  /**
   * Spark 2.0 changed the type of the mutable internal row.
   */
  type MutableRow = org.apache.spark.sql.catalyst.expressions.SpecificInternalRow

  /**
   * Spark 2.0 changed the signature of the function `schemaString`.
   * Spark 2.2 changed the signature again.
   *
   * Spark 2.0:
   * {{{
   *    JdbcUtils.schemaString(Schema,String): String
   * }}}
   *
   * Spark 2.2+:
   * {{{
   *    JdbcUtils.schemaString(Schema,String,Option[String]): String
   * }}}
   *
   * We use reflection to search for the more recent and, if found, invoke it,
   * otherwise we drop back to calling the version we are compiled with.
   */
  def schemaString(df: DataFrame,url: String) =
  {
    call(JdbcUtils,"schemaString",df,url,None).getOrElse(// Call Spark 2.2?
         JdbcUtils. schemaString (df.schema,url))        // Call Spark 2.1
  }

  /**
   * Spark 2.0 changed the signature of the function `saveTable`.
   * Spark 2.2 changed the signature again.
   *
   * Spark 2.0:
   * {{{
   *    JdbcUtils.saveTable(DataFrame,String,String,JDBCOptions): Unit
   * }}}
   *
   * Spark 2.2+:
   * {{{
   *    JdbcUtils.schemaString(DataFrame,Option[Schema],Boolean,JDBCOptions): Unit
   * }}}
   *
   * We use reflection to search for the more recent and, if found, invoke it,
   * otherwise we drop back to calling the version we are compiled with.
   */
  def saveTable(df: DataFrame,address: Address,table: String,options: Map[String,String] = Map()) =
  {
   val o = new JDBCOptions(options +
     ("driver"   → "com.intersystems.jdbc.IRISDriver",   // Add JDBC driver
      "url"      → address.jdbc,                         // Add JDBC string
      "user"     → address.user,                         // Add user account
      "password" → address.password,                     // Add user password
      "dbtable"  → table) )                              // Add target table

    call(JdbcUtils,"saveTable",df,Option(df.schema).as[Option[_]],box(false).as[Boolean],o).getOrElse(
         JdbcUtils. saveTable (df,address.jdbc,table,o)) // Call Spark 2.1
  }

  /**
   * Pairs the given value with the type of the method parameter with which it
   * corresponds.
   *
   * @param  value  A method argument.
   * @param  klass  The type of the method parameter to which the given value
   *                corresponds.
   */
  private implicit
  class Typed(val value: AnyRef)
  {
    def klass: Class[_] = value.getClass
  }

  /**
   * Extends the given value with an `as` method that enables the runtime type
   * at which the value is passed to the method via reflection to be specified
   * explicitly, overriding the type that would otherwise be selected by class
   * `Typed`.
   */
  private implicit
  class TypedAs(value: AnyRef)
  {
    def as[α](implicit tag: ClassTag[α]): Typed = new Typed(value)
    {
      override
      def klass: Class[_] = tag.runtimeClass             // Pass value as `α`
    }
  }

  /**
   * Reflect upon the methods of the given object, searching for a method with
   * the given name and parameter types, and if found,  call it with the given
   * arguments.
   *
   * This mechanism allows us to set up a call at runtime to a method that may
   * not in fact exist in the version of the library we were compiled against.
   *
   * @param o     The object whose method is to be called.
   * @param name  The name of the method to be called.
   * @param args  The method arguments, each paired with the type of parameter
   *              to which it corresponds.
   *
   * @return      The result of the method call, or `None` if the method could
   *              not be found.
   */
  private
  def call[ρ](o: AnyRef,name: String,args: Typed*): Option[ρ] =
  {
    Try(o.getClass.getMethod(name,args.map(_.klass):_*)) // Search for method
     .toOption                                           // Convert to Option
     .map(_.invoke(o,args.map(_.value):_*)               // Invoke via handle
           .asInstanceOf[ρ])                             // ...cast result
  }
}

//****************************************************************************
