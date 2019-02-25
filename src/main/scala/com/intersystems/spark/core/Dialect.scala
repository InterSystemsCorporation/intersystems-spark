//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Specializes class JdbcDialect for use with InterSystems SQL.
//*
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark
package core

import java.sql.Types._

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getCommonJDBCType
import org.apache.spark.sql.jdbc.{JdbcDialect,JdbcDialects,JdbcType}
import org.apache.spark.sql.types._

/**
 * Specializes class JdbcDialect for use with InterSystems SQL.
 *
 * The dialect interface abstracts the mapping between the Catalyst types that
 * that Spark uses internally to reason about the representation of values and
 * the JDBC types used by an external database.
 *
 * @see    The file "readme.md" for details of the type mapping we implement
 *         here.
 * @see    [[http://docs.intersystems.com/latest/csp/docbook/DocBook.UI.Page.cls?KEY=RSQL_datatype
 *         IRIS SQL Data Types]] for details on how we project types to
 *         xDBC.
 * @author Jonathon Bell
 */
object Dialect extends JdbcDialect
                       with Logging
{
  /**
   * Returns true if the given connection string looks like one of ours.
   *
   * @param url  A JDBC connection string.
   *
   * @return true if the given URL identifies our own JDBC driver.
   */
  def canHandle(url: String): Boolean =
  {
    url.startsWith("jdbc:IRIS://")                       // Is it one of ours?
  }

  /**
   * Maps the given Catalyst type to the JDBC type that best describes it.
   *
   * @param datatype  A Catalyst type descriptor.
   *
   * @return The JDBC type that best describes the given Catalyst data type.
   * @see    The file "readme.md" for details of the type mapping we implement
   *         here.
   * @see    [[http://docs.intersystems.com/latest/csp/docbook/DocBook.UI.Page.cls?KEY=RSQL_datatype
   *         IRIS SQL Data Types]] for details on how we project types to
   *         xDBC.
   */
  override
  def getJDBCType(datatype: DataType): Option[JdbcType] = datatype match
  {
    case BinaryType    ⇒ Some(JdbcType( "VARBINARY('')",  VARBINARY))
    case BooleanType   ⇒ Some(JdbcType( "BIT",            BIT      ))
    case ByteType      ⇒ Some(JdbcType( "TINYINT",        TINYINT  ))
    case DateType      ⇒ Some(JdbcType( "DATE",           DATE     ))
    case d:DecimalType ⇒ Some(JdbcType(s"NUMERIC(${d.precision},${d.scale})",NUMERIC))
    case DoubleType    ⇒ Some(JdbcType( "DOUBLE",         DOUBLE   ))
    case FloatType     ⇒ Some(JdbcType( "DOUBLE",         DOUBLE   ))
    case IntegerType   ⇒ Some(JdbcType( "INTEGER",        INTEGER  ))
    case LongType      ⇒ Some(JdbcType( "BIGINT",         BIGINT   ))
    case ShortType     ⇒ Some(JdbcType( "SMALLINT",       SMALLINT ))
    case StringType    ⇒ Some(JdbcType( "VARCHAR(,)",     VARCHAR  ))
    case TimestampType ⇒ Some(JdbcType( "TIMESTAMP",      TIMESTAMP))
    case _             ⇒ None
  }

  /**
   * Maps the given JDBC type to the Catalyst type that best describes it.
   *
   * @param code       The JDBC type code.
   * @param precision  The precision, if the given code is the numeric type.
   * @param scale      The scale,     if the given code is the numeric type.
   *
   * @return The Catalyst type that best describes the given JDBC data type.
   * @see    The file "readme.md" for details of the type mapping we implement
   *         here.
   * @see    [[http://docs.intersystems.com/latest/csp/docbook/DocBook.UI.Page.cls?KEY=RSQL_datatype
   *         IRIS SQL Data Types]] for details on how we project types to
   *         xDBC.
   */
  def getCatalystType(code: ℤ,precision: ⇒ ℕ,scale: ⇒ ℕ): Option[DataType] = code match
  {
    case UNIQUEIDENTIFIER ⇒ Some(StringType)
    case BIT              ⇒ Some(BooleanType)
    case TINYINT          ⇒ Some(ByteType)
    case BIGINT           ⇒ Some(LongType)
    case LONGVARBINARY    ⇒ Some(BinaryType)
    case VARBINARY        ⇒ Some(BinaryType)
    case LONGVARCHAR      ⇒ Some(StringType)
    case NUMERIC          ⇒ Some(DecimalType(precision,scale))
    case INTEGER          ⇒ Some(IntegerType)
    case SMALLINT         ⇒ Some(ShortType)
    case DOUBLE           ⇒ Some(DoubleType)
    case VARCHAR          ⇒ Some(StringType)
    case DATE             ⇒ Some(DateType)
    case TIME             ⇒ Some(TimestampType)
    case TIMESTAMP        ⇒ Some(TimestampType)
    case _                ⇒ None
  }

  /**
   * Maps the given JDBC type to the Catalyst type that best describes it.
   *
   * @param code  The JDBC type code.
   * @param name  The JDBC type name.
   * @param size  The size of the representation for the type in bytes.
   * @param meta  An object capable of recovering the meta data for the given
   *              JDBC type as obtained from class ResultSetMetaData
   *
   * @return The Catalyst type that best describes the given JDBC data type.
   * @see    The file "readme.md" for details of the type mapping we implement
   *         here.
   * @see    [[http://docs.intersystems.com/latest/csp/docbook/DocBook.UI.Page.cls?KEY=RSQL_datatype
   *         IRIS SQL Data Types]] for details on how we project types to
   *         xDBC.
   */
  override
  def getCatalystType(code: ℤ,name: String,size: ℕ,meta: MetadataBuilder): Option[DataType] =
  {
    getCatalystType(code,size,meta.build.getLong("scale").toInt)
  }

  /**
   * Returns the best JDBC type for sending a null value of the given catalyst
   * type to IRIS.
   *
   * @param datatype  A Catalyst type descriptor.
   *
   * @return The corresponding null type code.
   * @see    java.sql.Types
   */
  def getJDBCNullType(datatype: DataType): ℤ =
  {
    getJDBCType(datatype)                                // Search our dialect
   .getOrElse(getCommonJDBCType(datatype)                // Search the defaults
   .getOrElse(argex(s"No JDBC type for ${datatype.simpleString}")))
   .jdbcNullType
  }

  /**
   * Adds this object to the list of dialects recognized by the Spark library.
   */
  lazy val register: Unit =
  {
    log.info("Registering custom JDBC dialect")          // Trace registration

    JdbcDialects.registerDialect(this)                   // Add it to the list
  }

  /**
   * The constant used by IRIS to denote the JDBC projection of the unique id
   * type, as implemented by class `%Library.UniqueIdentifier`.
   *
   * @see    The file "readme.md" for details of the type mapping we implement
   *         here.
   * @see    [[http://docs.intersystems.com/latest/csp/docbook/DocBook.UI.Page.cls?KEY=RSQL_datatype
   *         IRIS SQL Data Types]] for details on how IRIS projects types to
   *         xDBC.
   */
  val UNIQUEIDENTIFIER: ℤ = -11                          // Also known as GUID
}

//****************************************************************************
