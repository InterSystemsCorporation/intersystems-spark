//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Package object for the IRIS Spark Connector core.
//*
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark

/**
 * Package object for the IRIS Spark Connector core.
 *
 * @author Jonathon Bell
 */
package object core extends Utilities with SparkAPI
{
  /**
   * A natural number; that is, a non-negative integer.
   */
  type ℕ = Int

  /**
   * An integer, represented as a 32 bit signed integer.
   */
  type ℤ = Int

  /**
   * A real number, represented as a double precision floating point number.
   */
  type ℝ = Double

  /**
   * Pairs a query with the connection details of the IRIS instance that will
   * execute it.
   */
  type Factor = com.intersystems.sqf.Factor

  /**
   * Describes the number and type of columns of a relational table, DataFrame
   * or Dataset.
   */
  type Schema = org.apache.spark.sql.types.StructType
}

//****************************************************************************
