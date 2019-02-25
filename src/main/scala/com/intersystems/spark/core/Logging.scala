//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Initializes a dedicated logger that instances can write to.
//*
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark
package core

/**
 * Initializes a dedicated logger that instances of this class can write to.
 *
 * @author Jonathon Bell
 */
trait Logging
{
  /**
   * The name of the logger that instances of this class can write to.
   */
  def logName: String =
  {
    this.getClass.getName.stripSuffix("$")               // For Scala objects
  }

  /**
   * A dedicated logger that instances of this class can write to.
   */
  @transient
  lazy val log: org.slf4j.Logger =
  {
    org.slf4j.LoggerFactory.getLogger(logName)           // Initialize logger
  }
}

//****************************************************************************
