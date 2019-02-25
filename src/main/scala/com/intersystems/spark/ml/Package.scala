//***************************** Copyright © InterSystems. All rights reserved.
//*
//*
//*  Version : $Header:$
//*
//*
//*  Purpose : Extensions for working with the Spark ML library.
//*
//*
//*  Comments: This file uses a tab size of 2 spaces.
//*
//*
//****************************************************************************

package com.intersystems.spark

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types.StructType
import org.jpmml.model.MetroJAXBUtil
import org.jpmml.sparkml.ConverterUtil
import org.dmg.pmml.PMML

import core.Master

/**
 * Extensions for working with the Spark ML library.
 *
 * @author Benjamin De Boe
 */
package object ml
{
  /**
   * Extends the given PipelineModel with IRIS specific methods.
   *
   * @param model  A PipelineModel.
   *
   * @author Benjamin De Boe
   */
  implicit class PipelineModelEx(model: PipelineModel)
  {
    /**
     * Saves the PMML definition for the  given model as a Class Definition on
     * the master instance identified by `address` for subsequent execution on
     * the cluster.
     *
     * Makes use of the JPMML library (and JAXB dependencies) for building the
     * PMML document.
     *
     * @param klass    The name of the COS class definition to create. If this
     *                 class already exists it will be overwritten.
     *
     * @param schema   The schema of the source Dataset fed into the model, to
     *                 base the PMML file's data dictionary on.
     *
     * @param address  The master instance to which the class will be written.
     *
     * @see    [[https://github.com/jpmml/jpmml-sparkml]] for more on JPMML.
     */
    def iscSave(klass: String,schema: StructType,address: Address = Address()): Unit =
    {
      val pmml = ConverterUtil.toPMML(schema,model)      // Convert to PMML
      val baos = new java.io.ByteArrayOutputStream()     // Output stream

      MetroJAXBUtil.marshalPMML(pmml,baos)               // Serialize the PMML

      iscPMMLStr2Class(klass, baos.toString(), address)
    }

    def iscPMMLStr2Class(klass: String, pmml: String, address: Address = Address()): Unit =
    {
      Master(address).invokeWithStatus("%DeepSee.PMML.Utils",      // Class name
                                  "CreateFromString",    // Method name
                                  pmml,         // Serialized model
                                  klass,                 // Class to create
                                  new Integer(1),
                                  new Integer(1),
                                  new Integer(0))

    }
  }
}

//****************************************************************************
