/*******************************************************************************
 * Copyright (c) 2015 Ericsson, Inc. All Rights Reserved.
 *
 * LICENSED MATERIAL - PROPERTY OF ERICSSON
 * Possession and/or use of this material is subject to the provisions
 * of a written license agreement with Ericsson
 *
 * ERICSSON CONFIDENTIAL - RESTRICTED ACCESS
 * This document and the confidential information it contains shall
 * be distributed, routed or made available solely to authorized persons
 * having a need to know within Ericsson, except with written
 * permission of Ericsson.
 *
 *******************************************************************************/

package com.ericsson.eea.ark.sli.grouping.exceptions;



/**
 * Class representing exceptions thrown when missing data error occurs.
 */
public class DataNotFoundException extends SystemException {

   private static final long serialVersionUID = -1574524824748169270L;

   /**
    * Constructs the exception object with the specified message.
    *
    * @param message The message qualifying the exception.
    */
   public DataNotFoundException(String message) {

      super(message);
   }

   /**
    * Constructs the exception object, by wrapping the specified exception.
    *
    * @param ex The exception being wrapped by this exception.
    */
   public DataNotFoundException(Throwable ex) {

      super(ex);
   }

   /**
    * Constructs the exception object with the specified message and by wrapping
    * the specified exception.
    *
    * @param ex The exception being wrapped by this exception.
    * @param message The message qualifying the exception.
    */
   public DataNotFoundException(Throwable ex, String message) {

      super(ex, message);
   }
}