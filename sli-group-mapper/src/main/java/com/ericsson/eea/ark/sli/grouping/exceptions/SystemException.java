/*******************************************************************************
 * Copyright (c) 2014 Ericsson, Inc. All Rights Reserved.
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
 *
 */
public class SystemException extends UncheckedException {

   private static final long serialVersionUID = -1729538641386042685L;


   /**
    * @param message
    */
   public SystemException(String message) {

      super(message);
   }


   public SystemException(String message, Throwable t) {

      super(t, message);
   }


   /**
    * @param ex
    */
   public SystemException(Throwable ex) {

      super(ex);
   }


   /**
    * @param ex
    * @param message
    */
   public SystemException(Throwable ex, String message) {

      super(ex, message);
   }
}