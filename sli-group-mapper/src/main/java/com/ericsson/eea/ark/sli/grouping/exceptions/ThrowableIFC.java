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


import java.io.PrintStream;
import java.io.PrintWriter;


/**
 * Interface implemented by all the Transport related exceptions classes. It
 * declares the methods added to the standard Java Throwable class.
 */
public interface ThrowableIFC {

   /** Returns the contained message string. */
   String getMessage();

   /** Returns the wrapped exception which caused this exception to be thrown. */
   Throwable getException();

   /** Appends the specified message to the ThrowableIFC's error message. */
   ThrowableIFC append(String msg);

   /** Pre-appends the specified message to the ThrowableIFC's error message. */
   ThrowableIFC prepend(String msg);

   /** Prints the stack trace to the standard output. */
   void printStackTrace();

   /** Prints the stack trace to the specified PrintStream. */
   void printStackTrace(PrintStream ps);

   /** Prints the stack trace to the specified PrintWriter. */
   void printStackTrace(PrintWriter pw);

   /** Indicates if one of the wrapped exceptions is instance of this class. */
   boolean isCauseOf(ThrowableIFC except);

   // /** Rethrows the exception as the received exception class.*/
   // public static void rethrow( ThrowableIFC except ) throws ThrowableIFC;
}