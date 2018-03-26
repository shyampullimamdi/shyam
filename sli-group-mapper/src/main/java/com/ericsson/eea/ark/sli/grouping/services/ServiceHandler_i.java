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

package com.ericsson.eea.ark.sli.grouping.services;


/** Interface implemented by the supported service handler providers.*/
public interface ServiceHandler_i {

   /** Initialize the service handler.*/
   public ServiceHandler_i init(ServiceHandler svc) throws Exception;

   /** Shutdown the service handler's end-point.*/
   public void stop();

   /** Start up the service handler's end-point.*/
   public void start();

//   /** Receive a message.*/
//   public Serializable receive(long timeout);
//
//   /** Send a message.*/
//   public boolean send(Serializable message, long timout);
}