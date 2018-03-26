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

package com.ericsson.eea.ark.offline.rules.events;

import lombok.Getter;
import lombok.ToString;
import lombok.AllArgsConstructor;

import org.drools.runtime.StatefulKnowledgeSession;


/**
 * @param <T>  defines event interface as an object with a time-stamp.
 */
@ToString
@AllArgsConstructor
public class Event<T> {

   @Getter private final long timestamp;
   @Getter private final T    event;


   public void insertInto(StatefulKnowledgeSession kb) {

      if (kb != null) {

         kb.insert(this);
      }
   }
}
