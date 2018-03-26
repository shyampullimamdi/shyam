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

import java.util.Map;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.eea.ark.offline.rules.utils.Util;



/**
 * This class provides convenient functions around the Canonical SLI produced by the pre-processor.
 */
@ToString(of={"timestamp","imsi"})
public class Event_ {

   @SuppressWarnings("unused")
   private static final Logger log = LoggerFactory.getLogger(Event_.class.getName());

   @Getter private Long                timestamp;
   @Getter private String              imsi = null;
   /**/    private Map<String,Object>  attrs = null;


   public Event_(String imsi) {

      this.imsi      = imsi;
      this.timestamp = getTimestamp();
   }


   public Event_(String imsi, Map<String,Object> attrs) {

      this.imsi      = imsi;
      this.attrs     = attrs;
      this.timestamp = getTimestamp();
   }


   /** Typed interface for accessing SLI attributes using a phantom witness types.*/
   public <T> T get(Class<T> type, String attr) {

      @SuppressWarnings("unchecked")
      T val = attrs == null ? null : (T) attrs.get(attr);

      return val;
   }


   /** Generalized getter to reach any field through a list of indices (Integers and Strings).*/
   public Object get(Object[] indices) {

      return Util.get(attrs, indices);
   }


   /** Generalized getter to reach any field through a list of indices (Integers and Strings).*/
   public Object get(List<Object> indices) {

      return Util.get(attrs, indices);
   }


   public boolean containsKey(String key) {

      boolean res = ("imsi"     .equals(key))
         ||         ("timestamp".equals(key)) || attrs.containsKey(key);

    //if (log.isDebugEnabled()) log.debug("Event_(imsi='"+imsi+"').contains(key:'"+key+"'): "+res);

      return res;
   }


   public Object getRuleParameter(String ruleId, String parameterKey) {

      return Util.getRuleParameter(ruleId, parameterKey);
   }


   public <T> T getRuleParameter(String ruleId, String parameterKey, Class<T> type) {

      return Util.getRuleParameter(ruleId, parameterKey, type);
   }
}
