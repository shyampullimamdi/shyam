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

package com.ericsson.eea.ark.offline.rules.refdata;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.EqualsAndHashCode;

import org.apache.commons.lang3.StringUtils;

import net.minidev.json.JSONValue;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.ParseException;


/** Helper class to maintain the rule thresholds for CEA rules. */
@ToString
@EqualsAndHashCode
public class RuleThreshold {

   private static final Logger log = LoggerFactory.getLogger(RuleThreshold.class.getName());

   @Getter @Setter private String              ruleID       = null;
   @Getter @Setter private boolean             notifyFlag   = false;
   @Getter @Setter private boolean             evaluateFlag = true;
   @Getter @Setter private Map<String, Object> thresholdMap = null;


   public RuleThreshold() {

      this.thresholdMap = new HashMap<String, Object>();
   }


   public RuleThreshold(boolean evaluateFlag, boolean notifyFlag, Map<String,Object> thresholdMap) {

      this.notifyFlag   = notifyFlag;
      this.evaluateFlag = evaluateFlag;
      this.thresholdMap = thresholdMap;
   }


   public RuleThreshold(String line) throws ParseException {

      this.ruleID       = null;
      this.evaluateFlag = false;
      this.notifyFlag   = false;
      this.thresholdMap = new HashMap<String, Object>();

      // #RuleID;Execution;Notify;Threshold; -- Threshold is expressed as a JSON string expected length is 2 or 3
      String[] row = line.split(";"); if (row == null || row.length < 3) {

         log.error("Invalid rule threshold line: '"+line+"'");
         return;
      }
      row[0] = row[0].trim(); row[1] = row[1].trim(); row[2] = row[2].trim(); row[3] = row[3].trim();

      this.ruleID       = row[0];
      this.evaluateFlag = row[1].equalsIgnoreCase("yes") || row[1].equalsIgnoreCase("true"); // Execution
      this.notifyFlag   = row[2].equalsIgnoreCase("yes") || row[2].equalsIgnoreCase("true"); // Notify
      String json       = row[3];

      if (row.length < 4 || "NULL".equalsIgnoreCase(json)) return;

      // The JSON parser may miss some invalid defined parameters so we do a pre-validation before we parse.
      RuleThreshold.prevalidate(json);

      // Change all "^" to comma. That was only enforce to do input validation.
      String jsonStr = json.replace("[\t ]*^[\t ]*", ","); try {

         Object      obj     = JSONValue.parseStrict(jsonStr);
         JSONObject  jsonObj = (JSONObject) obj;
         Set<String> keys    = jsonObj.keySet();
         Iterator<String> it = keys.iterator(); while (it.hasNext()) {

            String key   = it.next();
            Object value = jsonObj.get(key);

            this.thresholdMap.put(key, value);
         }
      }
      catch (ParseException ex) {

         this.ruleID = null;

         log.error("Error parsing JSON threshold from rule entry: '"+line+"'", ex);

         throw ex;
      }
      catch (Exception ex) {

         this.ruleID = null;

         log.error("Unexpected error processing JSON theshold from rule entry: '"+line+"'", ex);

         throw ex;
      }
   }


   /** The JSON to be validate based on expected format.*/
   public static void prevalidate(String jsonStr) throws ParseException {

      // { "KEY":value, "KEY2":"value ...}
      if (!jsonStr.startsWith("{") || !jsonStr.endsWith("}")) {

         throw new ParseException(0, 0, "Invalid paramater, expected data in JSON format: '"+jsonStr+"'");
      }

      // remove "{" and "}"
      jsonStr = jsonStr.substring(jsonStr.indexOf("{") + 1, jsonStr.indexOf("}"));

      String[] keyValues = jsonStr.split(","); if (keyValues.length == 0) {

         throw new ParseException(0, 0, "invalid JSON paramater String: " + jsonStr);
      }

      // validate each pair.
      for (String item : keyValues) {

         String[] pair = item.split(":"); if (pair.length != 2) {

            throw new ParseException(0, 0, "invalid JSON paramater String: " + jsonStr);
         }

         // Validate the key, making sure it starts and ends in '"'.
         String key = pair[0].trim(); if (!key.startsWith("\"") || !key.endsWith("\"")) {

            throw new ParseException(0, 0, "invalid key " + key + " key must be enclosed in double quotes: " + jsonStr);
         }

         int occurance = StringUtils.countMatches(key, "\""); if (occurance != 2) {

            throw new ParseException(0, 0, "invalid key " + key + " key must be enclosed in double quotes: " + jsonStr);
         }

         // Validate the value
         String value = pair[1].trim(); if (value.contains("\"") && (!value.startsWith("\"") || !value.endsWith("\""))) {

            throw new ParseException(0, 0, "invalid String value: " + value + " for key: " + key+ " String value  must be enclosed in double quotes: " + jsonStr);
         }
      }
   }
}
