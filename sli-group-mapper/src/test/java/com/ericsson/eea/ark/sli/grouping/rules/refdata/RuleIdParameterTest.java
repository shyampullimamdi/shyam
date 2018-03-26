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

package com.ericsson.eea.ark.sli.grouping.rules.refdata;

import org.junit.Test;

import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.IOUtils;

import com.ericsson.eea.ark.offline.utils.fs.InputFile;
import com.ericsson.eea.ark.sli.grouping.util.Util;
import com.ericsson.eea.ark.offline.rules.refdata.RuleIdParameter;



public class RuleIdParameterTest {

   private static Logger log = LoggerFactory.getLogger(RuleIdParameterTest.class);

   private static final String data = Util.intersperse("\n", new String[] {
      "#This file threshold parameters key in by rule id. Parameters are defined down to the RULE_ID only!!!"
    , "#Every row is expressed as csv row."
    , "#RuleID: Base rule id as defined in the drl file I.e WEB_01"
    , "#Execution: allowed values yes or no. A value of No excludes the rules from the RE knowledge base."
    , "#Notify:allowed values yes or no. A value of No excludes the rule from notification."
    , "#Threshold: This must be expressed as valid JSON string expressed as { \"PARAMATER_KEY1\":VALUE1,\"PARAMATER_KEY2\":VALUE2,...}"
    , "#if threshold are not needed for a given rule the word NULL can be placed or no value"
    , "#RuleID;Execution;notify;Parameters;"
    , "RETRACT_WHO_IS_NOT_HOME_USER;no;no;NULL;"
    , "IMEITAC_01;no;no;{ \"BLOCKINGCAT\":\"1d\" };"
    , ""
    , "MBB_01A;yes;no;{ \"TRIGGER_TYPE\":\"failed attach\","
    , "  \"TRIGGER_CAUSE1\":\"GPRS_SERVICE_NOT_ALLOWED\","
    , "  \"TRIGGER_CAUSE2\":\"GPRS_SERVICES_NOT_ALLOWED_IN_THIS_PLMN\","
    , "  \"TRIGGER_CAUSE3\":\"GPRS_SERVICE_AND_NON-GPRS_SERVICE_NOT_ALLOWED\","
    , "  \"TRIGGER_SUB_CAUSE\":\"ROAMING\",\"CUSTOMER_STATUS\":\"INS\","
    , "  \"BLOCKINGCAT\":\"15m\" };"
   });

   @Test
   public void test_loadRulesThresholds() throws Exception {

      RuleIdParameter ruleIdParm = new RuleIdParameter(); try {

//        if (log.isInfoEnabled()) log.info("loading rules thresholds from file: '" +rulesParametersFile+ "'");

         InputStream in = IOUtils.toInputStream(data, "UTF-8");

         InputFile file = new InputFile(in); if (file.ready()) {

            ruleIdParm.loadRulesThresholds(file.inputStream());
         }
      }
      catch (/*IOException|ParseException|*/Exception ex) {

         String msg = "Error parsing the rule parameters file."; log.error(msg, ex); throw new RuntimeException(msg, ex);
      }
   }
}
