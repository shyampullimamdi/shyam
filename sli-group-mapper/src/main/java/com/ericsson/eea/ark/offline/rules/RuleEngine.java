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

package com.ericsson.eea.ark.offline.rules;

//import lombok.Getter;

//import java.io.File;
import java.util.List;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.eea.ark.offline.config.Config;
import com.ericsson.eea.ark.offline.utils.NGEEAlarmHelper;
//import com.ericsson.eea.ark.offline.utils.NGEEAlarmHelper.AlarmType;
import com.ericsson.eea.ark.offline.rules.RuleEngine;
import com.ericsson.eea.ark.offline.rules.config.SimpleConfig;



public class RuleEngine {

   private static final Logger log = LoggerFactory.getLogger(RuleEngine.class);

   private static List<String>  ruleFiles              = null;

   public  static boolean       matchCorrelatorCrm     = true;
 //private static int           correlatorSegments     = -1;
   public  static List<String>  listOfEsrSources       = new ArrayList<String>();

   public  static SimpleConfig  config                 = null;
   private static boolean       initialized            = false;

   public  static String        rules_file             = "/eea_config/sli-group-mapper/rules/SLI-offline-rules-base.drl";
   public  static String        rules_customDir        = "/eea_config/sli-group-mapper/rules/custom";
   public  static String        rules_parametersFile   = "/eea_config/sli-group-mapper/rules/rule_parameters.csv";
   public  static String        rules_descriptionsFile = "/eea_config/sli-group-mapper/rules/rule_descriptions.csv";


   public static void init() {

      if (initialized) return;

      rules_file             = Config.cfg.getString(Config.cfg_prefix, "rules.file"            , rules_file            );
      rules_parametersFile   = Config.cfg.getString(Config.cfg_prefix, "rules.parametersFile"  , rules_parametersFile  );
      rules_descriptionsFile = Config.cfg.getString(Config.cfg_prefix, "rules.descriptionsFile", rules_descriptionsFile);
      rules_customDir        = Config.cfg.getString(Config.cfg_prefix, "rules.customDir"       , rules_customDir       );

      // set all rule files from directories to be used later through different threads
      ruleFiles = getRuleList(); if (ruleFiles.isEmpty()) {

         String msg = "Error initializing the rule engine. There are no rule files defined."; log.error(msg);

         NGEEAlarmHelper.logAlarm(NGEEAlarmHelper.AlarmType.rulesConfigurationError, "No rule files defined.");

         throw new RuntimeException();
      }

//      // set correlator segment info
//      matchCorrelatorCrm = config.matchCorrelatorForCrm();
//    //correlatorSegments = config.numberOfSegments();
   }


//   /**
//    * @return true if the CRM should be segmented based on correlator logic.
//    */
//   public static boolean isSegmentEsr() {
//
//      return matchCorrelatorCrm;
//   }
//
//
//   /**
//    * @return: the number of correlator segments for ESR source inputs.
//    */
//   public static int getCorrelatorSegments() {
//
//      return correlatorSegments;
//   }


   /**
    * @param eID
    *            : Rule Engine ID
    */
   public static void setFilterSource(int eID) {

      /*
       * This method sets the filter based on the eID. sets:
       * RuleEngine.configESRzmq; RuleEngine.configESR; RuleEngine.configINC;
       */
      log.info("setFilerSource called for " + eID);

      // get source
      String source = RuleEngine.config.sourceFilter(eID); if (source == null || source.length() == 0) {

         log.warn("No Filtering specified in configuration file for rule engine Id"+ eID);

         // This checking is now relax to allow to use one single configuration file
         // that contain a mix of csv, and esr sources combined.
         /*
         NGEEAlarmHelper.logAlarm(NGEEAlarmHelper.CONFIGURATION_ERROR, "no Filtering specified for in configuration file for engine Id" + eID);
         System.exit(-1);
         */
         return;
      }

//      if (source.contains("esr")) {
//
////         RuleEngine.configESRfile = true;
////         RuleEngine.configINC     = false;
//
//         // set the esr_filter.
//         RuleEngine.listOfEsrSources = RuleEngine.getListOfEsrSources(RuleEngine.config, RuleEngine.engineID);
//         /*
//         String[] sourceFilters = RuleEngine.config.sourceFilter(
//               RuleEngine.engineID).split(",");
//         for (String filter : sourceFilters) {
//            String sourceSegment = filter.replaceAll("esr", "").trim();
//            RuleEngine.listOfEsrSources.add(sourceSegment);
//         }
//         */
//
////         log.info("configESR=" + RuleEngine.configESRfile + ", configINC=" + RuleEngine.configINC); // + ", esr_filter=" + RuleEngine.esr_filter);
//      }
////      else if (source.contains("inc")) { // Rele 1.1 not using this option, here for future reference
////
////         RuleEngine.configESRfile = false;
////         RuleEngine.configINC     = true;
////
////         // set the incident file number.
////         RuleEngine.incidentfilenumber = eID;
////
////         log.info("configESR=" + RuleEngine.configESRfile + ", configINC=" + RuleEngine.configINC + ", incidentfilenumber=" + RuleEngine.incidentfilenumber);
////      }
   }


   /** Return a set of Drool rule file paths.*/
   public static List<String> getRuleList() {

      // Get the list of drool files from local files
      List<String> files = new ArrayList<String>();

      // Always get the rule base first to avoid validation problems
      files.add(rules_file);

//      // Add any additional custom rule files
//      String customFileDir = config.customRuleDirectory(); if (customFileDir !=null) {
//
//         File f1 = new File(customFileDir); if (f1.isDirectory()) {
//
////          List<String> //drlFiles = new ArrayList<String>();
//
////          drlFiles = TestRules.getDrlFileList(customFileDir);
//
////          files.addAll(drlFiles);
//         }
//      }

      return files;
   }
}
