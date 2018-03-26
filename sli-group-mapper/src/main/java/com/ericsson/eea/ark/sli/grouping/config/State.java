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

package com.ericsson.eea.ark.sli.grouping.config;

import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;

import com.ericsson.eea.ark.offline.config.Config;



/**
 * The sole purpose of this class is capture in one place all of internal stated needed by the running application.<p>
 *
 * With this separation, this class provides the underpinnings necessary for features like:<ul>
 *  <li>Stop/Resume computations;</li>
 *  <li>Concurrent execution strategies;</li>
 *  <li>Persistence state across invocations;</li>
 *  <li>Etc.</li></ul><p>
 *
 * Note that this are examples of possible functionality. As far as this application is concerned, we only interested on
 * the concurrency enabling capability above.
 */
public class State extends Configured {

   protected String  _cn_                = Config._cn_;
 //protected String  cfg_prefix          = Config.cfg_prefix;

   protected String  statsDir            = Config.statsDir;
   protected String  workingDir          = Config.workingDir;

   protected String  configFile          = Config.configFile;

   protected String  dfsPrefix           = Config.dfsPrefix;

   protected String  inputDate           = Config.inputDate;
   protected String  endDate             = Config.endDate;
   protected String  inputDataType       = Config.inputDataType;

   protected Boolean wait                = Config.wait;

   protected boolean alarmsEnabled       = Config.alarmsEnabled;
   protected boolean dataGridEnabled     = Config.dataGridEnabled;

   protected boolean checkLicense        = Config.checkLicense;
   protected boolean checkDataGridLoader = Config.checkDataGridLoader;

   protected boolean saveRejectedRecords = Config.saveRejectedRecords;

   protected String  inputDateFormat     = Config.inputDateFormat;
   protected String  tableNameMemDb      = Config.tableNameMemDb;


   @Override
   public Configuration getConf() {

      // Get Hadoop's configuration and parse the received command line arguments.
      Configuration conf = super.getConf(); if (conf == null) {

         // Create a new Hadoop Configuration for this processes, checking for and loading
         // Oozie's configuration if started by the Oozie Scheduler.
         String     oozieCfg = System.getProperty("oozie.action.conf.xml");
         boolean useOozieCfg = oozieCfg != null;

         conf = new Configuration(/*loadDefaults*/!useOozieCfg); if (useOozieCfg) {

            //if (log.isInfoEnabled()) log.info(_cn_+": Will create Hadoop configuration from Oozie config: " +oozieCfg);
            conf.addResource(new Path("file:///", oozieCfg));
         }

       //if (log.isInfoEnabled()) log.info(_cn_+": Created default Hadoop configuration: " +conf);
         setConf(conf);
      }

      return conf;
   }
}