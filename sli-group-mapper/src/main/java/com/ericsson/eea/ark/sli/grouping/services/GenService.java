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

import org.apache.log4j.Logger;

import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;

import com.ericsson.eea.ark.sli.grouping.config.State;


/**
 * This class will setup the jobs to be run by the Hadoop cluster for processing the input file(s).
 */
public class GenService extends    State
   /**/                 implements Callable<Integer> {

   final protected static Logger log = Logger.getLogger(GenService.class);

   /** Leftover command-line arguments.*/
   protected String[] args = null;


   /** Task execution method, invoked from the {@linkplain ServiceHandler}.*/
   public Integer call() throws Exception {

      log.info("Loading Hadoop Configuration.");

      Configuration conf = getConf();

      // Check for the Hadoop Windows patch, to have it working with NTFS.
      conf = checkForWindowsPatch(conf);

      // Launch the Hadoop Driver
      int rc = 0; /*try {

         @SuppressWarnings("rawtypes")
         HadoopDriver driver = new HadoopDriver(conf);

         rc = driver.run(args);
      }
      catch (Throwable t) {

         String msg = Config._cn_ + ": Cannot create/execute HadoopDriver."; log.error(msg, t); throw t;
      }*/

      // Return result code
      return rc;
   }


   protected Configuration checkForWindowsPatch(Configuration conf) {

      if (System.getProperty("os.name").toLowerCase().indexOf("windows") == 0) try {

         Class.forName("com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
         conf.set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
      }
      catch (Exception e) {

         String msg = "Error: The 'HADOOP-7682' patch Jar file was not found in the Maven class-path."
            +        " Run 'mvn install' to have it installed in the local Maven repository.";

         System.err.println(msg); System.exit(1);
      }

      return conf;
   }
}
