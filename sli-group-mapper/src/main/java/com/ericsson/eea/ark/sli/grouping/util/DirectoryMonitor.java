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

package com.ericsson.eea.ark.sli.grouping.util;

import java.io.File;

import org.apache.log4j.Logger;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;



/** This class watches a directory for created files and dispatches update tasks to the associated {@linkplain ExecutorService}.*/
public class DirectoryMonitor extends DirectorySubscriber {

   protected static final Logger log = Logger.getLogger(DirectoryMonitor.class);

   /** The {@linkplain ExecutorService} that actually processes the updated ESR records.*/
   protected ExecutorService exec = null;


   /** Create a new monitor associated with an {@linkplain ExecutorService}.*/
   public DirectoryMonitor(ExecutorService exec) {

      if (exec == null) { final String m = "Cannot create a MonitorRecords object without an ExecutorService.";

         log.error(m); throw new RuntimeException(m);
      }

      this.exec = exec;
   }


   /** For all modified files in the directory being monitored, check that their filenames match the ESR record file
    *  naming convention. For ESR record files, create a new task and dispatch it to the
    *  {@linkplain ExecutorService} associated with this class.*/
   @SuppressWarnings("rawtypes")
   public Future path_created(final File file) {

      // Ensure that the given path corresponds to an ESR record file. --------------------------------------------------------
      Future<Integer> future = null; /*try {

         if (Filesystem.isValidFile(file)) {

            // Create a new task for updating the ESR records in the given file. ----------------------------------------------
            UpdateRecords updater = new UpdateRecords(null) {

               // Capture the state necessary for the UpdateRecords task to run
               protected void init(String[] args) {

                  super.init(args);

                  super.inputDir  = null;
                  super.readDir   = false;
                  super.inputFile = file.getPath();
                  super.timeStamp = Util.getTimeStamp();
               }
            };

            // Dispatch the task to the associated execution service ----------------------------------------------------------
            future = exec.submit(updater);
//          new FutureTask<Integer>(updater).run();  // This would run it in the current thread, instead.
         }
      }
      catch (Exception ex) {

         // Ignore it
      }
*/
      return future;
   }
}
