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

package com.ericsson.eea.ark.sli.grouping;

import java.util.Arrays;

import org.apache.log4j.Logger;

import com.ericsson.eea.ark.offline.utils.ArkCon;
import com.ericsson.eea.ark.offline.config.Config;
import com.ericsson.eea.ark.sli.grouping.config.Args;
import com.ericsson.eea.ark.sli.grouping.services.ServiceHandler;



/**
 * This class implements a command-line tool for invoking the EEA Offline Group-mapper.
 */
public class CLI {

   /** Prefix used for tracing.*/
   public static final String _cn_ = Config._cn_ = Config.cfg_prefix = "group-mapper";

   protected static final Logger log = Logger.getLogger(CLI.class);


   /** Main entry point.*/
   public static void main(String[] args) {

      if (log.isInfoEnabled()) log.info(_cn_+": Starting up. Arguments: "+Arrays.asList(args));

      int rc = 0; try {

         // Process the command-line options. ----------------------------------
         Config.bootstrap(args);

         args = Args.parseCommandLine(args);


         // Update the "Current Working Directory" property --------------------
         if (Config.workingDir != null) {

            // Note: The "current working directory" is a concept from the OS and not Java. Whenever creating a File object,
            // this application uses this "working directory" as the parent path. For consistency, here we are only setting
            // the "user.dir" property. The launch script is the one that actually updates the PWD before starting this
            // application, so core-files, etc. would go to the proper place.
            String cwd = System.getProperty("user.dir"); if (!Config.workingDir.equals(cwd)) System.setProperty("user.dir", Config.workingDir);
         }


         // Start the service handler in a daemon thread -----------------------
         Runnable runnable = new ServiceHandler(args);
         Thread   service  = new Thread(runnable); {

            service.setDaemon(true);
         }

         service.start();


         // Wait for the service handler to shutdown ---------------------------
         service.join();


         // TODO: figure out a way to receive the exit code and set 'rc' correctly
      }
      catch (Throwable t) { log.error(_cn_+": Unexpected error: ", t); }
      finally             { log.info (_cn_+": Shutting down..."     ); }

      ArkCon.close();

      if (rc > 0) System.exit(rc);
   }
}
