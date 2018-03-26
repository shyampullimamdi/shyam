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

package com.ericsson.eea.ark.sli.grouping.config;

import java.util.Iterator;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;
import org.apache.commons.io.Charsets;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.lang.StringUtils;
import com.ericsson.eea.ark.sli.grouping.CLI;
import com.ericsson.eea.ark.offline.config.Config;


/**
 * This class handles the command line arguments for the application. Notice that the command line arguments have precedence
 * over the parameters in the applicaiton's configuration file.
 */
public class Args extends Config {

   /** Prefix used for tracing.*/
   public static final String _cn_ = CLI._cn_
      ,                       _sp_ = _cn_.replaceAll(".", " ");

   protected static final Logger log = Logger.getLogger(Args.class);

   static {

      Config._cn_      = Config.cfg_prefix = "group-mapper";
      Config.cfgEnvVar                     = "GROUP_MAPPER_CONF";
   }


   // =========================================================================================================================
   /** Display a help message to the user.*/
   private static void help(OutputStream os) { try { for (String s : new String[] {

        ""
      , "Offline Analytics Group-mapper"
      , ""
      , "This tool is used for invoking the offline analytics group-mapper, which maps users into groups, based upon their KPIs."
      , ""
      , "Usage:"
      , "   "+_cn_+" (-d|--input-date) <start-date> [(-e|--end-date) <end-date>] [(-h|--help)]"
      , ""
      , "Where:"
      , "   [(-h | --help)]                      "+          helpMsg
      , "   [(-c | --conf)       <config-file>]  "+    configFileMsg + " (default: "+ configFile           + ")"
    //, "   [(-S | --stats-dir)    <stats-dir>]  "+      statsDirMsg + " (default: "+ statsDir             + ")"
      , "   [(-d | --input-date)  <start-date>]  "+     inputDateMsg + " (default: "+ inputDate            + ")"
      , "   [(-e | --end-date)      <end-date>]  "+       endDateMsg + " (default: "+ endDate              + ")"
      , "   [(-w | --wait)]                      "+          waitMsg + " (default: "+ wait                 + ")"
      , "   [(-i | --inputDataType)]             "+ inputDateTypeMsg + " (default: "+ inputDataType        + ")"
      , ""
      , "For detailed information, please consult the manual page: " +_cn_+ "(1)"
      , ""
      , "For any questions/suggestions on this tool, please contact the EEA Offline Development Team."
      , ""

   }) os.write((s+"\n").getBytes(Charsets.UTF_8)); } catch (IOException ex) { /*ignore*/ }}


   /** The command-line processing object.*/
   private static Options opt = new Options();

   /** The command-line options accepted by the application.*/
   static {
      //                   flag,      flag-name,   arg?,     description             Required?
      //                   ----  ---------------  -----  ---------------  --------------------
      Option o = new Option("h",          "help", false,              helpMsg); o.setRequired(false); opt.addOption(o)
         ;   o = new Option("c",          "conf",  true,         configFileMsg); o.setRequired(false); opt.addOption(o)
         ;   o = new Option("S",     "stats-dir",  true,           statsDirMsg); o.setRequired(false); opt.addOption(o)
       //;   o = new Option("W",   "working-dir",  true,         workingDirMsg); o.setRequired(false); opt.addOption(o)
         ;   o = new Option("d",    "input-date",  true,          inputDateMsg); o.setRequired(true ); opt.addOption(o)
         ;   o = new Option("e",      "end-date",  true,            endDateMsg); o.setRequired(false); opt.addOption(o)
         ;   o = new Option("i", "inputDataType",  true, inputDateTypeMsg); o.setRequired(false); opt.addOption(o)
         ;   o = new Option("w",          "wait", false,        waitMsg); o.setRequired(false); opt.addOption(o); o=null;
   }





   /** Parses the command-line arguments.*/
   public static String[] parseCommandLine(String[] args) {

      // Create the command-line parser ---------------------------------------------------------------------------------------
      int               rc       = 0;
      boolean           showHelp = false;
      String[]          appArgs  = null;
      CommandLineParser parser   = new GnuParser(); try {

         CommandLine opts = parser.parse(opt, args, /*stopAtNonOption*/true);

         Iterator<Option> itr = opts.iterator(); while (itr.hasNext()) {

            Option opt = (Option) itr.next(); char o = (char) opt.getId(); switch (o) {

               case 'h':   showHelp  = true;        help  (System.out); System.exit(0);          break;
               case 'c':  /*cfg.readConfig((*/configFile =                 opt.getValue()/*))*/; break;
               case 'd':                       inputDate =                 opt.getValue();       break;
               case 'e':                         endDate =                 opt.getValue();       break;
             //case 'S':                        statsDir =                 opt.getValue();       break;
               case 'W':                      workingDir =                 opt.getValue();       break;
               case 'w':                            wait = !wait;                                break;
               case 'i':                   inputDataType =                 opt.getValue();       break;

               default: ++rc; System.err.println("Invalid option '-" + (char) o + "'");
            }
         }
         setDefaultValuesForNull();
         // The command-line parser stop at the first non-recognized option, which is then passed to the application. I'd rather
         // collect them in a separate list to be passed on...
         appArgs = opts.getArgs();
      }
      catch(Exception ex) {

         ++rc; System.err.println("Error. Invalid command-line arguments: " +ex.getMessage());
      }


      // Constraints ----------------------------------------------------------------------------------------------------------
      if (!showHelp && rc > 0) { System.err.println("\nPlease, see the help message for more information."); System.exit(rc); }


      // Exit normally? -------------------------------------------------------------------------------------------------------
      if (showHelp) { help(System.out); System.exit(0); }

      return appArgs;
   }
      static void setDefaultValuesForNull(){

          //Set DPA to inputdata type, if it's not passed from ARK console or having empty value
          if(inputDataType==null ||StringUtils.isEmpty(inputDataType)){
              log.info("Setting default input datatype, as empty/invalid value passed from console!");
              inputDataType=InputDataTypeEnum.dpa.mapperName;
          }
   }
}