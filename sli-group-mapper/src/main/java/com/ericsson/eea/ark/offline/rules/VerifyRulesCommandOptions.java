package com.ericsson.eea.ark.offline.rules;

import java.util.List;
import java.util.ArrayList;

import lombok.Getter;
import lombok.Setter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.CmdLineException;



/** Process and validates ShowEsr user options.*/
public class VerifyRulesCommandOptions {

   private static final Logger log = LoggerFactory.getLogger(VerifyRulesCommandOptions.class.getName());

   // Command line options
   @Getter @Setter @Option(name="-c", usage="configuration file. I.e -c config.text"                ) private  String configfilenameOption  = null;
   @Getter @Setter @Option(name="-w", usage="Use this option to suppress warning"                   ) private Boolean supressWarningOption  = null;
   @Getter @Setter @Option(name="-a", usage="Use this option to suppress verifier analysis of rules") private Boolean supressAnalysisOption = null;
   @Getter @Setter @Option(name="-s", usage="source option: directory or file"                      ) private  String sourceOption          = null;

   // receives other command line parameters than options
   @Argument
   private List<String> arguments = new ArrayList<String>();

   private String[] args = null;

   public VerifyRulesCommandOptions(String[] args) {
      this.args = args;
   }

   /**
    * This constructor is to be used when the options will not be set via command line.
    */
   public VerifyRulesCommandOptions() {
   }

   public boolean processOptions() {

      CmdLineParser parser = new CmdLineParser(this);

      // If you have a wider console, you could increase the value; here 80 is also the default
      parser.setUsageWidth(80);

      if (this.args == null || args.length == 0) {

         log.info("no options provided");
         parser.printUsage(System.out);
         return false;
      }

      try {

         // Parse the arguments.
         parser.parseArgument(args);

      }
      catch (CmdLineException e) {

         // If there's a problem in the command line, you'll get this exception. this will report an error message.
         System.out.println("invalid arguments:" + e.getMessage());
         log.error("invalid arguments:" + e.getMessage());

         // print the list of available options
         parser.printUsage(System.out);

         return false;
      }

      return true;
   }
}
