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

package com.ericsson.eea.ark.sli.grouping.services.hadoop;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
//import org.apache.avro.Schema;
//import org.apache.avro.SchemaBuilder;
//import org.apache.avro.SchemaBuilder.FieldBuilder;
//import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC.Server;
//import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.ericsson.eea.ark.offline.config.Config;
import com.ericsson.eea.ark.offline.config.PropertyManager;
//import com.ericsson.eea.ark.offline.utils.NGEEAlarmHelper;
//import com.ericsson.eea.ark.offline.utils.NGEEAlarmHelper.AlarmType;

//import com.ericsson.cea.bigdata.gridnotify.OffloadReload;

//import com.ericsson.cea.bigdata.common.service.model.ProfilerDataDictionary;
//import com.ericsson.cea.bigdata.common.service.model.ProfilerAttributeMetadata;


import com.ericsson.eea.ark.offline.utils.ArkCon;
import com.ericsson.eea.ark.offline.utils.HadoopUtils;
import com.ericsson.eea.ark.offline.utils.UserGroupsProviderFactory;
//import com.ericsson.eea.ark.offline.utils.UserGroups.UserGroup;
import com.ericsson.eea.ark.offline.utils.fs.FileUtil;
import com.ericsson.eea.ark.offline.utils.fs.FileUtil.SRM;
import com.ericsson.eea.ark.sli.grouping.config.Args;
import com.ericsson.eea.ark.sli.grouping.config.InputDataTypeEnum;
import com.ericsson.eea.ark.sli.grouping.config.State;
import com.ericsson.eea.ark.sli.grouping.memdb.ImsiGrpMemDbMapReduceUtil;
import com.ericsson.eea.ark.sli.grouping.memdb.ImsiGrpMemDbOutputFormat;
//import com.ericsson.eea.ark.sli.grouping.rpc.FileAndStatsManagerImpl;
import com.ericsson.eea.ark.sli.grouping.util.Util;
//import com.ericsson.eea.sli.grouping.rpc.FileAndStatsManager;
//import com.ericsson.eea.sli.grouping.profiler.data.MetadataHelper;
//import com.ericsson.eea.sli.grouping.profiler.data.MetadataConstants;
import com.ericsson.eea.ark.sli.groups.client.data.UserGroupsProvider_MemDb;



@SuppressWarnings("rawtypes")
public class GroupMapperDriver<I extends InputFormat
   ,                           M extends Reducer> extends    State
   /**/                                           implements Tool
   ,                                                         Callable<Integer> {

    public static final boolean win = (System.getProperty("os.name").toLowerCase().indexOf("windows") == 0);

   /** The available output formats to produced, which can be controlled by the Adapters via the 'adapters.inputFormats'
    * configuration parameter.*/
   public static enum InputFormat { avro, parquet };

   protected static final Logger log = Logger.getLogger(GroupMapperDriver.class);

   /** The configured time-zone.*/               protected String   timezone           = "GMT";

/** Reference to the property manager this class relies upon. */
   public static PropertyManager cfg = null;

   /** The base input directory.*/               protected String   baseInputDir       = null;
   /** The base output directory.*/              protected String   baseOutputDir      = null;
   /** The base configuration directory.*/       protected String   baseConfigDir      = null;
   /** The populated output formats.*/           protected String   inputFormats       = "parquet";
// /** Whether to clean an output path.*/        protected Boolean  deleteOutputPath   = false;
// /** Whether to write JSON output.*/           protected boolean  writeJson          = true;
   /** Data-grid wait in seconds.*/              protected int      datagridWaitSecs   = 5;
   /** Number data-grid connect attempts.*/      protected int      maxDatagridRetries = 3;
   /** Number of reducer tasks to be used.*/     protected int      numberOfReducers   = -1;
   /** List of sort-keys to operate by.*/        protected String[] sortKeys           = {"imsi"};
   /** The profile job types to get data from.*/ protected String[] jobTypes           = {"dpa"};
   /** The profile input job types to get data from.*/protected InputDataTypeEnum inputDataType;
   /** Leftover command-line arguments. */
   protected String[] args = null;


   /** Create a new {@linkplain GroupMapperDriver} task with no arguments. */
   public GroupMapperDriver(String[] args) {

      this.args = args;

      init(null);  // Delayed initialization
   }

   /** Create a new {@linkplain GroupMapperDriver} task with no arguments. */
   public GroupMapperDriver(Configuration conf) {

      init(conf);  // Delayed initialization
   }

   /** Create a new {@linkplain GroupMapperDriver} task with the specified arguments {@linkplain GroupMapperDriver#args}.*/
   public GroupMapperDriver(String[] args, Configuration conf) {

      this.args = args;

      init(conf);  // Delayed initialization
   }


   /** Delayed initialization method, which just record the Hadoop configuration.*/
   public void init(Configuration conf) {

      // Load the configuration
      Config._cn_           = "group-mapper";
      Config.cfg_prefix     = "group-mapper";
      Config.cfgEnvVar      = "GROUP_MAPPER_CONF";

      setConf(conf);
   }


   /** Actual initialization method, which is invoked from within {@linkplain run}.*/
   private void init() {

      if (cfg == null) return;



      if (inputDataType == null && Args.inputDataType != null) {
          inputDataType = InputDataTypeEnum.valueOf(Args.inputDataType);

      }

      // Parameters from the application's configuration file
      timezone           =                  cfg.getString (Config.cfg_prefix, "timezone"          , timezone          );
      baseInputDir          = Util.interpolate(cfg.getString(Config.cfg_prefix, inputDataType.getInputDir(), baseInputDir)); //CH-2
      baseOutputDir      = Util.interpolate(cfg.getString (Config.cfg_prefix, "baseOutputDir"     , baseOutputDir    ));
      baseConfigDir      = Util.interpolate(cfg.getString (Config.cfg_prefix, "baseConfigDir"     , baseConfigDir    ));
      inputFormats       =                  cfg.getString (Config.cfg_prefix, "inputFormats"      , inputFormats      );
    //deleteOutputPath   =                  cfg.getBoolean(Config.cfg_prefix, "deleteOutputPath"  , deleteOutputPath  );
    //writeJson          =                  cfg.getBoolean(Config.cfg_prefix, "writeJson"         , writeJson         );
      datagridWaitSecs   =                  cfg.getInt    (Config.cfg_prefix, "datagridWaitSecs"  , datagridWaitSecs  );
      numberOfReducers   =                  cfg.getInt    (Config.cfg_prefix, "numberOfReducers"  , numberOfReducers  );
      maxDatagridRetries =                  cfg.getInt    (Config.cfg_prefix, "maxDatagridRetries", maxDatagridRetries);
      inputDate          =                  cfg.getString (Config.cfg_prefix, "inputDate"         , inputDate         );
      endDate            =                  cfg.getString (Config.cfg_prefix, "endDate"           , endDate           );
      tableNameMemDb     =                  cfg.getString (Config.cfg_prefix, "tableNameMemDb"    , tableNameMemDb    );
      sortKeys           =                  cfg.getList   (Config.cfg_prefix, "sortKeys"          , sortKeys          );
      jobTypes           =                  cfg.getList   (Config.cfg_prefix, "jobTypes"          , jobTypes          );

      // Parameters received via Hadoop's configuration object (Note: the parameter(s) below will override the ones above).
      Configuration conf = getConf(); if (conf != null) {

         inputDate       =                 conf.get       (Config.cfg_prefix+".inputDate"         , inputDate         );
         endDate         =                 conf.get       (Config.cfg_prefix+".endDate"           , endDate           );
         numberOfReducers=                 conf.getInt    (Config.cfg_prefix+".numberOfReducers"  , numberOfReducers  );
      }
   }


   /**
    * This {@linkplain Callable#call} method implementation is invoked by the framework's service handler which in turn,
    * invokes the {@linkplain ToolRunner#run} method.
    */
   @Override
public Integer call() throws Exception {

    //args = Args.parseCommandLine(args);

          // Update the '-libjars' values, by referring to the jars from where the application is invoked from
          String baseDir = System.getProperty("base.dir");
        //URI        uri = URI.create(baseDir);
        //String  scheme = (uri.getScheme() == null) ? scheme = "file" : null;

          if (baseDir != null && dfsPrefix != null && !baseDir.equals(dfsPrefix)) {

             String[]   libJars = null;
             boolean hasLibJars = false;
             String[]   tmpFiles = null;
             boolean hasTmpFiles = false;
             String    homeDir  = configFile.replace("/config/"+Config.cfg_prefix+".conf", "")/*.replace(dfsPrefix, "")*/;

             if (!homeDir.equals(baseDir)) {
                 for (int j=0; j<args.length; ++j) {

                String arg = args[j]; if ("-libjars".equals(arg)) { hasLibJars = true; } else if (hasLibJars && arg != null) {

                   String prefix = null; libJars = arg.split(", *"); if (libJars != null && libJars.length >0) for (int i=0; i<libJars.length; ++i) {

                      String jar = libJars[i]; if (prefix == null && jar != null && jar.length() >0) {

//	                     if (jar.startsWith(dfsPrefix+baseDir)) libJars[i] = jar.replace(dfsPrefix, "");

                         libJars[i] = /*("file".equals(scheme)) ? scheme+"://"+jar
                            :          */                         jar.replace(baseDir, homeDir);  // TODO: Only modify the path of the files we find in HDFS...
                      }
                   }

                   args[j] = Util.intersperse(",", libJars); break;
                }
             }
                 for (int j=0; j<args.length; ++j) {

                     String arg = args[j]; if ("-files".equals(arg)) { hasTmpFiles = true; } else if (hasTmpFiles && arg != null) {

                        String prefix = null; tmpFiles = arg.split(", *"); if (tmpFiles != null && tmpFiles.length >0) for (int i=0; i<tmpFiles.length; ++i) {

                           String tmpFile = tmpFiles[i]; if (prefix == null && tmpFile != null && tmpFile.length() >0) {

//	 	                     if (tmpFile.startsWith(dfsPrefix+baseDir)) tmpFiles[i] = tmpFile.replace(dfsPrefix, "");

                              tmpFiles[i] = /*("file".equals(scheme)) ? scheme+"://"+tmpFile
                                 :          */                         tmpFile.replace(baseDir, homeDir);  // TODO: Only modify the path of the files we find in HDFS...
                           }
                        }

                        args[j] = Util.intersperse(",", tmpFiles); break;
                     }
                  }
          }
          }


      return ToolRunner.run(this, args);
   }


   /** This is the main entry point into the framework as invoked by Hadoop's {@linkplain ToolRunner}.*/
   @Override
@SuppressWarnings("unchecked")
   public int run(String[] args) throws Exception {

      if (log.isInfoEnabled()) log.info(_cn_+": Starting Hadoop Driver. Arguments: "+Arrays.asList(args));

      Exception                except              = null;
      Server                   server              = null;
      boolean                  jobsSuceeded        = false;
      FileSystem               fs                  = null;
      String                   oozieStatus         = null
         ,                     oozieErrmsg         = null;
      ArrayList<Job>           jobs                = new ArrayList<Job>(); try {

         // Get Hadoop's configuration and load the application's configuration. ----------------------------------------------
         Configuration conf = super.getConf(); if (conf == null) {

            throw new RuntimeException("Error. Cannot create Hadoop configuration.");
         }
         conf.set("mapreduce.job.user.classpath.first","true");
         if (conf.get("fs.defaultFS"      ) == null) conf.set("fs.defaultFS"      , "file:///");
         if (conf.get("mapred.job.tracker") == null) conf.set("mapred.job.tracker", "local"   );

         // Store the path/URN to the configuration file into the Hadoop configuration, so it is accessible for the subsequent Hadoop Job(s).
         if (log.isInfoEnabled()) {

            log.info(_cn_+": Setting up hadoop configuration: configDir=\""+Config.configFile+"\" and configFile=\""+PropertyManager.configFile+"\"");
         }

         if (PropertyManager.configFile == null) {
             Path path = getPathToConfigFile();
             if (path != null) {
                 PropertyManager.configDir = path.getParent().toString();
                 PropertyManager.configFile = path.getName();
                 log.info(_cn_+": found config file on classpath. configDir=\""+PropertyManager.configDir+"\" and configFile=\""+PropertyManager.configFile+"\"");
             } else {
                 log.info(_cn_+": did not find config file on classpath.");
             }
         }

         String cfgDir  = "config"                 ;
         String cfgFile = Config.cfg_prefix+".conf";

         if (PropertyManager.configDir  != null) {
             cfgDir = PropertyManager.configDir;
             conf.set("group-mapper.configurationDir" , PropertyManager.configDir);
         }
         if (PropertyManager.configFile != null) {
             cfgFile = PropertyManager.configFile;
             conf.set("group-mapper.configurationFile", cfgFile);
         }

         cfg = new PropertyManager(); cfg.readConfig((cfgDir == null ? "" : cfgDir + "/") + cfgFile);
         if (Config.cfg == null) {
             Config.cfg = cfg;
         }
         init();

         // Initialize User Group Provider
         conf.set("group-mapper.earliestTimestamp", Long.valueOf(System.currentTimeMillis()).toString());
         conf.set("group-mapper.tableNameMemDb", tableNameMemDb);

         ArkCon.serialize(conf);

         UserGroupsProviderFactory.setProviderClass(UserGroupsProvider_MemDb.class);
         UserGroupsProviderFactory.getUserGroupsProvider(conf);

         // Start the alarms interface ----------------------------------------------------------------------------------------
//         if (alarmsEnabled) Alarms.start();


         // Process the command line arguments --------------------------------------------------------------------------------
         args = new GenericOptionsParser(conf, args).getRemainingArgs();


         // Check for the Data Loader to ensure that the Data Grid is not updating as we run ----------------------------------
         // TODO: Move the data-grid stuff into its own abstracting class...
//         OffloadReload offloadReload = null; if (super.checkDataGridLoader) try {
//
//            offloadReload = OffloadReload.build();
//
//            for (int tries=0; tries <maxDatagridRetries && offloadReload.isLoading(); ++tries) {
//
//               Thread.sleep(datagridWaitSecs *1000);
//            }
//         }
//         catch (InterruptedException ex) {
//
//            log.error(_cn_+": Interrupted while waiting for data-grid loader; exiting..."); return 0;
//         }
//         catch (Exception ex) {
//
//            if (alarmsEnabled) NGEEAlarmHelper.logAlarm(AlarmType.dataGridIsDown);
//
//            oozieErrmsg = _cn_+": Cannot connect to Data Grid.";
//
//            log.error(_cn_+": Error connecting to data-grid. Cannot check for load status:", ex); return -1;
//         }
//         finally {
//
//            if (offloadReload != null) offloadReload.close(); offloadReload = null;
//         }


         // Ensure this application's job is not already running in Hadoop ----------------------------------------------------

         // Generate the list of output formats
         ArrayList<InputFormat> fmts = new ArrayList<InputFormat>(); for (String fmt : inputFormats.split("[ \t]*,[ \t]*")) {

            fmts.add(Enum.valueOf(InputFormat.class, fmt.trim()));
         }

         // Create a list of adapter job names; Do not proceed if any of them are running...
         HashMap<String,String> jobNames = new HashMap<String,String>(); for (InputFormat fmt : fmts) {

            // The Job's name needs to be sanitized for Hadoop
            String jobName = Config.cfg_prefix+"-"+fmt.name().replaceAll("[-._/]+", ""); jobNames.put(fmt.name(), jobName);
         }

         // Check if there is a mapper job already running for the several job names
         int nbrJobsRunning = 0; for (String jobName : jobNames.values()) {

            RunningJob runningJob = null; try {

               runningJob = HadoopUtils.getRunningJob(jobName);
            }
            catch (Throwable t) { /*no-op*/ }; if (runningJob != null) {

               ++nbrJobsRunning;
            }
         }

         if (nbrJobsRunning >0) {

            String msg = "Fatal: Cannot submit "+Config.cfg_prefix+" job as there"+(nbrJobsRunning==1?" is ":" are ")+nbrJobsRunning
               +                                                     " mapper job"+(nbrJobsRunning==1?    "":"s"    )+" running.";

            oozieErrmsg = "A "+_cn_+": Job is already running."; log.fatal(msg); throw new RuntimeException(msg);
         }


         // Create the controller job, iterate over the adapters, create a ControledJob for each adapter, and submit ----------
         // them to Hadoop for mapping.
         JobControl jc = new JobControl(Config.cfg_prefix);

//         String jobType=inputDataType.getMapperName();

            // Initialize the meta-data layer --------------------------------------------------------------------------------
//            MetadataHelper.getInstance().init(jobType, inputDate);

            // Don't start a Job if the required input directory is not populated (Hadoop would bail otherwise).
            SimpleDateFormat sdf = new SimpleDateFormat(inputDateFormat);
            Date     date        =                            sdf.parse(inputDate);
            Date     dateEnd     = (endDate == null) ? date : sdf.parse(  endDate);

            /*List<Path> inputPaths = new ArrayList<Path>();*/ Path inPath = null; for (long ts = date.getTime(), te = dateEnd.getTime(); ts <= te; ts = ts + (24 * 3600 * 1000)) {

               String dt = sdf.format(new Date(ts)); //conf.set("", dt);

               try {
                   Path myp=null;
                   switch (inputDataType) {

                   case dpa:  myp = FileUtil.instance(conf, SRM.ARK).getFile(baseInputDir+"/"+dt+"/");
                   log.info("dpa input path");
                   break;
                   case sncd:  myp = FileUtil.instance(conf, SRM.ARK).getFile(baseInputDir+"/");

                   if (log.isDebugEnabled()) log.debug("Is directory"+FileUtil.instance(conf, SRM.ARK).listSubDirLeavesRecursively(myp, null).size());

                   break;
                   default:      log.warn("Input Data Type not supported.."); continue;
                }
                   log.info("Adding file path for parquet input datatype :" + inputDataType+" => "+myp.toUri());
                   if (FileUtil.instance().getFS(myp).exists(myp)) inPath = myp;
               }
               catch (Exception ex) {

                /*no-op for now*/
               }
            /*}*/

            if (/*inputPaths.isEmpty()*/ inPath == null) {

               if (log.isInfoEnabled()) log.info("Cannot find input directory (no input files to process) at: '"+baseInputDir+"', between '"+inputDate+"' and '"+endDate+"'");

               /*return -1;*/ continue;
            }
//            Path[] inPaths = inputPaths.toArray(new Path[inputPaths.size()]); Arrays.sort(inPaths, new Comparator<Path>() {
//
//               @Override public int compare(Path p1, Path p2) { return p1.compareTo(p2); }
//            });
//
//            for (Path inPath : inPaths) {

               ControlledJob jobc = new ControlledJob(conf); for (InputFormat fmt : fmts) {

                  String name = fmt.name();

                  // Create a new Hadoop job
                  Job job = jobc.getJob(); {
                      if (win) {
                         Collection<File> files;
                         try {
                             files = FileUtils.listFiles(new File("build/libs"), new String[]{"jar"}, false);
                         } catch (Exception e) {
                             files = null;
                         }
                         if (files == null) files = new ArrayList<>();
                         File maxJar = null;
                         for (File file : files) {
                             if (maxJar == null || maxJar.getName().compareTo(file.getName()) < 0) {
                                 maxJar = file;
                             }
                         }
                         if (maxJar == null) {
                             throw new IllegalStateException("you have to do a gradle jar before you can run this unit test");
                         }
                         String absPath = maxJar.getAbsolutePath();
                         log.info("JOB.SETJAR("+absPath+")");
                          job.setJar(absPath);
                      } else {
                          job.setJarByClass         (        GroupMapperDriver.class);
                      }
                     job.setJobName            (             jobNames.get(name)+"-"+dt);
                     job.setMapOutputKeyClass  (                     Text.class);
                     job.setMapOutputValueClass(AvroGenericRecordWritable.class); if (numberOfReducers > -1) {
                     job.setNumReduceTasks     (               numberOfReducers); }
                     job.setOutputKeyClass     (   Text.class);
                     job.setOutputFormatClass  (        ImsiGrpMemDbOutputFormat.class);
                  }

                  Class inFmtClass    = null
                     ,  inMapperClass = null; switch (fmt) {

                     //case json:    inFmtClass    = JsonInputFormat               .class; break;
                     case parquet: inFmtClass    = AvroParquetInputFormat        .class;
                     inMapperClass = inputDataType.getMapperClass();

                     break;

                     default:      log.warn("Input format: '"+name+"' not supported, skiping Job: '"+job.getJobName()+"'"); continue;
                  }

                     for (String sortKey : sortKeys) {

                         Configuration c = job.getConfiguration(); {

                            c.set(Config.cfg_prefix+".sortKey"          , sortKey);
                            c.set(Config.cfg_prefix+".configurationDir" , cfgDir );
                            c.set(Config.cfg_prefix+".configurationFile", cfgFile);
                         }

                         Path inputPath=null;
                         switch (inputDataType) {

                         case dpa:  inputPath    = new Path(inPath, new Path(sortKey));
                         MultipleInputs.addInputPath(job,inputPath, inFmtClass, inMapperClass);

                         break;
                         case sncd:

                            List<Path> pathLs= FileUtil.instance(conf,SRM.ARK).listSubDirNovRecursively(inPath,null);

                            log.info("SNCD subdirecories count: "+pathLs.size());
                            for(Path validDir:getValidInputPaths(pathLs)){
                                  MultipleInputs.addInputPath(job,validDir, inFmtClass, inMapperClass);

                                log.info("Subdirectories within the time range: "+validDir.getName());
                                   List<Path> mapperInputs= FileUtil.instance(conf,SRM.ARK).listSubDirLeavesRecursively(validDir,null);
                                   log.info("Subdirectories within ts dir: "+mapperInputs.size());
                                   if(mapperInputs.size()>1){
                                   log.info("SNCD: Adding input path =>"+inputPath);
                                for(Path mapperInput:mapperInputs){
                                MultipleInputs.addInputPath(job,mapperInput, inFmtClass, inMapperClass);
                                }
                                }
                            }
                          break;
                         default:      log.warn("Input format: '"+name+"' not supported, skiping Job: '"+job.getJobName()+"'"); continue;
                      }
                  AvroParquetInputFormat.setInputPathFilter(job, InputFilesFilter.class);

                  addTmpFiles(job, args);

                  addLibJars(job, args);

                     ImsiGrpMemDbMapReduceUtil.initTableReducerJob(GroupMapperReducerMemDb.class, job);

                  if (log.isInfoEnabled()) log.info(_cn_+": Submitting Job: " +job.getJobName());

                  if (!wait) job.submit();
                  else       jc.addJob(jobc);

                  job.getConfiguration().set("inputDataType", Config.inputDataType);
                  jobs.add(job);
               }
            }



         // Start the RPC server to receive status/metrics regarding the several file chunks consumed -------------------------
         if (jobs.size() == 0) {

            if (log.isInfoEnabled()) log.info(_cn_+": No jobs were submitted; exiting...");

            return 0;
         }

         // Wait for all jobs to finish executing. ----------------------------------------------------------------------------
         if (wait) for (ControlledJob j : jc.getWaitingJobList()) {

            if (log.isInfoEnabled()) log.info(_cn_+": Waiting for jobs to complete");

            j.getJob().waitForCompletion(log.isDebugEnabled());
         }


         // Final status
         jobsSuceeded = true;

      }
      }
      catch (Throwable t) {

         except     = new RuntimeException(t);
         String msg = _cn_+": Error submitting/executing Hadoop job"; log.error(msg, t);
      }
      finally {

        // Stop the RPC Server
         try {

            if (server              != null) { server             .stop()             ; server              = null; }
          //if (fileAndStatsManager != null) { fileAndStatsManager.deleteMarkedFiles(); fileAndStatsManager = null; }
         }
         catch (Throwable t) {

            log.fatal("Fatal: Error cleaning up input files that were consumed.", t);
         }

         // Return status to Oozie // To debug, inspect: System.setProperty("oozie.action.output.properties", "oozie.properties")
         String oozieProp = System.getProperty("oozie.action.output.properties"); if (oozieProp != null) try {

            if (dfsPrefix.matches("^hdfs://.*$")) oozieProp = dfsPrefix+oozieProp; Properties props = new Properties();

            if (                                 oozieStatus == null) oozieStatus = jobsSuceeded ? "SUCCESS" : "FAILURE";
            if ("FAILURE".equals(oozieStatus) && oozieErrmsg == null) oozieErrmsg = "GroupMapper Job Failed";

            /**/                   props.setProperty("status"    , oozieStatus);
            if (!jobsSuceeded)     props.setProperty("errmsg"    , oozieErrmsg);

            OutputStream os = null; try {

               if (dfsPrefix.matches("^hdfs://.*$")) { Path propFile = new Path(oozieProp); os = fs.create(propFile, /*overwrite*/ true); }
               else                                  { File propFile = new File(oozieProp); os = new FileOutputStream(propFile)         ; }

               props.store(os, "");
            }
            finally {

               if (os != null) { os.close(); os = null; }
            }

            if (log.isInfoEnabled()) log.info(_cn_+": Set Oozie output properties at: '"+oozieProp+"'");
         }
         catch (Exception ex) {

            if (log.isInfoEnabled()) log.info(_cn_+": Cannot create Oozie properties file at: '"+oozieProp+"'");
         }

         // Close the file system
         if (fs != null) fs.close();

         // Stop the alarms interface
//         if (alarmsEnabled) Alarms.stop();

         // In case of any errors, and after having cleaned things up, throw the exception.
         if (except != null) throw except;
      }

      // Exit normally
      return 0;
   }


   /**
    *                         	     	 List all the sub-directories
    *
    * @param pathLs
    * @return
    */
   private List<Path> getValidInputPaths(List<Path> pathLs) {

       List<Path> validDirList = new ArrayList<>();
       for(Path path:pathLs){
           String dirName=path.getName();
           try{
           log.info("Processing "+dirName);
           long day = (dirName.length()==8)?  Long.parseLong(dirName) :0;

           long startDay=Long.parseLong(Config.inputDate);
           long endDay=Long.parseLong(Config.endDate);;
           if(day>0 && day>=startDay && day<=endDay){
               log.info("Complying with the conditions, add to the list=>"+dirName);
               validDirList.add(path);
           continue;
           }

           DateTimeFormatter formatter = DateTimeFormat.forPattern(inputDateFormat);

           DateTime startDate = formatter.parseDateTime(Config.inputDate);
           startDate=startDate.withTime(0, 0, 0, 0);
           DateTime endDate = formatter.parseDateTime(Config.endDate);
           endDate=endDate.withTime(23,59, 59, 999);

           String[] tsSplitterAry=dirName.split("=");
           String tsStr;

           if(tsSplitterAry.length >1)
               tsStr=tsSplitterAry[1];
           else
               tsStr=tsSplitterAry[0];
           log.info(tsStr);

           long dirTs=Long.parseLong(tsStr);
           boolean isInMillis=tsStr.length()>12;
           //Convert into milli seconds, when it comes in seconds
           dirTs=isInMillis?dirTs:dirTs*1000;

           log.info((startDate.getMillis()/1000)+ " - "+(endDate.getMillis()/1000));

           if(dirTs>startDate.getMillis() && dirTs<=endDate.getMillis()){
               log.info("Complying with the conditions, add to the list=>"+dirName);
           validDirList.add(path);
           }
           }catch(Exception e) {
               log.error("Ignoring th nvalid Directory pattern => "+dirName);
               continue;
           }

   }
    return validDirList;

}

private Path getPathToConfigFile() {
        ClassLoader loader = GroupMapperDriver.class.getClassLoader();
        URL url = loader.getResource("group-mapper.conf");
        if (url == null) {
            log.info(_cn_+":url for \"group-mapper.conf\" returns null...");
            return null;
        }
        String toReturn = url.getPath();

        toReturn = toReturn.replaceAll("\\+", "%2B");
        try {
            toReturn = URLDecoder.decode(toReturn, "UTF-8");
        } catch (Exception e) {
            log.info(_cn_+":cannot decode url to \"group-mapper.conf\" url is " + toReturn);
            return null;
        }
        toReturn = toReturn.replaceAll("!.*$", "");

        if ( ! toReturn.startsWith("file:")) {
            toReturn = "file:" + toReturn;
        }

        Path p = new Path(toReturn);
        return p;
   }

   private void addLibJars(Job job, String[] args)  {

        Configuration conf = job.getConfiguration();

        Set<String> cpJarsToAdd = new HashSet<>();
        String jobJarName = job.getJar().replaceAll("^.*[\\\\/]", "");

        String splitter =
            win ?
                    ";" : ":";

        String javaClassPath = System.getProperty("java.class.path");
        String cps[] = javaClassPath.split(splitter);
        String cpJarPath = "";
        for (String cp : cps) {
            if (cp.equals(jobJarName) || cp.endsWith("/" + jobJarName)) {
                cpJarPath = cp.substring(0, cp.length() - jobJarName.length());
                break;
            }
        }
        for (String cp : cps) {
            if (cp.startsWith(cpJarPath)) {
                String candidateJarName = cp.substring(cpJarPath.length());
                if ( ! candidateJarName.equals(jobJarName) &&
                        candidateJarName.matches("[-_.A-Za-z0-9]+\\.jar")) {
                    cpJarsToAdd.add(cp);
                } else if (win && cp.endsWith(".jar")) {
                    cpJarsToAdd.add(cp);
                    System.out.println(cp);
                }
            }
        }

                Set<String> allTmpJars = new HashSet<>();
                // Add jars that are already in the tmpjars variable
                allTmpJars.addAll(conf.getStringCollection("tmpjars"));
                Set<String> origAllTmpJars = log.isInfoEnabled() ? new HashSet<>(allTmpJars) : null;
                if (log.isInfoEnabled()) log.info("orig from tmpjars: " + origAllTmpJars);
                int sz = allTmpJars.size();

                String[] libJarsFromArgs = {};
                if (args != null && args.length > 0) {
                    int i;
                    for (i = 0; i < args.length; i++) {
                        if ("-libjars".equals(args[i])) break;
                    }
                    if (++i < args.length  && args[i] != null) {
                        libJarsFromArgs = args[i].split(", *");
                        if (libJarsFromArgs.length == 1 && libJarsFromArgs[0].isEmpty()) libJarsFromArgs = new String[]{};
                    }
                }
                // add anything found in -libjars arg
                cpJarsToAdd.addAll(Arrays.asList(libJarsFromArgs));

                for (String libjar : cpJarsToAdd) {
                    if (libjar == null || libjar.isEmpty()) continue;
                    try {
                      Path path = qualifiedPathToLibJar(libjar);
                      if (path == null) {
                        log.warn("Could not find jar \"" + libjar + "\" to put into distributed cache.");
                        continue;
                      }
                      if ( ! FileUtil.instance(conf,SRM.ARK).getLocalFS().exists(path)) {
                        log.warn("Could not validate jar file \"" + path + "\" for \""
                                 + libjar + "\" to put into distributed cache.");
                        continue;
                      }
                      allTmpJars.add(path.toString());
                    } catch (IOException e) {
                        log.warn("Could not find jar \"" + libjar +
                                 " to put into distributed cache.");
                    }
                }
                if (allTmpJars.size() == sz) {
                    // nothing new to add
                } else {

                    // set tmpjars with any additions form classpath &/or -libjars program argument:
                    conf.set("tmpjars", StringUtils.arrayToString(allTmpJars.toArray(new String[allTmpJars.size()])));

                    if (log.isInfoEnabled() && origAllTmpJars != null) {
                        Set<String> newTmpJars = new HashSet<>(conf.getStringCollection("tmpjars"));
                        newTmpJars.removeAll(origAllTmpJars);
                        log.info("addLibJars: added new tmpjars to distributed cache: " + newTmpJars);
                    }
                }
    }

   private Path qualifiedPathToLibJar(String libJar)
              throws IOException {
               String toReturn;
            if (win) {
                toReturn = "file:///"+libJar.replaceAll("\\\\", "/");
            } else {
                ClassLoader loader = GroupMapperDriver.class.getClassLoader();
                URL url = loader.getResource(libJar);
                if (url == null) return null;
                toReturn = url.getPath();
            }

            toReturn = toReturn.replaceAll("\\+", "%2B");
            toReturn = URLDecoder.decode(toReturn, "UTF-8");
            toReturn = toReturn.replaceAll("!.*$", "");

            if ( ! toReturn.startsWith("file:")) {
                toReturn = "file:" + toReturn;
            }

            Path p = new Path(toReturn);
            return p;
          }


    private void addTmpFiles(Job job, String[] args)  {

        Configuration conf = job.getConfiguration();

                Set<String> tmpFilesSet = new HashSet<String>();
                //get list of
                //tmpFiles already in "tmpfiles" property or "mapreduce.job.cache.files"
                tmpFilesSet.addAll(conf.getStringCollection("tmpfiles"));
                int sz = tmpFilesSet.size();
                if (sz > 0) {
                    log.info("orig from tmpfiles: " + tmpFilesSet);
                }
                URI[] cfs; try { cfs = job.getCacheFiles(); } catch (IOException e1) { e1.printStackTrace(); cfs = new URI[]{}; } if (cfs == null) cfs = new URI[]{};
                for (URI cf : cfs) {
                    if (cf != null && ! cf.toString().isEmpty()) {
                        tmpFilesSet.add(cf.toString());
                    }
                }
                if (tmpFilesSet.size() > sz) {
                    log.info("orig job.getCacheFiles(\"mapreduce.job.cache.files\"): " + Arrays.toString(cfs));
                }
                sz = tmpFilesSet.size();
                Set<String> sav = log.isInfoEnabled() ? new HashSet<String>(tmpFilesSet) : null;

                String[] tmpFilesFromArgs = {};
                if (args != null && args.length > 0) {
                    int i;
                    for (i = 0; i < args.length; i++) {
                        if ("-files".equals(args[i])) break;
                    }
                    if (++i < args.length && args[i] != null) {
                        tmpFilesFromArgs = args[i].split(", *");
                        if (tmpFilesFromArgs.length == 1 && tmpFilesFromArgs[0].isEmpty()) tmpFilesFromArgs = new String[]{};
                    }
                }

                for (String tmpFileFromArg : tmpFilesFromArgs) {

                    if (tmpFileFromArg == null || tmpFileFromArg.isEmpty()) continue;

                    try {
                        Path path;
                      try {
                          path = FileUtil.instance(conf,SRM.ARK).getFile(tmpFileFromArg);
                      } catch (Exception e) {
                          path = null;
                      }
                      if (path == null) {
                        log.warn("addTmpFiles: Could not find tmpFile \"" + tmpFileFromArg +
                                 "\" to put into distributed cache.");
                        continue;
                      }
                      if (!FileUtil.instance(conf,SRM.ARK).getFS(path).exists(path)) {
                          log.warn("addTmpFiles: Could not validate tmpFile \"" + path + "\" for \""
                                 + tmpFileFromArg + "\" to put into distributed cache.");
                        continue;
                      }

                      tmpFilesSet.add(path.toString());
                      job.addCacheFile(path.toUri());
                    } catch (IOException e) {
                        log.warn("addTmpFiles: Could not find tmpFile \"" + tmpFileFromArg +
                                 "\" to put into distributed cache.");
                    }
                }

                // if config file was loaded from classpath, make sure to add it to
                // the distributed cache so it's available downstream...
                if (PropertyManager.configDir != null &&
                        PropertyManager.configFile != null &&
                        PropertyManager.configDir.startsWith("file:")) {
                    tmpFilesSet.add(PropertyManager.configDir + "/" + PropertyManager.configFile);
                }

                if (tmpFilesSet.size() == sz) {
                    // nothing new to add
                } else {
                    conf.set("tmpfiles", StringUtils.arrayToString(tmpFilesSet.toArray(new String[tmpFilesSet.size()])));
                    if (log.isInfoEnabled() && sav != null) {
                        Set<String> newTmpFilesSet = new HashSet<>(tmpFilesSet);
                        newTmpFilesSet.removeAll(sav);
                        log.info("addTmpFiles: added new tmpfiles to distributed cache: " + newTmpFilesSet);
                    }
                }

        }

//   @SuppressWarnings("serial")
//   private void addAttributeToSchema(ProfilerDataDictionary targetdd, SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {
//
//      String               dataType = targetdd.getDataType();
//      FieldBuilder<Schema> fb       = fieldAssembler.name(targetdd.getAttributePath());
//
//      log.info(targetdd.getAttributePath() + " : " + dataType);
//
//      switch (dataType) {
//
//         case MetadataConstants.TYPE_INTEGER        : fb.type().nullable().   intType().                         intDefault(  0 ); break;
//         case MetadataConstants.TYPE_LONG           : fb.type().nullable().  longType().                        longDefault(  0l); break;
//         case MetadataConstants.TYPE_FLOAT          : fb.type().nullable(). floatType().                       floatDefault(0.0f); break;
//         case MetadataConstants.TYPE_DOUBLE         : fb.type().nullable().doubleType().                      doubleDefault(0.0 ); break;
//         case MetadataConstants.TYPE_STRING         : fb.type().nullable().stringType().                      stringDefault(  ""); break;
//         case MetadataConstants.TYPE_SET            : fb.type().nullable().     array(). items().stringType(). arrayDefault(new ArrayList<String>() {{add("");}}); break;
//         case MetadataConstants.TYPE_SETWITHINTEGER : fb.type().nullable().       map().values().   intType().    noDefault(    ); break;
//         case MetadataConstants.TYPE_SETWITHLONG    : fb.type().nullable().       map().values().  longType().    noDefault(    ); break;
//         case MetadataConstants.TYPE_SETWITHFLOAT   : fb.type().nullable().       map().values(). floatType().    noDefault(    ); break;
//         case MetadataConstants.TYPE_SETWITHDOUBLE  : fb.type().nullable().       map().values().doubleType().    noDefault(    ); break;
//      }
//   }


//   @SuppressWarnings("unused")
//   private Schema getAvroSchema(String ProfilerSortKey, String jobType) {
//
//      Collection<ProfilerAttributeMetadata> atts = MetadataHelper.getInstance().getAttributeMetadata();
//
//      SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(jobType)
//               .namespace("com.ericsson.eea.ark.offline.entity").fields();
//
//      // add the mandatory fields
//      fieldAssembler.name(ProfilerSortKey).type().stringType().noDefault();
//      fieldAssembler.name("SOURCE").type().stringType().noDefault();
//      fieldAssembler.name("DATE").type().stringType().noDefault();
//
//      for (ProfilerAttributeMetadata att : atts) {
//
//         if (att.isEnabled() && att.getSortKey().equals(ProfilerSortKey)) {
//
//            // check if its an attribute group as target
//            if (att.getTargetAttributeGroup() == null) {
//
//               ProfilerDataDictionary targetdd = MetadataHelper.getInstance().getTargetDictionary(att);
//               if ((targetdd == null) || !targetdd.getSource().equals(jobType)) continue;
//               addAttributeToSchema(targetdd, fieldAssembler);
//            }
//            else {
//
//             //Long                         attributeGroupId = att.getTargetAttributeGroup();
//               List<ProfilerDataDictionary> attList          = MetadataHelper.getInstance().getTargetAttributeList(att);
//               for (ProfilerDataDictionary targetdd : attList) {
//
//                  if ((targetdd == null) || !targetdd.getSource().equals(jobType)) continue;
//                  addAttributeToSchema(targetdd, fieldAssembler);
//               }
//            }
//         }
//
//      }
//
//      Schema avroSchema = fieldAssembler.endRecord();
//
//      log.info("avroSchema for ProfilerSortKey " + ProfilerSortKey + ", jobType " + jobType + " is: " + avroSchema);
//
//      return avroSchema;
//   }


//   @SuppressWarnings("unused")
//   private Schema getAvroSchemaFlat(String ProfilerSortKey, String jobType) {
//
//      Collection<ProfilerAttributeMetadata> atts = MetadataHelper.getInstance().getAttributeMetadata();
//
//      SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(jobType)
//         .                                                                namespace("com.ericsson.eea.ark.offline.entity")
//         .                                                                fields();
//
//      // add the mandatory fields
//      fieldAssembler.name(ProfilerSortKey).type().stringType().noDefault();
//      fieldAssembler.name("SOURCE"       ).type().stringType().noDefault();
//      fieldAssembler.name("DATE"         ).type().stringType().noDefault();
//
//      for (ProfilerAttributeMetadata att : atts) {
//
//         if (att.isEnabled() && att.getSortKey().equals(ProfilerSortKey)) {
//
//            // check if its an attribute group as target
//            if (att.getTargetAttributeGroup() == null) {
//
//               ProfilerDataDictionary targetdd = MetadataHelper.getInstance().getTargetDictionary(att);
//               if ((targetdd == null) || !targetdd.getSource().equals(jobType)) continue;
//
//               addAttributeToSchemaFlat(targetdd, fieldAssembler);
//            }
//            else {
//
//             //Long                         attributeGroupId = att.getTargetAttributeGroup();
//               List<ProfilerDataDictionary> attList          = MetadataHelper.getInstance().getTargetAttributeList(att);
//               for (ProfilerDataDictionary targetdd : attList) {
//
//                  if ((targetdd == null) || !targetdd.getSource().equals(jobType)) continue;
//
//                  addAttributeToSchemaFlat(targetdd, fieldAssembler);
//               }
//            }
//         }
//
//      }
//      Schema avroSchema = fieldAssembler.endRecord();
//      log.info("FlatAvroSchema for ProfilerSortKey " +ProfilerSortKey+ ", jobType " + jobType + " is: " + avroSchema);
//      return avroSchema;
//   }


//   private void addAttributeToSchemaFlat(ProfilerDataDictionary targetdd, SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {
//
//      String dataType = targetdd.getDataType();
//      log.info(targetdd.getAttributePath() + " : " + dataType);
//      if (dataType == null) return;
//
//      if (!dataType.startsWith(MetadataConstants.TYPE_SETWITH)) {
//
//         FieldBuilder<Schema> fb = fieldAssembler.name(targetdd.getAttributePath());
//
//         switch (dataType) {
//            case MetadataConstants.TYPE_INTEGER : fb.type().nullable().intType().intDefault(0);         break;
//            case MetadataConstants.TYPE_LONG    : fb.type().nullable().longType().longDefault(0l);      break;
//            case MetadataConstants.TYPE_FLOAT   : fb.type().nullable().floatType().floatDefault(0.0f);  break;
//            case MetadataConstants.TYPE_DOUBLE  : fb.type().nullable().doubleType().doubleDefault(0.0); break;
//            case MetadataConstants.TYPE_STRING  : // for FLAT schema, SET values are coded as strings
//            case MetadataConstants.TYPE_SET     : fb.type().nullable().stringType().stringDefault("");  break;
//         }
//      }
//      // SetWith*
//      else {
//
//         FieldBuilder<Schema> fb = fieldAssembler.name(targetdd.getAttributePath() + "_KEY");
//         fb.type().nullable().stringType().stringDefault("");
//         fb = fieldAssembler.name(targetdd.getAttributePath() + "_VALUE");
//
//         switch (dataType) {
//            case MetadataConstants.TYPE_SETWITHINTEGER : fb.type().nullable().intType().intDefault(0);         break;
//            case MetadataConstants.TYPE_SETWITHLONG    : fb.type().nullable().longType().longDefault(0l);      break;
//            case MetadataConstants.TYPE_SETWITHFLOAT   : fb.type().nullable().floatType().floatDefault(0.0f);  break;
//            case MetadataConstants.TYPE_SETWITHDOUBLE  : fb.type().nullable().doubleType().doubleDefault(0.0); break;
//         }
//      }
//   }
}


class InputFilesFilter extends Configured implements PathFilter {

   private static Logger log = Logger.getLogger(InputFilesFilter.class);

   static String dir;
   Configuration conf;
   Pattern       dirsPattern;
   Pattern       filesPattern;

   @Override
   public boolean accept(Path path) {

      boolean rc = false; try {

         boolean isDir = FileUtil.instance(conf, SRM.ARK).isDirectory(path);
         Pattern pat   = isDir ? dirsPattern : filesPattern;
         String  what  = isDir ? "directory" : "file";

         Matcher m       = pat.matcher(path.toString());
         boolean matches = m.matches();

         log.info(Config._cn_+" "+(matches?"accepted":"rejected")+" "+what+": '"+path+"'");

         rc = matches;
      }
      catch (IOException ex) {

         log.error("Unexpected error while inspecting path: '"+path+"': ", ex); rc = false;
      }

      return rc;
   }

   @Override
   public void setConf(Configuration conf) {

      this.conf = conf; if (conf != null) {

//
          switch(Config.inputDataType){
          case "dpa":
              dirsPattern  = Pattern.compile(conf.get(Config.cfg_prefix+".inputDirsPattern" , Config.dpaInputDirsPattern ));
              break;
          case "sncd":
              dirsPattern  = Pattern.compile(conf.get(Config.cfg_prefix+".inputDirsPattern" , Config.sncdInputDirsPattern ));
              break;
              default:
                  log.error("Invalid input datatype "+Config.inputDataType);

          }

         filesPattern = Pattern.compile(conf.get(Config.cfg_prefix+".inputFilesPattern", Config.inputFilesPattern));
      }
   }
}