
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

package com.ericsson.eea.ark.sli.grouping.services.hadoop;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import java.io.IOException;
import java.net.URI;
import java.rmi.server.UID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.avro.generic.GenericRecord;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.ericsson.ark.core.ArkContext;
import com.ericsson.eea.ark.offline.config.Config;
import com.ericsson.eea.ark.offline.utils.ArkCon;
import com.ericsson.eea.ark.offline.utils.fs.*;

import com.ericsson.eea.ark.offline.config.PropertyManager;
import com.ericsson.eea.ark.offline.rules.data.DataGridDataProvider;
import com.ericsson.eea.ark.offline.rules.refdata.RuleIdParameter;

import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;

/**
 * Mapper to filter based upon the rule parameters and to sort based on a given
 * key.
 */
public class CommonGroupMapper extends Mapper<Void, GenericRecord, Text, AvroGenericRecordWritable> {

    private static Logger log = LoggerFactory.getLogger(CommonGroupMapper.class);

    private Long jobStart = 0L;

    protected Context context = null;
    private RuleIdParameter ruleIdParams = null;
    private Set<String> ruleParams = null;

    protected String rules_customDir = "/eea_config/sli-group-mapper/rules/custom";
    protected String rules_parametersFile = "/eea_config/sli-group-mapper/rules/rule_parameters.csv";
    protected String rules_descriptionsFile = "/eea_config/sli-group-mapper/rules/rule_descriptions.csv";
    private Context contextAtSetupTime;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

        super.setup(this.context = context);
        this.contextAtSetupTime = context;
        jobStart = System.currentTimeMillis();

        Configuration conf = context.getConfiguration();
        if (conf == null) {

            throw new RuntimeException("Error. The 'setup' method was invoked outside of a Hadoop container.");
        }
        conf.set("mapreduce.job.user.classpath.first", "true");
        // conf.set("mapreduce.output.fileoutputformat.compr‌​ess.codec","org.apache.hadoop.io.compress.GzipCodec");
        ArkCon.getArkConfiguration(conf);

        // Load the group-mapper's configuration file
        Config._cn_ = "group-mapper";
        Config.cfg_prefix = "group-mapper";
        Config.cfgEnvVar = "GROUP_MAPPER_CONF";
        Config.cfg = init(conf);

        // Load the configuration parameters
        if (Config.cfg != null) {

            Config.rules_customDir = rules_customDir = Config.cfg.getString(Config.cfg_prefix, "rules.customDirectory",
                    rules_customDir);
            Config.rules_parametersFile = rules_parametersFile = Config.cfg.getString(Config.cfg_prefix,
                    "rules.parametersFile", rules_parametersFile);
            Config.rules_descriptionsFile = rules_descriptionsFile = Config.cfg.getString(Config.cfg_prefix,
                    "rules.descriptionsFile", rules_descriptionsFile);
            Config.dataGridEnabled = Config.cfg.getBoolean(Config.cfg_prefix, "dataGrid.available",
                    Config.dataGridEnabled);

        }
        // Initialize the data grid connection
        DataGridDataProvider dgrdp = DataGridDataProvider.getInstance();
        if (dgrdp != null)
            dgrdp.init();

        // Load the rule parameters
        ruleIdParams = getRuleIdParameter();
    }

    public PropertyManager init(Configuration conf) {

        if (conf == null) {

            String msg = "Error. Cannot intialize group-mapper's configuration from a null Hadoop configuration object.";

            log.error(msg);
            throw new RuntimeException(msg);
        }

        String cfgDir = PropertyManager.configDir = conf.get(Config.cfg_prefix + ".configurationDir");
        String cfgFile = PropertyManager.configFile = conf.get(Config.cfg_prefix + ".configurationFile");

        if ((PropertyManager.configDir == null && PropertyManager.configFile == null)
                || (PropertyManager.configDir != null && PropertyManager.configDir.startsWith("file:"))) {
            Path path = null;
            String ps = getPathToConfigFileFromDistCache();
            if (ps != null)
                path = new Path(ps);
            if (path != null) {
                cfgDir = PropertyManager.configDir = path.getParent().toString();
                cfgFile = PropertyManager.configFile = path.getName();
                conf.set(Config.cfg_prefix + ".configurationDir", PropertyManager.configDir);
                conf.set(Config.cfg_prefix + ".configurationFile", PropertyManager.configFile);
            }
        }
        if (cfgDir == null || cfgDir.isEmpty() || cfgFile == null || cfgFile.isEmpty()) {

            String msg = "Error. Reducer did not receive 'group-mapper.configurationDir' and/or 'group-mapper.configurationFile' parameter(s).";

            log.error(msg);
            throw new RuntimeException(msg);
        }

        if (log.isInfoEnabled()) {

            log.info("[" + Config._cn_ + "] Received adapter configuration: configDir=\"" + PropertyManager.configDir
                    + "\" and configFile=\"" + PropertyManager.configFile + "\"");
        }

        if (Config.cfg != null)
            return Config.cfg;
        synchronized (GroupMapperMapper.class) {

            if (Config.cfg != null)
                return Config.cfg;

            // Load the adapter's configuration
            Config.cfg = new PropertyManager(Config._cn_, Config.cfgEnvVar);
            Config.cfg.readConfig((cfgDir == null || cfgDir.length() == 0 ? "" : cfgDir + "/") + cfgFile);
        }

        return Config.cfg;
    }

    protected String getPathToConfigFileFromDistCache() {
        Path path = null;
        URI[] cfs;
        try {
            cfs = contextAtSetupTime.getCacheFiles();
        } catch (IOException e) {
            cfs = null;
        }
        if (cfs == null)
            cfs = new URI[] {};
        try {
            for (URI cf : cfs) {
                if (cf.getPath().endsWith("group-mapper.conf")) {
                    path = new Path(cf.getPath().replaceFirst("#.*$", ""));
                    break;
                }
            }
        } catch (Exception e) {
            log.warn("failed to process URIs " + e);
        }
        return path == null ? null : path.toString();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Take down the data grid connection
        DataGridDataProvider dgrdp = DataGridDataProvider.getInstance();
        if (dgrdp != null)
            dgrdp.close();

        ArkContext arkContext = ArkCon.getArkContext(null);
        if (arkContext != null)
            arkContext.close();
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        setup(context);
        try {

            while (context.nextKeyValue()) {

                map(context.getCurrentKey(), context.getCurrentValue(), context);
            }
        } catch (Exception ex) {

            String errFile = "UNKNOWN";
            try {

                FileSplit fileSplit = (FileSplit) context.getInputSplit();
                errFile = fileSplit.getPath().getName();
            } catch (Exception e2) {
                /* ignore */ }

            log.error("Error in parsing file: " + errFile + ", ignoring the file", ex);

            throw ex;
        } finally {

            cleanup(context);
        }
    }

    public void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {
        // try{
        // Time the actual execution
        long startTime = System.currentTimeMillis();
        try {

            // Lazily create the superset of parameters from all rules
            if (ruleParams == null) {

                ruleParams = new HashSet<String>();
                if (ruleIdParams != null) {

                    Set<String> ruleIds = ruleIdParams.getRuleIds();
                    if (ruleIds != null)
                        for (String ruleId : ruleIds) {

                            Map<String, Object> thresholds = ruleIdParams.getThresholdMap(ruleId);
                            if (thresholds != null) {

                                Set<String> ps = thresholds.keySet();
                                if (ps != null)
                                    for (String p : ps)
                                        ruleParams.add(p);
                            }
                        }
                }
            }

            // Check if this event can fire a rule
            boolean hasFiringRules = false;
            if (ruleParams != null)
                for (String parm : ruleParams) {

                    Object val = value.get(parm);
                    if (val != null) {

                        hasFiringRules = true;
                        break;
                    }
                }

            // It wont fire any rule, drop the event.
            if (!hasFiringRules)
                return;

            // It will file a rule, send record to reducer.
            GenericRecord record = context.getCurrentValue();
            AvroGenericRecordWritable agrw = new AvroGenericRecordWritable(record);
            agrw.setRecordReaderID(new UID((short) 0));
            agrw.setFileSchema(record.getSchema());
            context.write(new Text("imsi"), agrw);

        } catch (Exception e) {
            log.error("Error occured in mapper" + e);

        } finally {

            long estimatedTime = System.currentTimeMillis() - startTime;

            context.getCounter("group-mapper", "total-map-time-millis").increment(estimatedTime);
        }
    }

    /** Load the rule parameters and thresholds. */
    private RuleIdParameter getRuleIdParameter() {

        RuleIdParameter ruleIdParm = new RuleIdParameter();
        try {

            log.info("loading rules thresholds from file(CGM): '" + rules_parametersFile + "'");

            InputFile file = new InputFile(rules_parametersFile);
            if (file.ready()) {

                ruleIdParm.loadRulesThresholds(file.inputStream());
            }
        } catch (Exception ex) {

            String msg = "Error parsing the rule parameters file.";
            log.error(msg, ex);
            throw new RuntimeException(msg, ex);
        }

        return ruleIdParm;
    }
}
