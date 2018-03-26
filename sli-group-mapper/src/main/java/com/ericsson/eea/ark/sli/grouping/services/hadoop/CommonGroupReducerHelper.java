package com.ericsson.eea.ark.sli.grouping.services.hadoop;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.slf4j.Logger;

import com.ericsson.ark.core.ArkContext;
import com.ericsson.eea.ark.offline.config.Config;
import com.ericsson.eea.ark.offline.config.DistCacheIfc;
import com.ericsson.eea.ark.offline.config.PropertyManager;
import com.ericsson.eea.ark.offline.rules.data.DataGridDataProvider;
import com.ericsson.eea.ark.offline.utils.ArkCon;
import com.ericsson.eea.ark.sli.grouping.rules.GroupMapperRules;

public abstract class CommonGroupReducerHelper<KEYOUT,VALUEOUT> implements DistCacheIfc {
       private final Logger log;

       private ReduceContext<Text,AvroGenericRecordWritable,KEYOUT,VALUEOUT> contextSavedAtSetupTime;

       protected      GroupMapperRules rulesExecuter = null;

       CommonGroupReducerHelper(Logger log) {
           this.log = log;
        }


    public void cleanup(ReduceContext context) throws IOException, InterruptedException {

        // Take down the data grid connection
        DataGridDataProvider dgrdp = DataGridDataProvider.getInstance();
        if (dgrdp != null)
            dgrdp.close();

        ArkContext arkContext = ArkCon.getArkContext(null);
        if (arkContext != null)
            arkContext.close();

    }

        public void setup(ReduceContext context) throws IOException, InterruptedException {

        this.contextSavedAtSetupTime = context;
        String inputDataType = context.getConfiguration().get("inputDataType");
        log.info("Input type in Helper [contextSavedAtSetupTime]" + inputDataType);

        Configuration conf = context.getConfiguration();
        if (conf == null) {

            throw new RuntimeException("Error. The 'setup' method was invoked outside of a Hadoop container.");
        }
        conf.set("mapreduce.job.user.classpath.first", "true");
        ArkCon.getArkConfiguration(conf);

        // Load the configuration
        Config._cn_ = "group-mapper";
        Config.cfg_prefix = "group-mapper";
        Config.cfgEnvVar = "GROUP_MAPPER_CONF";
        Config.cfg = init(conf);
        Config.rules_file = "/eea_config/sli-group-mapper/rules/SLI-user_groups-rules-base.drl";
        // Prepare the RE data needed for the reducer
        rulesExecuter = new GroupMapperRules(conf, this);

        // Initialize the data grid connection
        DataGridDataProvider dgrdp = DataGridDataProvider.getInstance();
        if (dgrdp != null)
            dgrdp.init();

    }

    private com.ericsson.eea.ark.offline.config.PropertyManager init(Configuration conf) {

        if (conf == null) {

            String msg = "Error. Cannot intialize group-mapper's configuration from a null Hadoop configuration object.";

            log.error(msg);
            throw new RuntimeException(msg);
        }

        String cfgDir = PropertyManager.configDir = conf.get(Config.cfg_prefix + ".configurationDir");
        // String cfgHostPort = conf.get(Config.cfg_prefix+".rpcHostPort" ); if
        // (cfgHostPort != null) rpcHostPort = Util.getPair(cfgHostPort, ':');
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
            System.out.println("[" + Config._cn_ + "] Received adapter configuration: configDir=\""
                    + PropertyManager.configDir + "\" and configFile=\"" + PropertyManager.configFile + "\"");
        }

        if (Config.cfg == null) {
            synchronized (GroupMapperSncdReducerHelper.class) {
                if (Config.cfg == null) {

                    // Load the adapter's configuration
                    Config.cfg = new PropertyManager(Config._cn_, Config.cfgEnvVar);
                    Config.cfg.readConfig((cfgDir == null || cfgDir.length() == 0 ? "" : cfgDir + "/") + cfgFile);
                }
                // if (rpcHostPort == null)
                // rpcHostPort = Config.cfg.getPair("adapters", "rpcHostPort",
                // null, ':');
            }
        }

        return Config.cfg;
    }

    @Override
    public String getPathToConfigFileFromDistCache() {
        Path path = null;
        URI[] cfs;
        try {
            cfs = contextSavedAtSetupTime.getCacheFiles();
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

      public  ReduceContext<Text,AvroGenericRecordWritable,KEYOUT,VALUEOUT > getContext() {
         return contextSavedAtSetupTime;
     }
      public abstract void reduce(GroupMapperReducer trampoline, Text user, Iterable values,
              ReduceContext context) throws IOException, InterruptedException;
}