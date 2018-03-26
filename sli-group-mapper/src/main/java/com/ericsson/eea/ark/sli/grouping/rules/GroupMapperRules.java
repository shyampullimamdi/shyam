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

package com.ericsson.eea.ark.sli.grouping.rules;

//import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.drools.runtime.StatefulKnowledgeSession;

import com.ericsson.eea.ark.offline.config.Config;
import com.ericsson.eea.ark.offline.config.DistCacheIfc;
import com.ericsson.eea.ark.offline.config.PropertyManager;
import com.ericsson.eea.ark.common.service.model.CrmCustomerDeviceInfo;
import com.ericsson.eea.ark.offline.rules.RuleEngine;
import com.ericsson.eea.ark.offline.rules.SegmentLogic;
import com.ericsson.eea.ark.offline.rules.VerifyRulesCommandOptions;
import com.ericsson.eea.ark.offline.rules.data.DataGridDataProvider;
import com.ericsson.eea.ark.offline.rules.refdata.CRM;
import com.ericsson.eea.ark.offline.rules.refdata.REreference;
import com.ericsson.eea.ark.offline.rules.refdata.RuleIdParameter;
import com.ericsson.eea.ark.offline.rules.utils.Util;
//import com.ericsson.eea.ark.sli.grouping.util.InputFile;
import com.ericsson.eea.ark.sli.grouping.rules.events.Event_DPA;
import com.ericsson.eea.ark.sli.grouping.rules.events.Event_SNCD;
//import com.ericsson.eea.ark.offline.utils.fs.FileUtil;
//import com.ericsson.eea.ark.offline.utils.fs.FileUtil.SRM;
import com.ericsson.eea.ark.offline.utils.fs.InputFile;
import com.ericsson.eea.ark.offline.rules.VerifyRulesCommandOptions;

public class GroupMapperRules {

    private static Logger log = LoggerFactory.getLogger(GroupMapperRules.class);

    private static boolean initialized = false;
 // private static final String cfg_prefix = Config.cfg_prefix;

    protected String rules_file = "/eea_config/sli-group-mapper/rules/SLI-user_groups-rules-base.drl";
    protected String rules_customDir = "/eea_config/sli-group-mapper/rules/custom";
    protected String rules_parametersFile = "/eea_config/sli-group-mapper/rules/rule_parameters.csv";
    protected String rules_descriptionsFile = "/eea_config/sli-group-mapper/rules/rule_descriptions.csv";

    private RuleIdParameter ruleIdParms = null;
 // private REreference reReference = null;

    private static final ThreadLocal<SegmentLogic> localSegmentLogic = new ThreadLocal<SegmentLogic>() {

        @Override protected SegmentLogic initialValue() {

            SegmentLogic seg = null; try {

                seg = new SegmentLogic(0);
            } catch (Throwable ex) {

                String msg = "Exception initializing Drools Session"; log.error(msg, ex); throw new RuntimeException(msg, ex);
            }

            return seg;
        }
    };

    public GroupMapperRules() {

        init(null,null);
    }

    // private FileUtil fu;
    public GroupMapperRules(Configuration hadoopConfig, DistCacheIfc distCacheIfc) {

        init(hadoopConfig,distCacheIfc);
    };

    public GroupMapperRules(PropertyManager cfg, Configuration hadoopConfig, DistCacheIfc distCacheIfc) {

        Config.cfg = cfg;

        init(hadoopConfig, distCacheIfc);
    }

    /**
     * Initialize the parameters taken from the application's configuration* file.*/
    private void init(Configuration hadoopConfig, DistCacheIfc distCacheIfc) {

        if (initialized == true) return;

        if (Config.cfg == null) {

            if (hadoopConfig == null) {

                throw new RuntimeException("Fatal: The Hadoop configuration is null; Config object can't be initialized.");
            }

            Config.cfg = Config.init(hadoopConfig, distCacheIfc);
        }
        // fu = FileUtil.instance(hadoopConfig,SRM.ARK);


        rules_file             = Config.cfg.getString(Config.cfg_prefix, "rules.file", rules_file);
        rules_customDir        = Config.cfg.getString(Config.cfg_prefix, "rules.customDir", rules_customDir);
        rules_parametersFile   = Config.cfg.getString(Config.cfg_prefix, "rules.parametersFile", rules_parametersFile);
        rules_descriptionsFile = Config.cfg.getString(Config.cfg_prefix, "rules.descriptionsFile", rules_descriptionsFile);
        Config.dataGridEnabled = Config.cfg.getBoolean(Config.cfg_prefix, "dataGrid.available", Config.dataGridEnabled);


        // Initialize the data grid connection.
        if (DataGridDataProvider.getInstance() != null)
            DataGridDataProvider.getInstance().init();

        // Load the rule parameters file.
        ruleIdParms = getRuleIdParameter(); {

            REreference.setRuleIdParms(ruleIdParms);
        }

        // Validate the rules
        if (!validatesRuleFiles()) {

            String msg = "Invalid Rules: can not proceed!"; log.error(msg); throw new RuntimeException(msg);
        }

        initialized = true;
    }

    @SuppressWarnings("unused")
    public void execute(Event_DPA dpa) { // TridentTuple tuple, TridentCollector // collector) {

        String encryptedEsr = null;
        Long   start        = null;
        try {

            // Insert the DPA event into the knowledge base
            SegmentLogic             sink = localSegmentLogic.get();
            StatefulKnowledgeSession ks = sink.getKnowledgeSession();

            if (log.isDebugEnabled()) log.debug("Inserting DPA event into knowledge base: " + dpa);

            ks.insert(dpa);

            if (log.isDebugEnabled()) log.debug("Knowledge facts count: " + ks.getFactCount());

            // Fire the rules over the set of events
            ks.fireAllRules(); // TODO: It seems 'ks.insert' already fired the rules, so this is superfluous...
        } catch (/* RuntimeException| */Exception ex) {

            String msg = "Failed Execute rules:"; log.error(msg, ex); throw new RuntimeException(msg, ex);
        }
        finally {

            Util.cleanup();
        }
    }
    @SuppressWarnings("unused")
    public void execute(Event_SNCD sncd) { // TridentTuple tuple, TridentCollector // collector) {

        String encryptedEsr = null;
        Long   start        = null;
        try {

            // Insert the SNCD event into the knowledge base
            SegmentLogic             sink = localSegmentLogic.get();
            StatefulKnowledgeSession ks = sink.getKnowledgeSession();

            if (log.isDebugEnabled()) log.debug("Inserting SNCD event into knowledge base: " + sncd);

            ks.insert(sncd);

            if (log.isDebugEnabled()) log.debug("Knowledge facts count: " + ks.getFactCount());

            // Fire the rules over the set of events
            ks.fireAllRules(); // TODO: It seems 'ks.insert' already fired the rules, so this is superfluous...
        } catch (/* RuntimeException| */Exception ex) {

            String msg = "Failed Execute rules:"; log.error(msg, ex); throw new RuntimeException(msg, ex);
        }
        finally {

            Util.cleanup();
        }
    }

    // /**
    // * @param Qitem
    // * @return: Typed cast QueueItem to Event_DPA or Event_Incident
    // * @throws CastingException
    // */
    // public static Event<?> getEvent(QueueItem Qitem) throws CastingException
    // {
    //
    // if (Qitem == null) return null;
    //
    // Object item = (Qitem).item; if (item == null) return null;
    //
    // if (item instanceof Event_DPA ) { Event_DPA               ev = (Event_DPA     ) item; return new Event<Event_DPA     >(ev);     }
    // else if (item instanceof Event_Trigger ) { Event_Trigger  ev = (Event_Trigger ) item; return new Event<Event_Trigger >(ev); }
    // else if (item instanceof Event_Incident) { Event_Incident ev = (Event_Incident) item; return new Event<Event_Incident>(ev); }
    //
    // log.error("unknown event type in feeding");
    //
    // return null;
    // }

    /** Load the rule parameters and thresholds. */
    private RuleIdParameter getRuleIdParameter() {

        RuleIdParameter ruleIdParm = new RuleIdParameter();
     // InputStream is = null;
        InputFile file = null;
        try {

            log.info("loading rules thresholds from file: '" + rules_parametersFile + "'");

            // is = fu.getInputStreamForRead(rules_parametersFile); {
            file = new InputFile(rules_parametersFile); if (file.ready()) {

                ruleIdParm.loadRulesThresholds(/* is */file.inputStream());
            }
        }
        catch (/* IOException|ParseException| */Exception ex) {

            String msg = "Error parsing the rule parameters file.";
            log.error(msg, ex);

        } finally {
            // if (is != null) {
            // try { is.close(); } catch (IOException e) { e.printStackTrace();
            // }
            // is = null;
            // }
            if (file != null) {
                try { file.close(); } catch (Exception e) { e.printStackTrace();
                }
                file = null;
            }
        }

        return ruleIdParm;
    }


    /** Return a populated CRM object if the IMSI is found in the data-grid or an
     * empty CRM object otherwise.
     */
    protected static CRM getCRM(String encr_imsi) {

        CRM c = new CRM(); try {

            if (!Config.dataGridEnabled)
                return c;

            CrmCustomerDeviceInfo ccdi;

            DataGridDataProvider dgrdp = DataGridDataProvider.getInstance();
            if (dgrdp != null) {
                ccdi = dgrdp.getCrmCustomerDeviceInfo(encr_imsi);
            } else {
                ccdi = null;
            }
            if (ccdi == null) {
                if (log.isInfoEnabled()) log.info("NO CRM defined for encrypted IMSI: " + encr_imsi);
                return c;
            }

            c.setAccount_number     (ccdi.getAccountNumber    ());
            c.setCustomer_class     (ccdi.getCustomerClass    ());
            c.setCustomer_full_name (ccdi.getCustomerFullName ());
            c.setCustomer_status    (ccdi.getCustomerStatus   ());
            c.setCustomer_type      (ccdi.getCustomerType     ());
            c.setCustomer_value     (ccdi.getCustomerValue    ());
            c.setDevice_status      (ccdi.getDeviceStatus     ());
            c.setImei(new BigDecimal(ccdi.getImei             ().toString()));
            c.setImsi               (ccdi.getImsi             ().toString());
            c.setMaxdata            (ccdi.getMaxdata          ());
            c.setMaxrate_dl         (ccdi.getMaxrateDl        ());
            c.setMaxrate_ul         (ccdi.getMaxrateDl        ());
            c.setPlan_description   (ccdi.getPlanDescription  ());
            c.setPlan_name          (ccdi.getPlanName         ());
            c.setPlan_type          (ccdi.getPlanType         ());
            c.setSite_data          (ccdi.getSiteData         ());
            c.setTac                (ccdi.getTac              ());
        } catch (Exception ex) {

            log.error("getCRM exception: ", ex);
        }

        return c;
    }

    /**
     * @return "list of rule engines remote paths. We only support single level
     *         for rules. It is expected that base rule file is defined under
     *         /CEA/RE/common
     */
    public List<String> getRuleList() {

        List<String> allDrlFiles = new ArrayList<String>();
//        InputStream is = null;
//        try {

        // Add the base rule file

//        is = fu.getInputStreamForRead(rules_file);
        InputFile drlBaseFile = null;
        try {
            drlBaseFile = new InputFile(rules_file);
            if (drlBaseFile.ready()) {

                allDrlFiles.add(rules_file);
            }

//             // Add any additional Rule files
//             String customDrlNode = config.customRuleZknode(); if
//             (customDrlNode != null) {
//
//             customDrlNode = config.customRuleZknode().trim(); if
//            (zkDsForFiles.isExists(customDrlNode)) {
//
//             List<String> l =
//             zkDsForFiles.getChildrenFullyQualified(customDrlNode);
//
//             // check that every file is drl.file
//             Iterator<String> it = l.iterator(); while (it.hasNext()) {
//
//             String zkDrlFile = it.next(); if (!zkDrlFile.endsWith(".drl")) {
//
//             if (log.isInfoEnabled()) log.info("Ignoring bad file, you should
//             remove: " + zkDrlFile);
//
//             continue;
//             }
//
//             allDrlFiles.add(zkDrlFile);
//             }
//             }
//             }
//             }
        } catch (Exception e) {
            log.error("Error: Cannot locate rule files.");
        } finally {
            if (drlBaseFile != null) try { drlBaseFile.close(); } catch (Exception e2) { e2.printStackTrace(); }
            drlBaseFile = null;
        }
        // catch (Exception ex) {
        //
        // log.error("Error: Cannot locate rule files.");
        // } finally {
        // if (is != null) {
        // try { is.close(); } catch (Exception e) { e.printStackTrace(); }
        // is = null;
        // }
        // }

        if (log.isInfoEnabled())
            log.info("Found the following rules: " + allDrlFiles);

        return allDrlFiles;
    }

    /**
     * Check that all '.drl' files are syntactically correct and valid. That is:
     * <p>
     * <ul>
     * <li>The files do not include unsupported JAVA imports</li>
     * <li>The files set the ruleId meta data map:
     * {@linkplain RuleEngine.ruleIdMetaDataMap}</li>
     * </ul>
     */
    private boolean validatesRuleFiles() {

        VerifyRulesCommandOptions commandOptions = new VerifyRulesCommandOptions();
        {

            commandOptions.setConfigfilenameOption("config.txt");

            String customFileDir = rules_customDir;
            if (customFileDir != null) {

                commandOptions.setSourceOption(customFileDir);
            }
            commandOptions.setSupressAnalysisOption(true);
        }

        // VerifyRules verifier = new VerifyRules();
        //
        // // Add source to verify. Get a list of all rules
        // List<String> allDrlFiles = getRuleList();
        // Iterator<String> it = allDrlFiles.iterator(); while (it.hasNext()) {
        //
        // String filePath = it.next();
        // if (filePath == null || filePath.length() == 0) continue;
        //
        // try {
        //
        // InputFile file = new InputFile(filePath);
        // InputStream is = file.inputStream(); if (is != null) {
        //
        // verifier.addResource(is, filePath);
        // }
        // }
        // catch (Exception ex) {
        //
        // String msg = "Error: Cannot verify rule file: '"+filePath+"'";
        // log.error(msg, ex); return false;
        // }
        // }
        //
        // REreference reReference = REreference.getInstance();
        // Map<String, RuleDesc> ruleDescMap = reReference.getRuleDeschash();
        //
        // // TODO: set RuleEngine.ruleIdMetaDataMap, pass it to testRules
        // if (!verifier.testRules(commandOptions, false, ruleDescMap,
        // RuleEngine.ruleIdMetaDataMap)) {
        //
        // System.err.println("Rule Validations failed !!!!!");
        // log.error("Rule Validations failed !!!!!");
        // return false;
        // }
        // else { try {
        //
        // SQLConnection reSqlConnection = reReference.getReSqlConnection();
        // Set<String> rulecatSet = reReference.getRulecats();
        //
        // verifier.updateRuleDescTbl(true, config, false, rulecatSet,
        // reSqlConnection);
        //
        // log.info("Rules files successfully validated");
        //
        // reReference.setRuleIdMetaDataMap(RuleEngine.ruleIdMetaDataMap);
        // }
        // catch (ClassNotFoundException|SQLException|IOException/*|Exception*/
        // ex) {
        //
        // log.error("Exception: ", ex);
        // return false;
        // }}

        return true;
    }
}
