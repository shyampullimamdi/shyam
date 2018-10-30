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

import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;

import javax.crypto.BadPaddingException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.slf4j.Logger;

import com.ericsson.bigdata.esr.parserlogic.CastingException;
import com.ericsson.eea.ark.offline.config.Config;
import com.ericsson.eea.ark.offline.rules.RuleEngine;
import com.ericsson.eea.ark.offline.rules.refdata.REreference;
import com.ericsson.eea.ark.sli.grouping.rules.events.Event_SNCD;

public class GroupMapperSncdReducerHelper<KEYOUT, VALUEOUT> extends CommonGroupReducerHelper<KEYOUT, VALUEOUT> {

    private final Logger log;

    private ReduceContext<Text, AvroGenericRecordWritable, KEYOUT, VALUEOUT> contextSavedAtSetupTime;

    GroupMapperSncdReducerHelper(Logger log) {
        super(log);
        this.log = log;
    }

    @Override
    public void reduce(GroupMapperReducer trampoline, Text user, Iterable values, ReduceContext context)
            throws IOException, InterruptedException {
        String rules_file = "/eea_config/sli-group-mapper/rules/SLI-user_groups-rules-base.drl";
        String rules_customDir = "/eea_config/sli-group-mapper/rules/customs";
        String rules_parametersFile = "/eea_config/sli-group-mapper/rules/rule_parameters.csv";
        String rules_descriptionsFile = "/eea_config/sli-group-mapper/rules/rule_descriptions.csv";

        rules_file = Config.cfg.getString(Config.cfg_prefix, "rules.file", rules_file);
        rules_customDir = Config.cfg.getString(Config.cfg_prefix, "rules.customDir", rules_customDir);
        rules_parametersFile = Config.cfg.getString(Config.cfg_prefix, "rules.parametersFile", rules_parametersFile);
        rules_descriptionsFile = Config.cfg.getString(Config.cfg_prefix, "rules.descriptionsFile",
                rules_descriptionsFile);

        RuleEngine.rules_customDir = rules_customDir;
        RuleEngine.rules_descriptionsFile = rules_descriptionsFile;
        RuleEngine.rules_file = rules_file;
        RuleEngine.rules_parametersFile = rules_parametersFile;

        REreference ref = REreference.getInstance();

        Iterable<AvroGenericRecordWritable> writableValues = values;
        for (AvroGenericRecordWritable val : writableValues) {

            GenericRecord rec = val.getRecord();
            // =? TODO

            // Parse all attributes as an ESR/JSON Object
            Event_SNCD sncd = null;
            try {
                Config.imsiKey=Config.cfg.getString(Config.cfg_prefix, "sncdevent.imsiKey");
                Object imsiVal =  rec.get(Config.imsiKey);
                String imsi = imsiVal == null ? null : imsiVal.toString();
                Config.imeitacKey=Config.cfg.getString(Config.cfg_prefix, "sncdevent.imeitacKey");
                Object imeitacVal = rec.get(Config.imeitacKey);
                String imeitac = imeitacVal == null ? null : imeitacVal.toString();

                sncd = new Event_SNCD(rec.toString());
                sncd.setTrampoline(trampoline);

                if (ref != null && imsi != null)
                    sncd.setCrm(ref.getCrm(imsi));
                if (ref != null && imeitac != null)
                    sncd.setTerminal(ref.getTerminal(imeitac));

            } catch (InvalidAlgorithmParameterException | BadPaddingException | InvalidKeyException
                    | CastingException ex) {

                String msg = "Error: cannot parse SNCD record:";
                log.error(msg, ex);
                throw new RuntimeException(msg, ex);
            }
            // Inject it into the rule engine's knowledge base
            rulesExecuter.execute(sncd);
        }
    }
}