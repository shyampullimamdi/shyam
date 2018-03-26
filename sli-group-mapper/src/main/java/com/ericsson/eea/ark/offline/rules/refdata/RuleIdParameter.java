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

package com.ericsson.eea.ark.offline.rules.refdata;

import lombok.Getter;

import java.io.Reader;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Set;
//import java.util.Scanner;
import java.util.HashMap;
import java.util.LinkedHashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.eea.ark.offline.config.Config;
import com.ericsson.eea.ark.offline.utils.fs.InputFile;
import com.ericsson.eea.ark.offline.rules.refdata.RuleThreshold;
import com.ericsson.eea.ark.offline.rules.utils.Util;

/** This maintain the parameters (Threshold associated with rules). */
public class RuleIdParameter {

    private static final Logger log = LoggerFactory.getLogger(RuleIdParameter.class.getName());

    @Getter
    private Set<String> doNotNotify = new LinkedHashSet<String>();
    @Getter
    private Set<String> rulesToExclude = new LinkedHashSet<String>();
    private Map<String, RuleThreshold> ruleIdMap = new HashMap<String, RuleThreshold>();

    public RuleIdParameter() {

    }

    public Set<String> getRuleIds() {

        Set<String> ruleIds = ruleIdMap.keySet();

        return ruleIds;
    }

    public Map<String, Object> getThresholdMap(String ruleId) {

        Map<String, Object> map = null;
        if (ruleIdMap.containsKey(ruleId)) {

            map = ruleIdMap.get(ruleId).getThresholdMap();
        }

        return map;
    }

    public Object getParameterValue(String ruleId, String parameterKey) {

        Object obj = null;
        if (ruleIdMap.containsKey(ruleId)) {

            obj = ruleIdMap.get(ruleId).getThresholdMap().get(parameterKey);
            return obj;
        }

        return null;
    }

    public void loadRulesThresholds() throws Exception {

        InputFile file = null;
        try {

            String rules_parametersFile = Config.cfg.getString(Config.cfg_prefix, "rules.parametersFile", null);
            if (rules_parametersFile == null) {

                String msg = "Error. Parameter not configured: '" + Config.cfg_prefix + ".rules.parametersFile'";
                log.error(msg);
                throw new RuntimeException(msg);
            }

            file = new InputFile(rules_parametersFile);

            loadRulesThresholds(file);
        } finally {

            if (file != null) {
                file.close();
                file = null;
            }
        }
    }

    public void loadRulesThresholds(InputFile file) throws Exception {

        loadRulesThresholds(file.inputStream());
    }

    public void loadRulesThresholds(InputStream is) throws Exception {

        Reader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);

        // // Set the delimiter used in file
        // scanner.useDelimiter(";");

        // Get all tokens and store them in some data structure. It is expected
        // that that there 4 parameters as:
        // #RuleID;Execution;notify;Threshold
        //
        // Where the Threshold is a JSON string containing parameters, where the
        // JSON text can span multiple lines.

        String line = null;
        String[] tokens = new String[4];
        for (String l = br.readLine(); l != null; l = br.readLine()) {

            if (l.matches("^([\t ]*#.*)?$"))
                continue;
            while (!l.matches("^.*;[\t ]*$")) {

                String s = br.readLine();
                if (s.matches("^([\t ]*#.*)?$"))
                    continue;

                l = l + " " + s.trim();
            }
            l = l.replaceAll("\"[\t ]*:[\t ]*", "\": ").replaceAll("\"[\t ]*,[\t ]*", "\", ")
                    .replaceAll("[\t ]*,[\t ]*\"", ", \"").replaceAll("[\t ]+\\^[\t ]+", "^");

            // There are 4 tokens per entry (where the JSON part can span
            // multiple lines)
            String[] ts = l.split(";");
            int i = 0;
            for (String t : ts) {

                if (t != null)
                    tokens[i++] = t.trim();
                if (i >= 4)
                    break;
            }

            line = Util.intersperse("; ", tokens);

            // Process the accumulated tokens
            line = line.replaceAll("\\s+", " "); // TODO: Note that this would
                                                    // mess up the spaces inside
                                                    // a JSON String value,
                                                    // which is OK now, but
                                                    // might break things in the
                                                    // future.

            RuleThreshold v = new RuleThreshold(line);
            if (v != null && v.getRuleID() != null) {

                /**/ this.ruleIdMap.put(v.getRuleID(), v);
                if (!v.isEvaluateFlag())
                    this.rulesToExclude.add(v.getRuleID());
                if (!v.isNotifyFlag())
                    this.doNotNotify.add(v.getRuleID());
            }

            if (log.isDebugEnabled())
                log.debug("s=" + line);

            line = null;
        }
    }
}
