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

import java.util.ArrayList;
import java.util.Properties;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.commons.io.Charsets;


/** Load the environment variables into a Property object.*/
public final class Environment {

    /** Reference to the logging facility.*/
    private static final Logger log = Logger.getLogger(Environment.class);

    /** The cached array of environment variable assignments strings.*/
    private String[] cachedEnvLinesArray;

    /** The properties containing the environment variables.*/
    private final Properties envVars = new Properties();

    /** The dynamic array list of environment variable assignments strings.*/
    private final ArrayList<String> envLinesArray = new ArrayList<String>();

    /** Reference to the singleton instance.*/
    private static Environment self = null;


    /** Default constructor.*/
    public Environment() {
        init();
    }


    /** Default constructor.*/
    public static Environment instance() {

        if (self == null) {
            self = new Environment();
        }

        return self;
    }


    /** Default constructor.*/
    public static void setInstance(Environment inst) {

        self = inst;
    }


    /** Initializer method.*/
    private void init() {
        try {
            Process p = null;
            Runtime r = Runtime.getRuntime();
            String OS = System.getProperty("os.name").toLowerCase();

            // Windows 9x
            if (OS.indexOf("windows 9") > -1) {
                p = r.exec("command.com /c set");
            }
            // Windows NT
            else if ((OS.indexOf("nt") > -1)
                  || (OS.indexOf("windows xp") > -1)
                  || (OS.indexOf("windows 7") > -1)
                  || (OS.indexOf("windows 2000") > -1)) {
                p = r.exec("cmd.exe /c set");
            }
            // Assume this is a UNIX(R) host.
            else {
                p = r.exec("env");
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream(), Charsets.UTF_8));

            // Parse each line comprising the environment.
            String line;

            while ((line = br.readLine()) != null) {
                int idx = line.indexOf('=');
                if (idx == -1) {
                    break;
                }

                envLinesArray.add(line);

                String key = line.substring(0, idx);
                String value = "";
                if ((idx + 1) < line.length()) {
                    value = line.substring(idx + 1);
                }

                envVars.setProperty(key, value);
            }
        }
        catch (Throwable t) {
            log.error("Unable to load environment variables: " + t.getMessage());
        }
    }


    public String get(String var) {


        String value = null; if (var != null) {

            value = envVars.getProperty(var); if (value == null) {

               value = System.getenv(var);
            }
        }

        return value;
    }


    public String[] getEnv() {

        if (cachedEnvLinesArray == null) {

            int nLines = envLinesArray.size();

            cachedEnvLinesArray = new String[nLines];

            if (nLines > 0) {
                cachedEnvLinesArray =
                    (String[]) envLinesArray.toArray(cachedEnvLinesArray);
            }
        }

        return cachedEnvLinesArray;
    }


    public String add(String name, String value) {

        // Fetch the old value (if any) associate to the name.
        String oldValue = null;
        if (envVars.contains(name)) {
            oldValue = envVars.getProperty(name);
        }

        // Add the name-value pair to the environment.
        envVars.setProperty(name, value);

        // Update the list of environment lines.
        if (oldValue == null || ! value.equals(oldValue)) {

            cachedEnvLinesArray = null; // Invalidate the cache.

            if (oldValue == null) {
                envLinesArray.add(name + "=" + value); // Add a new line
            }
            else {
                envLinesArray.remove(name + "=" + oldValue); // Remove old line
                envLinesArray.add(name + "=" + value);       // Add a new line
            }
        }

        return oldValue;
    }
}
