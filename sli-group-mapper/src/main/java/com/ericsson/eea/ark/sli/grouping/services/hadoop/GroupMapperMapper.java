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
public class GroupMapperMapper extends CommonGroupMapper {

    private static Logger log = LoggerFactory.getLogger(GroupMapperMapper.class);



    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

    }

    public  PropertyManager init(Configuration conf) {
        return super.init(conf);
    }

    protected  String getPathToConfigFileFromDistCache() {
        return super.getPathToConfigFileFromDistCache();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        super.run(context);
    }

    public void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }

   }