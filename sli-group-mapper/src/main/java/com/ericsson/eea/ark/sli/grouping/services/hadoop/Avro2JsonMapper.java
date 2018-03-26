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

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;


@SuppressWarnings("rawtypes")
public class Avro2JsonMapper extends Mapper<AvroKey, NullWritable, Void, Text> {

   @Override
   protected void map(AvroKey key, NullWritable value, Context context) throws IOException, InterruptedException {

      context.write(null, new Text(key.toString()));
   }
}
