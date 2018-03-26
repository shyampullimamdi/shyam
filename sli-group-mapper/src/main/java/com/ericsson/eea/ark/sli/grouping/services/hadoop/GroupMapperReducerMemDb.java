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

import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.eea.ark.sli.grouping.config.InputDataTypeEnum;
import com.ericsson.eea.ark.sli.grouping.memdb.ImsiGrpMemDbPut;


public class GroupMapperReducerMemDb extends Reducer<Text, AvroGenericRecordWritable, Text, ImsiGrpMemDbPut> implements GroupMapperReducer<ImsiGrpMemDbPut> {

    private static Logger log = LoggerFactory.getLogger(GroupMapperReducerMemDb.class);

    private CommonGroupReducerHelper<Text, ImsiGrpMemDbPut> helper;

       @Override
       protected void setup(Context context) throws IOException
          ,                                         InterruptedException {

         super.setup(context);
         String inputDataType=context.getConfiguration().get("inputDataType");

             if(inputDataType!= null&& inputDataType.equalsIgnoreCase(InputDataTypeEnum.dpa.getMapperName())){
             helper = new GroupMapperDpaReducerHelper<Text, ImsiGrpMemDbPut>(log);
         }else if(inputDataType!= null&& inputDataType.equalsIgnoreCase(InputDataTypeEnum.sncd.getMapperName())){
             helper = new GroupMapperSncdReducerHelper<Text, ImsiGrpMemDbPut>(log);
         }

         helper.setup(context);
       }

       protected void setup(Context context,CommonGroupReducerHelper helper) throws IOException
          ,                                         InterruptedException {

         super.setup(context);
         this.helper = helper;
         this.helper.setup(context);
       }


    @Override
       protected void reduce(Text user, Iterable<AvroGenericRecordWritable> values,
            Context context)
            throws IOException, InterruptedException {


        helper.reduce(this, user, values, context);

       }


       @Override
       protected void cleanup(Context context) throws IOException
          ,                                           InterruptedException {

           helper.cleanup(context);
       }

   /** Write the user group into db.*/
   @Override
public boolean write_userGroup(String user
      ,                           String group
      ,                           String percentage) throws IOException
      ,                                                     InterruptedException {
       ImsiGrpMemDbPut put  = new ImsiGrpMemDbPut(group, percentage);
       helper.getContext().write(new Text(user), put);

       return true;
   }

}