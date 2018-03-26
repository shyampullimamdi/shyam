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

package com.ericsson.eea.ark.sli.grouping.data;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.rmi.server.UID;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;

//import net.minidev.json.JSONValue;
//import net.minidev.json.JSONObject;






import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

//import net.minidev.json.parser.ParseException;






import org.apache.avro.generic.GenericRecord;
//import org.apache.avro.generic.GenericData.Record;
//import org.apache.avro.generic.GenericDatumReader;
//import org.apache.avro.generic.GenericDatumWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.log4j.Logger;

import com.ericsson.eea.ark.offline.config.Config;
import com.ericsson.eea.ark.offline.config.PropertyManager;
import com.ericsson.eea.ark.sli.grouping.services.hadoop.GroupMapperReducerMemDb;
import com.ericsson.eea.ark.sli.grouping.util.Util;
import com.ericsson.eea.ark.sli.grouping.memdb.ImsiGrpMemDbPut;
import com.ericsson.eea.ark.sli.grouping.memdb.ImsiGrpMemDbPutSerialization;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UserGroupsTest extends BaseTest {

    protected final static Logger  logger = Logger.getLogger(UserGroupsProviderTest.class);
    @Override public Logger logger() { return logger; }
//   static final byte[] CF_Info       = "Info:"     .getBytes(Charsets.UTF_8)
//      ,                CF_User       = "User:"     .getBytes(Charsets.UTF_8)
//      ,                CF_Group      = "Group:"    .getBytes(Charsets.UTF_8)
//      ,                CQ_imsi       = "imsi"      .getBytes(Charsets.UTF_8)
//      ,                CQ_name       = "name"      .getBytes(Charsets.UTF_8)
//      ,                CQ_percentage = "percentage".getBytes(Charsets.UTF_8)
//      ,                CQ_user_id    = "user_id"   .getBytes(Charsets.UTF_8)
//      ,                CQ_group_id   = "group_id"  .getBytes(Charsets.UTF_8)
//   ;


       static final String rec_json = "{ "
      +   "'SOURCE'"                      +":'raw'"
      + ", 'DATE'"                        +":'20141210'"
      + ", 'video_init_time_avg'"         +":0.0"
      + ", 'web_tp_avg'"                  +":0.0"
      + ", 'imeitac_top'"                 +":35974101"
      + ", 'data_dl_sum'"                 +":646732.53"
      + ", 'data_ul_sum'"                 +":512333.91"
      + ", 'rtt_term_avg'"                +":0.0"
      + ", 'encryption_set_count'"        +":{'AdMob':1}"
      + ", 'wp_time_avg'"                 +":0.0"
      + ", 'cellid_set_count'"            +":{'lacci_1920_19856':15"
         +                                 ", 'eci_39614029':59"
         +                                 ", 'lacsac_121_58384':31}"
      + ", 'pkt_loss_term_avg'"           +":0.0"
      + ", 'pkt_loss_net_avg'"            +":0.0"
      + ", 'ftp_dl_tp_avg'"               +":0.0"
      + ", 'msisdn'"                      +":'XMXnQCPn+nIl27qYoScCbQ=='"
      + ", 'tcp_tp_ul_avg'"               +":0.0"
      + ", 'n_esr'"                       +":3"
      + ", 'tcp_tp_dl_avg'"               +":0.0"
      + ", 'crm_site_data_last'"          +":'{'who':'ESR'}'"
      + ", 'cellid_set_data_dl_sum'"      +":{'lacsac_121_58384':646732.53}"
      + ", 'imeitac_distinct_count'"      +":1"
      + ", 'rat_set_data_dl_sum'"         +":{'3G':646732.53}"
      + ", 'svc_provider_set_count'"      +":{'Chrome':1}"
      + ", 'functionality_set_count'"     +":{'Xbox':1}"
      + ", 'imsi'"                        +":'euHlXgZx59dIWJ0Uw7VEAIjkydgaerMbp3uzCT9t6ps='"
      + ", 'rat_set_count'"               +":{'4G':59"
         +                                 ", '2G':15"
         +                                 ", '3G':31}"
      + ", 'wp_acc_time_avg'"             +":0.0"
      + ", 'video_rebuf_avg'"             +":0.0"
      + ", 'imsicopy'"                    +":'euHlXgZx59dIWJ0Uw7VEAIjkydgaerMbp3uzCT9t6ps='"
      + ", 'client_application_set_count'"+":{'Android Media Player':1}"
      + ", 'duration'"                    +":28602"
      + ", 'n_sli_updates'"               +":0.0"
      + ", 'rtt_net_avg'"                 +":0.0"
   +"}".replace("'", "\\\"");


   @SuppressWarnings("rawtypes")
   private static ReduceDriver reduceDriver = null;

   @BeforeClass
   public static void setUp() throws Exception {

      { @SuppressWarnings({ "rawtypes" })
      ReduceDriver rd = ReduceDriver.newReduceDriver(new GroupMapperReducerMemDb());
      reduceDriver = rd; }

      Configuration conf = reduceDriver.getConfiguration(); {

         conf.set("group-mapper.configurationDir" , PropertyManager.configDir);
         conf.set("group-mapper.configurationFile", PropertyManager.configFile);

         conf.setStrings("io.serializations"
                 ,   conf.get("io.serializations")
                 ,   ImsiGrpMemDbPutSerialization.class.getName());
      }
   }


   //@Test
   @SuppressWarnings({ "rawtypes", "unchecked"})
   public void test_02_testMemDbInsert() throws IOException {

      String strKey = "12345", strValue = "{\"default\":1.0}"; //"default";  //no tag 'data_dl_dy_avg_14d' => null

      List<AvroGenericRecordWritable> list = new ArrayList<AvroGenericRecordWritable>(); {

         AvroReadSupport readSupport   = new AvroReadSupport();
         ParquetReader   parquetReader = new ParquetReader(new Path("data/dpa/20140901/imsi/nested/part-r-00000.snappy.parquet"), readSupport);

         Object rec = null; while ((rec = parquetReader.read()) != null) {

           // list.add(new AvroGenericRecordWritable((GenericRecord) rec));
             AvroGenericRecordWritable agrw = new AvroGenericRecordWritable((GenericRecord) rec);
             agrw.setRecordReaderID(new UID((short)0));
             agrw.setFileSchema(((GenericRecord)rec).getSchema());
            list.add(agrw);

         }
         parquetReader.close();
      }

      // Setup Input, mimic what mapper would have passed to the reducer and run test
      reduceDriver.withInput(new Text(strKey), list);

      // run the reducer and get its output
      List<Pair<Text, ImsiGrpMemDbPut>> result = reduceDriver.run();

      // extract key from result and verify
      Text key_b = (result.size() > 0) ? result.get(0).getFirst() : null;
      String key = (key_b != null) ? key_b.toString() : null;
      assertEquals("Test error: key != strKey", key, strKey);

      // extract value for CF/QUALIFIER and verify
      ImsiGrpMemDbPut a = result.size() > 0 ? result.get(0).getSecond() : null;

      // since in our case all that the reducer is doing is appending the records that
      // the mapper sends, we should get the following back
      String expectedOutput = strValue;

      String v = a.toJsonString();
      assertTrue("Test error: Not null result"    , v != null && v.length() > 0);

      assertEquals("Test error: Incorrect result", expectedOutput, v);   }

}
