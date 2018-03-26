package com.ericsson.eea.ark.sli.grouping.services.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.rmi.server.UID;
import java.util.ArrayList;
import java.util.List;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.ericsson.eea.ark.offline.config.Config;
import com.ericsson.eea.ark.offline.config.PropertyManager;
import com.ericsson.eea.ark.sli.grouping.data.BaseTest;
import com.ericsson.eea.ark.sli.grouping.memdb.ImsiGrpMemDbPut;
import com.ericsson.eea.ark.sli.grouping.memdb.ImsiGrpMemDbPutSerialization;

//This isn't actually used anywhere:
class GroupMapperReducer_MemDb extends Reducer<Text, AvroGenericRecordWritable, Text, ImsiGrpMemDbPut> {

    public void reduce(Text user, Iterable<Text> values, Context context) throws IOException
      ,                                                                         InterruptedException {

       String group = null; // MemDb
       Double percentage = null; // MemDb
       JSONObject json = null; // MemDb

       json = new JSONObject();
         int i = 0; for (Text val : values) {

            String v = val.toString();

            switch(i++) {
               case 0:
                  group = v;
                  percentage = null;
                  break;
               case 1:
                  if (group != null) percentage = Double.valueOf(v);
                  break;
               default:      throw new RuntimeException("Unexpected column value at position "+i+": '"+v+"'");
            }
            if (group != null && percentage != null) {
                json.put(group,  percentage);
                group = null;
                percentage = null;
            }
         }
          String value = json.toString();
          context.write(user, new ImsiGrpMemDbPut(user.toString(), value));
   }
}

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GroupMapperReducerTest extends BaseTest {
    protected final static Logger logger = Logger.getLogger(GroupMapperReducerTest.class);
    @Override protected Logger logger() { return logger; }

   static ReduceDriver<Text, AvroGenericRecordWritable, ? extends WritableComparable<?>, ?> reduceDriver;

   protected static final String[][] testVectors = {
      {"{'SOURCE':'dpa','DATE':'20140901','imsi':'12345','data_dl_dy_avg_14d':1.0,'cellid_counts_dpa_7_KEY'     :'cell2','cellid_counts_dpa_7_VALUE'     :null}", "{'name'=12345,'group'='Video User','percentage'=0.25}"}
  //, {"{'SOURCE':'dpa','DATE':'20140901','imsi':'54678','cellid_counts_topn_dpa_7_KEY':'cell2','cellid_counts_topn_dpa_7_VALUE':null}", "{'name'=12345,'group'='Video User','percentage'=0.25}"}
   };

   protected static final String schema
     = "{'type'      : 'record'"
     + ",'name'      : 'dpa'"
     + ",'namespace' : 'com.ericsson.eea.ark.offline.entity'"
     + ",'fields'    : ["
     +    "{'name'   : 'imsi'                             ,'type' :  'string'}"
     +   ",{'name'   : 'SOURCE'                           ,'type' :  'string'}"
     +   ",{'name'   : 'DATE'                             ,'type' :  'string'}"
     +   ",{'name'   : 'video_dl_sum'                     ,'type' : [  'long' , 'null'], 'default' :   0}"
     +   ",{'name'   : 'video_dl_daily_avg'               ,'type' : ['double' , 'null'], 'default' : 0.0}"
     +   ",{'name'   : 'data_dl_dy_avg_14d'               ,'type' : ['double' , 'null'], 'default' : 0.0}"
     +   ",{'name'   : 'video_dl_sum_1_day'               ,'type' : [  'long' , 'null'], 'default' :   0}"
     +   ",{'name'   : 'video_dl_label'                   ,'type' : ['string' , 'null'], 'default' :  ''}"
     +   ",{'name'   : 'cellid_counts_dpa_7_KEY'          ,'type' : ['string' , 'null'], 'default' :  ''}"
     +   ",{'name'   : 'cellid_counts_dpa_7_VALUE'        ,'type' : [  'long' , 'null'], 'default' :   0}"
     +   ",{'name'   : 'cellids_7'                        ,'type' : ['string' , 'null'], 'default' :  ''}"
     +   ",{'name'   : 'cellid_counts_topn_dpa_7_KEY'     ,'type' : ['string' , 'null'], 'default' :  ''}"
     +   ",{'name'   : 'cellid_counts_topn_dpa_7_VALUE'   ,'type' : [  'long' , 'null'], 'default' :   0}"
     +   ",{'name'   : 'cellid_counts_bottomn_dpa_7_KEY'  ,'type' : ['string' , 'null'], 'default' :  ''}"
     +   ",{'name'   : 'cellid_counts_bottomn_dpa_7_VALUE','type' : [  'long' , 'null'], 'default' :   0}"
     +   ",{'name'   : 'cellid_topn_dpa_7'                ,'type' : ['string' , 'null'], 'default' :  ''}"
     +   ",{'name'   : 'video_by_cell_dpa_7_KEY'          ,'type' : ['string' , 'null'], 'default' :  ''}"
     +   ",{'name'   : 'video_by_cell_dpa_7_VALUE'        ,'type' : [  'long' , 'null'], 'default' :   0}"
     +   ",{'name'   : 'video_by_cell_dpa_7_top2_KEY'     ,'type' : ['string' , 'null'], 'default' :  ''}"
     +   ",{'name'   : 'video_by_cell_dpa_7_top2_VALUE'   ,'type' : [  'long' , 'null'], 'default' :   0}]}"
   ;
   protected static Schema recSchema = null;  /*
{ "name"     : "CanonicalCdr"
, "type"     : "record"
, "namespace": "com.ericsson.eea.ark.offline.pre_proc.adapter.cdr.data"
, "fields"   : [
    {"name"  : "SOURCE"                            ,"type" : "string"}
  , {"name"  : "TIMESTAMP"                         ,"type" : "long"}
  , {"name"  : "callDataRecordType"                ,"type" : "string"}
  , {"name"  : "imsi"                              ,"type" : "string"}
  , {"name"  : "imei"                              ,"type" : "string"}
  , {"name"  : "timeForStartOfCharge"              ,"type" : "long"}
  , {"name"  : "timeForStopOfCharge"               ,"type" : "long"}
  , {"name"  : "chargeableDuration"                ,"type" : "long"}
  , {"name"  : "faultCode"                         ,"type" : "int"}
  , {"name"  : "teleServiceCode"                   ,"type" : "int"}
  , {"name"  : "callPosition"                      ,"type" : "int"}
  , {"name"  : "internalCause"                     ,"type" : "int"}
  , {"name"  : "internalLocation"                  ,"type" : "int"}
  , {"name"  : "smsResult"                         ,"type" : "int"}
  , {"name"  : "messageTypeIndicator"              ,"type" : "int"}
  , {"name"  : "unsuccessfulPositioningDataReason" ,"type" : "int"}
  , {"name"  : "callIdentificationNumber"          ,"type" : "int"}
  , {"name"  : "partialOutputRecNum"               ,"type" : "int"}
  , {"name"  : "lastPartialOutput"                 ,"type" : "boolean"} ]
}
    */


   @BeforeClass
   public static void setUp() {

    Reducer<Text, AvroGenericRecordWritable, ? extends WritableComparable<?>, ?>
        reducer = new GroupMapperReducerMemDb();
      recSchema                  = new Schema.Parser().parse(schema.replace('\'', '"'));

      reduceDriver = (ReduceDriver<Text, AvroGenericRecordWritable, ? extends WritableComparable<?>, ?>)
              ReduceDriver.newReduceDriver(reducer);

      Configuration conf = reduceDriver.getConfiguration(); {

         conf.set("group-mapper.configurationDir" , PropertyManager.configDir);
         conf.set("group-mapper.configurationFile", PropertyManager.configFile);
         conf.set("group-mapper.tableNameMemDb", Config.tableNameMemDb);

         conf.setStrings("io.serializations"
            ,   conf.get("io.serializations")
            ,            ImsiGrpMemDbPutSerialization.class.getName());
      }
   }

   @Test
   public void testInsert() throws Exception {

       testInsertMemDb();
   }

   public void testInsertMemDb() throws Exception {

      String user = null;
      for (String[] tv : testVectors) {

          JSONObject in  = (JSONObject) JSONValue.parseWithException(tv[0].replace('\'', '"'));

         List<AvroGenericRecordWritable> list = new ArrayList<AvroGenericRecordWritable>(); {

            if (user == null)        user = (String) in.get("imsi");
            Record                    rec = new Record(recSchema);
            AvroGenericRecordWritable val = new AvroGenericRecordWritable(rec);
            val.setRecordReaderID(new UID((short)0));
            val.setFileSchema(recSchema);
            for (String k : in.keySet()) {
              rec.put(k, in.get(k));
            }


            list.add(val);
         }

         reduceDriver.withInput(new Text(user), list);

         List<?> tresult = reduceDriver.run();
         @SuppressWarnings("unchecked")
        List<Pair<Text, ImsiGrpMemDbPut>> result = (List<Pair<Text, ImsiGrpMemDbPut>>) tresult;
         assertTrue("Empty result", result.size() >0);

         // extract key from result and verify

             assertEquals(result.get(0).getFirst().toString(), user);

         ImsiGrpMemDbPut a =
                     result.get(0).getSecond();
         String group = a.getGroup();
         Double percent = a.getPercentage();
         assertEquals(user, "12345");
         assertEquals(group, "default");
         assertEquals(percent, 1.0D, .000000001D);

      }
   }

}
