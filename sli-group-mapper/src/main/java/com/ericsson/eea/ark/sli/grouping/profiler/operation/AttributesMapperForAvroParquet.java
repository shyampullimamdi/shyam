package com.ericsson.eea.ark.sli.grouping.profiler.operation;

//import java.util.Map;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import org.apache.hadoop.conf.Configuration;

//import net.minidev.json.JSONValue;
//import net.minidev.json.JSONObject;
//import net.minidev.json.parser.ParseException;

import org.apache.avro.generic.GenericRecord;
//import org.apache.avro.generic.GenericData.Record;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//import com.ericsson.eea.sli.grouping.profiler.data.MetadataHelper;

import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;


/**
 * generic mapper to sort based on a given key
 *
 */
public class AttributesMapperForAvroParquet extends Mapper<Void, GenericRecord, Text, AvroGenericRecordWritable> {

   private static Logger log = LoggerFactory.getLogger(AttributesMapperForAvroParquet.class);

//   private AttributesMapperImpl ami = null;


   @Override
   public void setup(Context context) throws IOException
      ,                                      InterruptedException {

//      ami = new AttributesMapperImpl(); {
//
//         ami.setup(context);
//      }
   }


   @Override
   protected void cleanup(Context context) throws IOException
      ,                                           InterruptedException {

//      ami.cleanup();
   }


   @Override
   public void run(Context context) throws IOException
      ,                                    InterruptedException {

      setup(context); /* <<<=== Kludge !!! */ try {

         while (context.nextKeyValue()) {

//            map(context.getCurrentKey(), context.getCurrentValue(), context);
            context.write(new Text("imsi"), new AvroGenericRecordWritable(context.getCurrentValue()));
         }

         cleanup(context);
      }
      catch (Exception ex) {

         String errFile = "UNKNOWN"; try {

            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            errFile             = fileSplit.getPath().getName();
         }
         catch (Exception e2) { /* ignore */ }

         log.error("Error in parsing file: " + errFile + ", ignoring the file", ex);

         throw ex;
      }
   }


//   public void map(Void          key
//      ,            GenericRecord value
//      ,            Context       context) throws IOException
//      ,                                          InterruptedException {
//
////      // do not convert to map, losing lots of cpu and increases n/w io between map and reducer
////      JSONObject json; try {
////
////         json = (JSONObject) JSONValue.parseWithException(value.toString());
////      }
////      catch (ParseException ex) {
////
////         log.error("Failed to parse input", ex);
////
////         throw ex;
////      }
//
//      // Time the actual execution
//      long startTime = System.currentTimeMillis();
//
//      ami.process(context, null, (Record) value);
//
//      long estimatedTime = System.currentTimeMillis() - startTime;
//      context.getCounter("profiler", "TOTAL_MAP_TIME_MILLIS").increment(estimatedTime);
//   }
}
