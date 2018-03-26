package com.ericsson.eea.ark.sli.grouping.util;

import java.io.IOException;

import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;

import net.minidev.json.JSONValue;
import net.minidev.json.JSONObject;

import org.apache.parquet.avro.AvroReadSupport;

import org.apache.parquet.hadoop.ParquetReader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import net.minidev.json.parser.ParseException;



public class ShowParquetFile {

   @SuppressWarnings({ "unchecked", "resource", "rawtypes", "unused" })
   public static void main(String[] args) throws IOException {

      String  c = null
         ,    f = null
         ,    l = null;
      boolean s = false; for (int i=0, len=args.length; i<len; ++i) {

         String a = args[i]; if (a == null || a.isEmpty()) continue;

         if      ("-c".equals(a) && i <len -1) c = args[++i];
         else if ("-f".equals(a) && i <len -1) f = args[++i];
         else if ("-l".equals(a) && i <len -1) l = args[++i];
         else if ("-s".equals(a)             ) s = true;
         else {

            System.err.println("Error: Invalid command line option: '"+a+"'");
         }
      }

      if (f == null) {

         System.err.println("Error: Please, specify an input file with '-f <file>'."); return;
      }

      AvroReadSupport readSupport   = new AvroReadSupport();
      ParquetReader   parquetReader = new ParquetReader(new Path(f), readSupport);

      if (s) {

         Object rec = parquetReader.read(); if (rec != null) {

            Schema schema = ((GenericData.Record) rec).getSchema();
            System.out.println(schema.toString());
         }
      }
      else {

         Object rec = null; while ((rec = parquetReader.read()) != null) {  /*((GenericData.Record)rec).get("esr")*/

            JSONObject json   = null;
            String     recStr = rec.toString(); try {

               json = (JSONObject) JSONValue.parseWithException(recStr);
            }
            catch (ParseException e) {

               System.err.println("Error: Cannot parse JSON Record: "+recStr); return;
            }

            prune(json);

            System.out.println(json);
         }
      }
   }


   /**
    * Helper static method to prune empty nodes in a JSON tree.
    */
   @SuppressWarnings("unchecked")
   public static void prune(Map<String, Object> node) {

      Iterator<Entry<String, Object>> it = node.entrySet().iterator(); while (it.hasNext()) {

         Entry<String, Object> child = it.next(); if (child.getValue() == null) {

            it.remove();
         }
         else if (child.getValue() instanceof Map<?, ?>) {

            Map<String, Object> m = (Map<String, Object>) child.getValue();

            prune(m); if (m.isEmpty()) {

               it.remove();
            }
         }
         else if (child.getValue() instanceof List<?>) {

            ArrayList<Object> list = (ArrayList<Object>) child.getValue(); for (Object o : list) {

               if (o instanceof Map<?, ?>) {

                  prune((Map<String, Object>) o);
               }
            }
         }
      }
   }
}
