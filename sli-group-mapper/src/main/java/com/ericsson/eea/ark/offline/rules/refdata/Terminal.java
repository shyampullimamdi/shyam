package com.ericsson.eea.ark.offline.rules.refdata;


//import java.sql.ResultSet;
//import java.sql.Statement;

import java.util.Map;
//import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

//import java.io.FileReader;
//import java.io.IOException;
//import java.io.BufferedReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

//import com.ericsson.bigdata.ruleengine.RuleEngine;

import com.ericsson.bigdata.common.column.Dimension;

//import com.ericsson.bigdata.ruleengine.utils.SQLConnection;
//import com.ericsson.bigdata.ruleengine.utils.NGEEAlarmHelper;

import com.ericsson.eea.ark.common.service.model.Imeitac;



/** Imeitac Record class.*/
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class Terminal {

   private static final Logger log = LoggerFactory.getLogger(Terminal.class);

   @Getter @Setter public Integer imeitac             = null;
   @Getter @Setter public String  vendor              = null;
   @Getter @Setter public String  model               = null;
   @Getter @Setter public String  marketing_name      = null;
   @Getter @Setter public String  ue_type             = null;
   @Getter @Setter public String  access_capability   = null;
   @Getter @Setter public String  os                  = null;
// @Getter @Setter public String  input_mode          = null;
   @Getter @Setter public Integer screen_resolution_x = null;
   @Getter @Setter public Integer screen_resolution_y = null;
   @Getter @Setter public String  screen_diagonal     = null;
   @Getter @Setter public Short   global_release_year = null;
   @Getter @Setter public Integer global_release_date = null;


   public Terminal(Imeitac i) {

      super(); if (i == null) return;

      this.imeitac             =             i.getTac();
      this.vendor              =             i.getVendor();
      this.model               =             i.getModel();
      this.marketing_name      =             i.getMarketingName();
      this.ue_type             =             i.getUeType();
      this.access_capability   =             i.getAccessCapability();
      this.os                  =             i.getOs();
//    this.input_mode          =             i.getInputMode();
      this.screen_resolution_x = new Integer(i.getScreenResolutionX());
      this.screen_resolution_y = new Integer(i.getScreenResolutionY());
      this.screen_diagonal     = null;
      this.global_release_year =             i.getGlobalReleaseYear();
      this.global_release_date =             i.getGlobalReleaseDate();
   }


   /**
    * @param line
    *            : Tab separated IMETAC record
    */
   public Terminal(String line) {

      String[] row = line.replaceAll("\\\\N", "NULL").split("\t",13); try {

         int n = 0;
         this.imeitac                                                                              = Integer.parseInt   (row[n].trim()); ++n;
         this.vendor                                                                               =                     row[n];         ++n;
         this.model                                                                                =                     row[n];         ++n;
         this.marketing_name                                                                       =                     row[n];         ++n;
         this.ue_type                                                                              =                     row[n];         ++n;
         this.access_capability                                                                    =                     row[n];         ++n;
         this.os                                                                                   =                     row[n];         ++n;
//       this.input_mode                                                                           =                     row[n];         ++n;
         if (row[n] == null || row[n].isEmpty() || row[n].equals("NULL")) this.screen_resolution_x = 0;
         else                                                             this.screen_resolution_x = Integer.parseInt   (row[n].trim()); ++n;
         if (row[n] == null || row[n].isEmpty() || row[n].equals("NULL")) this.screen_resolution_y = 0;
         else                                                             this.screen_resolution_y = Integer.parseInt   (row[n].trim()); ++n;
         if (row[n] == null || row[n].isEmpty() || row[n].equals("NULL")) this.screen_diagonal     = "";
         else                                                             this.screen_diagonal     =                     row[n];         ++n;
         if (row[n] == null || row[n].isEmpty() || row[n].equals("NULL")) this.global_release_year = 0;
         else                                                             this.global_release_year = Short.parseShort   (row[n].trim()); ++n;
         if (row[n] == null || row[n].isEmpty() || row[n].equals("NULL")) this.global_release_date = 0;
         else                                                             this.global_release_date = Integer.parseInt (row[n].trim()); ++n;
      }
      catch (NumberFormatException ex) {

         String msg = "NumberFormatException, line: " +line; log.error(msg, ex);
      }
   }


   /**
    * @param imeitacMap
    *            : imeitac colum definition
    */
   @Deprecated
   public Terminal(Map<Dimension, Object> imeitacMap) {

      super();

      Iterator<Entry<Dimension, Object>> it = imeitacMap.entrySet().iterator(); while (it.hasNext()) {

         Entry<Dimension,Object> pairs = it.next();
         Dimension               key   = (Dimension) pairs.getKey();
         Object                  value = (Object)    pairs.getValue(); if (value == null) continue;

         try {

            if      (key.getName().equalsIgnoreCase("vendor"             ) && (value instanceof String )) this.vendor              = (String ) value;
            else if (key.getName().equalsIgnoreCase("model"              ) && (value instanceof String )) this.model               = (String ) value;
            else if (key.getName().equalsIgnoreCase("marketing_name"     ) && (value instanceof String )) this.marketing_name      = (String ) value;
            else if (key.getName().equalsIgnoreCase("ue_type"            ) && (value instanceof String )) this.ue_type             = (String ) value;
            else if (key.getName().equalsIgnoreCase("access_capability"  ) && (value instanceof String )) this.access_capability   = (String ) value;
            else if (key.getName().equalsIgnoreCase("os"                 ) && (value instanceof String )) this.os                  = (String ) value;
//          else if (key.getName().equalsIgnoreCase("input_mode"         ) && (value instanceof String )) this.input_mode          = (String ) value;
            else if (key.getName().equalsIgnoreCase("screen_resolution_x") && (value instanceof Integer)) this.screen_resolution_x = (Integer) value;
            else if (key.getName().equalsIgnoreCase("screen_resolution_y") && (value instanceof Integer)) this.screen_resolution_y = (Integer) value;
            else if (key.getName().equalsIgnoreCase("screen_resolution_x") && (value instanceof Integer)) this.screen_resolution_x = (Integer) value;
            else if (key.getName().equalsIgnoreCase("screen_diagonal"    ) && (value instanceof String )) this.screen_diagonal     = (String ) value;
            else if (key.getName().equalsIgnoreCase("global_release_year") && (value instanceof Short)) this.global_release_year = (Short) value;
            else if (key.getName().equalsIgnoreCase("global_release_date") && (value instanceof Integer   )) this.global_release_date = (Integer) value;
         }
         catch (NumberFormatException e) {

            log.error(key.getName() + ": " + e.getMessage(), e);
         }
      }
   }


   /**
    * @return true if all elements of Terminal are null
    */
   public boolean isEmpty() {

      if (this.imeitac             == null && this.vendor              == null && this.model == null
      &&  this.marketing_name      == null && this.ue_type             == null
      &&  this.access_capability   == null && this.os                  == null
//    &&  this.input_mode          == null && this.screen_resolution_x == null
      &&  this.screen_resolution_y == null
      &&  this.screen_diagonal     == null
      &&  this.global_release_year == null
      &&  this.global_release_date == null) return true;
      else                                  return false;
   }


//   /*
//    * imeitac table definition -- Table: cea.imeitac
//    *
//    * -- DROP TABLE cea.imeitac;
//    *
//    * CREATE TABLE cea.imeitac (tac                 integer
//    *    ,                      vendor              text
//    *    ,                      model               text
//    *    ,                      marketing_name      text
//    *    ,                      ue_type             text
//    *    ,                      access_capability   text
//    *    ,                      os                  text
//    *    ,                      input_mode          text
//    *    ,                      screen_resolution_x smallint
//    *    ,                      screen_resolution_y smallint
//    *    ,                      screen_diagonal     text
//    *    ,                      global_release_year smallint
//    *    ,                      global_release_date integer ) WITH ( OIDS=FALSE ); ALTER TABLE cea.imeitac OWNER TO perfmon;
//    *
//    * -- Index: cea.idx_tac_imeitac
//    * -- DROP INDEX cea.idx_tac_imeitac;
//    *
//    * CREATE INDEX idx_tac_imeitac ON cea.imeitac USING btree (tac);
//    */
//
//   // query(): load cell from DB
//   public static void query(HashMap<Integer,Terminal> terminalhash
//      ,                     SQLConnection             reSqlConnection) {
//
//      // clear the map before loading
//      if (terminalhash != null) terminalhash.clear();
//
//      if (terminalhash == null) {
//
//         //just in case one can never be too careful
//         terminalhash = new HashMap<Integer, Terminal>();
//      }
//
//      if (reSqlConnection == null) {
//
//         log.error("reSqlConnection is null query returnning");
//         return;
//      }
//
//      try {
//
//         String    query     = "select * from " + RuleEngine.config.databaseSchema() + "." + RuleEngine.config.imeitactablename();
//         Statement sql       = reSqlConnection.getNewStatement(RuleEngine.config.databaseName());
//         ResultSet resultset = sql.executeQuery(query); while (resultset.next()) {
//
//            Integer imeitac = resultset.getInt("tac");
//            if (resultset.wasNull()) {
//
//               imeitac = 0;
//            }
//
//            String vendor               = resultset.getString("vendor");
//            String model                = resultset.getString("model");
//            String marketing_name       = resultset.getString("marketing_name");
//            String ue_type              = resultset.getString("ue_type");
//            String access_capability    = resultset.getString("access_capability");
//            String os                   = resultset.getString("os");
//            String input_mode           = resultset.getString("input_mode");
//
//            Integer screen_resolution_x = resultset.getInt   ("screen_resolution_x"); if (resultset.wasNull()) screen_resolution_x = 0;
//            Integer screen_resolution_y = resultset.getInt   ("screen_resolution_y"); if (resultset.wasNull()) screen_resolution_y = 0;
//            Double  screen_diagonal     = resultset.getDouble("screen_diagonal"    ); if (resultset.wasNull()) screen_diagonal     = 0.0;
//            Integer global_release_year = resultset.getInt   ("global_release_year"); if (resultset.wasNull()) global_release_year = 0;
//            Integer global_release_date = resultset.getInt   ("global_release_date"); if (resultset.wasNull()) global_release_date = 0;
//
//            Terminal t = new Terminal(imeitac
//               ,                      vendor
//               ,                      model
//               ,                      marketing_name
//               ,                      ue_type
//               ,                      access_capability
//               ,                      os
//               ,                      input_mode
//               ,                      screen_resolution_x
//               ,                      screen_resolution_y
//               ,                      screen_diagonal
//               ,                      global_release_year
//               ,                      global_release_date);
//
//            terminalhash.put(t.imeitac, t);
//         }
//
//         sql.getConnection().close();
//
//         log.info(terminalhash.size() + " read Terminal from table ");
//
//         // System.out.println(PerfMon_Incident_v2.terminalhash +" read Terminal from table ");
//      }
//      catch (Exception ex) {
//
//         log.error("Exception loading table: " +RuleEngine.config.imeitactablename(), ex);
//
//         NGEEAlarmHelper.logAlarm(NGEEAlarmHelper.DATA_BASE_ERROR, e.getMessage());
//
//         log.error("exception: ", e);
//      }
//
//   }


//   public static void load(String                    fn
//      ,                    HashMap<Integer,Terminal> terminalhash) throws IOException {
//
//      FileReader     fr     = new FileReader(fn);
//      BufferedReader reader = new BufferedReader(fr);
//
//      String line = ""; while ((line = reader.readLine()) != null) {
//
//         if (!line.startsWith("#")) {
//
//            Terminal t = new Terminal(line); terminalhash.put(t.imeitac, t);
//         }
//      }
//
//      fr.close(); reader.close();
//   }
}
