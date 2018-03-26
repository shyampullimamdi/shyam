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

import java.sql.ResultSet;
import java.sql.Statement;
//import java.sql.SQLException;

import java.util.Map;
//import java.util.Set;
import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.Properties;
//import java.util.Enumeration;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

//import java.io.FileReader;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.BufferedReader;
//import java.io.FileNotFoundException;


import com.ericsson.eea.ark.offline.utils.fs.InputFile;
import com.ericsson.eea.ark.offline.utils.NGEEAlarmHelper;
import com.ericsson.eea.ark.offline.utils.NGEEAlarmHelper.AlarmType;
import com.ericsson.eea.ark.offline.rules.config.SimpleConfig;
import com.ericsson.eea.ark.offline.rules.db.SQLConnection;

//import com.ericsson.eea.ark.offline.rules.VerifyRules;
//import com.ericsson.eea.ark.offline.rules.utils.RuleMetaData;



/**
 * RuleDesc Class is used to maintain static rule values from
 *         rule_desc table. The column value represent label values that needs
 *         to be localized base on language specific property file:
 *         rule_desc.properties /** Read the Rule descriptions from file/Db.
 *         This information is used to populate the incident table.
 *
 *         CREATE TABLE cea.rule_desc ( rule_id text NOT NULL, rule_cat text NOT
 *         NULL, rule_name text NOT NULL, impact text, reason text, message_soc
 *         text, message_cc text, message_cclite text, kpi_name text, CONSTRAINT
 *         pk_rule_desc PRIMARY KEY (rule_id)
 */

@ToString
@NoArgsConstructor
@AllArgsConstructor
public class RuleDesc {

   private static final Logger log = LoggerFactory.getLogger(RuleDesc.class.getName());


   @Getter @Setter String rule_id        = null;
   @Getter @Setter String rule_cat       = null;
   @Getter @Setter String rule_name      = null;
   @Getter @Setter String impact         = null;
   @Getter @Setter String reason         = null;
   @Getter @Setter String message_soc    = null;
   @Getter @Setter String message_cc     = null;
   @Getter @Setter String message_cclite = null;
   @Getter @Setter String kpi_name       = null;
   @Getter @Setter String kpi_source     = null;


   /**
    * @param ruleDesc_location_row
    *           : row of tab separated field. Empty field are represented by
    *           "\N" parses the fix location row to create RuleDEc object.
    */
   public RuleDesc(String ruleDesc_location_row) {

      String[] row = ruleDesc_location_row.replaceAll("\\\\N", "NULL").split("\t");

      // validate the row
      if (row.length != 10) {

         if (log.isDebugEnabled()) log.debug("Invalid ruleDesc line ignored: '"+ruleDesc_location_row+"'"); return;
      }

      this.rule_id        =                                                  row[0]        ;
      this.rule_cat       =                                                  row[1]        ;
      this.rule_name      =                                                  row[2]        ;
      this.impact         = (!"NULL".equals(row[3])) ? this.impact         = row[3] : "\\N";
      this.reason         = (!"NULL".equals(row[4])) ? this.reason         = row[4] : "\\N";
      this.message_soc    = (!"NULL".equals(row[5])) ? this.message_soc    = row[5] : "\\N";
      this.message_cc     = (!"NULL".equals(row[6])) ? this.message_cc     = row[6] : "\\N";
      this.message_cclite = (!"NULL".equals(row[7])) ? this.message_cclite = row[7] : "\\N";
      this.kpi_name       = (!"NULL".equals(row[8])) ? this.kpi_name       = row[8] : "\\N";
      this.kpi_source     = (!"NULL".equals(row[9])) ? this.kpi_source     = row[9] : "\\N";
   }


   public boolean isEmpty() {

      return rule_id     == null && rule_cat   == null && rule_name      == null && impact   == null && reason     == null
         &&  message_soc == null && message_cc == null && message_cclite == null && kpi_name == null && kpi_source == null;
   }


   /** Loads the rule description from a file.*/
   public static void load(Map<String,RuleDesc> ruleDeschash
      ,                    String               rulesDescrFile) throws Exception {

      InputFile file = null; try {

         String line = null;
         file        = new InputFile(rulesDescrFile); while ((line = file.getLine()) != null) {

            if (!line.matches("^([\t ]+#.*)?$")) {

               RuleDesc r = new RuleDesc(line); ruleDeschash.put(r.rule_id, r);
            }
         }
      }
      finally {

         if (file != null) { file.close(); file = null; }
      }
   }


   /**
    * query(): loads the rule description from table.
    *
    * @param ruleDeschash
    * @param config
    */
   public static void query(Map<String, RuleDesc> ruleDeschash, SimpleConfig config, SQLConnection reSqlConnection) {

      // clear the map before loading.
      if (ruleDeschash != null) ruleDeschash.clear(); else {

         ruleDeschash = new HashMap<String, RuleDesc>();
      }

      if (reSqlConnection == null) {

         log.error("null Sql connection returning"); return;
      }

      try {

         String    query     = "select * from " + config.databaseSchema() + "." + config.ruleDesctablename();
         Statement sql       = reSqlConnection.getNewStatement(config.databaseName());
         ResultSet resultset = sql.executeQuery(query);

         while (resultset.next()) {

            String rule_id        =                          resultset.getString("rule_id"       )        ;
            String rule_cat       =                          resultset.getString("rule_cat"      )        ;
            String rule_name      =                          resultset.getString("rule_name"     )        ;
            String impact         = (!resultset.wasNull()) ? resultset.getString("impact"        ) : "\\N";
            String reason         = (!resultset.wasNull()) ? resultset.getString("reason"        ) : "\\N";
            String message_soc    = (!resultset.wasNull()) ? resultset.getString("message_soc"   ) : "\\N";
            String message_cc     = (!resultset.wasNull()) ? resultset.getString("message_cc"    ) : "\\N";
            String message_cclite = (!resultset.wasNull()) ? resultset.getString("message_cclite") : "\\N";
            String kpi_name       = (!resultset.wasNull()) ? resultset.getString("kpi_name"      ) : "\\N";
            String kpi_source     = (!resultset.wasNull()) ? resultset.getString("kpi_source"    ) : "\\N";

            RuleDesc r = new RuleDesc(rule_id, rule_cat, rule_name, impact, reason, message_soc, message_cc, message_cclite, kpi_name, kpi_source);

            ruleDeschash.put(r.rule_id, r);
            log.debug("query db " + r.toString());
         }
         resultset.close();
         sql.getConnection().close();
         if (log.isInfoEnabled()) log.info(ruleDeschash.size() + " read RuleDesc from table ");

      }
      catch (Exception ex) {

         log.error("Exception loading from table " + config.ruleDesctablename(), ex);

         NGEEAlarmHelper.logAlarm(AlarmType.databaseError, ex.getMessage());
      }
   }


//   /**
//    * @param rulesMD
//    *           : rules meta data
//    * @param config
//    *           : Rule Engine configuration
//    * @throws ClassNotFoundException
//    * @throws SQLException
//    * @throws Exception
//    */
//   public static synchronized void updateRuleDesc(Set<RuleMetaData> rulesMD
//      ,                                           SimpleConfig      config
//      ,                                           SQLConnection     reSqlConnection) throws Exception
//      ,                                                                                     SQLException
//      ,                                                                                     ClassNotFoundException {
//
//      if (rulesMD == null || rulesMD.isEmpty()) return; // nothing to do
//
//      if (reSqlConnection == null) {
//         log.error("reSqlConnection is null updateRuleDesc() returning");
//         return;
//      }
//      Iterator<RuleMetaData> it = rulesMD.iterator();
//      Statement sql = reSqlConnection.getNewStatement(config.databaseName());
//      Set<String> rulesAdded = new HashSet<String>();
//
//      try {
//         sql.getConnection().setAutoCommit(false);
//         while (it.hasNext()) {
//            RuleMetaData ruleMetaData = it.next();
//            if (ruleMetaData.getRuleId() != null && ruleMetaData.getRuleId().length() > 0 && ruleMetaData.getRuleName() != null
//                     && ruleMetaData.getRuleName().length() > 0 && ruleMetaData.getRulecat() != null
//                     && ruleMetaData.getRulecat().length() > 0 && ruleMetaData.getKpi_name() != null
//                     && ruleMetaData.getKpi_name().length() > 0 && ruleMetaData.getKpi_source() != null
//                     && ruleMetaData.getKpi_source().length() > 0) {
//               // delete rule and add it back in.
//               String deleteStmt = "delete from " + config.databaseSchema() + "." + config.ruleDesctablename()
//                        + " where rule_id = '" + ruleMetaData.getRuleId().trim() + "'";
//
//               if (log.isInfoEnabled()) log.info("deleteStmt : " + deleteStmt);
//               sql.executeUpdate(deleteStmt);
//               String insertStmt = "insert into " + config.databaseSchema() + "." + config.ruleDesctablename()
//                        + " (rule_id, rule_cat, rule_name, kpi_name, kpi_source)" + " values ('" + ruleMetaData.getRuleId()
//                        + "', '" + ruleMetaData.getRulecat() + "', '" + ruleMetaData.getRuleName() + "', '"
//                        + ruleMetaData.getKpi_name() + "', '" + ruleMetaData.getKpi_source() + "')";
//               if (log.isInfoEnabled()) log.info("insertStmt : " + insertStmt);
//               sql.executeUpdate(insertStmt);
//               rulesAdded.add(ruleMetaData.getRuleId());
//            }
//         }
//         // commit all updates
//         sql.getConnection().commit();
//         System.out.println("The following custom rules were added/update: " + rulesAdded);
//      }
//      catch (SQLException e) {
//         log.error("SQLException: ", e);
//         throw e;
//      }
//      catch (Exception e) {
//         log.error("Exception: ", e);
//         throw e;
//      }
//      finally {
//         try {
//            sql.close();
//         }
//         catch (SQLException e) {
//            log.error("SQLException: ", e);
//            throw e;
//         }
//      }
//
//   }


//   public static Map<String, String> getDefinedRulecats(SimpleConfig config) throws SQLException {
//
//      // load a properties file
//      Properties rule_descProp = new Properties(); try {
//
//         InputStream stream = VerifyRules.class.getResourceAsStream("rule_desc.properties");
//         rule_descProp.load(stream);
//         stream.close();
//        if (log.isInfoEnabled()) log.info(rule_descProp.toString());
//      }
//      catch (IOException ex) {
//
//         log.error(e.getMessage(), ex); throw ex;
//      }
//
//      return getRuleDescCategories(rule_descProp);
//   }


//   /**
//    * @param rule_descProp
//    * @return a map of rul desc key by value and property name.
//    */
//   private static Map<String, String> getRuleDescCategories(Properties rule_descProp) {
//
//      Enumeration<?> e = rule_descProp.propertyNames();
//      Map<String, String> m = new HashMap<String, String>();
//      while (e.hasMoreElements()) {
//         String key = (String) e.nextElement();
//         String value = (String) rule_descProp.getProperty(key);
//         if (key.matches("rule_cat[0-9]+$")) {
//            // Reverse the map
//            m.put(value, key);
//         }
//      }
//
//      return m;
//   }
}
