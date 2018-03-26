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

package com.ericsson.eea.ark.sli.grouping.config;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Properties;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import nl.chess.it.util.config.Config;



/** Provides getter methods for config.text configuration parameters.*/
public class SimpleConfig extends Config {

 //private static final Logger log = LoggerFactory.getLogger(SimpleConfig.class.getName());

   //This dbPassword is not set from file, derived and assigned from outside
   @Getter @Setter String         dbPassword = null;

   //ZK_SUPPORT is not in confif file, this derived, and assigned from outside
   @Getter @Setter public boolean ZK_SUPPORT = true;


   public SimpleConfig(Properties p) {

      super(p);
   }


   public SimpleConfig(String resourceName) {

      super(resourceName);
   }


 //private String                      csvTag(String tagName) { try { return  getString(tagName                            ); } catch (Exception e) { return   null; } }
   public  String  actuationIncidentTableName()               { try { return  getString("actuation.incident.tablename"     ); } catch (Exception e) { return   null; } }
   public  String     alarmdbConnectionString()               { try { return  getString("alarmdb.jdbc.connection.string"   ); } catch (Exception e) { return   null; } }
   public  String                 alarmdbUser()               { try { return  getString("alarmdb.user"                     ); } catch (Exception e) { return   null; } }
   public boolean           byPassDigitalSign()               { try { return getBoolean("byPassDigitalSign"                ); } catch (Exception e) { return  false; } }
   public boolean        byPassRuleValidation()               { try { return getBoolean("byPassRuleValidation"             ); } catch (Exception e) { return  false; } }
   public boolean                    database()               {       return getBoolean("database"                         ); }
   public  String                databaseName()               {       return  getString("database.databasename"            ); }
   public  String                databaseHost()               {       return  getString("database.host"                    ); }
   public  String            databasePassword() { return dbPassword == null ? getString("database.password"                ) : dbPassword; }
   public  String                databasePort()               {       return  getString("database.port"                    ); }
   public  String              databaseSchema()               { try { return  getString("database.schema"                  ); } catch (Exception e) { return  "cea"; } }
   public  String            databaseUsername()               {       return  getString("database.username"                ); }
   public  String          datagridConnection()               { try { return  getString("datagrid.connection.string"       ); } catch (Exception e) { return   null; } }
   public boolean                     esrfile()               {       return getBoolean("datasource.esr.file"              ); }
   public boolean               esrfileSorted()               {       return getBoolean("datasource.esr.sorted"            ); }
   public boolean                      esrzmq()               { try { return getBoolean("datasource.esr.zmq"               ); } catch (Exception e) { return  false; } }
   public boolean         subscriberincidents()               {       return getBoolean("datasource.subscriberincidents"   ); }
   public     int          incidentfilenumber()               {       return     getInt("datasource.subscriberincidents.filestreamnumber"); }
   public     int       incidentfile_timestep()               {       return     getInt("engine.incidentfile_timestep"     ); }
   public boolean               esrEncryption()               { try { return getBoolean("esr.encryption"                   ); } catch (Exception e) { return   true; } }
   public  String                sourceFilter(int eId)        { try { return  getString("filterForEngineID" + eId          ); } catch (Exception e) { return   null; } }
   public  String             sourceCsvFilter(int eId)        { try { return  getString("filterForEngineID" + eId + ".csv" ); } catch (Exception e) { return   null; } }
   public boolean                 doActuation()               { try { return getBoolean("input.actuation"                  ); } catch (Exception e) { return   true; } }
   public boolean            backOperatorFact()               { try { return getBoolean("input.backOperatorFact"           ); } catch (Exception e) { return   true; } }
   public boolean        celllocationdatabase()               {       return getBoolean("input.celllocation.database"      ); }
   public boolean            celllocationfile()               {       return getBoolean("input.celllocation.file"          ); }
   public  String        celllocationfilename()               {       return  getString("input.celllocation.filename"      ); }
   public  String       celllocationtablename()               {       return  getString("input.celllocation.tablename"     ); }
   public     int            numberOfSegments()               {       return     getInt("input.correlator.numberOfSegments"); }
   public  String                  crmColumns()               {       return  getString("input.CRM.columns"                ); }
   public boolean                 CRMdatabase()               {       return getBoolean("input.CRM.database"               ); }
   public boolean                     CRMfile()               {       return getBoolean("input.CRM.file"                   ); }
   public  String                 CRMfilename()               {       return  getString("input.CRM.filename"               ); }
   public  String                CRMtablename()               {       return  getString("input.CRM.tablename"              ); }
   public boolean                  processCsv()               { try { return getBoolean("input.csv"                        ); } catch (Exception e) { return  false; } }
   public boolean    removeCsvAfterProcessing()               { try { return getBoolean("input.csv.remove_after_processing"); } catch (Exception e) { return  false; } }
   public boolean          isCsvStrictMapping()               { try { return getBoolean("input.csv.strictMapping"          ); } catch (Exception e) { return  false; } }
   public  String         customRuleDirectory()               { try { return  getString("input.custom.rule_directory"      ); } catch (Exception e) { return   null; } }
 //public  String            customRuleZknode()               { try { return  getString("input.custom.rule_zknode"         ); } catch (Exception e) { return   null; } }
   public boolean             dataGridSupport()               {       return getBoolean("input.dataGrid"                   ); }
   public  String               csv_directory()               {       return  getString("input_directory.csv"              ); }
   public  String               esr_directory()               {       return  getString("input_directory.esr"              ); }
   public  String          incident_directory()               {       return  getString("input_directory.incidents"        ); }
   public  String       EncryptedPasswordFile()               {       return  getString("input.EncryptedPasswordFile"      ); }
   public boolean                  processEsr()               { try { return getBoolean("input.esr"                        ); } catch (Exception e) { return  false; } }
   public boolean         existingAPNdatabase()               {       return getBoolean("input.existingAPN.database"       ); }
   public boolean             existingAPNfile()               {       return getBoolean("input.existingAPN.file"           ); }
   public  String         existingAPNfilename()               {       return  getString("input.existingAPN.filename"       ); }
   public  String        existingAPNtablename()               {       return  getString("input.existingAPN.tablename"      ); }
 //public  String              factsDirectory()               { try { return  getString("input.generic.facts_directory"    ); } catch (Exception e) { return   null; } }
   public  String                facts_zknode()               { try { return  getString("input.generic.facts_zknode"       ); } catch (Exception e) { return   null; } }
   public boolean             imeitacdatabase()               {       return getBoolean("input.imeitac.database"           ); }
   public boolean                 imeitacfile()               {       return getBoolean("input.imeitac.file"               ); }
   public  String             imeitacfilename()               {       return  getString("input.imeitac.filename"           ); }
   public  String            imeitactablename()               { try { return  getString("input.imeitac.tablename"          ); } catch (Exception e) { return "imeitac"; } }
   public  String         orecustomRuleZknode()               { try { return  getString("input.IOIcustom.rule_zknode"      ); } catch (Exception e) { return   null; } }
   public  String             ioirulefilename()               { try { return  getString("input.ioirulefilename"            ); } catch (Exception e) { return   null; } }
   public  String          ioirulefile_zknode()               {       return  getString("input.ioirulefilename_zknode"     ); }
   public  String                     keyFile()               {       return  getString("input.keyFile"                    ); }
   public  String                   keyZkFile()               {       return  getString("input.keyZkFile"                  ); }
   public boolean       matchCorrelatorForCrm()               {       return getBoolean("input.matchCorrelatorForCrm"      ); }
   public boolean     msisdn_imsi_mapDatabase()               { try { return getBoolean("input.msisdn_imsi_map.database"   ); } catch (Exception e) { return  false; } }
   public     int           oreQueryFrequency()               { try { return     getInt("input.ORE.query.frequency"        ); } catch (Exception e) { return     20; } } // Default to 20 seconds
   public     int               oreQueryLimit()               { try { return     getInt("input.ORE.query.limt"             ); } catch (Exception e) { return 300000; } } // Default to 300000
   public  String      technologyMaptablename()               {       return  getString("input.radio_technology_map.tablename"); }
   public  String                     redisDb()               {       return  getString("input.redisDb"                    ); }
   public  String                   redisHost()               {       return  getString("input.redisHost"                  ); }
   public boolean            ruleDescdatabase()               {       return getBoolean("input.ruleDesc.database"          ); }
   public boolean                ruleDescfile()               {       return getBoolean("input.ruleDesc.file"              ); }
   public  String            ruleDescfilename()               {       return  getString("input.ruleDesc.filename"          ); }
   public  String           ruleDesctablename()               {       return  getString("input.ruleDesc.tablename"         ); }
   public  String                rulefilename()               {       return  getString("input.rulefilename"               ); }
   public  String            track_input_file()               {       return  getString("input.track.imsi.file"            ); }
   public  String             trackImsiZkfile()               {       return  getString("input.track.imsi.zkfile"          ); }
   public  String     zkEncryptedPasswordFile()               {       return  getString("input.zkEncryptedPasswordFile"    ); }
   public  String     netPromoterScoreSources()               { try { return  getString("net_promoter_score_sources"       ); } catch (Exception e) { return   null; } }
   public  String   operatorIncidentTableName()               { try { return  getString("operator.incident.tablename"      ); } catch (Exception e) { return   null; } }
   public  String     operatorOutputDirectory()               { try { return  getString("operator.output.directory"        ); } catch (Exception e) { return   null; } }
   public  String                    timezone()               {       return  getString("operator.timezone"                ); }
   public  String                  ottSources()               { try { return  getString("ott_sources"                      ); } catch (Exception e) { return   null; } }
 //public  String    IncidentReportsDirectory()               {       return  getString("output.IncidentReportsDirectory"  ); }
   public String            track_output_file()               {       return  getString("output.track.imsi.file"           ); }
   public boolean          passwordEncryption()               {       return getBoolean("passwordEncryption"               ); }
   public  String    track_rule_desc_property()               { try { return  getString("rule_desc_property.file"          ); } catch (Exception e) { return   null; } }
   public  String    rule_desc_propertyZkfile()               {       return  getString("rule_desc_property.Zkfile"        ); }
   public  String zk_track_rule_desc_property()               { try { return  getString("rule_desc_property.Zkfile"        ); } catch (Exception e) { return   null; } }
   public  String subscriberIncidentTableName()               { try { return  getString("subscriber.incident.tablename"    ); } catch (Exception e) { return   null; } }
   public boolean                  track_imsi()               { try { return getBoolean("track_imsi"                       ); } catch (Exception e) { return  false; } }
   public List<String>        operatorMNCList() { String s =""; try {     s = getString("operator.mnc"                     ); } catch (Exception e) { return null; } return Arrays.asList(s.split("\\s*,\\s*")); }
   public List<String>        operatorMCCList() { String s =""; try {     s = getString("operator.mcc"                     ); } catch (Exception e) { return null; } return Arrays.asList(s.split("\\s*,\\s*")); }

   public List<String> operatorAPNpostfixList() {

      List <String> mncList = operatorMNCList();
      List <String> mccList = operatorMCCList(); if (mncList == null || mccList == null) return null;

      int listSize = mncList.size(); if (mccList.size() < listSize) {

         listSize = mccList.size();
      }

      List <String> l = new ArrayList<String>(); for (int i = 0; i < listSize; i++) {

         l.add(".mnc" +  mncList.get(i) + ".mcc" + mccList.get(i) + ".gprs");
      }

      return l;
   }


//   public CsvSourceMap csvSMap(String source) {
//
//      CsvSourceMap c = new CsvSourceMap();                                                                                  c.setSource       (source);
//
//      String msisdn        = csvTag(source + "." + "msisdn"       ); if (msisdn        != null && !msisdn       .isEmpty()) c.setMsisdnCol    (msisdn       .trim());
//      String imsi          = csvTag(source + "." + "imsi"         ); if (imsi          != null && !imsi         .isEmpty()) c.setImsiCol      (imsi         .trim());
//      String imei          = csvTag(source + "." + "imei"         ); if (imei          != null && !imei         .isEmpty()) c.setImeiCol      (imei         .trim());
//      String dateTime      = csvTag(source + "." + "dateTime"     ); if (dateTime      != null && !dateTime     .isEmpty()) c.setDateCol      (dateTime     .trim());
//      String timezone      = csvTag(source + "." + "timezone"     ); if (timezone      != null && !timezone     .isEmpty()) c.setTimeZoneCol  (timezone     .trim());
//      String separator     = csvTag(source + "." + "separator"    ); if (separator     != null && !separator    .isEmpty()) c.setSeparator    (separator    .charAt(0));
//      String quotechar     = csvTag(source + "." + "quotechar"    ); if (quotechar     != null && !quotechar    .isEmpty()) c.setQuotechar    (quotechar    .charAt(0));
//      String dateColFormat = csvTag(source + "." + "dateColFormat"); if (dateColFormat != null && !dateColFormat.isEmpty()) c.setDateColFormat(dateColFormat.trim());
//
//      return c;
//   }
}
