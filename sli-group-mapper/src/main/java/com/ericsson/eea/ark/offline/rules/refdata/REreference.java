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

import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.eea.ark.common.service.model.CellLocationMapper;
import com.ericsson.eea.ark.common.service.model.CrmCustomerDeviceInfo;
import com.ericsson.eea.ark.common.service.model.DgdKpiMetaInfo;
import com.ericsson.eea.ark.common.service.model.KpiMetaInfo;
import com.ericsson.eea.ark.common.service.model.Imeitac;
import com.ericsson.eea.ark.offline.rules.RuleEngine;
import com.ericsson.eea.ark.offline.rules.config.SimpleConfig;
import com.ericsson.eea.ark.offline.rules.data.DataGridDataProvider;
import com.ericsson.eea.ark.offline.rules.refdata.Cell;
import com.ericsson.eea.ark.offline.config.Config;
import com.ericsson.eea.ark.offline.utils.Util;



/** Singleton class that maintains the reference data for the RE. */
final public class REreference {

   private static final Logger log = LoggerFactory.getLogger(REreference.class);

   // Static member holds only one instance of the class
   private static REreference instance;

   // Late addition {
   /*
    * The following data objects were added late in development cycle so this
    * class can be used outside of the RE i.e for other POC that may need to use
    * this class. The following data members were added to remove all static
    * reference to the RuleEngine static public members that were expected to be
    * initialized before this class can be used. For now these members must be
    * set by the application using this class before an instance can be properly
    * created.
    */

//   private SQLConnection reSqlConnection = null;


//   public SQLConnection getReSqlConnection() {
//
//      return reSqlConnection;
//   }

   @Getter @Setter private static SimpleConfig config = null;
   @Getter @Setter private static List<String> listOfCsvSources = new ArrayList<String>();
   @Getter @Setter private static List<String> listOfEsrSources = new ArrayList<String>();


   // Local Hash Maps used when dataGrid is not being used
   // RE based on configuration parameter support dataGrid if not set it will
   // default
   // to using DB.
   // These local Maps which read reference data straight from data base or
   // file.
   public final static String         DATA_GRID    = "datagrid";
//   private static WhiteList         whiteList    = null;

//   private HashMap<Long, CRM>         crmhash      = null;
//   private HashMap<String,Cell>       cellhash     = new HashMap<String, Cell>();
//   private HashMap<Integer,Terminal>  terminalhash = new HashMap<Integer,Terminal>();
   private HashMap<String, RuleDesc>  ruleDescriptions    = null;

   private Map<String, String>        rulecats     = null;

   private static  RuleIdParameter    ruleIdParms  = null;

//                 private HashMap<String, CsvSourceMap> csvSourcehash = null;
   @Getter @Setter private static Set<Long>         externalImsi      = new HashSet<Long>();
   @Getter @Setter private Properties               rule_descProp     = new Properties();

   @Getter         private Map<String,RuleMetaData> ruleIdMetaDataMap = null;  // Can only be set after an instance is created


   public Map<String, String> getRulecats() {

      return rulecats;
   }


   public static synchronized void setRuleIdParms(RuleIdParameter ruleIdParms) {

      REreference.ruleIdParms = ruleIdParms;
   }


   public RuleIdParameter getRuleIdParms() {

      return REreference.ruleIdParms;
   }



   public synchronized void setRuleIdMetaDataMap(Map<String, RuleMetaData> ruleIdMetaDataMap) {

      if (this.ruleIdMetaDataMap != null) this.ruleIdMetaDataMap.clear();

      this.ruleIdMetaDataMap = ruleIdMetaDataMap;
   }


   public HashMap<String, RuleDesc> getRuleDeschash() {

      return ruleDescriptions;
   }

   // External optional List of IMSI




   private String referenceType = REreference.DATA_GRID; //"dbSql"; // defaults to SQL

//   private Set<String> crmUsedCols   = null;


//   private static Set<String> getUsedColumns() {
//
//      Set<String> s = new HashSet<String>();
//      try {
//         String cols = REreference.config.crmColumns();
//         if (cols != null && cols.length() > 0) {
//            String[] colArray = cols.split(",");
//            for (String item : colArray) {
//               s.add(item.trim());
//            }
//         }
//         return s;
//      }
//      catch (nl.chess.it.util.config.MissingPropertyException me) {
//         return s;
//      }
//   }


   /**
    * REreference prevents any other class from instantiating
    */
   private REreference() {

//      // initialize all the reference data
//      crmUsedCols = getUsedColumns();
//     if (log.isInfoEnabled()) log.info("current memory before Reference Data allocation: " + SegmentThread.getMemoryUsage());
//
//      if (REreference.config.database()) {
//         try {
//            reSqlConnection = new SQLConnection(REreference.config.databaseUsername(), REreference.config.databasePassword());
//
//            reSqlConnection.addConnection(new SQLConnectionBase(REreference.config.databaseHost(), REreference.config
//                     .databasePort(), REreference.config.databaseName()));
//         }
//         catch (Exception ex) {
//            log.error("config database Exception", ex);
//            NGEEAlarmHelper.logAlarm(NGEEAlarmHelper.DATA_BASE_ERROR, ex.getMessage());
//         }
//         log.info("OK.\n");
//      }
//      else {
//         System.out.print("no database configured.\n");
//         log.info("no database configured.\n");
//      }

      // HS63227 Do the initial work first before calling the mapper because the data limits the maximum amount
      // of concurrent query that the RE can run, so across multiple JVM it may cause an issue.
      ruleDescriptions = new HashMap<String, RuleDesc>(); if (RuleEngine.rules_descriptionsFile != null) try {

         RuleDesc.load(ruleDescriptions, RuleEngine.rules_descriptionsFile);
      }
      catch (Exception ex) {

         log.error("Cannot load rule description file: '"+RuleEngine.rules_descriptionsFile+"'", ex);
      }

//      if (REreference.config.ruleDescdatabase()) {
//         RuleDesc.query(ruleDescriptions, REreference.config, reSqlConnection);
//      }
//
//      try {
//         rulecats = RuleDesc.getDefinedRulecats(REreference.config);
//      }
//      catch (Exception e) {
//         log.error(e.getMessage(), e);
//      }
//
//      if (REreference.config.processCsv()) {
//         // Initialize ratMap static map.
//         RatMap.init(reSqlConnection, REreference.config);
//         // Initialize CSV mapping.
//         setSourceHashMap();
//      }
//
//      // Set up dataGrid objects.
//      if (REreference.config.dataGridSupport()) {
//         referenceType = REreference.DATA_GRID;
//         try {
//            // initialize the data grid.
//            refData   = Services.getReferenceDataOnline();
//            whiteList = Services.getWhiteList();
//         }
//         catch (Exception e) {
//            log.error("failed to init data grid: " + e.getMessage());
//         }
//
//      }
//      else {
//         // datGrid not supported perform a Db or file loading.
//         // This is really never used in production.!!!!!!!!
//         // This interface is for Research prototype
//         log.info("Using traditional Db reference approach: ");
//         cellhash = new HashMap<String, Cell>();
//         terminalhash = new HashMap<Integer, Terminal>();
//         crmhash = new HashMap<Long, CRM>(100000);
//         crmUsedCols = getUsedColumns();
//
//         // Initialize cell_location
//         if (REreference.config.celllocationfile()) {
//            try {
//               Cell.load(REreference.config.celllocationfilename(), cellhash);
//            }
//            catch (IOException e) {
//               log.error(e.getMessage(), e);
//            }
//         }
//         if (REreference.config.celllocationdatabase()) {
//            Cell.query(cellhash, reSqlConnection);
//         }
//
//         // Initialize imeitac
//         log.info("Init...\tterminal IMEI TAC\t\t\t\n");
//         if (REreference.config.imeitacfile()) {
//            try {
//               Terminal.load(REreference.config.imeitacfilename(), terminalhash);
//            }
//            catch (IOException e) {
//               log.error(e.getMessage(), e);
//            }
//         }
//         else if (REreference.config.imeitacdatabase()) {
//            Terminal.query(terminalhash, reSqlConnection);
//         }
//
//         // Initialize CRM
//         log.info("Init...\tCRM\t\t\t\n");
//         if (REreference.config.CRMfile()) {
//            try {
//               CRM.load(crmhash, crmUsedCols);
//            }
//            catch (IOException e) {
//               log.error(e.getMessage(), e);
//            }
//         }
//         else if (REreference.config.CRMdatabase()) {
//            CRM.query(crmhash, crmUsedCols, reSqlConnection);
//         }
//
//      }
//
//      if (log.isInfoEnabled()) log.info("current memory after Reference Data allocation: " + SegmentThread.getMemoryUsage());
   }


   /** Provides a global point of access to the rule engine.*/
   public static REreference getInstance() {

      if (null == instance) synchronized (REreference.class) { if (null == instance) {

//         if (REreference.config == null) {
//
//            log.error("The rule engine must be initialized before it can be instantiated.");
//            return null;
//         }

         instance = new REreference();
      }}

      return instance;
   }


   // Access methods:
   public RuleDesc getRuleDesc(String ruleId) {

      RuleDesc r = (ruleDescriptions == null || !ruleDescriptions.containsKey(ruleId)) ? null :  ruleDescriptions.get(ruleId);

      return r;
   }


//   /**
//    * @param imsi
//    *           : IMSI
//    * @return: true if the IMSI is being tracked externally, false otherwise
//    */
//   public boolean isImsiTrack(Long imsi) {
//
//      // if (log.isInfoEnabled()) log.info("incoming IMSI" + imsi + "set of Imsis" +
//      // this.getExternalImsi());
//      // if (this.getExternalImsi().contains(imsi))
//      // if (log.isInfoEnabled()) log.info("found a track IMSI!!!!" + imsi);
//
//      return REreference.getExternalImsi().contains(imsi);
//   }


//   /**
//    * @param source
//    * @return CsvSourceMap for the source
//    */
//   public CsvSourceMap getCsvSourceMap(String source) {
//
//      if (csvSourcehash == null) {
//         CsvSourceMap c = new CsvSourceMap();
//         return c;
//      }
//      return csvSourcehash.get(source);
//
//   }


//   /**
//    * setSourceHashMap set a list of CsvSourceMap per source
//    */
//   private void setSourceHashMap() {
//
//      if (listOfCsvSources == null || listOfCsvSources.isEmpty()) {
//         log.error("listOfCsvSources is not set");
//         return;
//      }
//      Iterator<String> it = listOfCsvSources.iterator();
//      csvSourcehash = new HashMap<String, CsvSourceMap>();
//      while (it.hasNext()) {
//         String source = it.next();
//         CsvSourceMap c = config.csvSMap(source);
//         csvSourcehash.put(source, c);
//      }
//   }


//   /**
//    * @param msisdn
//    * @return imsi
//    */
//   public Long getImsiFromMsidn(Long msisdn) {
//
//      if (Config.dataGridEnabled) try {
//
//         return getRefData().getMsisdnImsi(msisdn);
//      }
//      catch (Exception e) {
//
//         log.error("error getting IMSI from msisdn: " + e.getMessage());
//      }
//      return null;
//   }


//   /**
//    * @param imei
//    * @return imsi
//    */
//   public Long getImsiFromImei(Long imei) {
//
//      if (Config.dataGridEnabled) try {
//
//         return getRefData().getImeiImsi(imei);
//      }
//      catch (Exception e) {
//
//         log.error("error getting IMSI from IMEI: " + e.getMessage());
//      }
//
//      return null;
//   }


   /**
    * @param key
    *           : location identifier
    * @return CellLocationMapper record from reference data grid.
    */
   public Cell getCell(String key) {

      Cell cell = null;  if (Config.dataGridEnabled) try {

         if (referenceType.equals(REreference.DATA_GRID)) {

            CellLocationMapper c = getCellLocationMapper(key);
            cell = new Cell(c);
            if (cell.getLocation_identifier() == null || cell.getLocation_identifier().isEmpty())
               cell.setLocation_identifier(key);
         }
//         else if (cellhash != null) {
//
//            cell = cellhash.get(key);
//         }
      }
      catch (Exception e) {

         log.error(" getCellLocationMapper error " + e.getMessage());
      }

      return cell;
   }


   /**
    * @param imsi
    *           : imsi
    * @return: CRM record.
    */
   public CRM getCrm(String imsi) {

      //Long key = Long.parseLong(imsi);
      CRM crm = null; if (Config.dataGridEnabled) try {

         if (referenceType.equals(REreference.DATA_GRID)) {

            CrmCustomerDeviceInfo c = getCrmCustomerDeviceInfo(imsi);
            if (c != null) { crm = new CRM(c); {

               if (crm.getImsi() == null) crm.setImsi(/*""+Util.decImsi(*/imsi/*)*/);
            }}
         }
      }
      catch (Exception ex) {

         log.error("Cannot fetch CrmCustomerDeviceInfo for IMSI: '"+imsi+"'", ex);
      }

      return crm;
   }


   /**
    * @param imsi
    *           : imsi
    * @return: CRM record.
    */
   public CrmCustomerDeviceInfo getCrmCustomerDeviceInfo(String imsi) {

      CrmCustomerDeviceInfo c = null; if (Config.dataGridEnabled) try {

         if (referenceType.equals(REreference.DATA_GRID)) {

                 DataGridDataProvider dgrdp = DataGridDataProvider.getInstance();
              if (dgrdp != null) {
                  c = dgrdp.getCrmCustomerDeviceInfo(imsi);
              } else {
                  c = null;
              }
            ;
         }
      }
      catch (Exception e) {

         log.error("CrmCustomerDeviceInfo error" + e.getMessage());
      }

      return c;
   }


   public Terminal getTerminal(Long imeitac) {
          Terminal t = null; if (Config.dataGridEnabled) try {

              if (referenceType.equals(REreference.DATA_GRID)) {

                 Imeitac i = getImeitac(imeitac);

                 t = new Terminal(i); {

                     Long di = imeitac;
                    if (t.getImeitac() == null) t.setImeitac(di == null ? null : di.intValue());
                 }
              }
           }
           catch (Exception e) {

              log.error("Imeitac error" + e.getMessage());
           }

           return t;
    }

   /** Fetch the Terminal record.*/
   public Terminal getTerminal(String imeitac) {

      Terminal t = null; if (Config.dataGridEnabled) try {

         if (referenceType.equals(REreference.DATA_GRID)) {

            Imeitac i = getImeitac(imeitac);

            t = new Terminal(i); {

                Long di = Util.decImeitac(imeitac);
               if (t.getImeitac() == null) t.setImeitac(di == null ? null : di.intValue());
            }
         }
      }
      catch (Exception e) {

         log.error("Imeitac error" + e.getMessage());
      }

      return t;
   }


//   /**
//    * @param imsi
//    * @return true if the incident should be hidden. Note: default is not hide
//    *         incident.
//    */
//   public WhiteListStatus hideIncident(Long imsi) {
//
//      if (whiteList == null) return WhiteListStatus.ALLOW;
//
//      try {
//
//         return whiteList.createIncidents(imsi);
//      }
//      catch (Exception e) {
//
//         log.error("White list exception: " + e.getMessage());
//      }
//
//      return WhiteListStatus.ALLOW;
//   }
//
//
//   /**
//    * @param imsi
//    *           * @param crm
//    * @return true if the incident should be hidden. Note: default is not hide
//    *         incident.
//    */
//   public WhiteListStatus hideIncident(Long imsi, CrmCustomerDeviceInfo crm) {
//
//      if (whiteList == null) return WhiteListStatus.ALLOW;
//
//      try {
//
//         return whiteList.createIncidents(imsi, crm);
//      }
//      catch (Exception e) {
//
//         log.error("White list exception: " + e.getMessage());
//      }
//
//      return WhiteListStatus.ALLOW;
//   }


   public Boolean doActuation() {

      return REreference.getConfig().doActuation();
   }


   /**
    * @param source
    * @param kpiName
    * @param rat
    * @return kpiMeta
    */
   public KpiMetaDataRecord getKpiMeta(String source, String kpiName, String rat, boolean gdg) {

      KpiMetaDataRecord r = null; if (Config.dataGridEnabled) try {

         if (referenceType.equals(REreference.DATA_GRID)) {

            r = (gdg) ? new KpiMetaDataRecord(getDgdKpiMetaInfo(source, kpiName, rat))
               :        new KpiMetaDataRecord(getKpiMetaInfo   (source, kpiName, rat));
         }
      }
      catch (Exception e) {

         log.error(" getKpiMeta error " + e.getMessage());
      }

      return r;
   }

private CellLocationMapper getCellLocationMapper(String key) throws Exception {
      DataGridDataProvider dgrdp = DataGridDataProvider.getInstance();
      if (dgrdp != null) {
          return dgrdp.getCellLocationMapper(key);
      } else {
         return null;
      }
   }

private Imeitac getImeitac(Long imeitac) {
      DataGridDataProvider dgrdp = DataGridDataProvider.getInstance();
      if (dgrdp != null) {
          return dgrdp.getImeitac(imeitac);
      } else {
          return null;
      }
}

   private Imeitac getImeitac(String imeitac) {
          DataGridDataProvider dgrdp = DataGridDataProvider.getInstance();
          if (dgrdp != null) {
              return dgrdp.getImeitac(imeitac);
          } else {
              return null;
          }
   }

   private KpiMetaInfo getKpiMetaInfo(String source, String kpiName,
        String rat) {

          DataGridDataProvider dgrdp = DataGridDataProvider.getInstance();
          if (dgrdp != null) {
              return dgrdp.getKpiMetaInfo(source,kpiName,rat);
          } else {
              return null;
          }

   }

   private DgdKpiMetaInfo getDgdKpiMetaInfo(String source, String kpiName,
        String rat) {
          DataGridDataProvider dgrdp = DataGridDataProvider.getInstance();
          if (dgrdp != null) {
              return dgrdp.getDgdKpiMetaInfo(source,kpiName,rat);
          } else {
              return null;
          }
   }


}
