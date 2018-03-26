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

package com.ericsson.eea.ark.sli.grouping.rules.refdata;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.math.BigDecimal;
//import java.math.BigInteger;

import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import net.minidev.json.JSONValue;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.ParseException;

import com.ericsson.bigdata.common.column.Dimension;
import com.ericsson.eea.ark.offline.utils.NGEEAlarmHelper;
import com.ericsson.eea.ark.offline.utils.NGEEAlarmHelper.AlarmType;
import com.ericsson.eea.ark.common.service.model.CrmCustomerDeviceInfo;
import com.ericsson.eea.ark.sli.grouping.db.SQLConnection;
import com.ericsson.eea.ark.sli.grouping.rules.refdata.REreference;



/** CRM information.*/
@Getter
@Setter
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class CRM {

   private static final Logger log = LoggerFactory.getLogger(CRM.class);

   public String     account_number     = null;
   public Long       imsi               = null; // PK
   public BigDecimal imei               = null;
   public Integer    tac                = null;
   public String     device_status      = null;
   public String     customer_full_name = null;
   public Integer    customer_value     = null;
   public String     customer_type      = null;
   public String     customer_class     = null;
   public String     customer_status    = null;
   public String     plan_type          = null;
   public String     plan_name          = null;
   public String     plan_description   = null;
   public Integer    maxrate_dl         = null;
   public Integer    maxrate_ul         = null;
   public Integer    maxdata            = null;
   public String     site_data          = null; // A JSON string
   public String     tracking_option    = null;
   public Boolean    notify             = false;

   /** Type declaration used to support JSON string field: site_data.*/
   public static final String TYPE_DOUBLE     = "Double"
      ,                       TYPE_INTEGER    = "Integer"
      ,                       TYPE_BIGINTEGER = "BigInteger"
      ,                       TYPE_FLOAT      = "Float"
      ,                       TYPE_STRING     = "String"
      ,                       TYPE_LONG       = "Long"
   ;


   public CRM(CrmCustomerDeviceInfo c) {

      this.account_number     = c.getAccountNumber();
      this.imsi               = c.getImsi();
      this.imei               = c.getImei() == null ? null : new BigDecimal(c.getImei().toString());
      this.tac                = c.getTac();
      this.device_status      = c.getDeviceStatus();
      this.customer_full_name = c.getCustomerFullName();
      this.customer_value     = c.getCustomerValue();
      this.customer_type      = c.getCustomerType();
      this.customer_class     = c.getCustomerClass();
      this.customer_status    = c.getCustomerStatus();
      this.plan_type          = c.getPlanType();
      this.plan_name          = c.getPlanName();
      this.plan_description   = c.getPlanDescription();
      this.maxrate_dl         = c.getMaxrateDl();
      this.maxrate_ul         = c.getMaxrateUl();
      this.maxdata            = c.getMaxdata();
      this.site_data          = c.getSiteData();
      this.tracking_option    = c.getTrackingOption();
      this.notify             = c.isNotify();

   }


   /** Create an CRM object from the fields in a line separated tab. null values are represented by "\N".*/
   public CRM(String line) {

      String[] row = line.replaceAll("\\\\N", "NULL").split("\t");

      int n = 0;
      /**/                       account_number                                              =                      row[n++] ;
      if (row[n].equals("NULL")) imsi               =                0L; else imsi           = Long.parseLong      (row[n++]);
      if (row[n].equals("NULL")) imei               = new BigDecimal(0); else imei           = new BigDecimal      (row[n++]);
      if (row[n].equals("NULL")) tac                =                0 ; else tac            = Integer.parseInt    (row[n++]);
      /**/                       device_status      =                                                               row[n++] ;
      /**/                       customer_full_name =                                                               row[n++] ;
      if (row[n].equals("NULL")) customer_value     =                0 ; else customer_value = Integer.parseInt    (row[n++]);
      /**/                       customer_type      =                                                               row[n++] ;
      /**/                       customer_class     =                                                               row[n++] ;
      /**/                       customer_status    =                                                               row[n++] ;
      /**/                       plan_type          =                                                               row[n++] ;
      /**/                       plan_name          =                                                               row[n++] ;
      /**/                       plan_description   =                                                               row[n++] ;
      if (row[n].equals("NULL")) maxrate_dl         =                0 ; else maxrate_dl     = Integer.parseInt    (row[n++]);
      if (row[n].equals("NULL")) maxrate_ul         =                0 ; else maxrate_ul     = Integer.parseInt    (row[n++]);
      if (row[n].equals("NULL")) maxdata            =                0 ; else maxdata        = Integer.parseInt    (row[n++]);
      /**/                       site_data          =                                                               row[n++] ;
      /**/                       tracking_option    =                                                               row[n++] ;
      if (row[n].equals("NULL")) notify             =            false ; else notify         = Boolean.parseBoolean(row[n++]);
   }



   /** Construct a CRM object from the fields in a line separated tab. null values are represented by "\N".*/
   public CRM(String line, Set<String> crmColsUsed) {

      if (line == null) return;

      String[] cols = line.replaceAll("\\\\N", "NULL").split("\t"); if (cols.length != 19) return;

      if ((crmColsUsed == null || crmColsUsed.isEmpty()) || (!cols[ 0].equals("NULL") && (!crmColsUsed.isEmpty() && crmColsUsed.contains("account_number"    )))) account_number     =                  cols[ 0];
      if ((crmColsUsed == null || crmColsUsed.isEmpty()) || (!cols[ 1].equals("NULL") && (!crmColsUsed.isEmpty() && crmColsUsed.contains("imsi"              )))) imsi               = Long.parseLong  (cols[ 1]);
      if ((crmColsUsed == null || crmColsUsed.isEmpty()) || (!cols[ 2].equals("NULL") && (!crmColsUsed.isEmpty() && crmColsUsed.contains("imei"              )))) imei               = new BigDecimal  (cols[ 2]);
      if ((crmColsUsed == null || crmColsUsed.isEmpty()) || (!cols[ 3].equals("NULL") && (!crmColsUsed.isEmpty() && crmColsUsed.contains("tac"               )))) tac                = Integer.parseInt(cols[ 3]);
      if ((crmColsUsed == null || crmColsUsed.isEmpty()) || (!cols[ 4].equals("NULL") && (!crmColsUsed.isEmpty() && crmColsUsed.contains("device_status"     )))) device_status      =                  cols[ 4];
      if ((crmColsUsed == null || crmColsUsed.isEmpty()) || (!cols[ 5].equals("NULL") && (!crmColsUsed.isEmpty() && crmColsUsed.contains("customer_full_name")))) customer_full_name =                  cols[ 5];
      if ((crmColsUsed == null || crmColsUsed.isEmpty()) || (!cols[ 6].equals("NULL") && (!crmColsUsed.isEmpty() && crmColsUsed.contains("customer_value"    )))) customer_value     = Integer.parseInt(cols[ 6]);
      if ((crmColsUsed == null || crmColsUsed.isEmpty()) || (!cols[ 7].equals("NULL") && (!crmColsUsed.isEmpty() && crmColsUsed.contains("customer_type"     )))) customer_type      =                  cols[ 7];
      if ((crmColsUsed == null || crmColsUsed.isEmpty()) || (!cols[ 8].equals("NULL") && (!crmColsUsed.isEmpty() && crmColsUsed.contains("account_number"    )))) customer_class     =                  cols[ 8];
      if ((crmColsUsed == null || crmColsUsed.isEmpty()) || (!cols[ 9].equals("NULL") && (!crmColsUsed.isEmpty() && crmColsUsed.contains("customer_status"   )))) customer_status    =                  cols[ 9];
      if ((crmColsUsed == null || crmColsUsed.isEmpty()) || (!cols[10].equals("NULL") && (!crmColsUsed.isEmpty() && crmColsUsed.contains("plan_type"         )))) plan_type          =                  cols[10];
      if ((crmColsUsed == null || crmColsUsed.isEmpty()) || (!cols[11].equals("NULL") && (!crmColsUsed.isEmpty() && crmColsUsed.contains("plan_name"         )))) plan_name          =                  cols[11];
      if ((crmColsUsed == null || crmColsUsed.isEmpty()) || (!cols[12].equals("NULL") && (!crmColsUsed.isEmpty() && crmColsUsed.contains("plan_description"  )))) plan_description   =                  cols[12];
      if ((crmColsUsed == null || crmColsUsed.isEmpty()) || (!cols[13].equals("NULL") && (!crmColsUsed.isEmpty() && crmColsUsed.contains("maxrate_dl"        )))) maxrate_dl         = Integer.parseInt(cols[13]);
      if ((crmColsUsed == null || crmColsUsed.isEmpty()) || (!cols[14].equals("NULL") && (!crmColsUsed.isEmpty() && crmColsUsed.contains("maxrate_ul"        )))) maxrate_ul         = Integer.parseInt(cols[14]);
      if ((crmColsUsed == null || crmColsUsed.isEmpty()) || (!cols[15].equals("NULL") && (!crmColsUsed.isEmpty() && crmColsUsed.contains("maxdata"           )))) maxdata            = Integer.parseInt(cols[15]);
      if ((crmColsUsed == null || crmColsUsed.isEmpty()) || (!cols[16].equals("NULL") && (!crmColsUsed.isEmpty() && crmColsUsed.contains("site_data"         )))) site_data          =                  cols[16];
      if ((crmColsUsed == null || crmColsUsed.isEmpty()) || (!cols[17].equals("NULL") && (!crmColsUsed.isEmpty() && crmColsUsed.contains("tracking_option"   )))) tracking_option    =                  cols[17];
      if ((crmColsUsed == null || crmColsUsed.isEmpty()) || (!cols[18].equals("NULL") && (!crmColsUsed.isEmpty() && crmColsUsed.contains("tracking_option"   )))) tracking_option    =                  cols[18];
   }


   /**
    * Default constructor.
    */
   @Deprecated
   public CRM(Map<Dimension, Object> crmColMap) {

      account_number     = null; imsi               = null; imei               = null; tac                = null;
      device_status      = null; customer_full_name = null; customer_value     = null; customer_type      = null;
      customer_class     = null; customer_status    = null; plan_type          = null; plan_name          = null;
      plan_description   = null; maxrate_dl         = null; maxrate_ul         = null; maxdata            = null;
      site_data          = null; tracking_option    = null; notify             = null;

      Iterator<Entry<Dimension,Object>> it = crmColMap.entrySet().iterator(); while (it.hasNext()) {

         Entry<Dimension,Object> pairs = it.next();
         Dimension               key   = (Dimension) pairs.getKey();
         Object                  value = (Object)    pairs.getValue(); if (value == null) continue;

         String keyName = key.getName(); try {

            if      (keyName.equalsIgnoreCase("account_number"    ) && (value instanceof  String)) account_number     =                  ( String) value;
            else if (keyName.equalsIgnoreCase("imei"              ) && (value instanceof Integer)) imei               = new BigDecimal  ((Integer) value);
            else if (keyName.equalsIgnoreCase("tac"               ) && (value instanceof Integer)) tac                =                  (Integer) value;
            else if (keyName.equalsIgnoreCase("device_status"     ) && (value instanceof  String)) device_status      =                  ( String) value;
            else if (keyName.equalsIgnoreCase("customer_full_name") && (value instanceof  String)) customer_full_name =                  ( String) value;
            else if (keyName.equalsIgnoreCase("customer_value"    ) && (value instanceof Integer)) customer_value     =                  (Integer) value;
            else if (keyName.equalsIgnoreCase("customer_type"     ) && (value instanceof  String)) customer_type      =                  ( String) value;
            else if (keyName.equalsIgnoreCase("customer_class"    ) && (value instanceof  String)) customer_class     =                  ( String) value;
            else if (keyName.equalsIgnoreCase("customer_status"   ) && (value instanceof  String)) customer_status    =                  ( String) value;
            else if (keyName.equalsIgnoreCase("plan_type"         ) && (value instanceof  String)) plan_type          =                  ( String) value;
            else if (keyName.equalsIgnoreCase("plan_name"         ) && (value instanceof  String)) plan_name          =                  ( String) value;
            else if (keyName.equalsIgnoreCase("plan_description"  ) && (value instanceof  String)) plan_description   =                  ( String) value;
            else if (keyName.equalsIgnoreCase("maxrate_dl"        ) && (value instanceof Integer)) maxrate_dl         =                  (Integer) value;
            else if (keyName.equalsIgnoreCase("maxrate_ul"        ) && (value instanceof  String)) maxrate_ul         = Integer.parseInt(( String) value);
            else if (keyName.equalsIgnoreCase("site_data"         ) && (value instanceof  String)) site_data          =                  ( String) value;
            else if (keyName.equalsIgnoreCase("tracking_option"   ) && (value instanceof  String)) tracking_option    =                  ( String) value;
            else if (keyName.equalsIgnoreCase("notify"            ) && (value instanceof Boolean)) notify             =                  (Boolean) value;
         }
         catch (NumberFormatException ex) {

            log.error("Error processing '"+keyName+"': ", ex); throw ex;
         }
      }
   }


   public boolean isSite_data() {

      if (site_data != null && site_data.length() > 0 && !site_data.equals("\\N")) return true;

      return false;
   }


   /**
    * @param key
    *           : site_data key
    * @param dataType
    *           : the data_type to which the value will be typecast to.
    * @return : an object of the specified dataType
    */
   public Object getSite_dataByKey(String key, String dataType) {

      if (isSite_data()) {

         try {

            if (key == null || key.isEmpty()) return null;

            Object obj = JSONValue.parseStrict(site_data); if (obj == null) return null; // in case there is an improper object which returned a null.

            JSONObject jsonObj  = (JSONObject) obj;
            Object     objValue = jsonObj.get(key); if (objValue == null) return null;

            if (dataType.equals(CRM.TYPE_STRING)) {

               if      (objValue instanceof  String) { return ((( String) objValue)           .isEmpty() ? null :            objValue)            ; }
               else if (objValue instanceof Integer) { return (((Integer) objValue).toString().isEmpty() ? null : ((Integer) objValue).toString()); }
               else if (objValue instanceof    Long) { return (((   Long) objValue).toString().isEmpty() ? null : ((   Long) objValue).toString()); }
               else if (objValue instanceof  Double) { return ((( Double) objValue).toString().isEmpty() ? null : (( Double) objValue).toString()); }
               else if (objValue instanceof  Double) { return ((( Double) objValue).toString().isEmpty() ? null : (( Double) objValue).toString()); }
            }
            else if (dataType.equals(CRM.TYPE_INTEGER)) {

               if      (objValue instanceof Integer) { return          objValue; }
               else if (objValue instanceof    Long) { long l = (Long) objValue; return (new Integer((int) l)); }
               else if (objValue instanceof  String) {
                  try {
                     return (new Integer((String) objValue));
                  }
                  catch (Exception e) {
                     log.error("can not cast string : " + (String) objValue + " to Integer");
                     return null;
                  }
               }
            }
            else if (dataType.equals(CRM.TYPE_DOUBLE)) {

               if      (objValue instanceof  Double) { return                        objValue                 ; }
               else if (objValue instanceof Integer) { return (new Double(((Integer) objValue).doubleValue())); }
               else if (objValue instanceof    Long) { return (new Double(((   Long) objValue).longValue  ())); }
               else if (objValue instanceof  String) {
                  try {
                     return (new Double((String) objValue));
                  }
                  catch (Exception e) {
                     log.error("can not cast string : " + (String) objValue + " to Double"); return null;
                  }
               }
            }
            else if (dataType.equals(CRM.TYPE_FLOAT)) { Float value = new Float((String) jsonObj.get(key)); return value.floatValue(); }
            else if (dataType.equals(CRM.TYPE_LONG )) { Long  value = new  Long((String) jsonObj.get(key)); return value.longValue() ; }

         }
         catch (ParseException e) {
             if (log.isWarnEnabled()) log.warn("error parsing site_data trying to get key: " + key + e.getMessage());
             log.debug("exception: ", e);
            log.debug("exception: ", e);
            return null;
         }
         catch (NumberFormatException e) {
             if (log.isWarnEnabled()) log.warn("error NumberFormatException site_data trying to parse key value: " + key + e.getMessage());
            log.debug("exception: ", e);
            return null;
         }

      }
      return null;
   }


   /**
    * @param key
    *           : site_data key.
    * @return: value from JSON string: site_data returned as String.
    */
   public String getSite_dataByKey(String key) {

      if (isSite_data()) {

         try {

            if (key == null || key.isEmpty()) return null;

            Object obj = JSONValue.parseStrict(site_data); if (obj == null) return null;

            JSONObject jsonObj  = (JSONObject) obj;
            Object     objValue = jsonObj.get(key); if (objValue == null) return null;

            if      (objValue instanceof  String) { return (( (String) objValue)           .isEmpty() ? null :            objValue .toString()); }
            else if (objValue instanceof Integer) { return (((Integer) objValue).toString().isEmpty() ? null : ((Integer) objValue).toString()); }
            else if (objValue instanceof    Long) { return ((   (Long) objValue).toString().isEmpty() ? null : (   (Long) objValue).toString()); }
            else if (objValue instanceof  Double) { return (( (Double) objValue).toString().isEmpty() ? null : ( (Double) objValue).toString()); }
            else if (objValue instanceof  Double) { return (( (Double) objValue).toString().isEmpty() ? null : ( (Double) objValue).toString()); }
         }
         catch (ParseException e) {

            log.warn("error parsing site_data trying to get key: " + key + e.getMessage()); return null;
         }
      }

      return null;
   }


   /*
    * load from db: Table definition: create table crm_customer_device_info (
    * account_number text not null, imsi bigint, imei bigint, tac integer,
    * device_status text, customer_full_name text, customer_value integer,
    * customer_type text, customer_class text, customer_status text, plan_type
    * text, plan_name text, plan_description text, maxrate_dl integer,
    * maxrate_ul integer, maxdata integer, site_data text ) without oids;
    *
    * alter table crm_customer_device_info add constraint
    * pk_crm_customer_device_info primary key (imsi);
    */
   /**
    * query(): fills local CRM local cache from DB.
    *
    * @param crmhash
    *           : hash map to hold CRMs from DB
    * @param crmUsedCols
    *           columns to be selected (columns used in rules)
    */
   public static synchronized void query(HashMap<Long,CRM> crmhash
      ,                                  Set<String>       crmUsedCols
      ,                                  SQLConnection     reSqlConnection) {

      if (reSqlConnection == null) {

         log.error("reSqlConnection == null, query returning"); return;
      }

      // Clear the map or pre-allocate a large one before loading.
      if (crmhash != null) crmhash.clear();
      else                 crmhash = new HashMap<Long, CRM>(100000);

      // Create the list of columns to select
      String       selectCols = null;
      List<String> colList    = new ArrayList<String>(); if (crmUsedCols == null || crmUsedCols.isEmpty()) {

         selectCols = " * ";
      }
      else {

         Iterator<String> it = crmUsedCols.iterator(); while (it.hasNext()) {

            String col = it.next(); colList.add(col);

            if (selectCols == null) selectCols  =        col;
            else                    selectCols += ", " + col;
         }
      }

      try {

         String query = "select " + selectCols + " from " + REreference.getConfig().databaseSchema() + "." + REreference.getConfig().CRMtablename();

//         if (RuleEngine.isSegmentEsr()) {
//
//            // redefine the select * just to get a subset of IMSIs;  get list of filter ID.
//            Iterator<String> it = RuleEngine.listOfEsrSources.iterator();
//            String filters = null; while (it.hasNext()) {
//
//               if (filters == null) filters  =       it.next();
//               else                 filters += "," + it.next();
//            }
//
//            query += " where mod(imsi," + RuleEngine.getCorrelatorSegments() + ") in (" + filters + ")";
//
//         }
//         log.info("query: " + query);

         Statement sql       = reSqlConnection.getNewStatement(REreference.getConfig().databaseName());
         ResultSet resultset = sql.executeQuery(query); while (resultset.next()) {

            CRM c = new CRM();
            if ((crmUsedCols == null || crmUsedCols.isEmpty()) || ((!crmUsedCols.isEmpty() && crmUsedCols.contains("account_number"    )))) c.setAccount_number    (resultset.getString    ("account_number"    ));
            if ((crmUsedCols == null || crmUsedCols.isEmpty()) || ((!crmUsedCols.isEmpty() && crmUsedCols.contains("imsi"              )))) c.setImsi              (resultset.getBigDecimal("imsi"              ).longValue());
            if ((crmUsedCols == null || crmUsedCols.isEmpty()) || ((!crmUsedCols.isEmpty() && crmUsedCols.contains("imei"              )))) c.setImei              (resultset.getBigDecimal("imei"              ));
            if ((crmUsedCols == null || crmUsedCols.isEmpty()) || ((!crmUsedCols.isEmpty() && crmUsedCols.contains("tac"               )))) c.setTac               (resultset.getInt       ("tac"               ));
            if ((crmUsedCols == null || crmUsedCols.isEmpty()) || ((!crmUsedCols.isEmpty() && crmUsedCols.contains("device_status"     )))) c.setDevice_status     (resultset.getString    ("device_status"     ));
            if ((crmUsedCols == null || crmUsedCols.isEmpty()) || ((!crmUsedCols.isEmpty() && crmUsedCols.contains("customer_full_name")))) c.setCustomer_full_name(resultset.getString    ("customer_full_name"));
            if ((crmUsedCols == null || crmUsedCols.isEmpty()) || ((!crmUsedCols.isEmpty() && crmUsedCols.contains("customer_value"    )))) c.setCustomer_value    (resultset.getInt       ("customer_value"    ));
            if ((crmUsedCols == null || crmUsedCols.isEmpty()) || ((!crmUsedCols.isEmpty() && crmUsedCols.contains("customer_type"     )))) c.setCustomer_type     (resultset.getString    ("customer_type"     ));
            if ((crmUsedCols == null || crmUsedCols.isEmpty()) || ((!crmUsedCols.isEmpty() && crmUsedCols.contains("customer_class"    )))) c.setCustomer_class    (resultset.getString    ("customer_class"    ));
            if ((crmUsedCols == null || crmUsedCols.isEmpty()) || ((!crmUsedCols.isEmpty() && crmUsedCols.contains("customer_status"   )))) c.setCustomer_status   (resultset.getString    ("customer_status"   ));
            if ((crmUsedCols == null || crmUsedCols.isEmpty()) || ((!crmUsedCols.isEmpty() && crmUsedCols.contains("plan_type"         )))) c.setPlan_type         (resultset.getString    ("plan_type"         ));
            if ((crmUsedCols == null || crmUsedCols.isEmpty()) || ((!crmUsedCols.isEmpty() && crmUsedCols.contains("plan_name"         )))) c.setPlan_name         (resultset.getString    ("plan_name"         ));
            if ((crmUsedCols == null || crmUsedCols.isEmpty()) || ((!crmUsedCols.isEmpty() && crmUsedCols.contains("plan_description"  )))) c.setPlan_description  (resultset.getString    ("plan_description"  ));
            if ((crmUsedCols == null || crmUsedCols.isEmpty()) || ((!crmUsedCols.isEmpty() && crmUsedCols.contains("maxrate_dl"        )))) c.setMaxrate_dl        (resultset.getInt       ("maxrate_dl"        ));
            if ((crmUsedCols == null || crmUsedCols.isEmpty()) || ((!crmUsedCols.isEmpty() && crmUsedCols.contains("maxrate_ul"        )))) c.setMaxrate_ul        (resultset.getInt       ("maxrate_ul"        ));
            if ((crmUsedCols == null || crmUsedCols.isEmpty()) || ((!crmUsedCols.isEmpty() && crmUsedCols.contains("maxdata"           )))) c.setMaxdata           (resultset.getInt       ("maxdata"           ));
            if ((crmUsedCols == null || crmUsedCols.isEmpty()) || ((!crmUsedCols.isEmpty() && crmUsedCols.contains("site_data"         )))) c.setSite_data         (resultset.getString    ("site_data"         ));
            if ((crmUsedCols == null || crmUsedCols.isEmpty()) || ((!crmUsedCols.isEmpty() && crmUsedCols.contains("tracking_option"   )))) c.setTracking_option   (resultset.getString    ("tracking_option"   ));
            if ((crmUsedCols == null || crmUsedCols.isEmpty()) || ((!crmUsedCols.isEmpty() && crmUsedCols.contains("notify"            )))) c.setNotify            (resultset.getBoolean   ("notify"            ));

            crmhash.put(c.imsi, c);
         }
         resultset.close();
         sql.getConnection().close();
         log.info(crmhash.size() + " read CRM from table ");
         System.out.println(crmhash.size() + " read CRM from table ");
      }
      catch (Exception ex) {

         log.error("Exception loading from table" + REreference.getConfig().CRMtablename()+"'", ex);
         NGEEAlarmHelper.logAlarm(AlarmType.databaseError, ex.getMessage());
      }

   }


   /**
    * @param crmhash
    *           : crmhash that holds CRM
    * @param crmColsUsed
    *           : List of CRM cols used in drl rules.
    * @throws IOException
    *            Loads the CRM cache from file.
    */
   public static synchronized void load(HashMap<Long, CRM> crmhash, Set<String> crmColsUsed) throws IOException {

      FileReader     fr     = null;
      BufferedReader reader = null; try {

         fr     = new FileReader(REreference.getConfig().CRMfilename());
         reader = new BufferedReader(fr);

         String line = null; while ((line = reader.readLine()) != null) {

            if (!line.startsWith("#")) {

               CRM c = new CRM(line, crmColsUsed);

//               if (RuleEngine.isSegmentEsr()) {
//
//                  if (!CRM.isIMSIinCrmSpace(c)) continue;
//               }

               crmhash.put(c.imsi, c);
            }
         }
      }
      finally {

         if (fr     != null) { fr    .close(); fr     = null; }
         if (reader != null) { reader.close(); reader = null; }
      }

      log.info(crmhash.size() + " CRM records loaded");
   }


//   /**
//    * @param crm
//    * @return true if the IMSI in the CRM matches the CRM space
//    */
//   public static boolean isIMSIinCrmSpace(CRM crm) {
//
//      BigInteger bigIntimsi            = new BigInteger(new Long(crm.getImsi()).toString());
//      Integer    noOfCorrelatorSegment = RuleEngine.getCorrelatorSegments();
//      BigInteger totalNumberOfSegment  = new BigInteger(noOfCorrelatorSegment.toString());
//      BigInteger imsiSpaceKey          = bigIntimsi.mod(totalNumberOfSegment);
//
//      return (RuleEngine.listOfEsrSources.contains(imsiSpaceKey.toString()));
//   }


   public boolean isEmpty() {

      return account_number   == null && imsi               == null && imei           == null && tac           == null
         &&  device_status    == null && customer_full_name == null && customer_value == null && customer_type == null
         &&  customer_class   == null && customer_status    == null && plan_type      == null && plan_name     == null
         &&  plan_description == null && maxrate_dl         == null && maxrate_ul     == null && maxdata       == null
         &&  site_data        == null && tracking_option    == null && notify         == null;
   }
}
