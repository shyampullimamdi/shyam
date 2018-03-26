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

package com.ericsson.eea.ark.offline.rules.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.List;
//import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
//import java.util.TreeMap;
import java.util.Map.Entry;
//import java.util.SortedMap;
//import java.util.Collection;
//import java.util.LinkedHashMap;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
//import lombok.EqualsAndHashCode;
//import lombok.NoArgsConstructor;
//import lombok.AllArgsConstructor;

import net.minidev.json.JSONObject;


//import java.io.IOException;
import java.lang.reflect.Method;
//import java.security.InvalidKeyException;
//import java.security.InvalidAlgorithmParameterException;

//import javax.crypto.BadPaddingException;


import com.ericsson.bigdata.common.util.MatchableMap;
import com.ericsson.bigdata.common.identifier.CellIdentifier;
import com.ericsson.bigdata.esr.ESR;
import com.ericsson.bigdata.esr.dataexchange.Bearer;
import com.ericsson.bigdata.esr.dataexchange.KpiDesc;
import com.ericsson.bigdata.esr.dataexchange.Trigger;
import com.ericsson.bigdata.esr.dataexchange.KpiValue;
import com.ericsson.bigdata.esr.dataexchange.KpiRecord;
import com.ericsson.bigdata.esr.dataexchange.TriggerList;
import com.ericsson.bigdata.esr.parserlogic.CastingException;
import com.ericsson.bigdata.esr.parserlogic.EsrFormatException;
import com.ericsson.cea.offline.profiler.operation.MapHunter;
import com.ericsson.cea.offline.profiler.operation.MapHunter.Node;
import com.ericsson.eea.ark.offline.rules.esr.EsrTriggers;
import com.ericsson.eea.ark.offline.rules.refdata.CRM;
import com.ericsson.eea.ark.offline.rules.refdata.REreference;
import com.ericsson.eea.ark.offline.rules.refdata.Terminal;
import com.ericsson.eea.ark.offline.rules.utils.Util;

import static com.ericsson.bigdata.esr.aggregation.AggregationType.MAX;
import static com.ericsson.bigdata.esr.aggregation.AggregationType.MIN;

import com.ericsson.eea.ark.offline.config.Config;


/**
 * This class provides convenient functions around the Canonical ESR produced by the pro-processor.
 */
@ToString(of={"timestamp","imsi","imeitac","esr"})
public class Event_ESR {

//   private static final long serialVersionUID = 4353148500477923872L;

   public class EventTypeKpiValue {

      @Getter @Setter Double value  = null;
      @Getter @Setter Double weight = null;
   };


//   @Override
//   public String toString() {
//
//      return "EventType_ESR [timestamp=" + timestamp + ", imsi=" + imsi + ", imeitac=" + imeitac + this.toJSONString() + "]";
//   }

   private static final Logger log = LoggerFactory.getLogger(Event_ESR.class.getName());

   /** Memoization table.*/
   private HashMap<String,Object> memo = new HashMap<String,Object>();

   // cut -d'	' -f1 ../../offline/pre-processor/esr/config/esr_kpi.csv |grep -v kpi_source |sort -u |tr '\n' '|' |sed 's/^/"/; s/|$/"/'
   private static String  kpi_sources = "auto|rtcp|ais_iucs|core|ran|sip|tcp|traf|ftraf|uplane_perf|video|voiceCs|volte|web";
   private static boolean initialized = false;


   @Getter         private long                          timestamp;
   @Getter         private String                        imsi                   = null;
   @Getter         private Integer                       imeitac                = null;

   @Getter @Setter         Map<String,Object>            esr                    = null;

   @Getter @Setter         CRM                           crm                    = null;
   /**/    @Setter         Terminal                      terminal               = null;
   @Getter @Setter private Boolean                       hide                   = false; // This field is set based on WhiteList
   @Getter @Setter private Set<String>                   kpiSetBySource         = null;  // KPI by source;kpi
   @Getter @Setter private Set<String>                   kpiSetBySourceAndlocId = null;  // KPI by source;kpi;locId
   @Getter @Setter private Map<KpiDesc,List<KpiRecord>>  ottKpiM                = null;
   /**/    @Setter         Map<String,TriggerList>       triggersByType         = null;  // TriggerList for the Esr by trigger type

   /**/            private Map<String,List<Object>>      sliCause               = new HashMap<String,List<Object>>();

   private static int      max_levels     = 1024;
   private static String[] exclude_fields = new String[] {"triggers"};


   @SuppressWarnings("unchecked")
   public Event_ESR(Map<String,Object> esr) throws Exception {

      Object o = null;

      this.esr = esr; //(esr instanceof ESR) ? esr  : (Map<String,Object>) esr.get("esr");
      this.crm = (esr instanceof ESR) ? null : ((o=esr.get("CRM")) != null) ? new CRM((Map<String,Object>) o) : null;

      imsi       = (String)  ((this.esr instanceof ESR ) ? ((ESR) this.esr).getImsi   () : this.esr.get("imsi"));
      imeitac    = (Integer) ((this.esr instanceof ESR ) ? ((ESR) this.esr).getImeiTac()  : this.esr.get("imeitac"));
      Object ts  =            (this.esr instanceof ESR ) ? ((ESR) this.esr).getStart  ()  : esr.get("TIMESTAMP"); if (ts != null)
      timestamp  =            (ts       instanceof Long) ? (Long) ts : Util.parseTimestamp((String) ts).getLeft();

      // Allow for the default KPI Source Names to be updated from the configuration file.
      if (!initialized) {

         if (Config.cfg != null) kpi_sources    = Config.cfg.getString(Config.cfg_prefix, "kpi-sources"   , kpi_sources);
         if (Config.cfg != null) max_levels     = Config.cfg.getInt   (Config.cfg_prefix, "max-levels"    , max_levels );
         if (Config.cfg != null) exclude_fields = Config.cfg.getList  (Config.cfg_prefix, "exclude-fields", exclude_fields);

         initialized = true;
      }

      setKpiSets();
   }


   public <T> T get(Class<T> type, String field) {

      @SuppressWarnings("unchecked")
      T res = (T) esr.get(field);

      return res;
   }


   public boolean isSliCause() {

      return sliCause.size() >0;
   }


   public void addSliCause(String service, Object incident) {

      if (service == null || incident == null) return;

      List<Object> incidents = sliCause.get(service); if (incidents == null) {

         sliCause.put(service, incidents = new ArrayList<Object>());
      }

      incidents.add(incident);
   }


   @SuppressWarnings("unchecked")
   private void setKpiSets() {

      Map<KpiDesc, List<KpiRecord>> ottKpiM = new HashMap<KpiDesc, List<KpiRecord>>();
      kpiSetBySource         = new HashSet<String>();
      kpiSetBySourceAndlocId = new HashSet<String>();

      Map<KpiDesc, List<KpiRecord>> kpiM = null;

      // For regular ESR records
      if (esr instanceof ESR) {

         try {

            kpiM = ((ESR) esr).getKpiRecords();
         }
         catch (CastingException | EsrFormatException e) {

            if (log.isDebugEnabled()) log.debug("failed to get Kpi list: " + e.getMessage()); return;
         }
      }
      // For flattened ESRs
      else {

         List<Map<String,Object>> bearers = (List<Map<String,Object>>) esr.get("bearers"); if (bearers == null) return; /*if (bearers.size() == 0) {

            if (log.isDebugEnabled()) log.debug("setKpiSets: An empty list of bearers was found in ESR for imsi: \""+esr.get("imsi")+"\", timestamp: \""+esr.get("TIMESTAMP")+"\". Skipping it..."); return;
         }*/

         for (int i=0; i<bearers.size(); ++i) {

            Map<String,Object>       bearer    = bearers.get(i); if (bearer == null) continue;
            List<Map<String,Object>> kpis_list = (List<Map<String,Object>>) bearer.get("kpis"); if (kpis_list == null) continue; /*if (kpis_list.size() == 0) {

               if (log.isDebugEnabled()) log.debug("setKpiSets: An empty list of kpis was found in ESR for imsi: \""+esr.get("imsi")+"\", timestamp: \""+esr.get("TIMESTAMP")+"\". Skipping it..."); continue; //return;
            }*/

               // Why get(0)???
               Map<String,Object> kpis = kpis_list.get(0); for (String k : kpis.keySet()) {

                  String  source = null
                     ,    name   = null;
                  Matcher m      = Pattern.compile("^("+kpi_sources+")_(.*)$").matcher(k); if (m.matches()) {

                     source = m.group(1); name = m.group(2);
                  }

                  if (!m.matches() || source == null || name == null) {

                     if (log.isWarnEnabled()) log.warn("Invalid KPI source/name: '"+k+"'; source name not in configured: '"+kpi_sources.replace('|',',')+"'"); continue;
                  }

                  KpiDesc         kpiDesc = new KpiDesc(source, name);
                  List<KpiRecord> recs    = new ArrayList<KpiRecord>(); {

//                     List<KpiRecord> recList = getKpiRecord(source, name); if (recList != null) recs.addAll(recList);
                     KpiRecord rec = getKpiRecord(source, name); if (rec != null) recs.add(rec);
                  }

                  if (recs.size() >0) {

                     if (kpiM == null) kpiM = new HashMap<KpiDesc,List<KpiRecord>>();

                     kpiM.put(kpiDesc, recs);
                  }
               }
         //   }
         }
      }

      if (kpiM == null || kpiM.isEmpty()) {

         if (log.isDebugEnabled()) log.debug("setKpiSets: Failed to get the list of KPIs..."); return;
      }

      for (Entry<KpiDesc, List<KpiRecord>> entry : kpiM.entrySet()) {

         KpiDesc         kpiDesc     = entry.getKey();
         List<KpiRecord> kl          = entry.getValue();
         String          ottBySource = kpiDesc.getSource() +";"+ kpiDesc.getName();
         kpiSetBySource.add(ottBySource);

//         if (otts != null && otts.contains(kpiDesc.getSource())) {
//
//            ottKpiM.put(kpiDesc, kl);
//         }
         for (KpiRecord kpi : kl) {

            if (kpi.getCellIdentifier() != null && kpi.getCellIdentifier().getLocationId() != null) {

               String ottBySourceAndCell = kpiDesc.getSource() +";"+ kpiDesc.getName() +";"+ kpi.getCellIdentifier().getLocationId();
               kpiSetBySourceAndlocId.add(ottBySourceAndCell);
            }

         }
      }

      this.setOttKpiM(ottKpiM);
   }


   public static List<Object> cloneList(List<Object> rec) {

      return cloneList(rec, null, null, new int[] {0}, null);
   }


   public static List<Object> cloneList(List<Object> rec, BooleanFunctor fun) {

      return cloneList(rec, null, null, new int[] {0}, fun);
   }


   public static List<Object> cloneList(List<Object> rec, Object[] filter, Map<Object,Object> replace, int[] levels) {

      return cloneList(rec, filter, replace, levels, null);
   }


   @SuppressWarnings("unchecked")
   public static List<Object> cloneList(List<Object> rec, Object[] filter, Map<Object,Object> replace, int[] levels, BooleanFunctor fun) {

      if (levels[0] > max_levels) {

         String msg = "Warning: Cycle detected at level: "+levels[0]; log.warn(msg); throw new RuntimeException(msg);
      }
      else levels[0] = ++levels[0];

      List<Object> newList = new ArrayList<Object>();

      // Get the filters for this and the next level
      Object   filt         = null;
      Object[] nestedFilter = null; if (filter != null && filter.length >0) {

         filt = filter[0]; if (filter.length >1) {

            nestedFilter = new String[filter.length -1]; for (int i=1; i<filter.length; ++i) {

               nestedFilter[i -1] = filter[i];
            }
         }
      }

      for (Object v : rec) {

         if (fun != null && !fun.apply(v)) {

            continue;
         }

         // Filter for this level
         if (filt != null && filt.equals(v)) {

            if (replace != null && replace.containsKey(filt)) newList.add(replace.get(filt));

            continue;
         }

         // Descend the next level down
         if      (v instanceof Map )   newList.add(cloneMap ((Map<String,Object>) v, nestedFilter, replace, levels));
         else if (v instanceof List)   newList.add(cloneList((List<      Object>) v, nestedFilter, replace, levels));
         else                          newList.add(                               v );
//         else {
//            throw new RuntimeException("Error. Not all types are being considered.");
//         }
      }

      return newList;
   }


   public static Map<String,Object> cloneMap(Map<String,Object> rec) {

      return cloneMap(rec, null, null, new int[] {0});
   }


   @SuppressWarnings("unchecked")
   public static Map<String,Object> cloneMap(Map<String,Object> rec, Object[] filter, Map<Object,Object> replace, int[] levels) {

      if (levels[0] > max_levels) {

         String msg = "Warning: Cycle detected at level: "+levels[0]; log.warn(msg); throw new RuntimeException(msg);
      }
      else levels[0] = ++levels[0];

      Map<String,Object> newMap = new HashMap<String,Object>();

      // Get the filters for this and the next level
      Object   filt         = null;
      Object[] nestedFilter = null; if (filter != null && filter.length >0) {

         filt = filter[0]; if (filter.length >1) {

            nestedFilter = new String[filter.length -1]; for (int i=1; i<filter.length; ++i) {

               nestedFilter[i -1] = filter[i];
            }
         }
      }

      for (Map.Entry<String,Object> e : rec.entrySet()) {

         String k = e.getKey(); Object v = e.getValue();

         // Filter at this level, possibly replacing the filtered content
         if (filt != null && filt.equals(k)) {

            if (replace != null && replace.containsKey(filt)) newMap.put((String) filt, replace.get(filt));

            continue;
         }

         // Descend at the next level down
         if      (v instanceof Map )    newMap.put(k, cloneMap ((Map<String,Object>) v, nestedFilter, replace, levels));
         else if (v instanceof List)    newMap.put(k, cloneList((List<      Object>) v, nestedFilter, replace, levels));
         else                           newMap.put(k,                                v );
//         else {
//            throw new RuntimeException("Error. Not all types are being considered.");
//         }
      }

      return newMap;
   }

   public static interface BooleanFunctor {

      public boolean apply(Object val);
   }

   @SuppressWarnings({ "unchecked", "rawtypes" })
   public static List<Map<String,Object>> splitESRs(Map<String,Object> esr) {

      List<Map<String,Object>> esrs     = new ArrayList<Map<String,Object>>();
      Map<String,Object>       esr_tmpl = new JSONObject();
      List<Object>             kpi_tmpl = new ArrayList<Object>(); kpi_tmpl.add(new HashMap<String,Object>());
      Map<Object,Object>       replace  = new HashMap<Object,Object>();

      try {

         // Iterate over the ESR's fields, capturing the 'bearers', and replacing them with a bearer without 'kpis'
         String                   field      = null;
         List<Map<String,Object>> bearers    = null;
         List<Object>             field_path = new ArrayList<Object>(); for (Map.Entry<String,Object> esr_entry : esr.entrySet()) {

            if (esr_entry == null) continue;

            String esr_entry_key = esr_entry.getKey(); Object esr_entry_value = esr_entry.getValue();

            field_path.add(esr_entry_key); field = Util.intersperse(".", field_path); try { // 1st level

                // Exclude any 1st level fields matching any of the paths in the configurable 'excluded_fields' parameter.
                if (exclude_fields != null && Util.contains(field, exclude_fields)) {

                   continue;
                }

                if ("bearers".equals(esr_entry_key)) {

                   if (esr_entry_value == null) continue; if (((List) esr_entry_value).size() == 0) {

                      if (log.isDebugEnabled()) log.debug("splitESRs: An empty list of bearers was found in ESR for imsi: \""+esr.get("imsi")+"\", timestamp: \""+esr.get("TIMESTAMP")+"\". Skipping it..."); continue; //return esrs;
                   }

                   // Clone the list of bearers, replacing all contained 'kpis' list with 'kpi_tmpl'
                   bearers         = (List<Map<String,Object>>) esr_entry_value;
                   replace.put("kpis", (List<Object>) cloneList(kpi_tmpl));
                   esr_entry_value = cloneList((List<Object>) esr_entry_value, new Object[]{null,"kpis"}, replace, new int[] {0}); /*, new BooleanFunctor() {

                      // Filter out all empty bearers of bearers that are not active. If an not-empty KPI is found in a inactive bearer block, place the KPI in the default bearer block.
                      @Override public boolean apply(Object val) {

                         boolean rc = false; if (val instanceof Map) {

                            Map<String,Object> bearer = (Map<String,Object>) val; if (bearer != null && !bearer.isEmpty()) {

                               Boolean active = (Boolean) bearer.get("active"); rc = active != null && active == true;

    //                           List<Map<String,Object>> kpis = (List<Map<String,Object>>) bearer.get("kpis"); if (kpis != null) for (Map<String,Object> kpi : kpis) {
    //
    //                              if (kpi != null && !kpi.isEmpty()) {
    //
    //                                 rc = true; break;
    //                              }
    //                           }
                            }
                         }
    //                     else if (val instanceof List) {
    //                     }

                         return rc;
                      }
                   });*/
                }

                esr_tmpl.put(esr_entry_key, esr_entry_value);
            }
            finally { // Trim from 1st level forward

               field_path = field_path.subList(0, 0);
            }
         }

         // Keep track of all KPIs to avoid duplicates
         HashMap<String,ArrayList<Map<String,Object>>> uniqueKpis = new HashMap<String,ArrayList<Map<String,Object>>>();

         // Iterate over the list of bearers
         int max_esr_idx = 0;
         if (bearers != null) for (int bearer_idx=0; bearer_idx<bearers.size(); ++bearer_idx) {

            Map<String,Object> bearer = bearers.get(bearer_idx); if (bearer == null) continue; /*if (bearer.isEmpty()) {

               if (log.isDebugEnabled()) log.debug("splitESRs: An empty list of bearers was found in ESR for imsi: \""+esr.get("imsi")+"\", timestamp: \""+esr.get("TIMESTAMP")+"\". Skipping..."); continue; //return esrs;
            }*/

            // Iterate over the list of named KPIs for the current bearer
            Map<String,Object> kpis_list = (Map<String,Object>) bearer.get("kpis"); if (kpis_list == null) continue; /*if (kpis_list.isEmpty()) {

               if (log.isDebugEnabled()) log.debug("splitESRs: An empty list of kpis was found in ESR for imsi: \""+esr.get("imsi")+"\", timestamp: \""+esr.get("TIMESTAMP")+"\". Skipping it..."); continue; //return esrs;
            }*/

            for (Map.Entry<String,Object> kpis_entry : kpis_list.entrySet()) {

               if (kpis_entry == null) continue;

               // For each KPI name, iterate over the list of the corresponding KPI records
               String                   kpi_name = kpis_entry.getKey();
               List<Map<String,Object>> kpi_recs = (List<Map<String,Object>>) kpis_entry.getValue(); if (kpi_recs != null) for (Map<String,Object> kpi : kpi_recs) {

                  if (kpi == null) continue; /*if (kpi.isEmpty()) {

                     if (log.isDebugEnabled()) log.debug("An empty kpi record was found in ESR for imsi: \""+esr.get("imsi")+"\", timestamp: \""+esr.get("TIMESTAMP")+"\". Skipping it..."); continue; //return esrs;
                  }*/

                  // Skip all KPIs containing a null value
//                  Object val = null; if ((val = kpi.get("value")) == null) {
//
//                     continue;
//                  }

                  // Store the KPI
                  boolean hasKey = uniqueKpis.containsKey(kpi_name);
                  ArrayList<Map<String,Object>> kpisList = hasKey ? uniqueKpis.get(kpi_name)
                     :                                              new ArrayList<Map<String,Object>>(); {

                     if (!hasKey) uniqueKpis.put(kpi_name, kpisList);
                  }

                  kpisList.add(kpi); int esr_idx = kpisList.size() -1;

                  // Create the path to re-insert this KPI into the containing ESR
                  ArrayList<Object> kpi_path = new ArrayList<Object>(); {

                     kpi_path.add(esr_idx   );  // ESR
                     kpi_path.add("bearers" );  // "bearers" list
                     kpi_path.add(bearer_idx);  // Bearer
                     kpi_path.add("kpis"    );  // "kpis" list
                     kpi_path.add(0         );  // Single KPI rec per list
                     kpi_path.add(kpi_name  );  // KPI source/name
                  }
                  kpi.put("%PATH"  , kpi_path                              ); // store this KPI's path
                //kpi.put("%BEARER", cloneMap(bearer, new String[]{"kpis"})); // Store the bearer without the KPIs

                  // Create an ESR record (without KPIs) for each unique KPIs
                  if (esr_idx > max_esr_idx) max_esr_idx = esr_idx;

                  if (esrs.isEmpty() || max_esr_idx >= esrs.size()) {

                     esrs.add(cloneMap(esr_tmpl));
                  }
               }
            }
         }

         // Now traverse the list of unique KPIs, adding them to the ESRs, according to each KPI's recorded path
         for (Map.Entry<String,ArrayList<Map<String,Object>>> uke : uniqueKpis.entrySet()) {

            if (uke == null) continue;

          /*String uk_key = uke.getKey();*/ List<Map<String,Object>> uk_value = (List<Map<String,Object>>) uke.getValue();

           for (Map<String,Object> kpi : uk_value) {

               if (kpi == null) continue;

             //Map<String,Object> bearer = (Map<String,Object>) kpi.remove("%BEARER");
               List<Object>       path   = (List<Object>)       kpi.remove("%PATH"  );

               //bearer.put(uk_key, kpi); path.remove(path.size() -1);  // Manually add kpi to bearer, so remove bearer from path

               insertRec(path, esrs, kpi);
            }
         }
      }
      catch (Throwable t) {

         log.debug("splitESRs: Error splitting ESR: ", t);
         log.error("splitESRs: Cannot split ESR for imsi: \""+esr.get("imsi")+"\", timestamp: \""+esr.get("TIMESTAMP")+"\". Skipping it...");
      }

      // Finally, return the resulting list of ESRs
      return esrs;
   }


   @SuppressWarnings("unchecked")
   public static void insertRec(List<Object> path, Object rec, Object val) {

      if      (rec instanceof List) insertRec(path, (List<      Object>) rec, val);
      else if (rec instanceof Map ) insertRec(path, (Map<String,Object>) rec, val);
      else if (rec == null) {

         log.debug("insertRec: cannot insert data into null list."); return;
      }
      else {
         throw new RuntimeException("Error. Not all types are being considered.");
      }
   }

   public static void insertRec(List<Object> path, List<Object> rec, Object val) {

      // Extract index
      Integer idx = null; if (path.size() >0) {

         Object idx_obj = path.get(0); if (!(idx_obj instanceof Integer)) {

            throw new RuntimeException("Error. Path selector for Lists must be an Integer.");
         }

         idx = (Integer) idx_obj;
      }

      // If we are at the end of the path, add 'val' to the 'rec' list and return
      if (path.size() == 1) {

         rec.add(val); return;
      }

      // Otherwise, select indexed object and recurse using nested path
      ArrayList<Object> nestedPath = new ArrayList<Object>(Math.max(0, path.size() -1)); for (int i=1; i<path.size(); ++i) {

         nestedPath.add(path.get(i));
      }

      if (idx <0 || idx >= rec.size()) {

         log.info("Error. Cannot descend into object at position: "+idx+", rec: '"+rec+"'"); return; //throw new RuntimeException("Error. Index out of bounds.");
      }

      insertRec(nestedPath, rec.get(idx), val);
   }


   public static void insertRec(List<Object> path, Map<String,Object> rec, Object val) {

      // Extract key
      String key = null; if (path.size() >0) {

         Object key_obj = path.get(0); if (!(key_obj instanceof String)) {

            throw new RuntimeException("Error. Path selector for Maps must be a String.");
         }

         key = (String) key_obj;
      }

      // If we are at the end of the path, put 'val' into the 'rec' map and return
      if (path.size() == 1) {

         rec.put(key, val); return;
      }

      // Otherwise, select indexed object and recurse using nested path
      ArrayList<Object> nestedPath = new ArrayList<Object>(Math.max(0, path.size() -1)); for (int i=1; i<path.size(); ++i) {

         nestedPath.add(path.get(i));
      }

      insertRec(nestedPath, rec.get(key), val);
   }


   public Terminal getTerminal() {

      if (imeitac == null) return null;

      if (terminal == null) {

         REreference reference = REreference.getInstance();
         terminal              = reference.getTerminal(imeitac == null ? null : imeitac.longValue()
                 );
      }

      return terminal;
   }


   /**
    */
   public Object getCrm(String fieldName) {

      if (imsi == null) return null;

      if (crm == null) {

         REreference reference = REreference.getInstance();
         crm                   = reference.getCrm(imsi);
      }

      return (crm != null && !crm.isEmpty()) ? Event_ESR.getValueFromObject(crm, fieldName) : null;
   }


   public boolean containKpi(String source, String name) {

      boolean rc = false; if (kpiSetBySource != null) {

         rc = kpiSetBySource.contains(source +";"+ name);
      }
      else {

         Object o = getKpiRecord(source, name); rc = (o != null);
      }

      if (log.isDebugEnabled()) log.debug("Event_ESR(imsi='"+imsi+"', timestamp='"+timestamp+"').containKpi(source: '"+source+"', name: '"+name+"'): "+rc);

      return rc;
   }


   @SuppressWarnings("unchecked")
   public boolean containKpiInCell(String source, String name, String locId) {

      boolean rc = false; if (kpiSetBySourceAndlocId != null) {

         rc = kpiSetBySourceAndlocId.contains(source +";"+ name +";"+ locId);
      }
      else {

         Object o = getKpiRecord(source, name); rc = (o != null && ((Map<String,Object>) o).containsKey("locId"));
      }

      if (log.isDebugEnabled()) log.debug("Event_ESR(imsi='"+imsi+"').containKpiInCell(source: '"+source+"', name: '"+name+"'): "+rc);

      return rc;
   }


   /**
    * @return: true if site_data is defined for a CRM record, false otherwise
    */
   public boolean isCrmSite_data() {

      boolean res = (imsi != null && crm != null && !crm.isEmpty()) ? crm.isSite_data() : false;

      return res;
   }


   /**
    * @param key  CRM site_data key.
    * @return: key value as string.
    */
   public String getCrmSite_dataByKey(String key) {

      String res = (imsi != null && crm != null && !crm.isEmpty()) ? crm.getSite_dataByKey(key) : null;

      return res;
   }


   /**
    * @return Object getSite_dataByKey() : value from JSON string: site_data
    */
   public Object getCrmSite_dataByKey(String key, String dataType) {

      Object res = (imsi != null && crm != null && !crm.isEmpty()) ? crm.getSite_dataByKey(key, dataType) : null;

      return res;
   }


   /**
    * @param fieldName  Terminal field name
    * @return: null or Terminal column value.
    */
   public Object getTerminal(String fieldName) {

      if (imeitac == null) return null;

      if (terminal == null) {
         REreference reference = REreference.getInstance();
         terminal = reference.getTerminal(imeitac == null ? null : imeitac.longValue());
      }
      if (terminal != null && !terminal.isEmpty())
         return Event_ESR.getValueFromObject(terminal, fieldName);
      else
         return null;
   }


//   /**
//    * @return the number of Cells in an ESR
//    */
//   public int getN_cells() throws EsrFormatException, CastingException {
//
//      return getCells().size();
//   }


//   /**
//    * @return the maximum distance between any 2 cells in the ESR
//    */
//   public Double getMaxCellDistance() throws EsrFormatException, CastingException {
//
//      Double max = null;
//      if (getN_cells() > 1) {
//         Set<CellIdentifier> cellset = getCells();
//         for (CellIdentifier src : cellset) {
//            if (src != null) {
//               for (CellIdentifier dst : cellset) {
//                  if (dst != null && dst != src) {
//                     Double distance;
//                     if ((distance = getCellDistance(src, dst)) != null) {
//                        if (max == null || max < distance) {
//                           max = distance;
//                        }
//                     }
//                  }
//               }
//            }
//         }
//      }
//      return max;
//   }


//   /**
//    * @param s  source CellIdentifier
//    * @param d  destination CellIdentifier
//    * @return return the distance betwen s and d
//    */
//   public Double getCellDistance(CellIdentifier s, CellIdentifier d) {
//
//      REreference reference = REreference.getInstance();
//      Cell src = reference.getCell(s.getLocationId());
//      Cell dst = reference.getCell(d.getLocationId());
//
//      // check that we have all the proper values:
//      if (src == null || dst == null) return null;
//      if (src.getLat() == null || src.getLon() == null || dst.getLat() == null || dst.getLon() == null) return null;
//      if (src.getLat().isNaN() || src.getLon().isNaN() || dst.getLat().isNaN() || dst.getLon().isNaN()) return null;
//
//      int radius = 6371; // Km
//      Double slat_rad = src.getLat() * Math.PI / 180;
//      Double slon_rad = src.getLon() * Math.PI / 180;
//      Double elat_rad = dst.getLat() * Math.PI / 180;
//      Double elon_rad = dst.getLon() * Math.PI / 180;
//      Double dlat = slat_rad - elat_rad;
//      Double dlon = slon_rad - elon_rad;
//      Double dist = radius * Math.sqrt(dlat * dlat + dlon * dlon);
//      if (log.isDebugEnabled()) log.debug("Dist: " + dist + " src: " + src.getLat() + " " + src.getLon() + " dst: " + dst.getLat() + " " + dst.getLon());
//      return dist;
//   }


   /**
    * @param object  any Object which provide getter for fieldName
    * @param fieldName  field name
    * @return: field name value or null
    */
   public static Object getValueFromObject(Object object, String fieldName) {

      if (fieldName == null || fieldName.length() == 0) {

         log.error("Empty field Name"); return null;
      }

      // get class
      @SuppressWarnings("rawtypes")
      Class clazz = object != null ? object.getClass() : null; if (clazz == null) return null;

      // get method value using reflection
      String getterName = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);

      // if (log.isDebugEnabled()) log.debug("getter name =" + getterName);
      Object valueObject = null; try {

         @SuppressWarnings("unchecked")
         Method method      = clazz.getMethod(getterName);
         /**/   valueObject = method.invoke(object, (Object[]) null);
      }
      catch (Exception e) {

         if (log.isErrorEnabled()) log.error("error gettting  fieldname : " + fieldName, e);
      }

      return valueObject;
   }


//   /**
//    * @return true/false if there is MCC matches for the Operator.
//    */
//   public boolean isMCCmatches() {
//
//      try {
//         Integer esrMcc = esr.getMcc();
//         if (log.isDebugEnabled()) log.debug("isMCCmatches(" + imsi + "): MCC=" + esrMcc + ", operatorMCC=" + RuleEngine.operatorMCCList);
//         if (esrMcc == null) return false;
//
//         if (RuleEngine.operatorMCCList == null) return false;
//         for (String mcc : RuleEngine.operatorMCCList) {
//            if (mcc == null) continue;
//
//            if (esrMcc == Integer.parseInt(mcc.trim())) {
//               if (log.isDebugEnabled()) log.debug("isMCCmatches(" + imsi + ") returning true");
//               return true;
//            }
//         }
//      }
//      catch (CastingException e) {
//         if (log.isErrorEnabled()) log.error("CastingException: " + e.getMessage());
//      }
//
//      if (log.isDebugEnabled()) log.debug("isMCCmatches(" + imsi + ") returning false");
//      return false;
//   }


//   /**
//    * @return true if the MNC is defined in the Operator MNC list.
//    */
//   public boolean isMNCmatches() {
//
//      if (RuleEngine.operatorMNCList == null) return false;
//      try {
//         Integer esrMnc = esr.getMnc();
//         for (String mnc : RuleEngine.operatorMNCList) {
//            if (mnc == null) continue;
//            if (esrMnc == Integer.parseInt(mnc.trim())) {
//               if (log.isDebugEnabled()) log.debug("isMNCmatches(" + imsi + ") returning true");
//               return true;
//            }
//         }
//      }
//      catch (CastingException e) {
//         if (log.isErrorEnabled()) log.error("CastingException: " + e.getMessage());
//      }
//
//      if (log.isDebugEnabled()) log.debug("isMNCmatches(" + imsi + ") returning false");
//      return false;
//   }


//   public boolean isHomeIMSI() {
//
//      return (isMCCmatches() && isMNCmatches());
//   }


   public long getTimestamp_sec() {

      return (long) (timestamp / 1000);
   }


   public String getUsercategory() {

      // The user category is being mapped from the customer_type from the CRM
      if (imsi == null) return null;

      String res = null; if (crm != null && !crm.isEmpty()) {

         res = crm.getCustomer_type();
      }

      return res;
   }


   // To keep possibility to add some weight based on User category. This should come from CRM data.
   public int getUsercategorymultiplier() {

      return 1;
   }


   /** Fetch a KpiRecord (filter by source and name).*/
   @SuppressWarnings("unchecked")
   public static List<KpiRecord> getKpiRecords(Map<String,Object> esr, final String source, final String name) {

      final ArrayList<KpiRecord> kpiRecList = new ArrayList<KpiRecord>();

      MapHunter mh = new MapHunter(new MapHunter.Processor() {

         @Override
         public void processOneValue(Object vals, List<Node> ancestors) {

            if (vals == null || !(vals instanceof Map)) return; try {

               String                      rat        = null, locationId = null, cellName = null;
               Integer                     bearerIdx  = null;
               Double                      value      = null, weight     = null;
               MatchableMap<String,String> dimensions = new MatchableMap<String,String>();

               Map<String, Object> valsMap = (Map<String, Object>) vals; for (Map.Entry<String,Object> e : valsMap.entrySet()) { if (e == null) continue;

               String k = e.getKey(); Object v = e.getValue();

                  if      ("rat"                .equals(k)) rat        = ( String) v;
                  else if ("bid"                .equals(k)) bearerIdx  = (Integer) v;
                  else if ("value"              .equals(k)) value      = v == null ? null : Double.parseDouble(v.toString());
                  else if ("weight"             .equals(k)) weight     = v == null ? null : Double.parseDouble(v.toString());
                  else if ("cell_name"          .equals(k)) cellName   = ( String) v;
                  else if ("location_identifier".equals(k)) locationId = ( String) v;
                  else if ("dimensions"         .equals(k)) dimensions = new MatchableMap<String,String>((Map<String,String>) v);
               }
               Object o = null; if (rat == null && (o=valsMap.get("cell_name_rat")) != null && o instanceof String) {

                  final String[] l = ((String) o).split("_"); if (l.length >1) rat = l[l.length -1];
               }

               // bearerIdx == null ? 0 : bearerIdx; value = value == null ? 0.0 : value; weight = weight == null ? 0.0;  // TODO: do we default or throw away the bad record? Throwing it away for now...
               if (value != null && weight != null) {

                  Bearer         bearer = new Bearer(bearerIdx);
                  CellIdentifier cell   = new CellIdentifier(rat, locationId, cellName);
                  KpiRecord      rec    = new KpiRecord(cell, bearer, dimensions, value, weight);

                  if (rec != null && log.isTraceEnabled()) log.trace("getKpiRecord(source: '"+source+"', name: '"+name+"'): "+rec);

                  kpiRecList.add(rec);
               }
            }
            catch (Exception ex) {

               log.error("Error collecting KPIs: ", ex);/*ignore it*/
            }
         }
      });

      mh.hunt(esr, "bearers.kpis."+source+"_"+name);

      return kpiRecList;
   }

   /** Fetch a KpiRecord (filter by source and name).*/
   public KpiRecord getKpiRecord(final String source, final String name) {

      String key = source+"/"+name; if (memo.containsKey(key)) return (KpiRecord) memo.get(key);

      KpiRecord rec = getKpiRecord(esr, source, name);

      memo.put(key, rec); return rec;
   }


   /** Fetch a KpiRecord (filter by source and name).*/
   public static KpiRecord getKpiRecord(Map<String,Object> esr, final String source, final String name) {

      KpiRecord       rec     = null;
      List<KpiRecord> recList = getKpiRecords(esr, source, name); if (recList != null && recList.size() >0) {

         if (recList.size() >1) {

            //throw new RuntimeException("Error. Found duplicate KPIs: "+recList);
            log.error("Error. Found "+recList.size()+" duplicate(s) KPIs in ESR:"); int i=0; for (KpiRecord r : recList) {

               if (log.isDebugEnabled()) log.debug("getKpiRecord: idx: "+(i++)+", KPI name: '"+source+"_"+name+"': {"+r+"}");
            }
         }
         else {

            if (log.isDebugEnabled()) log.debug("getKpiRecord: KPI name: '"+source+"_"+name+"': {"+recList.get(0)+"}");
         }

         rec = recList.get(0);
      }

      return rec;
   }


//   /** Fetch a KpiRecord (filter by source and name).*/
//   public static KpiRecord getKpiRecord(Map<String,Object> esr, final String source, final String name) {
//
//      Map<String,Object> vals = (Map<String,Object>) Util.get(esr, ("bearers.*.kpis."+source+"_"+name+".0").split("\\.")); if (vals != null) {
//
//         String                      rat        = null, locationId = null, cellName = null;
//         Integer                     bearerIdx  = null;
//         Double                      value      = null, weight     = null;
//         MatchableMap<String,String> dimensions = null;
//
//         for (Map.Entry<String,Object> e : vals.entrySet()) { if (e == null) continue;
//
//            String k = e.getKey(); Object v = e.getValue();
//
//            if      ("rat"                .equals(k)) rat        = ( String) v;
//            else if ("bid"                .equals(k)) bearerIdx  = (Integer) v;
//            else if ("value"              .equals(k)) value      = v == null ? null : Double.parseDouble(v.toString());
//            else if ("weight"             .equals(k)) weight     = v == null ? null : Double.parseDouble(v.toString());
//            else if ("cell_name"          .equals(k)) cellName   = ( String) v;
//            else if ("location_identifier".equals(k)) locationId = ( String) v;
//            else if ("dimensions"         .equals(k)) dimensions = new MatchableMap<String,String>((Map<String,String>) v);
//         }
//
//         // TODO: do we default or throw away the bad record? Throwing it away for now...
//         // bearerIdx == null ? 0 : bearerIdx; value = value == null ? 0.0 : value; weight = weight == null ? 0.0;
//
//         if (value != null && weight != null) {
//
//            Bearer         bearer = new Bearer(bearerIdx);
//            CellIdentifier cell   = new CellIdentifier(rat, locationId, cellName);
//            rec                   = new KpiRecord(cell, bearer, dimensions, value, weight);
//         }
//      }
//
//      if (rec != null && log.isDebugEnabled()) log.debug("getKpiRecord(source: '"+source+"', name: '"+name+"'): "+rec);
//
//      return rec;
//   }


//   /** Fetch a KpiRecord (filter by source & name and cellId).*/
//   public List<KpiRecord> getKpiRecordValue(String source, String name, CellIdentifier cellId) {
//
//     List<KpiRecord> matchingKpiRecList = new ArrayList<KpiRecord>();
//     List<KpiRecord> recList = getKpiRecord(source, name);
//     if (recList != null) {
//
//        for(KpiRecord rec: recList) {
//
//            CellIdentifier cid = rec.getCellIdentifier();
//            if (cid != null && cid.equals(cellId)) {
//
//               matchingKpiRecList.add(rec);
//            }
//         }
//      }
//
//      return matchingKpiRecList;
//   }

   /** Fetch a KpiRecord (filter by source, name and cellId).*/
   public KpiRecord getKpiRecordValue(String source, String name, CellIdentifier cellId) {

      KpiRecord rec = getKpiRecord(source, name); if (rec != null) {

         CellIdentifier cid = rec.getCellIdentifier(); if (cid != null && cid.equals(cellId)) {

            return rec;
         }
      }

      return null;
   }


   /**
    * @return Aggregate KpiRecord (filter by source and name).
    */
   public KpiRecord aggregateKpiRecord(String source
      ,                                String name) throws CastingException
      ,                                                    EsrFormatException {

      KpiRecord r  = (esr instanceof ESR) ? ((ESR) esr).aggregateKpiRecord(new KpiDesc(source, name))
         :                                 null; //Util.aggregateKpiRecord(esr, new KpiDesc(source, name));  //TODO
       //: KpiParserLogic.extractAggregatedKpi(esr, new KpiDescCollection(new KpiDesc(source, name)), null, null, KpiRecord.class).get(new KpiDesc(source, name));

      if (log.isDebugEnabled()) log.debug("aggregateKpiRecord(source: '"+source+"', name: '"+name+"'): "+r);

      return r;
   }


   /**
    * @return Aggregate KpiRecord (filter by source, name, and cellId).
    */
   public KpiRecord aggregateKpiRecord(String         source
      ,                                String         name
      ,                                CellIdentifier cellId) throws CastingException
      ,                                                              EsrFormatException {

      KpiRecord pattern = new KpiRecord(cellId, null, null, null);
      KpiRecord r       = (esr instanceof ESR) ? ((ESR) esr).aggregateKpiRecord(     new KpiDesc(source, name), pattern)
         :                                      null; //Util.aggregateKpiRecord(esr, new KpiDesc(source, name), pattern);  //TODO

      return r;
   }


   /**
    * @return Aggregate KpiRecord (filter by source, name, cellId, and dimensions).
    */
   public KpiRecord aggregateKpiRecord(String         source
      ,                                String         name
      ,                                CellIdentifier cellId
      ,                                String         dimensions) throws CastingException
      ,                                                                  EsrFormatException {

      MatchableMap<String, String> m = new MatchableMap<String, String>();

      String list1[] = dimensions.split(","); for (String pair : list1) {

         String list2[] = pair.split(":"); if (list2.length != 2) continue;

         String key   = list2[0].trim();
         String value = list2[1].trim();

         m.put(key, value);
      }

      KpiRecord pattern = new KpiRecord(cellId, null, m, null);
      KpiRecord r       = (esr instanceof ESR) ? ((ESR) esr).aggregateKpiRecord(     new KpiDesc(source, name), pattern)
         :                                      null; //Util.aggregateKpiRecord(esr, new KpiDesc(source, name), pattern);  //TODO

      return r;
   }


   /**
    * @param dimensions  a set string values of the format "key:value,key:value....."
    * @return aggregate  KpiRecord for source,name,dimensions
    */
   public KpiRecord aggregateKpiRecord(String source
      ,                                String name
      ,                                String dimensions) throws CastingException
      ,                                                          EsrFormatException {

      MatchableMap<String, String> m = new MatchableMap<String, String>();

      String list1[] = dimensions.split(","); for (String pair : list1) {

         String list2[] = pair.split(":"); if (list2.length != 2) continue;

         String key   = list2[0].trim();
         String value = list2[1].trim();

         m.put(key, value);
      }

      KpiRecord pattern = new KpiRecord(null, null, m, null);
      KpiRecord r       = (esr instanceof ESR) ? ((ESR) esr).aggregateKpiRecord(     new KpiDesc(source, name), pattern)
         :                                      null; //Util.aggregateKpiRecord(esr, new KpiDesc(source, name), pattern);  //TODO

      if (log.isDebugEnabled()) log.debug("aggregateKpiRecord(source: '"+source+"', name: '"+name+"', dimentions: '"+dimensions+"'): "+r);

      return r;
   }


   /**
    * @return aggregate KpiRecord for source,name,dimensions
    */
   public KpiRecord aggregateKpiRecord(String             source
      ,                                String             name
      ,                                Map<String,String> dimensions) throws CastingException
      ,                                                                      EsrFormatException {

      MatchableMap<String, String> m = new MatchableMap<String, String>();

      KpiRecord pattern = new KpiRecord(null, null, m, null);
      KpiRecord r       = (esr instanceof ESR) ? ((ESR) esr).aggregateKpiRecord(     new KpiDesc(source, name), pattern)
         :                                      null; //Util.aggregateKpiRecord(esr, new KpiDesc(source, name), pattern);  //TODO

      return r;
   }


   /**
    * @return EventTypeKpiValue which returns Double values which support null. this a convenience for rule writing.
    */
   public EventTypeKpiValue aggregateKpiRecordValue(String         source
      ,                                             String         name
      ,                                             CellIdentifier cellId) throws CastingException
      ,                                                                           EsrFormatException {

      EventTypeKpiValue v       = new EventTypeKpiValue();
      KpiRecord         pattern = new KpiRecord(cellId, null, null, null);
      KpiRecord         r       = (esr instanceof ESR) ? ((ESR) esr).aggregateKpiRecord(     new KpiDesc(source, name), pattern)
         :                                               null /*Util.aggregateKpiRecord(esr, new KpiDesc(source, name), pattern)*/; if (r == null) return v;  //TODO

      v.setValue (r.getValue());
      v.setWeight(r.getWeight());

      return v;
   }


   /**
    * @return Aggregate KpiValue for source, name. Return value could be null.
    */
   public KpiValue aggregateKPI(String source
       ,                        String name) throws CastingException
       ,                                            EsrFormatException {

      KpiValue v =  (esr instanceof ESR) ? ((ESR) esr).aggregateKpi(     new KpiDesc(source, name))
         :                                 null /*Util.aggregateKpi(esr, new KpiDesc(source, name))*/;  if (v == null) {  //TODO

         v = new KpiValue(0, 0);
      }

      if (log.isDebugEnabled()) log.debug("aggregateKPI(source: '"+source+"', name: '"+name+"'): "+v);

      return v;
   }


   /**
    * @return MIN aggregated KpiRecord filtered by source, name cell
    */
   public KpiRecord aggregateKpiRecordMin(String         source
      ,                                   String         name
      ,                                   CellIdentifier cell) throws CastingException
      ,                                                               EsrFormatException {

      KpiRecord pattern = new KpiRecord(cell, null, null, null);
      KpiDesc   kd      = new KpiDesc(source, name, MIN);

      return (esr instanceof ESR) ? ((ESR) esr).aggregateKpiRecord(     kd, pattern)
         :                         null; //Util.aggregateKpiRecord(esr, kd, pattern);  //TODO
   }


   /**
    * @return Max aggregated KpiRecord filtered by source, name cell
    */
   public KpiRecord aggregateKpiRecorddMax(String         source
      ,                                    String         name
      ,                                    CellIdentifier cell) throws CastingException
      ,                                                                EsrFormatException {

      KpiRecord pattern = new KpiRecord(cell, null, null, null);
      KpiDesc   kd      = new KpiDesc(source, name, MAX);

      return (esr instanceof ESR) ? ((ESR) esr).aggregateKpiRecord(     kd, pattern)
         :                         null; //Uni/.aggregateKpiRecord(esr, kd, pattern);  //TODO
   }


   /**
    * @return aggregated KPI for single dimension pair ( key, value)
    */
   public KpiRecord aggregateKpiRecord(String source
      ,                                String name
      ,                                String dimkey
      ,                                String dimvalue) throws CastingException
      ,                                                        EsrFormatException {

      HashMap<String, String> dim = new HashMap<String, String>(); {

         dim.put(dimkey, dimvalue); // "functionality","IM"
      }

      KpiRecord pattern = new KpiRecord(null, null, dim, null);
      KpiDesc   kd      = new KpiDesc(source, name);

      KpiRecord r =  (esr instanceof ESR) ? ((ESR) esr).aggregateKpiRecord(     kd, pattern)
         :                                 null; //Util.aggregateKpiRecord(esr, kd, pattern);  //TODO

      if (log.isDebugEnabled()) log.debug("aggregateKPI(source: '"+source+"', name: '"+name+"'): "+r);

      return r;
   }


//   public String getApn() throws CastingException {
//
//      Collection<Bearer> bearers = getBearers();
//      SortedMap<String, Integer> apnMap = new TreeMap<String, Integer>();
//      for (Bearer bearer : bearers) {
//         String apn = (String) bearer.get(Constants.APN);
//         if (apn != null) {
//            int value = 0;
//            if (apnMap.containsKey(apn)) {
//               value = apnMap.get(apn);
//            }
//            apnMap.put(apn, value + 1);
//         }
//      }
//      if (apnMap.size() > 0) {
//         if (apnMap.lastKey() != null) { return apnMap.lastKey(); }
//      }
//      return null;
//   }


   /**
    * @return all triggers
    */
   public EsrTriggers getEsrTriggers() {

      return getEsrTriggers(null, null);
   }


   /**
    * @return all triggers for that location
    */
   public EsrTriggers getEsrTriggers(String type, String location_id) {

      Map<String,TriggerList> t = getTriggersByType();
      EsrTriggers             l = new EsrTriggers(); if (t == null) return l;

      if (type != null && type.isEmpty()) return getEsrTriggersForType(type, location_id, t);

      for (TriggerList tl : t.values()) {

         for (Trigger trig : tl) {

            if (location_id != null && trig.getCellIdentifier() != null && !location_id.equals(trig.getCellIdentifier().getLocationId())) continue;

            l.add(trig);
         }
      }

      return l;
   }


   public Map<String, TriggerList> getTriggersByType() {

      if (triggersByType == null) {

         try {

            triggersByType = (esr instanceof ESR) ? ((ESR) esr).getTriggers()
               :                                   null; //Util.getTriggers(esr);  //TODO
         }
         catch (CastingException e) {

            triggersByType = new HashMap<String, TriggerList>();

            if (log.isErrorEnabled()) log.error("CastingException: " + e.getMessage());
         }
      }

      return triggersByType;
   }


//   /**
//    * Gets all triggers from the ESR in a Map, where the key is the trigger type and value is a TriggerList.
//    * @return triggers
//    * @throws CastingException
//    */
//   public static Map<String,TriggerList> getTriggers(Map<String,Object> esr) throws CastingException {
//
//      return (esr instanceof ESR) ? extractTriggers(((ESR) esr), null, cellMapper)
//         :             null; //Util.extractTriggers(       esr , null, cellMapper);  //TODO
//   }

   /**
    * @return a list of Triggers that match the type and location id.
    */
   public EsrTriggers getEsrTriggersForType(String type, String location_id, Map<String, TriggerList> t) {

      EsrTriggers l  = new EsrTriggers();
      TriggerList tl = t.get((String) type); if (tl == null) return l;

      for (Trigger trig : tl) {

         if (type != null && !type.equals(trig.getType())) continue;

         if (location_id != null && trig.getCellIdentifier() != null && !location_id.equals(trig.getCellIdentifier().getLocationId())) continue;

         l.add(trig);
      }

      return l;
   }


//   public static ESRset getEsrSet(List<Event_ESR> evs) {
//
//      ESRset set = null; for (Event_ESR ev : evs) if (ev != null && ev.getEsr() != null) {
//
//         if (set == null) set = new ESRset();
//
//         set.add(ev.getEsr());
//      }
//
//      return set;
//   }


   public static <T> T getRuleParameter(String ruleId, String parameterKey, Class<T> type) {

      return Util.getRuleParameter(ruleId, parameterKey, type);
   }


   /**
    * @param dimensionKey: First argument is dimension type, list of values to be checked for dimension
    * @return true/false. True if at least one of the incoming values matches the value of the dimension KPI.
    */
   public boolean checkDimensionValues(String dimensionKey, String dimensionValues) {

      if (dimensionKey == null || dimensionValues == null) {

         log.error("checkDimensionValues : invalid args" ); return false;
      }

//      // parse dimension types
//      boolean      rc    = false;
//      List<String> items = Arrays.asList(dimensionValues.split("\\s*,\\s*")); for (String inDimensionVal : items) {
//
//         if (inDimensionVal == null || inDimensionVal.isEmpty()) continue;
//
//         if (this.getOttRec() != null
//               &&  this.getOttRec().getDimensions() != null
//               &&  this.getOttRec().getDimensions().get(dimensionKey) != null
//               &&  this.getOttRec().getDimensions().get(dimensionKey).trim().equals(inDimensionVal.trim())) {
//
//            rc = true; break;
//         }
//      }
//
//      return rc;
      return true;
   }
}
