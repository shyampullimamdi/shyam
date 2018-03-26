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

package com.ericsson.eea.ark.offline.rules.incidents;

import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.Setter;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import java.text.DateFormat;
//import java.text.SimpleDateFormat;


import net.minidev.json.JSONValue;
import net.minidev.json.JSONObject;

//import java.io.File;
//import java.io.IOException;
//import java.io.PrintStream;
//import java.io.FileOutputStream;
//import java.io.FileNotFoundException;


//import java.util.Date;
import java.util.List;
//import java.util.ArrayList;
import java.util.concurrent.PriorityBlockingQueue;

import org.drools.runtime.StatefulKnowledgeSession;

//import java.security.InvalidKeyException;
//import java.security.InvalidAlgorithmParameterException;


import com.ericsson.bigdata.esr.dataexchange.Trigger;
import com.ericsson.bigdata.esr.dataexchange.KpiRecord;
import com.ericsson.bigdata.esr.dataexchange.TriggerList;
import com.ericsson.bigdata.common.identifier.CellIdentifier;
import com.ericsson.eea.ark.offline.rules.QueueItem;
import com.ericsson.eea.ark.offline.rules.events.Event_ESR;
import com.ericsson.eea.ark.offline.rules.events.Event_Timer;
import com.ericsson.eea.ark.offline.rules.refdata.CRM;
import com.ericsson.eea.ark.offline.rules.refdata.Cell;
import com.ericsson.eea.ark.offline.rules.refdata.KpiMetaDataRecord;
import com.ericsson.eea.ark.offline.rules.refdata.REreference;
import com.ericsson.eea.ark.offline.rules.refdata.RuleDesc;
import com.ericsson.eea.ark.offline.rules.refdata.RuleIdParameter;
import com.ericsson.eea.ark.offline.rules.refdata.RuleMetaData;
//import com.ericsson.eea.ark.offline.rules.RuleEngine;

//import com.ericsson.eea.ark.offline.rules.events.Event_Trigger;




public class SubscriberIncidentReport {

   private static final Logger log = LoggerFactory.getLogger(SubscriberIncidentReport.class.getName());

   /** Denotes ESR processing.*/ public static final int originESR = 0;
   /** Denotes  TA processing.*/ public static final int originTA  = 1;
   /** Denotes NPS processing.*/ public static final int originNPS = 2;
   /** Denotes OTT processing.*/ public static final int originOTT = 3;
   /** Denotes SLI processing.*/ public static final int originSLI = 4;

   /** to indicate that the incident must be correlated with network incidents by ORE.*/
   public static final Integer CORRELATION_ID           = 1000;

   /** to indicate that the incident will be used to generate new subscriber incidents.*/
   public static final Integer CORRELATION_COUNT        = 1001;

   /** to indicate that the incident must be correlated with network incidents by ORE and used to generate additional user subscriber incidents.*/
   public static final Integer CORRELATION_ID_AND_COUNT = 1002;

   private List<String>        npsSources               = null;

   @Getter @Setter protected int                              segmentId = -1;
   @Getter @Setter protected PriorityBlockingQueue<QueueItem> incident_queue;
   @Getter @Setter protected StatefulKnowledgeSession         session;


   public SubscriberIncidentReport(int                              segmentId
      ,                            StatefulKnowledgeSession         session) throws SQLException
      ,                                                                             ClassNotFoundException {

      this(segmentId, session, null);
   }


   public SubscriberIncidentReport(int                              segmentId
      ,                            StatefulKnowledgeSession         session
      ,                            PriorityBlockingQueue<QueueItem> incident_queue) throws SQLException
      ,                                                                             ClassNotFoundException {

      super();
      this.segmentId      = segmentId;
      this.incident_queue = incident_queue;
      this.session        = session;
      this.npsSources     = null;

//      // get list of NPS sources.
//      String s = RuleEngine.config.netPromoterScoreSources(); if (s != null && !s.isEmpty()) {
//
//         String sources[] = s.split(",");
//         this.npsSources = new ArrayList<String>(); for (String item : sources) {
//
//            npsSources.add(item.replaceAll("\\s+", ""));
//         }
//      }
   }


   public void log(String message) {

      System.out.println(message); log.info(message);
   }


   public void report(SubscriberIncident iRpt, Object event) {

      if (event == null || iRpt == null || iRpt.getRuleid() == null || iRpt.getRuleid().isEmpty()) {

         if (iRpt.getRuleid() != null || !iRpt.getRuleid().isEmpty())
            log.error("an event was not specified for rule Id: " + iRpt.getRuleid());
         else
            log.error("an event and a rule id must be specified");

         return;
      }

      RuleDesc ruleDesc = REreference.getInstance().getRuleDesc(iRpt.getRuleid()); if (ruleDesc == null || ruleDesc.isEmpty()) {

         log.error("unknown rule_id " + iRpt.getRuleid()
            + " incident will not be reported, use fully qualified report in rule for this rule Id");

//         NGEEAlarmHelper.logAlarm(NGEEAlarmHelper.CONFIGURATION_ERROR, "unknown rule_id " + iRpt.getRuleid()
//            + " incident will not be reported, use fully qualified report in rule for this rule Id");

         return;
      }

      if      (iRpt.getSource() == originESR || iRpt.getSource() == originOTT) reportEsr(iRpt, event, ruleDesc);
    //else if (iRpt.getSource() == originTA  || iRpt.getSource() == originNPS) reportTA (iRpt, event, ruleDesc);
      else {

         log.warn("Unknown report type, ignoring: " + iRpt.getSource());
         return;
      }
   }


   public void reportEsr(SubscriberIncident iRpt, Object event, RuleDesc ruleDesc) {

      if (event == null) {

         log.error("null event"); return;
      }

      SubscriberIncident oRpt      = new SubscriberIncident(iRpt);
      REreference        reference = REreference.getInstance();

      if (iRpt.getRuleid() == null || iRpt.getRuleid().isEmpty()) {

         log.error("rule Id was not specified."); return;
      }

      Event_ESR      esr           = null;
    //Event_Trigger  trigger       = null;
    //Event_OttKpi   ottKpi        = null;
      CRM            crm           = null;
      CellIdentifier cellID        = null;
      Integer        blocking_secs = 0;

      // Get ESR from event.
      if      (event instanceof Event_ESR    ) { esr     = (Event_ESR    ) event; oRpt.setTs_milli(esr    .getTimestamp());                         }
    //else if (event instanceof Event_Trigger) { trigger = (Event_Trigger) event; oRpt.setTs_milli(trigger.getTimestamp()); esr = trigger.getEsr(); }
    //else if (event instanceof Event_OttKpi ) { ottKpi  = (Event_OttKpi ) event; oRpt.setTs_milli(ottKpi .getTimestamp()); esr = ottKpi .getEsr(); }
      else {

         Class<? extends Object> cls = event.getClass();
         log.error("unExpected event Type : " + cls.getName());
         return;
      }

//      if (esr == null) {
//
//         log.error("missing event esr event");
//         return;
//      }

      // Default Location from esr.
      if (oRpt.getLocation() == null) oRpt.setLocation(esr);

      try {

         // Get Reporting fields from the esr.
         oRpt.setImsi        (esr.getImsi()                    );
         oRpt.setMsisdn      (esr.get(String .class, "msisdn" ));
         oRpt.setImeitac     (esr.get(Integer.class, "imeitac"));
         oRpt.setUsercategory(esr.getUsercategory()            );
         oRpt.setHidden      (esr.getHide()                    );

         // Encrypt the esr content in the report for further processing i.e ORE
         if (oRpt.getIncludeData() && esr != null) oRpt.setDataBlob(esr.getEsr());

         // Get CRM reference from event
         crm = esr.getCrm();
         if (crm == null) crm = reference.getCrm(esr.getImsi());

         if (crm != null) oRpt.setIncident_published(this.getPublished(crm, oRpt.getRuleid()));

         // Set fields from rule_desc table when there are not explicitly specified.
         setFieldsFromRuleDesc(ruleDesc, oRpt);

         // Set Location derived information
         if (oRpt.getLocation() != null) {

            // Extract Location information from location object received.
            if (oRpt.getLocation() instanceof KpiRecord) {

               KpiRecord kpi = (KpiRecord) oRpt.getLocation();
               if (kpi.getBearer() != null && kpi.getBearer().get("apn") != null) {

                  oRpt.setApn((String) kpi.getBearer().get("apn"));
                  oRpt.setRat(kpi.getRat());
               }
               if (oRpt.getOtherDims() == null || oRpt.getOtherDims().isEmpty()) {

                  // overwrite OtherDimesnions.
                  oRpt.getOtherDims().putAll(kpi.getDimensions());
               }
               cellID = kpi.getCellIdentifier();
            }
//            else if (oRpt.getLocation() instanceof OttKpiRecord) {
//               OttKpiRecord kpi = (OttKpiRecord) oRpt.getLocation();
//               if (kpi.getBearer() != null) {
//                  if (kpi.getBearer().get("apn") != null) {
//                     oRpt.setApn((String) kpi.getBearer().get("apn"));
//                     oRpt.setRat(kpi.getRat());
//                  }
//               }
//               if (oRpt.getOtherDims() == null || oRpt.getOtherDims().isEmpty()) {
//                  // overwrite OtherDimesnions.
//                  oRpt.getOtherDims().putAll(kpi.getDimensions());
//               }
//
//               cellID = kpi.getCellIdentifier();
//            }
            else if (oRpt.getLocation() instanceof TriggerList) {

               oRpt.setApn((String) ((TriggerList) oRpt.getLocation()).getMostFrequentValue("apn"));
               cellID = ((TriggerList) oRpt.getLocation()).getMostFrequentLocation();
            }
            else if (oRpt.getLocation() instanceof Event_ESR) {

             //oRpt.setApn(((Event_ESR) oRpt.getLocation()).getApn()); //TODO
               cellID = getCellIdentifier((Event_ESR) oRpt.getLocation());
            }
            else if (oRpt.getLocation() instanceof CellIdentifier) {

               cellID = (CellIdentifier) oRpt.getLocation();
            }
            else if (oRpt.getLocation() instanceof Trigger) {

               cellID = ((Trigger) oRpt.getLocation()).getCellIdentifier();
               oRpt.setSgsn((String) ((Trigger) oRpt.getLocation()).get("node"));
               oRpt.setApn ((String) ((Trigger) oRpt.getLocation()).get("apn"));
            }
//            else if (oRpt.getLocation() instanceof Event_Trigger) {
//
//               cellID = ((Event_Trigger) oRpt.getLocation()).getCellIdentifier();
//               oRpt.setApn(((Event_Trigger) oRpt.getLocation()).getApn());
//               oRpt.setSgsn((String) ((Event_Trigger) oRpt.getLocation()).get("node"));
//            }
            else {

               log.error("missing location information, unexpected object type received");
               @SuppressWarnings("rawtypes")
               Class cls = oRpt.getLocation().getClass();
               log.error("The type of the object is: " + cls.getName());
            }

            if (cellID != null) {

               Cell c = this.getCell(cellID.getLocationId()); if (c != null) {

                  oRpt.setCellName(c.getCell_name());
                  oRpt.setRat     (c.getRat()      );
               }
            }
         }

         // Set Meta data
         setMetaData(oRpt);

         // get blocking seconds
         if (oRpt.getBlockingcat() != null) {

            blocking_secs = SubscriberIncidentReport.getSeconds(oRpt.getBlockingcat());
         }

      }
      catch (Exception e) {

         log.error("report exception: " + e.getMessage(), e);
      }

      // Set PartionId . This field is now being used to give pass information to ORE
      oRpt.setPartionId(this.getSegmentId());

      // Perform special check for feedback
      if (oRpt.getFeedback()) {

         oRpt.setHidden(true);
         oRpt.setPartionId(SubscriberIncidentReport.CORRELATION_COUNT);
      }

      oRpt.setImsi(oRpt.getImsi());

      // If blocking required insert a TimerEvent to the knowledge base
      if (blocking_secs > 0) {

         Event_Timer event_timer = new Event_Timer((String) oRpt.getBlockingcat()
            ,                                      oRpt.getRuleid()
            ,                                      oRpt.getImsi()
            ,                                      cellID
            ,                                      oRpt.getTs_milli()
            ,                                      getJsonReportDimension(oRpt.getReportDims()));
         this.getSession().insert(event_timer);
      }

      // Finally write Report to Q.
      System.out.println("Adding CSV to incident queueth: '"+oRpt.getCopyMgrRecord()+"'");
    //getIncident_queue().add(new QueueItem(oRpt.getTs_milli(), (Object) oRpt.getCopyMgrRecord()));
   }


   private CellIdentifier getCellIdentifier(Event_ESR esr) {

      String rat = esr.get(String.class, "rat");
      String loc = esr.get(String.class, "location_identifier");

      CellIdentifier cid = new CellIdentifier(rat, loc);

      return cid;
   }


//   public void reportTA(SubscriberIncident iRpt, Object event, RuleDesc ruleDesc) {
//
//      SubscriberIncident oRpt = new SubscriberIncident(iRpt);
//      Event_TA ta = null;
//      if (event == null) {
//         log.error("null event");
//         return;
//      }
//      if (event instanceof Event_TA) {
//         ta = (Event_TA) event;
//      }
//      else {
//         Class<? extends Object> cls = event.getClass();
//         log.error("unExpected event Type : " + cls.getName());
//         return;
//      }
//
//      try {
//         CRM crm = ta.getCrm();
//
//         // Set Location information
//         Cell cellToUse = null;
//         CellIdentifier cellID = null;
//         if (oRpt.getReportingCell().equalsIgnoreCase("start")) {
//            cellToUse = ta.getStartCell();
//         }
//         else if (oRpt.getReportingCell().equalsIgnoreCase("end")) {
//            cellToUse = ta.getEndCell();
//         }
//         else if (oRpt.getReportingCell().equalsIgnoreCase("either")) {
//            cellToUse = ta.getStartCell();
//            if (cellToUse == null || cellToUse.getCell_name() == null) {
//               // try to get from the end.
//               cellToUse = ta.getEndCell();
//            }
//         }
//         if (cellToUse != null && cellToUse.getCell_name() != null && !cellToUse.getCell_name().isEmpty()) {
//            oRpt.setCellName(cellToUse.getCellname());
//            oRpt.setRat(cellToUse.getRat());
//            cellID = new CellIdentifier(cellToUse.getRat(), cellToUse.getLocation_identifier(), cellToUse.getCell_name());
//         }
//
//         // Get fields from event
//         oRpt.setTs_milli(ta.getTimestamp());
//         oRpt.setImsi(ta.getImsi());
//         oRpt.setMsisdn(ta.getMsisdn());
//         oRpt.setImeitac(ta.getImeitac());
//         oRpt.setUsercategory(ta.getUsercategory());
//         oRpt.setHidden(ta.getHide());
//         // Include the content in the report for further processing i.e
//         // ORE
//         if (oRpt.getIncludeData()) oRpt.setDataBlob(ta.toString());
//
//         // Set fields from rule_desc table when there are not explicitly
//         // specified.
//         setFieldsFromRuleDesc(ruleDesc, oRpt);
//
//         // Set Meta data
//         setMetaData(oRpt);
//
//         // Blocking time.
//         Integer blocking_secs = 0;
//         if (oRpt.getBlockingcat() != null) {
//            blocking_secs = SubscriberIncidentReport.getSeconds(oRpt.getBlockingcat());
//         }
//
//         // Set Partion Id for processing by ORE.
//         if (oRpt.getCorrelate()) {
//            oRpt.setPartionId(SubscriberIncidentReport.CORRELATION_ID);
//            oRpt.setHidden(true); // hide until correlation.
//         }
//         else {
//            if (ta != null && ta.getName() != null) {
//               String sourceName = ta.getName();
//               if (RuleEngine.taMapSourceToSegment != null && RuleEngine.taMapSourceToSegment.containsKey(sourceName))
//                  oRpt.setPartionId(RuleEngine.taMapSourceToSegment.get(sourceName));
//            }
//         }
//
//         // Perform special check for feedback
//         if (oRpt.getFeedback()) {
//            oRpt.setHidden(true);
//            if (oRpt.getPartionId().equals(SubscriberIncidentReport.CORRELATION_ID))
//               oRpt.setPartionId(SubscriberIncidentReport.CORRELATION_ID_AND_COUNT);
//            else
//               oRpt.setPartionId(SubscriberIncidentReport.CORRELATION_COUNT);
//         }
//
//         Long ts = oRpt.getTs_milli(); // preserve the Event insertion time.
//         // Record the time stamp as the time in the record.
//         if (ta.get("recordTimeStamp", Long.class) != null) {
//            // Use the record Time as the time of the incident.
//            oRpt.setTs_milli(ta.get("recordTimeStamp", Long.class));
//         }
//
//         // Set Incident Published.
//         if (crm != null) {
//            oRpt.setIncident_published(this.getPublished(crm, oRpt.getRuleid()));
//         }
//
//         // Redefine report source.
//         if (isNet_promoter_score(ta.getName())) oRpt.setSource(SubscriberIncidentReport.originNPS);
//
//         // Redefine IMSI . silly encryption thing!!!!!
//         oRpt.setImsi(IMSIhide.encrImsi(oRpt.getImsi(), oRpt.getHidden()));
//
//         if (blocking_secs > 0) {
//            Event_Timer event_timer = new Event_Timer(oRpt.getBlockingcat(), oRpt.getRuleid(), oRpt.getImsi(), cellID,
//                     ts, getJsonReportDimension(oRpt.getReportDims()));
//            this.getSession().insert(event_timer);
//         }
//         this.getIncident_queue().add(new QueueItem(ts, (Object) oRpt.getCopyMgrRecord()));
//
//         // write external VFE IMSI tracking
//         track_separate_incidents(oRpt.getImsi(), oRpt.getCopyMgrRecord());
//
//      }
//      catch (Exception e) {
//         log.error("report exception: " + e.getMessage(), e);
//      }
//   }


//   /**
//    * Write Incidents of track IMSIs to a daily file, when defined. This
//    * function is used for a client trial, that did not want to use GUI The
//    * function is out Sync because it's no longer used. Just Kept for reference
//    * in case of future reuse.
//    */
//   private void track_separate_incidents(Long imsi, String incident) {
//
//      if (!RuleEngine.config.track_imsi()) return;
//      if (REreference.getInstance().isImsiTrack(imsi)) {
//
//         // check if today's file exist.
//         String outputFilePath = RuleEngine.config.track_output_file();
//
//         // create folders in path
//         File targetFile = new File(outputFilePath);
//         File parent = targetFile.getParentFile();
//
//         if (parent != null) {
//            if (!parent.exists() && !parent.mkdirs()) {
//               log.error("Couldn't create dir: " + parent);
////               NGEEAlarmHelper.logAlarm(NGEEAlarmHelper.PROCESSING_ERROR, "Couldn't create dir: " + parent);
//               throw new IllegalStateException("Couldn't create dir: " + parent);
//            }
//         }
//
//         // check if today's file exist.
//         DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
//         Date date = new Date();
//         String today = dateFormat.format(date);
//         int postFixLoc = outputFilePath.lastIndexOf(".");
//         if (postFixLoc == -1)
//            outputFilePath = outputFilePath + "_" + RuleEngine.engineID + "_" + this.getSegmentId() + "_" + today + ".txt";
//         else
//            outputFilePath = outputFilePath.substring(0, postFixLoc) + "_" + RuleEngine.engineID + "_" + this.getSegmentId()
//                     + "_" + today + ".txt";
//
//         // check if the file exist
//         boolean printHeader = false;
//         String reportHeader = null;
//         if (!new File(outputFilePath).exists()) {
//            printHeader = true;
//            reportHeader = "ts" + '\t' + "imsi" + '\t' + "msisdn" + '\t' + "cellname" + '\t' + "imeitac" + '\t'
//                     + "usercategory" + '\t' + "incidentscore" + '\t' + "ruleid" + '\t' + "rulecat" + '\t' + "rulename" + '\t'
//                     + "impact" + '\t' + "reason" + '\t' + "message_soc" + '\t' + "message_cc" + '\t' + "message_cclite" + '\t'
//                     + "apn" + '\t' + "rat" + '\t' + "kpi_name";
//         }
//
//         try {
//            FileOutputStream daily_file = new FileOutputStream(outputFilePath, true);
//            PrintStream ps = new PrintStream(daily_file);
//            if (printHeader) {
//               ps.println(reportHeader);
//            }
//            String localizedIncident = localizeIncident(incident);
//            ps.println(localizedIncident);
//            ps.close();
//
//         }
//         catch (FileNotFoundException e) {
//            log.error(e.getMessage(), e);
//         }
//      }
//   }
//
//
//   private static String getRuleDesc(String rule) {
//
//      String msg = REreference.getInstance().getRule_descProp().getProperty(rule);
//
//      return (msg == null) ? "\\N" : msg;
//   }
//
//
//   private String localizeIncident(String report) {
//
//      String       splitReport[] = report.split("\t");
//      StringBuffer csv           = new StringBuffer(); {
//
//         int index = 0;
//         csv.append(            splitReport[index++] ).append('\t')  // ts
//            .append(            splitReport[index++] ).append('\t')  // imsi
//            .append(            splitReport[index++] ).append('\t')  // msisdn
//            .append(            splitReport[index++] ).append('\t')  // cellname
//            .append(            splitReport[index++] ).append('\t')  // imeitac
//            .append(            splitReport[index++] ).append('\t')  // usercategory
//            .append(            splitReport[index++] ).append('\t')  // incidentscore
//            .append(getRuleDesc(splitReport[index++])).append('\t')  // ruleid
//            .append(getRuleDesc(splitReport[index++])).append('\t')  // rulecat
//            .append(getRuleDesc(splitReport[index++])).append('\t')  // rulename
//            .append(getRuleDesc(splitReport[index++])).append('\t')  // impact
//            .append(getRuleDesc(splitReport[index++])).append('\t')  // reason
//            .append(getRuleDesc(splitReport[index++])).append('\t')  // message_soc
//            .append(getRuleDesc(splitReport[index++])).append('\t')  // message_cc
//            .append(getRuleDesc(splitReport[index++])).append('\t')  // message_cclite
//            .append(            splitReport[index++] ).append('\t')  // apn
//            .append(            splitReport[index++] ).append('\t')  // rat
//            .append(getRuleDesc(splitReport[index++]))               // kpi_name
//         ;
//      }
//
//      return csv.toString();
//   }


   public static int getSeconds(String blocking_category) throws Exception {

      if (blocking_category == null || blocking_category.length() == 0) {

         String msg = "Empty category"; log.error(msg); throw new Exception(msg);
      }

      if      (blocking_category.equalsIgnoreCase( "0" )) return 0;
      else if (blocking_category.equalsIgnoreCase( "1m")) return 60;
      else if (blocking_category.equalsIgnoreCase("15m")) return 60 * 15;
      else if (blocking_category.equalsIgnoreCase( "1h")) return 60 * 60;
      else if (blocking_category.equalsIgnoreCase( "1d")) return 60 * 60 * 24;
      else {

         String msg = "Unsupported blocker type: " + blocking_category; log.error(msg); throw new Exception(msg);
      }
   }


   public static String getBlocking_category(int seconds) throws Exception {

      if      (seconds ==  0          ) return  "0" ;
      else if (seconds == 60          ) return  "1m";
      else if (seconds == 60 * 15     ) return "15m";
      else if (seconds == 60 * 60     ) return  "1h";
      else if (seconds == 60 * 60 * 24) return  "1d";
      else {

         String msg = "Unsupported number of seconds: " + seconds; log.error(msg); throw new Exception(msg);
      }
   }


   public boolean isNet_promoter_score(String source) {

      if (this.npsSources == null) return false;
      if (this.npsSources.contains(source)) return true;
      return false;

   }


   /**
    * @param key
    *           : cell location identifier
    * @return
    */
   public Cell getCell(String key) {

      Cell c = null;
      REreference reference = REreference.getInstance();
      c = reference.getCell(key);
      return c;
   }


   /**
    * @param key
    *           : cell location identifier
    * @param fieldName
    *           : fieldname to get as defined in the database metada.
    * @return
    */
   public Object getCell(String key, String fieldName) {

      if (key == null || fieldName == null) return null;
      Cell c = getCell(key);

      if (c != null && !c.isEmpty())
         return Event_ESR.getValueFromObject(c, fieldName);
      else
         return null;
   }


   private Boolean getPublished(CRM crm, String ruleid) {

      REreference reference = REreference.getInstance();
      if (!reference.doActuation() || crm == null) return false;

      if (crm.getNotify() != null && crm.getNotify() && !reference.getRuleIdParms().getDoNotNotify().contains(ruleid))
         return true;

      return false;
   }


   public Object getRuleParameter(String ruleId, String parameterKey) {

      REreference reference = REreference.getInstance();
      RuleIdParameter ruleIdParameter = reference.getRuleIdParms();
      Object obj = ruleIdParameter.getParameterValue(ruleId, parameterKey);
      return obj;
   }


   /**
    * @param otherDimensions
    * @param reportDimensions
    * @param usedOttDimensions
    *           TODO
    * @param reason
    *           TODO
    * @return other Dimensions and usedDimensions into a Json String
    */
   public static String getJsonformatStr(Map<String,Object>  otherDimensions
      ,                                  Map<String, Object> reportDimensions
      ,                                  Map<String, Object> usedOttDimensions
      ,                                  String              reason) {

      if ((otherDimensions   == null || otherDimensions  .isEmpty())
       && (reportDimensions  == null || reportDimensions .isEmpty())
       && (usedOttDimensions == null || usedOttDimensions.isEmpty())) return null;

      JSONObject obj = null;
      if (otherDimensions != null && !otherDimensions.isEmpty())
         obj = new JSONObject(otherDimensions);
      else
         obj = new JSONObject();

      // Add report Dimensions Build "dimensions_used" + "reason" tuples
      Set<String> reportDimensionskeys = reportDimensions.keySet();
      StringBuffer b = new StringBuffer();
      StringBuffer reasonDims = new StringBuffer();
      for (String jKey : reportDimensionskeys) {
         Object value = reportDimensions.get(jKey);
         if (b.length() != 0) {
            b.append(",");
            reasonDims.append(",");
         }
         b.append(jKey);
         reasonDims.append(jKey);
         b.append(":");
         reasonDims.append("=");
         b.append(value.toString());
         reasonDims.append(value.toString());
      }

      // Set reason tuple in the Json Object
      if (b.length() > 0) {
         obj.put(reason, reasonDims.toString());
      }

      // Add Ott used dimensions when present to dimensions used.
      Set<String> usedOttDimensionskeys = usedOttDimensions.keySet();
      for (String jKey : usedOttDimensionskeys) {
         Object value = usedOttDimensions.get(jKey);
         if (b.length() != 0) {
            b.append(",");
         }
         b.append(jKey);
         b.append(":");
         b.append(value.toString());
      }

      // Set the dimensions_used for ORE.
      if (b.length() > 0) {
         obj.put("dimensions_used", b.toString());
      }

      return JSONValue.toJSONString(obj);
   }


   public KpiMetaDataRecord getKpiMeta(String source, String kpiName, String rat, boolean gdg) {

      return REreference.getInstance().getKpiMeta(source, kpiName, rat, gdg);
   }


//   private String getEncodedEsr(Event_ESR esr) {
//
//      try {
//         // Re-encode the ESR
//         return encryptor.Encode(esr.toJSONString());
//      }
//      catch (InvalidKeyException e) {
//         log.error("exception getting back encoded ESR: " + e.getMessage());
//      }
//      catch (InvalidAlgorithmParameterException e) {
//         log.error("exception getting back encoded ESR: " + e.getMessage());
//      }
//      catch (IOException e) {
//         log.error("exception getting back encoded ESR: " + e.getMessage());
//      }
//      return null;
//   }


   /**
    * @param inDimensions
    * @return a Json String from the map
    */
   public String getJsonReportDimension(Map<String, Object> inDimensions) {

      if (inDimensions == null || inDimensions.isEmpty()) return null;

      JSONObject obj = new JSONObject(inDimensions);
      return JSONValue.toJSONString(obj);
   }


   public SubscriberIncident createIncident(int type) {

      SubscriberIncident s = new SubscriberIncident();
      s.setSource(type);
      return s;
   }


//   public SubscriberIncident createSncdIncident() {
//
//      return (createIncident(SubscriberIncidentReport.originTA));
//   }


   public SubscriberIncident createEsrIncident() {

      return createIncident(SubscriberIncidentReport.originESR);
   }


   public SliIncident createSliIncident() {

      SliIncident s = new SliIncident(); {

         s.setSource(SubscriberIncidentReport.originSLI);
      }

      return s;
   }


   /**
    * @param r
    * @param oRpt
    *           setFieldsFromRuleDesc(): Set fields from rule_desc table when
    *           there are not explicitly specified.
    */
   private void setFieldsFromRuleDesc(RuleDesc r, SubscriberIncident oRpt) {

      String s = null;
      if ((s=oRpt.getRulecat()         ) == null || s.isEmpty()) oRpt.setRulecat         (r.getRule_cat()        );
      if ((s=oRpt.getRulename()        ) == null || s.isEmpty()) oRpt.setRulename        (r.getRule_name()       );
      if ((s=oRpt.getImpact()          ) == null || s.isEmpty()) oRpt.setImpact          (r.getImpact()          );
      if ((s=oRpt.getReason()          ) == null || s.isEmpty()) oRpt.setReason          (r.getReason()          );
      if ((s=oRpt.getMessage_soc()     ) == null || s.isEmpty()) oRpt.setMessage_soc     (r.getMessage_soc()     );
      if ((s=oRpt.getMessage_cc()      ) == null || s.isEmpty()) oRpt.setMessage_cc      (r.getMessage_cc()      );
      if ((s=oRpt.getMessage_cclite()  ) == null || s.isEmpty()) oRpt.setMessage_cclite  (r.getMessage_cclite()  );
      if ((s=oRpt.getKpi_source()      ) == null || s.isEmpty()) oRpt.setKpi_source      (r.getKpi_source()      );
      if ((s=oRpt.getKpi_name()        ) == null || s.isEmpty()) oRpt.setKpi_name        (r.getKpi_name()        );
      if ((s=oRpt.getKpi_name()        ) == null || s.isEmpty()) oRpt.setKpi_name        (r.getKpi_name()        );
    //if ((s=oRpt.getNext_best_action()) == null || s.isEmpty()) oRpt.setNext_best_action(r.getNext_best_action());
   }


   /**
    * @param RuleIdMetaDataMap
    * @param oRpt
    *           setMetaData() : Set Meta data related information.
    */
   private void setMetaData(SubscriberIncident oRpt) {

      REreference reference = REreference.getInstance();
      Map<String, RuleMetaData> ruleIdMetaDataMap = reference.getRuleIdMetaDataMap();

      if (ruleIdMetaDataMap != null) {
         RuleMetaData metaData = ruleIdMetaDataMap.get(oRpt.getRuleid());
         if (metaData != null && metaData.getFilename() != null) oRpt.setFileName(metaData.getFilename());
         if (metaData != null && metaData.getRelease()  != null) oRpt.setRelease(metaData.getRelease());
         if (metaData != null && metaData.getVersion()  != null) oRpt.setVersion(metaData.getVersion());
      }

      // Set the service and arim if they were not explicitly defined.
      KpiMetaDataRecord kpiMeta = null;
      if (oRpt.getService() == null || oRpt.getService().isEmpty() || oRpt.getArim() == null || oRpt.getArim().isEmpty()) {
         kpiMeta = getKpiMeta(oRpt.getKpi_source(), oRpt.getKpi_name(), oRpt.getRat(), false);
      }

      if ((oRpt.getService() == null || oRpt.getService().isEmpty()) && kpiMeta != null) {
         oRpt.setService(kpiMeta.getService());
      }

      if ((oRpt.getArim() == null || oRpt.getArim().isEmpty())) {
         oRpt.setArim((kpiMeta.getArim()));
      }
   }


   /** .*/
   public String getCellName(String locationId) {

      String res = null; if (locationId != null) {

         Cell c = getCell(locationId); if (c != null) res = c.getCell_name();
      }

      return res;
   }
}
