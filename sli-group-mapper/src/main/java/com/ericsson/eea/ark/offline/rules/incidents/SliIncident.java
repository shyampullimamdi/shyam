package com.ericsson.eea.ark.offline.rules.incidents;

import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.ArrayList;
import java.lang.reflect.Field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.NoArgsConstructor;

//import com.google.gson.Gson;
//import com.google.gson.reflect.TypeToken;


import com.ericsson.bigdata.common.util.MatchableMap;
import com.ericsson.eea.ark.offline.rules.utils.Util;

/**
 * This class represents the subscriber incident. Which is typically set via the Rule Engine interface. Drool in the case.
 */
@NoArgsConstructor
public class SliIncident {

   private static final Logger log = LoggerFactory.getLogger(SliIncident.class.getName());

   private static final ArrayList<String> guiOttdims = new ArrayList<String>(Arrays.asList(

      "protocol", "functionality", "encapsulation", "encryption", "service_provider", "client_application", "type"
   ));

   @Getter @Setter private Long                         ts_milli           = null;
   @Getter @Setter private String                       imsi               = null;
   @Getter @Setter private String                       msisdn             = null;
   @Getter @Setter private Object                       location           = null;
   @Getter @Setter private Integer                      imeitac            = null;
   @Getter @Setter private String                       usercategory       = null;
   @Getter @Setter private Integer                      incidentscore      = null;
   @Getter @Setter private String                       ruleid             = null;
   @Getter @Setter private String                       rulecat            = null;
   @Getter @Setter private String                       rulename           = null;
   @Getter @Setter private String                       impact             = null;
   @Getter @Setter private String                       reason             = null;
   @Getter @Setter private String                       message_soc        = null;
   @Getter @Setter private String                       message_cc         = null;
   @Getter @Setter private String                       message_cclite     = null;
   @Getter @Setter private String                       kpi_source         = null;
   @Getter @Setter private String                       kpi_name           = null;
   @Getter @Setter private String                       blockingcat        = null;
   @Getter @Setter private Boolean                      feedback           = false;
   @Getter @Setter private Boolean                      includeData        = false;
   @Getter @Setter private MatchableMap<String,Object>  otherDims          = new MatchableMap<String, Object>();
   @Getter @Setter private MatchableMap<String,Object>  reportDims         = new MatchableMap<String, Object>();
   @Getter @Setter private MatchableMap<String,Object>  usedOTTDims        = new MatchableMap<String, Object>();
   @Getter @Setter private String                       next_best_action   = null;
   @Getter @Setter private String                       service            = null;
   @Getter @Setter private String                       arim               = null;
   @Getter @Setter private Object                       dataBlob           = null;
   @Getter @Setter private Integer                      source             = SubscriberIncidentReport.originESR;
   @Getter @Setter private String                       apn                = null;
   @Getter @Setter private String                       rat                = null;
   @Getter @Setter private String                       sgsn               = null;
   @Getter @Setter private Boolean                      hidden             = false;
   @Getter @Setter private String                       fileName           = null;
   @Getter @Setter private String                       version            = null;
   @Getter @Setter private String                       release            = null;
   @Getter @Setter private String                       cellName           = null;
   @Getter @Setter private Long                         incident_id        = null;
   @Getter @Setter private List<Object>                 objectList         = null;

   // Generated by loader.
   @Getter @Setter private Boolean                      incident_published = false;
   @Getter @Setter private Integer                      partionId          = null;
   @Getter @Setter private String                       reportingCell      = "end";
   @Getter @Setter private Boolean                      correlate          = false;


   public SliIncident(SliIncident other) {

      this.ts_milli         = other.ts_milli;
      this.imsi             = other.imsi;
      this.msisdn           = other.msisdn;
      this.location         = other.location;
      this.imeitac          = other.imeitac;
      this.usercategory     = other.usercategory;
      this.incidentscore    = other.incidentscore;
      this.ruleid           = other.ruleid;
      this.rulecat          = other.rulecat;
      this.rulename         = other.rulename;
      this.impact           = other.impact;
      this.reason           = other.reason;
      this.message_soc      = other.message_soc;
      this.message_cc       = other.message_cc;
      this.message_cclite   = other.message_cclite;
      this.kpi_source       = other.kpi_source;
      this.kpi_name         = other.kpi_name;
      this.blockingcat      = other.blockingcat;
      this.feedback         = other.feedback;
      this.includeData      = other.includeData;
      this.otherDims        = other.otherDims;
      this.reportDims       = other.reportDims;
      this.usedOTTDims      = other.usedOTTDims;
      this.next_best_action = other.next_best_action;
      this.service          = other.service;
      this.arim             = other.arim;
      this.dataBlob         = other.dataBlob;
      this.source           = other.source;
      this.apn              = other.apn;
      this.rat              = other.rat;
      this.sgsn             = other.sgsn;
      this.hidden           = other.hidden;
      this.cellName         = other.cellName;
      this.partionId        = other.partionId;
      this.reportingCell    = other.reportingCell;
      this.correlate        = other.correlate;
   }


   public String getCopyMgrRecord() {

      String dimensionStr  = SubscriberIncidentReport.getJsonformatStr(otherDims, reportDims, usedOTTDims, reason);
      int    blocking_secs = 0; if (blockingcat != null) try {

         blocking_secs = SubscriberIncidentReport.getSeconds(blockingcat);
      }
      catch (Exception e) {

         log.error("Exception: " + e.getMessage());
      }

      //redefine source when possible:
      resetEsrSource();

      StringBuffer buffer = new StringBuffer(); {

         // TODO: automate generation from table description.
         buffer.append(Util.csvColumn(ts_milli / 1000   ))
            .   append(Util.csvColumn(imsi              ))
            .   append(Util.csvColumn(msisdn            ))
            .   append(Util.csvColumn(cellName          ))
            .   append(Util.csvColumn(imeitac           ))
            .   append(Util.csvColumn(usercategory      ))
            .   append(Util.csvColumn(incidentscore     ))
            .   append(Util.csvColumn(ruleid            ))
            .   append(Util.csvColumn(rulecat           ))
            .   append(Util.csvColumn(rulename          ))
            .   append(Util.csvColumn(impact            ))
            .   append(Util.csvColumn(reason            ))
            .   append(Util.csvColumn(message_soc       ))
            .   append(Util.csvColumn(message_cc        ))
            .   append(Util.csvColumn(message_cclite    ))
            .   append(Util.csvColumn(apn               ))
            .   append(Util.csvColumn(rat               ))
            .   append(Util.csvColumn(kpi_name          ))
            .   append(Util.csvColumn(release           ))
            .   append(Util.csvColumn(version           ))
            .   append(Util.csvColumn(fileName          ))
            .   append(Util.csvColumn(sgsn              ))
            .   append(Util.csvColumn(blocking_secs     ))
            .   append(Util.csvColumn(partionId         ))
            .   append(Util.csvColumn(source            ))
            .   append(Util.csvColumn(incident_id       ))
            .   append(Util.csvColumn(incident_published))
            .   append(Util.csvColumn(hidden            ))
            .   append(Util.csvColumn(dimensionStr      ))
            .   append(Util.csvColumn(kpi_source        ))
            .   append(Util.csvColumn(service           ))
            .   append(Util.csvColumn(arim              ))
            .   append(Util.csvColumn(next_best_action  ))
            .   append(Util.csvColumn(dataBlob          , /*no separator*/false));
      }

      return buffer.toString();
   }


   /** Return a key that uniquely identify this particular incident.*/
   public String getKey() {

      return imsi+'/'+ts_milli;
   }


   /** Return a map containing the specified fields.*/
   public Map<String,Object> getValues(String... values) {

      Field[]            fs  = getClass().getFields();
      Map<String,Object> res = new TreeMap<String,Object>(); for (String val : values) {

         //TODO
      }

      return res;
   }


   /**
    * @param key
    * @param dimensionValue
    * Add Pair value to report dimensions.
    */
   public void addReportDims(String key, Object dimensionValue) {

      if (key != null && dimensionValue != null) {

         reportDims.put(key, dimensionValue);
      }
   }

   /**
    * @param key
    * @param dimensionValue
    * Add Pair value to other dimensions.
    */
   public void addOthertDims(String key, Object dimensionValue) {

      if (key != null && dimensionValue != null) {

         otherDims.put(key, dimensionValue);
      }
   }

   /**
    * @param key
    * @param dimensionValue
    * Add Pair value to used OTT dimensions.
    */
   public void addUsedOttDims(String key, Object dimensionValue) {

      if (key != null && dimensionValue != null) {

         usedOTTDims.put(key, dimensionValue);
      }
   }

//   /**
//    * @param ruleParmDims
//    * Updates the reportDims map from a jsonString as defined via rules.
//    */
//   public void addReportDims(String ruleParmDims) {
//
//      reportDims.putAll(getDimsFromParameterStr(ruleParmDims));
//   }
//
//   /**
//    * @param ruleParmDims
//    * Updates the otherDims map from a jsonString as defined via rules.
//    */
//   public void addOtherDims(String ruleParmDims) {
//
//      otherDims.putAll(getDimsFromParameterStr(ruleParmDims));
//   }
//
//   /**
//    * @param ruleParmDims
//    * Updates the reportDims map from a jsonString as defined via rules.
//    */
//   public void addUsedOttDims(String ruleParmDims) {
//
//      usedOTTDims.putAll(getDimsFromParameterStr(ruleParmDims));
//   }
//
//
//   /**
//    * @param ruleParmDims; Dimension as defined typically via the Rule Parameter file
//    * @return a matchable map from the Json String
//    */
//   private MatchableMap<String, Object> getDimsFromParameterStr(String ruleParmDims) {
//
//      MatchableMap<String, Object> m = new MatchableMap<String, Object>(); if (ruleParmDims != null && !ruleParmDims.isEmpty()) {
//
//         Map<String, Object> retMap = new Gson().fromJson(ruleParmDims, new TypeToken<HashMap<String, Object>>() {}.getType());
//
//         m.putAll(retMap);
//      }
//
//      return m;
//   }


   /**
    * resetEsrSource(): reset the Source to originOTT if the incident has OTT dimensions.
    */
   private void resetEsrSource() {

      if (this.source != SubscriberIncidentReport.originESR) return;

      if (this.getOtherDims() != null && !this.getOtherDims().isEmpty()) {

         // if any of the dimensions found in the ott dimensions we reset the source.
         for (String ottDimType : this.getOtherDims().keySet()) {

            if (guiOttdims.contains(ottDimType)) {

               this.setSource(SubscriberIncidentReport.originOTT); break;
            }
         }
      }
   }

}
