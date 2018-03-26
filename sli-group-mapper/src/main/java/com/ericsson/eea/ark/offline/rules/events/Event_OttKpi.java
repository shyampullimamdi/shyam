package com.ericsson.eea.ark.offline.rules.events;

import java.util.Map;
import java.util.List;
import java.util.Arrays;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.AllArgsConstructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bigdata.esr.dataexchange.KpiDesc;
import com.ericsson.bigdata.esr.dataexchange.KpiRecord;
import com.ericsson.bigdata.common.util.MatchableMap;
import com.ericsson.bigdata.common.identifier.CellIdentifier;
import com.ericsson.eea.ark.offline.rules.refdata.OttKpiRecord;
import com.ericsson.eea.ark.offline.rules.utils.Util;


@ToString
@AllArgsConstructor
public class Event_OttKpi {

   private static final Logger log = LoggerFactory.getLogger(Event_OttKpi.class.getName());


   @Getter @Setter private KpiDesc   ottKpi    = null;
   @Getter @Setter private KpiRecord ottRec    = null;
   @Getter @Setter private Event_ESR esr       = null;
   @Getter @Setter public  long      timestamp;

   @Getter         private static Util util    = new Util();


   /**
    * @param source
    * @param name
    * @return OTT Kpi records if it matches source and name
    */
   public OttKpiRecord getKpi(String source, String name) {
      if (source == null || name == null || ottKpi == null || ottRec == null)
         return null;
      if (ottKpi.getSource().equalsIgnoreCase(source) && ottKpi.getName().equalsIgnoreCase(name))
         return new OttKpiRecord(ottRec);
      return null;
   }


   /**
    * @param source
    * @param name
    * @param dimensions
    * @return : returns OTT KpiRecords if it matches incoming source and name, and dimensions
    */
   public OttKpiRecord getKpi(String source, String name, String dimensions) {
      OttKpiRecord r = getKpi(source, name) ;
      if (r == null)
         return r;

      if (dimensions == null || dimensions.isEmpty())
         return r;

      //Dimensions were specified , see if we have a match
      MatchableMap<String, String> m = new MatchableMap<String, String>();
      KpiRecord pattern = new KpiRecord(null, null, m, null);
      String list1[] = dimensions.split(",");
      for (String pair: list1) {
         String list2[] = pair.split(":");
         if (list2.length != 2)
            continue;
         String key = list2[0].trim();
         String value = list2[1].trim();
         m.put(key, value);
      }

      if (r.matches(pattern))
         return r;

      return null;
   }

   /**
    * @param source
    * @param name
    * @param cellId
    * @param dimensions
    * @return Kpirecords when all argument match
    */
   public OttKpiRecord getKpi(String source, String name,
         CellIdentifier cellId, String dimensions) {
      OttKpiRecord r = getKpi(source, name, dimensions);
      if (r == null)
         return r;

      if (cellId == null || this.getOttRec().getCellIdentifier() == null) // Just
                                                         // check
                                                         // up
                                                         // to
                                                         // dimensions.
         return r;

      // cellId was specified , just check location Identifier, since it is
      // unique.
      if (cellId.getLocationId() != null
            && this.getOttRec().getCellIdentifier().getLocationId() != null
            && cellId.getLocationId().equals(
                  this.getOttRec().getCellIdentifier().getLocationId()))
         return r; // complete match.

      return null;
   }

   /**
    * @param dimensionTypes
    * @return the dimension value.
    */
   public String getDimensionValue(String dimensionTypes) {
      String dimensionTuple = getDimensionTuples(dimensionTypes);
      if (dimensionTuple == null)
         return null;

      String[] tupleArray = dimensionTuple.split(":");
      if (tupleArray.length > 1) {
         return tupleArray[1];
      }

      return null;
   }

   /**
    * @param dimensionTypes
    * @return A dimension tuplre inas String "key:value"
    */
   public String getDimensionTuples(String dimensionTypes) {
      Map<String, String> m = this.getOttRec().getDimensions();
      String returnStr = null;

      if (m == null || m.isEmpty() || dimensionTypes == null
            || dimensionTypes.isEmpty() || this.getOttRec().getDimensions() == null
            || this.getOttRec().getDimensions().isEmpty())
         return returnStr;
      // parse dimension types
      List<String> items = Arrays.asList(dimensionTypes.split("\\s*,\\s*"));

      StringBuffer b = new StringBuffer();
      for (String dimensionKey : items) {
         if (this.getOttRec().getDimensions().containsKey(dimensionKey)) {
            if (b.length() > 0)
               b.append(",");

            b.append(dimensionKey);
            b.append(":");
            b.append(this.getOttRec().getDimensions().get(dimensionKey));
         }
      }
      return b.toString();
   }

   /**
    * @param dimensionKey: First argument is dimension type, list of values to be checked for dimension
    * @return true/false. True if at least one of the incoming values matches the value of the dimension KPI.
    */
   public boolean checkDimensionValues(String dimensionKey, String dimensionValues) {

       if (dimensionKey == null || dimensionValues == null) {

           log.error("checkDimensionValues : invalid args" );
           return false;
       }

       // parse dimension types
       boolean      rc    = false;
       List<String> items = Arrays.asList(dimensionValues.split("\\s*,\\s*")); for (String inDimensionVal : items) {

           if (inDimensionVal == null || inDimensionVal.isEmpty()) continue;

           if (this.getOttRec() != null
                   &&  this.getOttRec().getDimensions() != null
                   &&  this.getOttRec().getDimensions().get(dimensionKey) != null
                   &&  this.getOttRec().getDimensions().get(dimensionKey).trim().equals(inDimensionVal.trim())) {

               rc = true; break;
           }
       }

       return rc;
   }
}
