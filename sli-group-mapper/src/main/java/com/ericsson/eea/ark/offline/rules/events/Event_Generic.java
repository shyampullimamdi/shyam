package com.ericsson.eea.ark.offline.rules.events;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import com.ericsson.bigdata.common.util.MatchableMap;

import com.ericsson.bigdata.esr.parserlogic.CastingException;


/**
 * This class is meant for creating generic object as {@code MatchableMap<String, Object>} which can be used to
 * support complex structures. The complex structure can be defined via a CSV file. In addition, generic
 * objects can be defined in the input.generic.facts_directory in the 'config.txt' file.
 */
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class Event_Generic {

   private static final Logger log = LoggerFactory.getLogger(Event_Generic.class.getName());

   @Getter @Setter private Long                         timestamp = null;
   @Getter @Setter private String                       name      = null;
   @Getter @Setter private MatchableMap<String, Object> map       = null;


   public Object get(String key) {

      if (map != null && !map.isEmpty()) {
         if (map.get(key) != null && map.get(key) instanceof String && ((String) map.get(key)).isEmpty()) {
            // return a null instead because rules writer are trained to check
            // for null
            return null;
         }
         return map.get(key);
      }
      return null;

   }


   public <T> T get(String key, Class<T> type) {

      Object o = this.get(key);
      if (o == null) return null;

      try {
         return Event_Generic.getValue(o, type);
      }
      catch (CastingException e) {
         // Put it just as debug as it may not be considered an error.
         log.warn("casting exception trying to get Type value for key=" + key + " type=" + type + " object=" + o);
      }
      return null;

   }


   /**
    * Generic casting method for all types.
    *
    * @param o
    *           - object to be casted
    * @param type
    *           - target class
    * @return kpi value or record
    * @throws CastingException
    */
   @SuppressWarnings("unchecked")
   public static <T> T getValue(Object o, Class<T> type) throws CastingException {

      // o is null
      if (o == null) {
         return null;
      }

      // T is String
      else if (type == String.class) {
         if (o instanceof String) {
            return (((String) o).isEmpty() ? null : (T) o);
         }
         else if (o instanceof Integer) {
            return (((Integer) o).toString().isEmpty() ? null : (T) ((Integer) o).toString());
         }
         else if (o instanceof Long) {
            return (((Long) o).toString().isEmpty() ? null : (T) ((Long) o).toString());
         }
         else if (o instanceof Double) {
            return (((Double) o).toString().isEmpty() ? null : (T) ((Double) o).toString());
         }
         else
            throw new CastingException(o.toString() + " cannot be cast to String.");
      }
      // T is Long
      else if (type == Long.class) {
         if (o instanceof Long) {
            return (T) o;
         }
         else if (o instanceof Integer) {
            return (T) (new Long(((Integer) o).longValue()));
         }
         else if (o instanceof java.lang.Double) {
            double d = (Double) o;
            if (d == Math.floor(d)) { return (T) (new Long((long) d)); }
         }
         else if (o instanceof String) {
            if (((String) o).isEmpty()) return null;
            try {
               return (T) (new Long((String) o));
            }
            catch (Exception e) {}
         }
         throw new CastingException(o.toString() + " cannot be cast to Long.");
      }
      // T is Double
      else if (type == Double.class) {
         if (o instanceof Double) {
            return (T) o;
         }
         else if (o instanceof Integer) {
            return (T) (new Double(((Integer) o).doubleValue()));
         }
         else if (o instanceof Long) {
            return (T) (new Double(((Long) o).longValue()));
         }
         else if (o instanceof String) {
            if (((String) o).isEmpty()) return null;

            try {
               return (T) (new Double((String) o));
            }
            catch (Exception e) {}
         }
         else
            throw new CastingException(o.toString() + " cannot be cast to Double.");
      }
      // T is Integer
      if (type == Integer.class) {
         if (o instanceof Integer) {
            return (T) o;
         }
         else if (o instanceof Long) {
            long l = (Long) o;
            if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) { throw new CastingException(l
                     + " cannot be cast to int without changing its value."); }
            return (T) (new Integer((int) l));
         }
         else if (o instanceof String) { // This is needed to handle integers
            // used as keys in a JSON Map
            if (((String) o).isEmpty()) return null;
            try {
               return (T) (new Integer((String) o));
            }
            catch (Exception e) {}
         }
         else if (o instanceof Double) {
            double d = (Double) o;
            if (d == Math.floor(d)) { return (T) getValue(new Long((long) d), Integer.class); }
         }
         throw new CastingException(o.toString() + " cannot be cast to Integer.");
      }
      throw new CastingException("Cannot cast to type " + type.getName());
   }
}
