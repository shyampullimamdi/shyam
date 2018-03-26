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

package com.ericsson.eea.ark.offline.rules.utils;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.GregorianCalendar;

import net.minidev.json.JSONArray;
//import net.minidev.json.JSONValue;
import net.minidev.json.JSONObject;

import org.apache.commons.lang3.tuple.Pair;

import com.ericsson.bigdata.esr.parserlogic.CastingException;
import com.ericsson.eea.ark.offline.rules.events.Event_Generic;
import com.ericsson.eea.ark.offline.rules.refdata.REreference;
import com.ericsson.eea.ark.offline.rules.refdata.RuleIdParameter;

public class Util {

   private static final Logger log = LoggerFactory.getLogger(Util.class.getName());

   /** Memoization table.*/
   private static HashMap<String,Object> memo = new HashMap<String,Object>();

   public static StringBuffer csvColumn(Object value) {

      return csvColumn(value, true);
   }


   public static void cleanup() {

      memo.clear();
   }

   public static StringBuffer csvColumn(Object value, boolean appendTab) {

      StringBuffer buffer = new StringBuffer(); {

         if (value == null) buffer.append("\\N");
         else               buffer.append(value);

         if (appendTab) buffer.append("\t");
      }

      return buffer;
   }


   public static boolean contains(String s, String[] ss) {

      if (ss != null) for (String x : ss) {

       if      (x == s)      return true;
       if      (x == null)   continue;
       else if (x.equals(s)) return true;
      }

      return false;
   }


   public static String intersperse(String sep, String[] ss) {

      StringBuffer b     = new StringBuffer();
      boolean      first = true; if (ss != null) for (String s : ss) { if (s == null) continue;

         if (first) { b.append(s); first = false; }
         else       { b.append(sep).append(s);    }
      }

      return b.toString();
   }


   public static <T> String intersperse(String sep, List<T> ss) {

      StringBuffer b     = new StringBuffer();
      boolean      first = true; if (ss != null) for (T s : ss) { if (s == null) continue;

         if (first) { b.append(s); first = false; }
         else       { b.append(sep).append(s);    }
      }

      return b.toString();
   }


   static Pattern ts_pat = Pattern.compile(
      "^([0-9][0-9][0-9][0-9])([0-9][0-9])([0-9][0-9])([0-9][0-9])?([0-9][0-9])?([0-9][0-9])?(\\.([0-9][0-9]?[0-9]?))?$"
   );

   public static Pair<Long,GregorianCalendar> parseTimestamp(String ts) {

      if (ts == null) return null;

      int yyyy = 0
         ,  mm = 0
         ,  dd = 0
         ,  HH = 0
         ,  MM = 0
         ,  SS = 0
         , sss = 0; Matcher ma = ts_pat.matcher(ts); if (ma.matches()) {

         String s = "";
         switch (ma.groupCount()) {
            case 8: s=ma.group(8); if (s!=null){ sss = Integer.parseInt(s); for (int i=(3-s.length()); i>0; i--) sss = sss * 10; } // fall-through
            case 6: s=ma.group(6); if (s!=null)   SS = Integer.parseInt(s);
            case 5: s=ma.group(5); if (s!=null)   MM = Integer.parseInt(s);
            case 4: s=ma.group(4); if (s!=null)   HH = Integer.parseInt(s);
            case 3: s=ma.group(3); if (s!=null)   dd = Integer.parseInt(s);
            case 2: s=ma.group(2); if (s!=null)   mm = Integer.parseInt(s) -1; // Month is 0-based in GregorianCalendar
            case 1: s=ma.group(1); if (s!=null) yyyy = Integer.parseInt(s);
         }
      }

      GregorianCalendar time    = new GregorianCalendar(yyyy, mm, dd, HH, MM, SS);
      long              time_ts = time.getTimeInMillis() + sss;

      time.setTimeInMillis(time_ts);

      return Pair.of(time_ts, time);
   }


   /**
    * @param ruleId  rule id as defined in drl file
    * @param parameterKey  parameter name as defined in drl
    * @return: the Object value defined in the rule_parameters.csv for composite key(ruleId,parameterKey)
    */
   public static Object getRuleParameter(String ruleId, String parameterKey) {

      String key = ruleId+"/"+parameterKey; if (memo.containsKey(key)) return memo.get(key);

      REreference     reference       = REreference.getInstance();
      RuleIdParameter RuleIdParameter = reference.getRuleIdParms();
      Object          obj             = RuleIdParameter.getParameterValue(ruleId, parameterKey);

      if (log.isDebugEnabled()) log.debug("Util.getRuleParameter(ruleId: '"+ruleId+"', parameterKey: '"+parameterKey+"'): "+obj);

      memo.put(key, obj); return obj;
   }


   /**
    * @return the parameter Key value of the requested type.
    */
   public static <T> T getRuleParameter(String ruleId, String parameterKey, Class<T> type) {

      String key = type.getSimpleName()+":"+ruleId+"/"+parameterKey; if (memo.containsKey(key)) return (T) memo.get(key);

      REreference     reference       = REreference.getInstance();
      RuleIdParameter RuleIdParameter = reference.getRuleIdParms();
      Object          o               = RuleIdParameter.getParameterValue(ruleId, parameterKey); if (o == null) return null;

      T ev = null; try {

         ev = Event_Generic.getValue(o, type);
      }
      catch (CastingException e) {

         // Put it just as warn as it may not be considered an error.
         log.warn("Type conversion error trying to get value for key=" + parameterKey + ", type=" + type + ", object=" + o);
      }

      if (log.isDebugEnabled()) log.debug("Util.getRuleParameter(ruleId: '"+ruleId+"', parameterKey: '"+parameterKey+"', type: '"+type.getName()+"'): "+ev);

      memo.put(key, ev); return ev;
   }


   /** Generalized getter to reach any field through a list of indices (Integers and Strings).*/
   public static Object get(JSONObject obj, Object[] indices) {

      Object o = obj.get(indices[0]); for(int i=1; i<indices.length; ++i) {

         if      (o instanceof JSONObject) o = ((JSONObject)o).get(          indices[i]);
         else if (o instanceof JSONArray ) o = ((JSONArray )o).get((Integer) indices[i]);
         else                              o = null;
      }
      return o;
   }


   /** Generalized getter to reach any field through a list of indices (Integers and Strings).*/
   public static Object get(JSONObject obj, List<Object> indices) {

      Object o = obj.get(indices.get(0)); for(int i=1; i < indices.size(); ++i) {

         if      (o instanceof JSONObject) o = ((JSONObject)o).get(         indices.get(i));
         else if (o instanceof JSONArray ) o = ((JSONArray )o).get((Integer)indices.get(i));
         else                              o = null;
      }
      return o;
   }


   /** Generalized getter to reach any field through a list of indices (Integers and Strings).*/
   @SuppressWarnings("unchecked")
   public static Object get(Map<String,Object> obj, Object[] indices) {

      Object o = obj.get(indices[0]); for(int i=1; i<indices.length; ++i) {

         if      (o instanceof Map       ) o = ((Map<String,Object>)o).get(          indices[i]);
         else if (o instanceof List      ) o = ((List<      Object>)o).get((Integer) indices[i]);
         else if (o instanceof JSONObject) o = ((JSONObject        )o).get(          indices[i]);
         else if (o instanceof JSONArray ) o = ((JSONArray         )o).get((Integer) indices[i]);
         else                              o = null;
      }
      return o;
   }


   /** Generalized getter to reach any field through a list of indices (Integers and Strings).*/
   @SuppressWarnings({"unchecked"})
   public static Object get(Map<String,Object> obj, List<Object> indices) {

      Object o = obj.get(indices.get(0)); for(int i=1; i < indices.size(); i++) {

         if      (o instanceof Map       ) o = ((Map<String,Object>)o).get(         indices.get(i));
         else if (o instanceof List      ) o = ((List<      Object>)o).get((Integer)indices.get(i));
         else if (o instanceof JSONObject) o = ((JSONObject        )o).get(         indices.get(i));
         else if (o instanceof JSONArray ) o = ((JSONArray         )o).get((Integer)indices.get(i));
         else                              o = null;
      }
      return o;
   }


   public static <K,V> V get(K key, Pair<K,V>[] map) {

      if (key != null) {

         int len = map.length; for (int i=0; i<len; ++i) {

            Pair<K,V> p = map[i]; if (key.equals(p.getLeft())) return p.getRight();
         }
      }

      return null;
   }


   public static <K,V> Pair<K,V>[] zip(K[] keys, V[] vals) {

      int                                        len = Math.min(keys.length, vals.length);
      @SuppressWarnings("unchecked") Pair<K,V>[] map = new Pair[len]; for (int i=0; i<len; ++i) {

         map[i] = Pair.of(keys[i], vals[i]);
      }
      return map;
   }


   /** Returns a string in the form {@code '<key-1>=<value-1>(, *<key-n>=<value-n>)*'} as an associative map (list of pairs of strings).*/
   public static <V> Pair<String,String>[] getMap(String mapString, Pair<String,String>[] def) {

      return getMap(mapString, def, '=');
   }


   /** Returns a string in the form {@code '<key-1><sep><value-1>(, *<key-n><sep><value-n>)*'} as an associative map (list of pairs of strings).*/
   @SuppressWarnings("unchecked")
   public static <V> Pair<String,String>[] getMap(String mapString, Pair<String,String>[] def, char sep) {

      Pair<String,String>[] map = def;

      if (mapString != null) {

         ArrayList<Pair<String,String>> entries = new ArrayList<Pair<String,String>>();

         String[] kvs = mapString.split("[ \t]*,[ \t]*"); if (kvs != null) for (String kvStr : kvs) {

            String[] kv = kvStr.split("[ \t]*"+sep+"[ \t]*"); if (kv != null && kv.length == 2) {

               String k = kv[0], v = kv[1]; if (k != null && v != null) entries.add(Pair.of(k, v));
            }
         }

         map = entries.toArray(new Pair[entries.size()]);
      }

      return map;
   }

}
