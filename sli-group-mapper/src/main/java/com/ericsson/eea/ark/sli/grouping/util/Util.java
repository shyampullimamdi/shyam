/*******************************************************************************
 * Copyright (c) 2014 Ericsson, Inc. All Rights Reserved.
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

package com.ericsson.eea.ark.sli.grouping.util;

import java.net.URI;

import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.GregorianCalendar;

import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.ericsson.eea.ark.offline.config.Config;


//import java.lang.reflect.Field;
//import java.lang.reflect.Modifier;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.ericsson.bigdata.esr.parserlogic.CastingException;
import com.ericsson.eea.ark.offline.rules.events.Event_Generic;
import com.ericsson.eea.ark.offline.rules.refdata.REreference;
import com.ericsson.eea.ark.offline.rules.refdata.RuleIdParameter;


/** This class implements some utility functions used by the application to deal with reflection.*/
public class Util {

   final protected static Logger  log   = Logger.getLogger(Util.class);
   final protected static boolean debug = log != null && log.isDebugEnabled();

   /** Memoization table.*/
   private static HashMap<String,Object> memo = new HashMap<String,Object>();

   /** Argument which is a place-holder for Class[] {}, which otherwise would
    *  need to be created throughout the code.*/
   @SuppressWarnings("rawtypes")
   public static final Class[] emptyClassArray_ = new Class[] {};

   /** Argument which is a place-holder for Object[] {}, which otherwise would
    *  need to be created throughout the code.*/
   public static final Object[] emptyObjectArray_ = new Object[] {};


   /** Return the current time in the form "yyyyMMddHHmmss.S" as a String.*/
   public static String getTimeStamp() {

      return new SimpleDateFormat("yyyyMMddHHmmss.S").format(new GregorianCalendar().getTime());
   }


   // --------------------------------------------------------------------------
   /** Creates the specified object by fetching the default Constructor associated
    *  to the requested object name.  This method always
    *  throws an exception in case of an error.*/
   // --------------------------------------------------------------------------
   public static Object create(String name) throws RuntimeException {

      return create(name, emptyObjectArray_, true);
   }


   // --------------------------------------------------------------------------
   /** Creates the specified object by fetching the default Constructor associated
    *  to the requested object name.  This method always
    *  throws an exception in case of an error.*/
   // --------------------------------------------------------------------------
   @SuppressWarnings("rawtypes")
   public static Object create(Class cls) throws RuntimeException {

      return create(cls, emptyObjectArray_, true);
   }


   // --------------------------------------------------------------------------
   /** Creates the specified object by fetching the Constructor associated
    *  to the requested object name and parameter types.  This method always
    *  throws an exception in case of an error.*/
   // --------------------------------------------------------------------------
   public static Object create(String name
      ,                        Object arg) throws RuntimeException {

      return create(name, arg, true);
   }


   // --------------------------------------------------------------------------
   /** Creates the specified object by fetching the Constructor associated
    *  to the requested object name and parameter types.  This method has a
    *  flag to indicate if an exception should be thrown.*/
   // --------------------------------------------------------------------------
   @SuppressWarnings("rawtypes")
   public static Object create(String  name
      ,                        Object  arg
      ,                        boolean throwExcp) throws RuntimeException {

      if (name == null || name.length() == 0) {

         throw new RuntimeException("A valid class name must be specified");
      }

      // Created the object ----------------------------------------------------
      Object           obj = null;
      RuntimeException excp = null;
      try {

         // Create the object instance by reflectively invoking the class' constructor.
         Constructor ctor = (Constructor) creator(name, arg, throwExcp);

         // Creates the array of arguments to be used on the constructor call.
         boolean isArray = arg == null ? false : arg.getClass().isArray();

         Object[] ctorArgs = isArray      ? (Object[]) arg
            :                (arg == null ? emptyObjectArray_
            :                               new Object[] {arg});

         obj = ctor.newInstance(ctorArgs);
      }
      catch (RuntimeException ex) {

         if (throwExcp) excp = ex;
      }
      catch (InstantiationException ex) { if (throwExcp)

         excp = new RuntimeException("Cannot Instantiate Class " +name, ex);
      }
      catch (InvocationTargetException ex) { if (throwExcp)

         excp = new RuntimeException("An exception was thrown by the " +name+ " class' constructor: " +ex, ex);
      }
      catch (IllegalAccessException ex) { if (throwExcp)

         excp = new RuntimeException("Cannot invoke non-public constructor in the " +name+" class.", ex);
      }

      // Should we throw exception if the ctor is not found ? ------------------
      if (throwExcp && excp != null) throw excp;

      // Return the constructed Object. ----------------------------------------
      return obj;
   }


   // --------------------------------------------------------------------------
   /** Creates the specified object by fetching the Constructor associated
    *  to the requested object name and parameter types.  This method has a
    *  flag to indicate if an exception should be thrown.*/
   // --------------------------------------------------------------------------
   @SuppressWarnings("rawtypes")
   public static Object create(Class   cls
      ,                        Object  arg
      ,                        boolean throwExcp) throws RuntimeException {

      if (cls == null) {

         throw new RuntimeException("A valid class must be specified");
      }

      // Created the object ----------------------------------------------------
      String           name = cls.getSimpleName();
      Object           obj  = null;
      RuntimeException excp = null;
      try {

         // Create the object instance by reflectively invoking the class' constructor.
         Constructor ctor = (Constructor) creator(cls, arg, throwExcp);

         // Creates the array of arguments to be used on the constructor call.
         boolean isArray = arg == null ? false : arg.getClass().isArray();

         Object[] ctorArgs = isArray      ? (Object[]) arg
            :                (arg == null ? emptyObjectArray_
            :                               new Object[] {arg});

         obj = ctor.newInstance(ctorArgs);
      }
      catch (RuntimeException ex) {

         if (throwExcp) excp = ex;
      }
      catch (InstantiationException ex) { if (throwExcp)

         excp = new RuntimeException("Cannot Instantiate Class " +name, ex);
      }
      catch (InvocationTargetException ex) { if (throwExcp)

         excp = new RuntimeException("An exception was thrown by the " +name+ " class' constructor: " +ex, ex);
      }
      catch (IllegalAccessException ex) { if (throwExcp)

         excp = new RuntimeException("Cannot invoke non-public constructor in the " +name+" class.", ex);
      }

      // Should we throw exception if the ctor is not found ? ------------------
      if (throwExcp && excp != null) throw excp;

      // Return the constructed Object. ----------------------------------------
      return obj;
   }


   // --------------------------------------------------------------------------
   /** Fetches the creator to be used for creating the specified object.
    *  This method always throws an exception in case of an error.*/
   // --------------------------------------------------------------------------
   public static Object creator(String name
      ,                         Object arg) throws RuntimeException {

      return creator(name, arg, true);
   }


   // --------------------------------------------------------------------------
   /** Fetches the creator to be used for creating the specified object.
    *  This method has a to indicate if an exception should be thrown.*/
   // --------------------------------------------------------------------------
   public static Object creator(String  name
      ,                         Object  arg
      ,                         boolean throwExcp) throws RuntimeException {

      // Get the constructor signature -----------------------------------------
      @SuppressWarnings("rawtypes")
      Class[] sig = getMethodSignature(arg);

      return creator(name, sig, throwExcp);
   }


   // --------------------------------------------------------------------------
   /** Fetches the creator to be used for creating the specified object.
    *  This method has a to indicate if an exception should be thrown.*/
   // --------------------------------------------------------------------------
   @SuppressWarnings("rawtypes")
   public static Object creator(Class   cls
      ,                         Object  arg
      ,                         boolean throwExcp) throws RuntimeException {

      // Get the constructor signature -----------------------------------------
      Class[] sig = getMethodSignature(arg);

      return creator(cls, sig, throwExcp);
   }


   // --------------------------------------------------------------------------
   /** Fetches the creator to be used for creating the specified object.
    *  This method has a to indicate if an exception should be thrown.*/
   // --------------------------------------------------------------------------
   @SuppressWarnings({ "rawtypes", "unchecked" })
   public static Object creator(String  name
      ,                         Class[] sig
      ,                         boolean throwExcp) throws RuntimeException {

      // Try to find the constructor matching the provided argument type(s),
      // if any. ---------------------------------------------------------------
      Constructor      ctor = null;
      RuntimeException excp = null;
      try {

         // Try fetching the ctor ----------------------------------------------
         Class cls = Class.forName(name);

         ctor = cls.getConstructor(sig);
      }
      catch (ClassNotFoundException ex) { if (throwExcp)

         excp = new RuntimeException("Cannot create class " +name, ex);
      }
      catch (IllegalArgumentException ex) { if (throwExcp)

         excp = new RuntimeException("Illegal argument passed on to the constructor: " +sig, ex);
      }
      catch (NoSuchMethodException ex) {if (throwExcp)

         excp = new RuntimeException("No matching constructor defined in the " +name+ " class, which takes " +sig, ex);
      }

      // Should we throw exception if the ctor is not found ? ------------------
      if (throwExcp && excp == null && ctor == null) {

         excp = new RuntimeException("No matching constructor defined in the " + name + " class.");
      }

      if (throwExcp && excp != null) throw excp;

      // Return the Constructor as an Object. ----------------------------------
      return ctor;
   }


   // --------------------------------------------------------------------------
   /** Fetches the creator to be used for creating the specified object.
    *  This method has a to indicate if an exception should be thrown.*/
   // --------------------------------------------------------------------------
   @SuppressWarnings({ "rawtypes", "unchecked" })
   public static Object creator(Class   cls
      ,                         Class[] sig
      ,                         boolean throwExcp) throws RuntimeException {

      // Try to find the constructor matching the provided argument type(s),
      // if any. ---------------------------------------------------------------
      String           name = cls.getSimpleName();
      Constructor      ctor = null;
      RuntimeException excp = null;
      try {

         // Try fetching the ctor ----------------------------------------------
         ctor = cls.getConstructor(sig);
      }
      catch (IllegalArgumentException ex) { if (throwExcp)

         excp = new RuntimeException("Illegal argument passed on to the constructor: " +sig, ex);
      }
      catch (NoSuchMethodException ex) {if (throwExcp)

         excp = new RuntimeException("No matching constructor defined in the " +name+ " class, which takes " +sig, ex);
      }

      // Should we throw exception if the ctor is not found ? ------------------
      if (throwExcp && excp == null && ctor == null) {

         excp = new RuntimeException("No matching constructor defined in the " + name + " class.");
      }

      if (throwExcp && excp != null) throw excp;

      // Return the Constructor as an Object. ----------------------------------
      return ctor;
   }


   /** Given a class, returns the class name without the package name.*/
   @SuppressWarnings("rawtypes")
   public static String getClassName(Class clazz) {

      String className   = clazz.getName();
      String packageName = clazz.getPackage().getName();

      int classNameLen   = className.length();
      int packageNameLen = packageName.length();

      String name = className.substring(packageNameLen +1, classNameLen);

      return name;
   }


   // --------------------------------------------------------------------------
   /** Obtain the constructor signature, based on the received argument(s).*/
   // --------------------------------------------------------------------------
   @SuppressWarnings({ "unchecked", "rawtypes" })
   public static Class[] getMethodSignature(Object arg) {

      // Creates the Class array representing the constructor signature. -------
      Class[] ctorSig = null;

      if (arg == null) {

         ctorSig = emptyClassArray_;
      }
      else {

         boolean isArray = arg.getClass().isArray(); if (!isArray) {

            ctorSig = new Class[] {arg.getClass()};
         }
         else {

            // Create the array of classes composing the ctor's signature ------
            Object[] args = (Object[]) arg;
            ArrayList array = new ArrayList();
            for (int idx=0; idx<args.length; idx++) {

               array.add(args[idx].getClass());
            }
            ctorSig = (Class[]) array.toArray(emptyClassArray_);
         }
      }

      return ctorSig;
   }


   public static <K,V> Pair<K,V>[] zip(K[] keys, V[] vals) {

      int                                        len = Math.min(keys.length, vals.length);
      @SuppressWarnings("unchecked") Pair<K,V>[] map = new Pair[len]; for (int i=0; i<len; ++i) {

         map[i] = Pair.of(keys[i], vals[i]);
      }
      return map;
   }


   @SuppressWarnings("unchecked")
   public static <K,V> K[] keys(Pair<K,V>[] map) {

      Object[] keys = null; if (map != null) {

         int len = map.length; keys = new Object[len]; int i=0; for (Pair<K,V> p : map) {

            keys[i++] = p.getLeft();
         }
      }

      return (K[]) keys;
   }


   public static <K,V> V get(K key, Pair<K,V>[] map) {

      if (key != null) {

         int len = map.length; for (int i=0; i<len; ++i) {

            Pair<K,V> p = map[i]; if (key.equals(p.getLeft())) return p.getRight();
         }
      }

      return null;
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


   public static <O extends Object> String show(O[] objs) {

      StringBuffer sb = new StringBuffer(); {

         sb.append('['); int i=0; for (Object o : objs) sb.append((i++ == 0) ? "\"" : ",\"").append(o.toString()).append('"'); sb.append(']');
      }
      return sb.toString();
   }


   public static Pair<String,String> getPair(String delimitedString, char delimiter) {

      Pair<String,String> p = null;

      if (delimitedString != null && delimitedString.length() >0) {

         int delimIdx = delimitedString.indexOf(delimiter); if (delimIdx >0) {

            String x = delimitedString.substring(0, delimIdx);
            String y = delimitedString.substring(delimIdx +1);

            p = Pair.of(x, y);
         }
      }

      return p;
   }


   /** Returns a string in the form {@code '<value-1>(, *<value-n>)*'} as list of strings.*/
   public static String[] getList(String listString, String[] def) {

      return getList(listString, def, ',');
   }


   /** Returns a string in the form {@code '<value-1>(, *<value-n>)*'} as list of strings.*/
   public static String[] getList(String listString, String[] def, char sep) {

      String[] list = def;

      if (listString != null) {

         ArrayList<String> entries = new ArrayList<String>();

         String[] vs = listString.split("[ \t]*,[ \t]*"); if (vs != null) for (String v : vs) {

            if (v != null && v.length() > 0) entries.add(v);
         }

         list = entries.toArray(new String[entries.size()]);
      }

      return list;
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


   public static Pair<String,Integer> getHostPort(String hostPortStr) {

      Pair<String,Integer> p = null;
      Pair<String,String> hp = getPair(hostPortStr, ':'); if (hp != null) {

         String host = hp.getKey();
         String port = hp.getValue();
         if (host != null && host.length() >0 && port != null && port.length() >0) {

            p = Pair.of(host,Integer.parseInt(port));
         }

         if (p == null) {

            log.error("Cannot convert string value to integer: \"" +port+ "\""); return null;
         }
      }

      return p;
   }


   public static String getCamelCaseName(String cppName) {

      String intermediateName = cppName.replaceAll("\\W",  "_");
      String outName = "";
      char chars[] = new char[intermediateName.length()];
      intermediateName.getChars(0, intermediateName.length(), chars, 0);
      boolean allUpper = intermediateName.toUpperCase().equals(intermediateName);
      if ( ! allUpper && intermediateName.indexOf('_') == -1
            && ! Character.isUpperCase(intermediateName.charAt(0))) {
         return outName = intermediateName;
      }
      // string either contains non-alpha-numeric characters (i.e. '_'),
      // or is all upper case
      // or starts with an upper-case letter
      // so...
      boolean beginning = true, nextIsUpper = false;
      for (char c : chars ) {
         if (c == '_') {
            if (beginning) {
               continue;
            } else {
               nextIsUpper = true;
            }
         } else {
            if (beginning) {
               beginning = false;
               if (Character.isDigit(c)) {
                  outName += "_";
               }
               outName += allUpper ? Character.toString(c).toLowerCase() : c;
            } else if (nextIsUpper) {
               nextIsUpper = false;
               outName += Character.toString(c).toUpperCase();
            } else {
               outName += allUpper ? Character.toString(c).toLowerCase() : c;
            }
         }
      }
      return outName;
   }


   public static String getAssociatedArrayName(String primitiveCamelCaseFieldName) {

      if (primitiveCamelCaseFieldName.endsWith("Array")) return primitiveCamelCaseFieldName;

      return primitiveCamelCaseFieldName + "Array";
   }


   public static String getAssociatedPrimitiveFieldName(String camelCaseArrayName) {

      if (camelCaseArrayName.endsWith("Array") && camelCaseArrayName.length() > 5) return camelCaseArrayName.substring(0,camelCaseArrayName.length()-5);

      return camelCaseArrayName;
   }


   public static String getArrayElementName(String camelCaseName, int i) {

      if (camelCaseName.endsWith("Array") && camelCaseName.length() > 5) {

         return camelCaseName.substring(0, camelCaseName.length() - 5) + "_" + (i + 1);
      }
      else {

         return camelCaseName + "_" + (i + 1);
      }
   }


   @SuppressWarnings("rawtypes")
   public static String[] enumNames(Enum... items) {

      if (items == null) return null;

      ArrayList<String> res = new ArrayList<String>(); if (items != null) for (Enum e : items) {

         if (e != null) res.add(e.name());
      }

      return res.toArray(new String[res.size()]);
   }


   private static final Pattern var_pat = Pattern.compile("^([^$]*)(\\$\\{([0-9A-Za-z_.]+)\\})?(.*)$");

   public static String interpolate(String text) {

      String res = text; if (text != null && text.length() >0) {

         res = ""; Matcher var_match = var_pat.matcher(text); while (var_match.matches()) {

            String prefix = var_match.group(1)     ; if (prefix != null && prefix.length() >0) res += prefix      ;
            String var    = var_match.group(3)     ; if (var    != null && var   .length() >0) {
               String val = System.getenv(var)     ; if (val    != null && val   .length() >0) res += val         ; else {
               /**/   val = System.getProperty(var); if (val    != null && val   .length() >0) res += val         ; else
               /**/                                                                            res += "${"+var+"}";
            }}
            String suffix = var_match.group(4)     ; if (suffix != null && suffix.length() >0) { var_match = var_pat.matcher(suffix); continue; }
            else                                                                                                                      break   ;
         }
      }

      return res;
   }


   public static String listAsString(String[] list, String sep) {

      String res = ""; if (list != null) {

         String s = Arrays.toString(list); if (s.length() >= 2) res = s.substring(1, s.length()-1).replaceAll(", *", sep);
      }

      return res;
   }


   public static <T extends Comparable<? super T>> ArrayList<T> asSortedList(Collection<T> c) {

      ArrayList<T> list = new ArrayList<T>(c); java.util.Collections.sort(list);

      return list;
   }


//   /** Dangerous method, use with extreme caution.*/
//   static void setFinalStatic(Field field, Object newValue) throws Exception {
//
//      field.setAccessible(true);
//
//      // remove final modifier from field
//      Field modifiersField = Field.class.getDeclaredField("modifiers"); {
//
//         modifiersField.setAccessible(true);
//         modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
//      }
//
//      field.set(null, newValue);
//   }


   public static byte[] getBytes(String text) {

      byte[] buf = null; try {

         buf = text.getBytes(Charsets.UTF_8);
      }
      /*catch ()*/finally {

         /*No-op*/
      }

      return buf;
   }


   public static byte[] getBytesMD5(String text) {

      byte[] buf = null; try {

         MessageDigest md = MessageDigest.getInstance("MD5");
         buf              = md.digest(text.getBytes());
//         StringBuffer sb = new StringBuffer();
//         for (int i = 0; i < buf.length; ++i) {
//
//            sb.append(Integer.toHexString((buf[i] & 0xFF) | 0x100).substring(1,3));
//         }
//         return sb.toString();
      }
      catch (NoSuchAlgorithmException e) {

         // The hash algorithm is not parameterized, MD5 is guaranteed to be available.
      }

      return buf;
   }


   public static String intersperse(String sep, String[] ss) {

      StringBuffer b     = new StringBuffer();
      boolean      first = true; if (ss != null) for (String s : ss) { if (s == null) continue;

         if (first) { b.append(s); first = false; }
         else       { b.append(sep).append(s);    }
      }

      return b.toString();
   }


   /**
    * Helper static method to prune empty nodes in a JSON tree.
    */
   @SuppressWarnings("unchecked")
   public static Map<String, Object> prune(Map<String, Object> node) {

      Iterator<Entry<String, Object>> it = node.entrySet().iterator(); while (it.hasNext()) {

         Entry<String, Object> child = it.next(); if (child.getValue() == null) {

            it.remove();
         }
         else if (child.getValue() instanceof Map<?, ?>) {

            Map<String, Object> m = (Map<String, Object>) child.getValue();

            prune(m); if (m.isEmpty()) {

               it.remove();
            }
         }
         else if (child.getValue() instanceof List<?>) {

            ArrayList<Object> list = (ArrayList<Object>) child.getValue(); for (Object o : list) {

               if (o instanceof Map<?, ?>) {

                  prune((Map<String, Object>) o);
               }
            }
         }
      }

      return node;
   }


   public static FileSystem getFilesystem(Configuration conf, String path) throws Exception {

      FileSystem fs = null;

      String prefix  = null, host = null, port = null; //, path = null

      if (path != null && !path.isEmpty()) {

         Pattern p = Pattern.compile("^((file|hdfs):///?)([^:]*):([^/]*)(.*)$");
         Matcher m = p.matcher(path); if (m != null && m.matches()) {

            prefix = m.group(1); host = m.group(3); port = m.group(4); //path = m.group(5);
         }
         //String fsPrefix = path.matches("^hdfs://.*$") ? "hdfs://" : "file:///";  //conf.get("fs.default.name")

         fs = (Config.dfsPrefix       != null) ? FileSystem.get(new URI(Config.dfsPrefix), conf)
            : (prefix == null || host == null) ? FileSystem.getLocal(conf)
            :                                    FileSystem.get(new URI(prefix+host+(port != null? ':'+port : "")), conf)
         ;
      }

      return fs;
   }


   /**
    * @return the parameter Key value of the requested type.
    */
   @SuppressWarnings("unchecked")
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
}
