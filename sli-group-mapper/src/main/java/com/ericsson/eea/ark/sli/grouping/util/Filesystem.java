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

import java.io.File;
import java.io.IOException;
import java.io.FilenameFilter;

import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



/** This class implements some filesystem-related utility functions that are used by the application.*/
public class Filesystem {

   /** The file name pattern used for fetching the ESR record files.*/
   protected static final Pattern fileNamePattern = Pattern.compile("esr_[0-9]+_([0-9]+)\\.log");


   /** Determine whether the given file matches the filename pattern for valid ESR record files.*/
   public static boolean isValidFile(File file) throws IOException {

      Matcher m = fileNamePattern.matcher(file.getName()); return m.matches();
   }


   /**
    * Return the @{linkplain File} handle for the specified directory, ensuring that such directory exists and is accessible.
    *
    * @param dirPath The path to the directory.
    */
   public static File getDirectory(String dirPath) throws IOException {

      File     dir = null;
      String   msg = null;
      Exception ex = null; try {

         dir = new File(/*Config.workingDir,*/ dirPath); if (!dir.exists())      msg =   "Cannot find directory: '" +dirPath+ "'.";
         else                                            if (!dir.isDirectory()) msg =         "Not a directory: '" +dirPath+ "'.";
      }
      catch (SecurityException exp) {                                  ex = exp; msg = "Cannot access directory: '" +dirPath+ "'."; }

      if (msg != null) throw (ex == null) ? new IOException(msg) : new IOException(msg, ex);

      return dir;
   }


   /**
    * Given the path for a directory, return the containing list of capture files.
    *
    * <P>The list only contains file names matching the {@linkplain Filesystem#fileNamePattern} file name pattern, for
    * which the start time stamps are not zero.
    *
    * @param dirPath The path to the directory containing capture files.
    */
   public static File[] getDirectoryFiles(String dirPath) throws IOException {

      FilenameFilter filter = null; {

         final String prefix = "esr_";

         filter = new FilenameFilter() {
            public boolean accept(File directory, String fileName) { return fileName.startsWith(prefix) && fileName.endsWith(".log"); }
         };
      }

      File[] files = Filesystem.getDirectoryFiles(dirPath, filter);

      return files;
   }


   /**
    * Return an array of @{linkplain File} handles for all directory entries matching the specified filter.
    *
    * @param dirPath The path to the directory.
    * @param filter  The directory file name filter.
    */
   public static File[] getDirectoryFiles(String         dirPath
      ,                                   FilenameFilter filter) throws IOException {

      File[] files = null;
      String   msg = null;
      Exception ex = null;
      File     dir = getDirectory(dirPath); try {

         files = dir.listFiles(filter);
      }
      catch (SecurityException exp) { ex = exp; msg = "Cannot access directory: '" +dir.getAbsolutePath() + "'."; }

      if (msg != null) throw (ex == null) ? new IOException(msg) : new IOException(msg, ex);

      return files;
   }


   /**
    * Sort a list files by the time-stamp in their names.
    *
    * <P>The list only contains file names matching the {@linkplain Filesystem#fileNamePattern} file name pattern, for
    * which the start time stamps are not zero. The resulting list is sorted by start time.
    */
   public static File[] sortFiles(File[] files) throws IOException {

      // Sort the files by the time-stamp in their file names. ----------------------------------------------------------------
      Comparator<File> c = new Comparator<File>() {

         public int compare(File a, File b) {

            // Extract the type, start, and end time-stamps from the file name (e.g.: esr_0_1395065112.bin)
            String ta = null
               ,   tb = null;

            Matcher ma = fileNamePattern.matcher(a.getName()); if (ma.matches()) switch (ma.groupCount()) { case 1: ta = ma.group(1); }  // fall-through
            Matcher mb = fileNamePattern.matcher(b.getName()); if (mb.matches()) switch (mb.groupCount()) { case 1: tb = mb.group(1); }  // fall-through

            long tsa = Long.parseLong(ta)
               , tsb = Long.parseLong(tb);

            int rc = 0; if (tsa  < tsb /*&& eta <= etb*/) rc = -1; // allow for range overlap
            else        if (tsa == tsb                  ) rc =  0;
            else                                          rc =  1;

            return rc;
         }
      };

      Arrays.sort(files, c);


      // Return the list of files. --------------------------------------------------------------------------------------------
      return files;
   }
}
