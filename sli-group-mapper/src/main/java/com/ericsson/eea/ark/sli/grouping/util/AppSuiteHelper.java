package com.ericsson.eea.ark.sli.grouping.util;

import java.util.ArrayList;
import java.util.regex.Pattern;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

public class AppSuiteHelper {

    public static void main(String[] args) {
        boolean explodeJars = false;
        if (args.length > 0 && "-explode".equals(args[0])) {
            explodeJars = true;
        }
        try {
            Collection<String> ress = getResources(explodeJars, Pattern.compile(".*"));
            for (String res : ress) {
                System.out.println(res);
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

       /**
        * for all elements of java.class.path get a Collection of resources Pattern
        * pattern = Pattern.compile(".*"); gets all resources
        *
        * @param pattern
        *            the pattern to match
        * @return the resources in the order they are found
        */
       public static Collection<String> getResources(
           final boolean explodeJars,
           final Pattern pattern){
           final ArrayList<String> retval = new ArrayList<String>();
           final String classPath = System.getProperty("java.class.path", ".");
           final String[] classPathElements = classPath.split(System.getProperty("path.separator"));
           for(final String element : classPathElements){
               try {
                   retval.addAll(getResources(explodeJars, element, pattern));
               } catch (Throwable t) {
                       System.out.println("***ERROR GETTING ALL RESOURCES FOR ELEMENT: \"" + element + "\" *** " + t);
               }
           }
           return retval;
       }

       private static Collection<String> getResources(
           final boolean explodeJars,
           final String element,
           final Pattern pattern){
           final ArrayList<String> retval = new ArrayList<String>();
           final File file = new File(element);
           if(file.isDirectory()){
               //System.out.println("GROUPMAPPERDRIVER.getResources: about to process directory(1): " + file.getAbsolutePath());
               retval.addAll(getResourcesFromDirectory(file, pattern));
           } else{
               if (file.toString().contains("*")) {
                   //System.out.println("GROUPMAPPERDRIVER.getResources: about to process jar files pattern: " + file.getAbsolutePath());
                   retval.addAll(getResourcesFromJarFiles(explodeJars, file, pattern));
               } else {
                   //System.out.println("GROUPMAPPERDRIVER.getResources: about to process jar file(1): " + file.getAbsolutePath());
                   retval.addAll(getResourcesFromJarFile(explodeJars, file, pattern));
               }
           }
           return retval;
       }

       private static Collection<? extends String> getResourcesFromJarFiles(final boolean explodeJars, final File jarfiles,
            final Pattern pattern) {
           final ArrayList<String> retval = new ArrayList<String>();
           File directory = jarfiles.getParentFile();
//	       System.out.println("directory: " + directory);
           String filenamepattern = jarfiles.getName();
//	       System.out.println("filenamepattern: " + filenamepattern);
           filenamepattern = filenamepattern.replaceAll("\\.","\\\\.");
           filenamepattern = filenamepattern.replaceAll("\\*",".*");
//	       System.out.println("filenamepattern2: " + filenamepattern);
           final Pattern patt = Pattern.compile(filenamepattern);
           final File[] fileList = directory.listFiles(new FilenameFilter(){
            @Override
            public boolean accept(File dir, String name) {
                return patt.matcher(name).matches();
            }});

           if (fileList == null) {
               //System.out.println("GROUPMAPPERDRIVER.getResourcesFromJarFiles: fileList returned *null* for pattern: \"" + filenamepattern + "\" = \"" + patt + "\"");
           } else {
               for(final File file : fileList){
                   //System.out.println("GROUPMAPPERDRIVER.getResources: about to process jar file(2): " + file.getAbsolutePath());
                   retval.addAll(getResourcesFromJarFile(explodeJars, file, pattern));
               }
           }
           return retval;
       }

       private static Collection<String> getResourcesFromJarFile(final boolean explodeJars,
           final File file,
           final Pattern pattern){
           final ArrayList<String> retval = new ArrayList<String>();
           if ( ! explodeJars) {
               retval.add(file.getAbsolutePath());
               return retval;
           }
           ZipFile zf;
           try{
               zf = new ZipFile(file);
           } catch(final ZipException e){
               throw new Error(e);
           } catch(final IOException e){
               throw new Error(e);
           }
           final Enumeration<? extends ZipEntry> e = zf.entries();
           while(e.hasMoreElements()){
               final ZipEntry ze = (ZipEntry) e.nextElement();
               final String fileName = ze.getName();
               final boolean accept = pattern.matcher(fileName).matches();
               if(accept){
                   retval.add(fileName);
               }
           }
           try{
               zf.close();
           } catch(final IOException e1){
               throw new Error(e1);
           }
           return retval;
       }

       private static Collection<String> getResourcesFromDirectory(
           final File directory,
           final Pattern pattern){
           final ArrayList<String> retval = new ArrayList<String>();
           final File[] fileList = directory.listFiles();
           for(final File file : fileList){
               if(file.isDirectory()){
                   //System.out.println("GROUPMAPPERDRIVER.getResources: about to process directory(2): " + file.getAbsolutePath());
                   retval.addAll(getResourcesFromDirectory(file, pattern));
               } else{
                   try{
                       final String fileName = file.getCanonicalPath();
                       final boolean accept = pattern.matcher(fileName).matches();
                       if(accept){
                           retval.add(fileName);
                       }
                   } catch(final IOException e){
                       throw new Error(e);
                   }
               }
           }
           return retval;
       }

}
