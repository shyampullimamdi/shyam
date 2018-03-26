package com.ericsson.eea.ark.sli.grouping.memdb;

import java.io.IOException;
//import java.lang.reflect.InvocationTargetException;
//import java.lang.reflect.Method;
//import java.net.URL;
//import java.net.URLDecoder;
//import java.util.Enumeration;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//import java.util.Set;
//import java.util.zip.ZipEntry;
//import java.util.zip.ZipFile;




//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.util.JarFinder;
//import org.apache.hadoop.util.StringUtils;


import com.ericsson.eea.ark.sli.grouping.services.hadoop.GroupMapperReducerMemDb;

public class ImsiGrpMemDbMapReduceUtil {

//	private final static Log LOG = LogFactory.getLog(ImsiGrpMemDbMapReduceUtil.class);

    public static void initTableReducerJob(
            Class<? extends GroupMapperReducerMemDb> reducer,
            Job job) throws IOException {

        Configuration conf = job.getConfiguration();
        job.setOutputFormatClass(ImsiGrpMemDbOutputFormat.class);
        if (reducer != null) job.setReducerClass(reducer);
        conf.setStrings("io.serializations", conf.get("io.serializations"),
            ImsiGrpMemDbPutSerialization.class.getName());
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ImsiGrpMemDbPut.class);
//	    addDependencyJars(job);
//	    initCredentials(job);
    }

}
