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

package com.ericsson.eea.ark.sli.grouping.db;

import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DatabaseMetaData;

import com.ericsson.eea.ark.offline.utils.NGEEAlarmHelper;
import com.ericsson.eea.ark.offline.utils.NGEEAlarmHelper.AlarmType;


/**
 * @author ejoepao SQLConnection : maintain SQL connections.
 */
public class SQLConnection {

   private static final Logger log = LoggerFactory.getLogger(SQLConnection.class);

   private String                    username;
   private String                    password;
   private Vector<SQLConnectionBase> connection = new Vector<SQLConnectionBase>();


   // This is never used.
   // public static String CellLocationTable;
   // public static String CellLocationDB;
   // public static int mapZoom;

   // this is never used.
   // public static String proxyHost;
   // public static String proxyPort;
   // public static HashMap<String,Integer> usergroup;


   public SQLConnection() {

      connection = new Vector<SQLConnectionBase>();
   }


   public SQLConnection(String un, String pw) {

      username = un;
      password = pw;
   }


   public synchronized void addConnection(SQLConnectionBase base) {

      connection.add(base);
   }


   public synchronized Statement getNewStatement(String dbname) throws ClassNotFoundException {

      return getNewStatement(dbname, null);
   }


   public synchronized Statement getNewStatement(String dbname, String info) throws ClassNotFoundException {

      String host = null;
      String port = null; for (int i = 0; i < connection.size(); i++) {

         if (connection.get(i).database.equals(dbname) && (info == null || connection.get(i).info.equals(info))) {

            host = connection.get(i).host;
            port = connection.get(i).port; break;
         }
      }
      if (host == null) return null;

      Class.forName("org.postgresql.Driver"); // load the driver

      String    database = null;
      Statement sql      = null; try {

         database              = "//" + host + ":" + port + "/" + dbname;
         Connection       db   = DriverManager.getConnection("jdbc:postgresql:" + database, username, password); // connect to the db
         DatabaseMetaData dbmd = db.getMetaData(); // get MetaData to confirm connection

         log.debug("Connection to " + dbmd.getDatabaseProductName() + " " + dbmd.getDatabaseProductVersion() + " successful.\n");

         sql = db.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
      }
      catch (Exception ex) {

         log.error("Database " + dbname + " at " + host + ":" + port + " is unreachable!", ex);

         NGEEAlarmHelper.logAlarm(AlarmType.databaseError, ex.getMessage()); return null;
      }

      return sql;
   }
}
