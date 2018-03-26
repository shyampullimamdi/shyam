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

package com.ericsson.eea.ark.sli.grouping.services;

import java.util.Vector;

import org.apache.log4j.Logger;

import java.util.concurrent.Callable;

//import com.ericsson.eea.sli.grouping.config.Config;



import com.ericsson.eea.ark.offline.config.ServiceType_e;
import com.ericsson.eea.ark.sli.grouping.services.ServiceHandler_i;
import com.ericsson.eea.ark.sli.grouping.services.hadoop.GroupMapperDriver;


public class ServiceHandler implements Runnable {

   /** Reference to the logging facility.*/
   private static Logger log = Logger.getLogger(ServiceHandler.class);

   /** The remaining command line arguments, to be passed into the instantiated services.*/
   private String[] args;

   /** Book-keeping for the client handlers.*/
   private Vector<ServiceHandler_i> handlers;

   /** The daemon's instantiated service.*/
   private Callable<Integer> service;

   public ServiceHandler(String[] args) {

      this.args = args;
   }

   /** This {@linkplain Runnable#run} method allows for the {@linkplain ServiceHandler} to run as a daemon thread when invoked
    * via the command-line tool.*/
   @Override
   public void run() {

      try                             { init(args); start();                }
      catch (InterruptedException ex) { /*Ignore it.*/                      }
      catch (Throwable             t) { System.err.println(t.getMessage()); }
      finally                         { /*stop();*/                         }
   }


   /** Initialize the underlying communications protocol.*/
   public ServiceHandler init(String[] args) throws Exception {

      this.args = args;

      handlers  = new Vector<ServiceHandler_i>();

      service   = createService(ServiceType_e.RunHadoop);

      return this;
   }

   @SuppressWarnings({ "rawtypes", "unchecked"/*, "unused"*/ })
   private Callable<Integer> createService(ServiceType_e type) {

      Callable<Integer> s = null; if (type != null) switch (type) {

       //case              RunOozie  :
         case              RunHadoop :   s = new    GroupMapperDriver(args); break;
       //case              RunLocally: //s = new        ReplayRecords(args); break;
       //case MonitorAndUpdateRecords: //s = new MonitorUpdateRecords(args); break;
         default:                        break;
      }
      if (s == null) throw new RuntimeException("Error: Service not implemented: '" +type+ "'");

      return s;
   }


   /** Start up the protocol's end-point.*/
   public void start() throws Exception {

      service.call();
   }


   /** Shutdown the protocol's end-point.*/
   public void stop() {

//      service.stop();
   }


//   /** Receive a message.*/
//   public Serializable receive(long timeout) {
//      return service.receive(timeout);
//   }
//
//
//   /** Send a message.*/
//   public boolean send(Serializable message, long timout) {
//      return service.send(message, timout);
//   }


   /** Add a new handler to deamon's processing loop.*/
   public void addHandler(ServiceHandler_i h) {

      synchronized (h) {
         handlers.add(h);
      }
   }


   /** Remove the specified handler from deamon's processing loop.*/
   public void removeHandler(ServiceHandler_i h) {

      synchronized (h) {
         handlers.remove(h);
      }
   }


   /** Return a list of outstanding handlers.*/
   public ServiceHandler_i[] getHandlers() {

      return handlers.toArray(new ServiceHandler_i[handlers.size()]);
   }


   /** Terminate all handlers that at this point are still open.*/
   public void closeHandlers() {

      log.info("Closing client handlers...");

      ServiceHandler_i hs[] = getHandlers();

      for (int i=0, l=hs.length; i<l; ++i) {

         log.info("Closing client connection " + i);
//         hs[i].close();
      }
   }
}
