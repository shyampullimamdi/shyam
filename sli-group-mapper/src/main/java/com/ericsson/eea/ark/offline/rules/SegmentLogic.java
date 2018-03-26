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

package com.ericsson.eea.ark.offline.rules;

import lombok.Getter;
import lombok.Setter;
import lombok.NonNull;

import java.util.Set;
import java.util.List;
import java.util.Iterator;
import java.util.Collection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.drools.io.ResourceFactory;
import org.drools.time.SessionPseudoClock;
import org.drools.conf.EventProcessingOption;
import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseFactory;
import org.drools.KnowledgeBaseConfiguration;
import org.drools.definition.KnowledgePackage;
import org.drools.builder.ResourceType;
import org.drools.builder.KnowledgeBuilder;
import org.drools.builder.KnowledgeBuilderError;
import org.drools.builder.KnowledgeBuilderErrors;
import org.drools.builder.KnowledgeBuilderFactory;
import org.drools.builder.KnowledgeBuilderConfiguration;

import java.util.concurrent.TimeUnit;
//import java.util.concurrent.PriorityBlockingQueue;


import org.drools.runtime.conf.ClockTypeOption;
import org.drools.runtime.StatefulKnowledgeSession;
import org.drools.runtime.rule.ConsequenceException;
import org.drools.runtime.KnowledgeSessionConfiguration;


//import com.ericsson.eea.ark.offline.rules.QueueItem;

import com.ericsson.eea.ark.offline.utils.fs.InputFile;

//import com.ericsson.eea.ark.offline.utils.NGEEAlarmHelper;

//import com.ericsson.eea.ark.offline.rules.refdata.CRM;
import com.ericsson.eea.ark.offline.rules.events.Event;
import com.ericsson.eea.ark.offline.rules.events.Event_Generic;
import com.ericsson.eea.ark.offline.rules.refdata.REreference;
import com.ericsson.eea.ark.offline.rules.utils.Util;

//import com.ericsson.eea.ark.offline.rules.incidents.SubscriberIncidentReport;



/** The {@linkplain SegmentLogic} class maintains the Drools Logical Interface for one IMSI space.*/
public class SegmentLogic {

   private static final Logger log      = LoggerFactory.getLogger(SegmentLogic.class.getName());
   private static final Logger logDrool = LoggerFactory.getLogger("com.ericsson.cea.offline.profiler.rules");

   @Getter         @NonNull SessionPseudoClock               clock;
// @Getter @Setter @NonNull PriorityBlockingQueue<QueueItem> event_queue;
   @Getter         @NonNull StatefulKnowledgeSession         knowledgeSession;
   @Getter @Setter          int                              segmentId     = -1;
   @Getter                  long                             lastTimestamp =  0;


   public SegmentLogic(int                              segmentId/*
      ,                PriorityBlockingQueue<QueueItem> event_queue*/) throws SQLException
      ,                                                                     ClassNotFoundException {

      this.segmentId        = segmentId;
    //this.event_queue      = event_queue;
      this.knowledgeSession = createSession();
      this.clock            = knowledgeSession.getSessionClock();
   }


   /** Introduce an event into the local Knowledge base.*/
   public synchronized void receive(Event<?> event) {

      Object o      = event.getEvent();
      long event_tm = event.getTimestamp()
         , clock_tm = clock.getCurrentTime(), adv = event_tm - clock_tm; try {

         knowledgeSession.insert(o);

         if (log.isDebugEnabled()) log.debug("Knowledge facts count: " +knowledgeSession.getFactCount());

         // Don't advance the clock for generic events
         if (!(o instanceof Event_Generic)) {

            clock.advanceTime(adv, TimeUnit.MILLISECONDS);

            if (log.isDebugEnabled()) log.debug("Event: "+ o.getClass().getName() +". Advanced the clock by "+ adv +"ms ("+ event_tm +" - "+ clock_tm +")");
         }

         lastTimestamp = clock_tm;

         knowledgeSession.fireAllRules();
      }
      catch (ConsequenceException ex) {

         String msg = "Consequence error. Event in error: "+ o.toString(); log.error(msg, ex); throw new RuntimeException(msg, ex);
      }
      catch (Exception ex) {

         String msg = "Unexpected error."; log.error(msg, ex); throw new RuntimeException(msg, ex);
      }
   }


   /** A stateful Knowledge session that used to support one single IMSI space.*/
   protected StatefulKnowledgeSession createSession() throws SQLException
      ,                                                      ClassNotFoundException {

      // Create a new knowledge session
      KnowledgeSessionConfiguration conf_ks = KnowledgeBaseFactory.newKnowledgeSessionConfiguration(); {

         conf_ks.setOption(ClockTypeOption.get("pseudo"));

         conf_ks.setProperty("drools.compiler"             , "JANINO");
         conf_ks.setProperty("drools.dialect.java.compiler", "JANINO");
      }


      // Finally, create a new stateful knowledge session
      KnowledgeBase            kb = loadRuleBase();
      StatefulKnowledgeSession ks = kb.newStatefulKnowledgeSession(conf_ks, null); {

         ks.setGlobal("util", new Util());

         // set logger for knowledgeSession.
         ks.setGlobal("log", logDrool);

       //ks.setGlobal("TYPE_DOUBLE" , CRM.TYPE_DOUBLE );
       //ks.setGlobal("TYPE_INTEGER", CRM.TYPE_INTEGER);
       //ks.setGlobal("TYPE_STRING" , CRM.TYPE_STRING );
       //ks.setGlobal("TYPE_LONG"   , CRM.TYPE_LONG   );

       //ks.setGlobal("services"    , new SegmentIncidentReport(this.getSegmentId(), this.getIncident_queue(), ks));
       //ks.setGlobal("services"    , new SubscriberIncidentReport(this.getSegmentId(), /*this.getIncident_queue(),*/ ks));
       //ks.setGlobal("apn_services", new EventType_APN(0, ""));
      }


      // Fire the rules in the newly populate knowledge session before returning
      ks.fireAllRules();

      return ks;
   }


   /**
    * Returns the Drools knowledgeBase.<p>
    *
    * The knowledge base is loaded from a rule file specified in the Rule Engine configuration file.
    */
   protected KnowledgeBase loadRuleBase() {

      // Create a new knowledge builder
      KnowledgeBuilderConfiguration conf_b = KnowledgeBuilderFactory.newKnowledgeBuilderConfiguration(); {

         // THIS DID NOT WORK, I HAD TO PATCH THE DROOLS-COMPILER, WITH AN UPDATED: 'drools.default.packagebuilder.conf'
//         // Switching compiler, since when running in Hadoop, the default drools compiler (ecj) conflicts with the Hadoop
//         // environment. Hadoop has 2 different Jars containing a different version of ecj: 'aspecttools-1.6.5' and 'core-3.1.1'
//         // They are both in front of the application's class path, thus shadowing the drool 'ecj-3.5.1'
         conf_b.setProperty("drools.compiler"             , "JANINO");
         conf_b.setProperty("drools.dialect.java.compiler", "JANINO");
      }
      KnowledgeBuilder builder = KnowledgeBuilderFactory.newKnowledgeBuilder(conf_b);


      // Get a list of all rules
      RuleEngine.init();
      List<String> allDrlFiles = RuleEngine.getRuleList(); for (String file : allDrlFiles) {

         if (log.isInfoEnabled()) log.info("Inserting rule file into the knowledge base: '"+file+"'");

         try {

//            if (!RuleEngine.ZK_SUPPORT) {

               builder.add(ResourceFactory.newInputStreamResource(new InputFile(file).inputStream()), ResourceType.DRL);
//            }
//            else {
//
//               // get content from Zookeeper
//               InputStream is = RuleEngine.zkDsForFiles.getInputStreamFromZK(filePath);
//
//               builder.add(ResourceFactory.newInputStreamResource(is), ResourceType.DRL);
//            }
         }
         catch (Exception ex) {

            String msg = "Unexpected error"; log.error(msg, ex); throw new RuntimeException(msg, ex);
         }
      }

      if (builder.hasErrors()) {

         KnowledgeBuilderErrors errors = builder.getErrors(); if (errors.size() > 0) {

            for (KnowledgeBuilderError error : errors) {

               log.error(error.getMessage());
               //NGEEAlarmHelper.logAlarm(NGEEAlarmHelper.PROCESSING_ERROR, "Could not build  knowledge base:" + error.getMessage());
            }
         }

         String msg = "Could not parse rule file(s): "+allDrlFiles; log.error(msg); throw new RuntimeException(msg);
      }


      // Create a new knowledge base
      KnowledgeBaseConfiguration conf_kb = KnowledgeBaseFactory.newKnowledgeBaseConfiguration(); {

         conf_kb.setOption(EventProcessingOption.STREAM);

         conf_kb.setProperty("drools.compiler"             , "JANINO");
         conf_kb.setProperty("drools.dialect.java.compiler", "JANINO");
      }

      KnowledgeBase kb = KnowledgeBaseFactory.newKnowledgeBase(conf_kb); {

         kb.addKnowledgePackages(builder.getKnowledgePackages());
      }


      // Remove the rules from base file, which are marked for exclusion
      Iterator<KnowledgePackage>   it3       = null;
      Collection<KnowledgePackage> kPackages = kb.getKnowledgePackages();
      Set<String> rulesToExclude = REreference.getInstance().getRuleIdParms().getRulesToExclude(); if (rulesToExclude != null) {

         Iterator<String> it2 = rulesToExclude.iterator(); while (it2.hasNext()) {

            String ruleName = it2.next();
            it3             = kPackages.iterator(); while (it3.hasNext()) try {

               KnowledgePackage kP          = it3.next();
               String           packageName = kP.getName();

               if (kb.getRule(packageName, ruleName) != null) {

                  kb.removeRule(packageName, ruleName);

                  if (log.isDebugEnabled()) log.debug("Removed excluded rule: "+ruleName+" from the Knowledge base.");
               }
            }
            catch (Exception ex) {

               log.warn("Rules exclusion: "+ex.getMessage());
            }
         }
      }


      // print all rules in knowledge base Informational only.
      it3 = kPackages.iterator(); while (it3.hasNext()) {

         KnowledgePackage                            kP    = it3.next();
         Collection<org.drools.definition.rule.Rule> rules = kP.getRules();

         Iterator<org.drools.definition.rule.Rule> it4 = rules.iterator(); while (it4.hasNext()) {

            org.drools.definition.rule.Rule r = it4.next();

            if (log.isInfoEnabled()) log.info("Active rule: " +kP.getName()+ "." +r.getName());
         }
      }

      return kb;
   }
}
