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

package com.ericsson.eea.ark.sli.grouping.rules.events;

import java.io.IOException;
import java.lang.reflect.Method;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.crypto.BadPaddingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bigdata.esr.parserlogic.CastingException;
import com.ericsson.eea.ark.offline.config.Config;
import com.ericsson.eea.ark.offline.rules.refdata.CRM;
import com.ericsson.eea.ark.offline.rules.refdata.REreference;
import com.ericsson.eea.ark.offline.rules.refdata.Terminal;
import com.ericsson.eea.ark.sli.grouping.services.hadoop.GroupMapperReducer;
import com.ericsson.eea.ark.sli.grouping.util.Util;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import net.minidev.json.JSONObject;

//import com.ericsson.bigdata.common.util.MatchableMap;
//import com.ericsson.bigdata.common.identifier.CellIdentifier;

import net.minidev.json.JSONValue;

/** Extends ESR to create convenient functions used to write Drools rules. */
// @ToString(of={})
public class Event_SNCD extends JSONObject {

    private static final Logger log = LoggerFactory.getLogger(Event_SNCD.class);

    private static final long serialVersionUID = 4351148500537923838L;

    @Data
    public class EventTypeKpiValue {

        Double value = null;
        Double weight = null;
    };

    @Getter         public  String                        imsi;
                    public  String                        imeitac;
    /**/ @Setter    public  CRM                           crm                    = null;
                    private Long                          timestamp              = null;
                    public  Terminal                      terminal               = null;


                    private Object trampoline = null;

    public Event_SNCD(String str) throws IOException
    ,                                   CastingException
    ,                                   InvalidKeyException
    ,                                   BadPaddingException
    ,                                   InvalidAlgorithmParameterException {

        super(Util.prune((JSONObject) JSONValue.parse(str)));

        initialise();
    }

    private void initialise() throws CastingException {
         Object imsiO= get(Config.cfg.getString(Config.cfg_prefix, "sncdevent.imsiKey"));
       if (imsiO !=null)
            this.imsi=imsiO.toString().isEmpty() ? null : imsiO.toString();
         Object imeitacO=get(Config.cfg.getString(Config.cfg_prefix, "sncdevent.imeitacKey"));
       if (imeitacO !=null)
             this.imeitac= imeitacO.toString().isEmpty() ? null : imeitacO.toString();
         this.timestamp =          getTimestamp();
//         this.setKpiSets();
         }


    public CRM getCrm() {

        if (imsi != null && crm == null) {

            REreference reference = REreference.getInstance();

            crm = reference.getCrm(imsi);
        }

        return (imsi == null || crm == null || crm.isEmpty()) ? null : crm;
    }

    public CRM getCrm(String fieldName) {

        if (imsi != null && crm == null) {

            REreference reference = REreference.getInstance();

            crm = reference.getCrm(imsi);
        }

        return (imsi == null || crm == null || crm.isEmpty()) ? null
                : (CRM) Event_SNCD.getValueFromObject(crm, fieldName);
    }

    public long getTimestamp() {

        if (timestamp == null) {
            Config.datefield=Config.cfg.getString(Config.cfg_prefix, "sncdevent.datefield");
            Object o = get(Config.datefield);
            //if (o == null)
              //  o = get("DATE");
            String ts = o == null ? null : o.toString();
            if (ts == null) {
               Config.datefieldFormat=Config.cfg.getString(Config.cfg_prefix, "sncdevent.datefieldFormat");
                log.info("Value of datefieldFormat"+Config.datefieldFormat );
            SimpleDateFormat sdf =  new SimpleDateFormat(Config.datefieldFormat);

                ts = sdf.format(new Date());
            }
            timestamp = Util.parseTimestamp(ts).getLeft();
        }

        return timestamp;
    }

    public Terminal getTerminal() {

        if (imeitac != null && terminal == null) {

            terminal = REreference.getInstance().getTerminal(imeitac);
        }

        return terminal;
    }

    public void setTerminal(Terminal terminal) {

        this.terminal = terminal;
    }

    public Object setTerminal(String tac) {

        if (tac != null && terminal == null) {

            if (imeitac == null)
                imeitac = tac;

            REreference reference = REreference.getInstance();

            terminal = reference.getTerminal(tac);
        }

        return terminal;
    }

    public Object getTerminal(String fieldName) {

        if (imeitac != null && terminal == null) {

            REreference reference = REreference.getInstance();

            terminal = reference.getTerminal(imeitac);
        }

        return (imeitac == null || terminal == null || terminal.isEmpty()) ? null
                : Event_SNCD.getValueFromObject(terminal, fieldName);
    }

    public Object getTerminal(String tac, String fieldName) {

        if (tac != null && terminal == null) {

            REreference reference = REreference.getInstance();

            terminal = reference.getTerminal(tac);
        }

        return (tac == null || terminal == null || terminal.isEmpty()) ? null
                : Event_SNCD.getValueFromObject(terminal, fieldName);
    }

    public Object get(String key, Object default_val) {

        return containsKey(key) ? get(key) : default_val;
    }

    @Override
    public String toString() {

        return "Event_SNCD(timestamp=" + timestamp + ", imsi=" + imsi + ", imeitac=" + imeitac + ", record="
                + this.toJSONString() + ")";
    }



       public static Object getValueFromObject(Object object, String fieldName) {

        if (fieldName == null || fieldName.length() == 0) {

            log.error("Empty field Name");
            return null;
        }

        // get class
        @SuppressWarnings("rawtypes")
        Class clazz = object != null ? object.getClass() : null;
        if (clazz == null) {

            return null;
        }

        // get method value using reflection
        String getterName = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);

        // log.debug("getter name =" + getterName);
        try {

            @SuppressWarnings("unchecked")
            Method method = clazz.getMethod(getterName);
            Object valueObject = method.invoke(object, (Object[]) null);

            return valueObject;
        } catch (Exception e) {

            log.error("error gettting  fieldname : " + fieldName, e);
        }

        return null;
    }

       public int getUsercategorymultiplier() {

        return 1;
    }


    public void setTrampoline(Object trampoline) {

        this.trampoline = trampoline;
    }

    /** Write the user group into MemDb. */
    public boolean write_userGroup(String user, String group, String percentage)
            throws IOException, InterruptedException {

        boolean rc = false;
        try {

            rc = ((GroupMapperReducer<?>) trampoline).write_userGroup(user, group, percentage);
        } catch (Throwable ex) {

            String msg = "Error in 'write_userGroup'";
            log.error(msg, ex);
        }

        return rc;
    }

    public <T> T getRuleParameter(String ruleId, String parameterKey, Class<T> type) {

        return Util.getRuleParameter(ruleId, parameterKey, type);
    }
}