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

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
//import lombok.ToString;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.Date;
import java.util.List;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.TreeMap;
//import java.util.SortedMap;
//import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

import javax.crypto.BadPaddingException;

import java.security.InvalidKeyException;
import java.text.SimpleDateFormat;
import java.security.InvalidAlgorithmParameterException;

//import com.ericsson.bigdata.common.util.MatchableMap;
//import com.ericsson.bigdata.common.identifier.CellIdentifier;

import net.minidev.json.JSONValue;
import net.minidev.json.JSONObject;

import com.ericsson.bigdata.esr.dataexchange.KpiDesc;
import com.ericsson.bigdata.esr.dataexchange.KpiRecord;
import com.ericsson.bigdata.esr.parserlogic.CastingException;
import com.ericsson.eea.ark.offline.config.Config;
import com.ericsson.eea.ark.offline.rules.refdata.CRM;
import com.ericsson.eea.ark.offline.rules.refdata.REreference;
import com.ericsson.eea.ark.offline.rules.refdata.Terminal;
import com.ericsson.eea.ark.sli.grouping.services.hadoop.GroupMapperReducer;
import com.ericsson.eea.ark.sli.grouping.util.Util;

/** Extends ESR to create convenient functions used to write Drools rules. */
// @ToString(of={})
public class Event_DPA extends JSONObject {

    private static final Logger log = LoggerFactory.getLogger(Event_DPA.class);

    private static final long serialVersionUID = 4351148500537923838L;

    @Data
    public class EventTypeKpiValue {

        Double value = null;
        Double weight = null;
    };

    @Getter         public  String                        imsi;
    /**/            public  String                        imeitac;
    /**/            private Long                          timestamp              = null;
    /**/ @Setter    public  CRM                           crm                    = null;
    /**/            public  Terminal                      terminal               = null;
    @Getter @Setter private Boolean                       hide                   = false; // This field is set based on WhiteList // processing
    @Getter @Setter private Set<String>                   kpiSetBySource         = null; // KPI by source;kpi
    @Getter @Setter private Set<String>                   kpiSetBySourceAndlocId = null; // KPI by   // source;kpi;locId
    @Getter @Setter private Map<KpiDesc, List<KpiRecord>> ottKpiM                = null;

    private Object trampoline = null;

    public Event_DPA(String str) throws IOException
    ,                                   CastingException
    ,                                   InvalidKeyException
    ,                                   BadPaddingException
    ,                                   InvalidAlgorithmParameterException {

        super(Util.prune((JSONObject) JSONValue.parse(str)));

        initialise();
    }

    // public Event_DPA(GenericRecord rec) throws IOException
    // , CastingException
    // , InvalidKeyException
    // , BadPaddingException
    // , InvalidAlgorithmParameterException {
    //
    // try {
    //
    // ObjectMapper mapper = new ObjectMapper();
    // String json = "";
    //
    // Map<String, Object> map = new HashMap<String, Object>();
    // map.put("name", "mkyong");
    // map.put("age", 29);
    //
    // // convert map to JSON string
    // json = mapper.writeValueAsString(map);
    //
    // System.out.println(json);
    //
    // json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map);
    //
    // // pretty print
    // System.out.println(json);
    //
    // }
    // catch (JsonGenerationException|JsonMappingException|IOException e) {
    //
    // e.printStackTrace();
    // }
    //
    // initialise();
    // }

    private void initialise() throws CastingException {

        this.imsi      = (String) get("imsi");
        this.imeitac   = (String) get("imeitac");
        this.timestamp =          getTimestamp();
        log.info("EVENT values => +"+imsi+"="+imeitac+"="+timestamp);
        // this.setKpiSets();
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
                : (CRM) Event_DPA.getValueFromObject(crm, fieldName);
    }

    public long getTimestamp() {

        if (timestamp == null) {

            Object o = get("date");
            if (o == null)
                o = get("DATE");
            String ts = o == null ? null : o.toString();
            if (ts == null) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
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
                : Event_DPA.getValueFromObject(terminal, fieldName);
    }

    public Object getTerminal(String tac, String fieldName) {

        if (tac != null && terminal == null) {

            REreference reference = REreference.getInstance();

            terminal = reference.getTerminal(tac);
        }

        return (tac == null || terminal == null || terminal.isEmpty()) ? null
                : Event_DPA.getValueFromObject(terminal, fieldName);
    }

    public Object get(String key, Object default_val) {

        return containsKey(key) ? get(key) : default_val;
    }

    @Override
    public String toString() {

        return "Event_DPA(timestamp=" + timestamp + ", imsi=" + imsi + ", imeitac=" + imeitac + ", record="
                + this.toJSONString() + ")";
    }

    public boolean containKpi(String source, String name) {

        if (kpiSetBySource != null) {

            return kpiSetBySource.contains(source + ";" + name);
        }

        return false;
    }

    public boolean containKpiInCell(String source, String name, String locId) {

        if (kpiSetBySourceAndlocId != null) {

            return kpiSetBySourceAndlocId.contains(source + ";" + name + ";" + locId);
        }

        return false;
    }

    public String getCrmSite_dataByKey(String key) {

        return (imsi == null || crm == null || crm.isEmpty()) ? null : crm.getSite_dataByKey(key);
    }

    public Object getCrmSite_dataByKey(String key, String dataType) {

        return (imsi == null || crm == null || crm.isEmpty()) ? null : crm.getSite_dataByKey(key, dataType);
    }

    /**
     * @return: true if site_data is defined for a CRM record, false otherwise
     */
    public boolean isCrmSite_data() {

        return (imsi == null || crm == null && crm.isEmpty()) ? null : crm.isSite_data();
    }

    // /**
    // * @return the number of Cells in an ESR
    // * @throws EsrFormatException
    // * @throws CastingException
    // */
    // public int getN_cells() throws EsrFormatException, CastingException {
    //
    // return getCells().size();
    // }

    // /**
    // * @return the maximum distance between any 2 cells in the DPA
    // * @throws EsrFormatException
    // * @throws CastingException
    // */
    // public Double getMaxCellDistance() throws EsrFormatException,
    // CastingException {
    //
    // Double max = null; if (getN_cells() > 1 ) {
    //
    // Set<CellIdentifier> cellset = getCells(); for (CellIdentifier src :
    // cellset) {
    //
    // if (src != null ) {
    //
    // for (CellIdentifier dst : cellset) {
    //
    // if (dst != null && dst != src) {
    //
    // Double distance; if ((distance = getCellDistance(src,dst)) != null) {
    //
    // if (max == null || max < distance) {max = distance;}
    // }
    // }
    // }
    // }
    // }
    // }
    //
    // return max;
    // }

    // /**
    // * @param s : source CellIdentifier
    // * @param d : destination CellIdentifier
    // * @return return the distance between s and d
    // */
    // public Double getCellDistance(CellIdentifier s, CellIdentifier d) {
    //
    // REreference reference = REreference.getInstance();
    // Cell src = reference.getCell(s.getLocationId());
    // Cell dst = reference.getCell(d.getLocationId());
    //
    // // Check that we have all the proper values:
    // if (src == null || dst == null
    // || src.getLat() == null || src.getLon() == null || dst.getLat() == null
    // || dst.getLon() == null
    // || src.getLat().isNaN() || src.getLon().isNaN() || dst.getLat().isNaN()
    // || dst.getLon().isNaN() ) {
    //
    // return null;
    // }
    //
    // int radius = 6371; //Km
    // Double slat_rad = src.getLat()*Math.PI/180;
    // Double slon_rad = src.getLon()*Math.PI/180;
    // Double elat_rad = dst.getLat()*Math.PI/180;
    // Double elon_rad = dst.getLon()*Math.PI/180;
    // Double dlat = slat_rad - elat_rad;
    // Double dlon = slon_rad - elon_rad;
    // Double dist = radius*Math.sqrt( dlat*dlat + dlon*dlon );
    //
    // if (log.isDebugEnabled()) log.debug("Dist: " + dist + " src: " +
    // src.getLat() + " " + src.getLon() + " dst: " + dst.getLat() + " " +
    // dst.getLon() );
    //
    // return dist;
    // }

    /**
     * @param object
     *            : any Object which provide getter for fieldName
     * @param fieldName
     *            : field name
     * @return: field name value or null
     */
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

    // /**
    // * @param imsi
    // * @return IMSI from IMSI
    // */
    // public static int getMCC(Long imsi) {
    //
    // return Integer.parseInt(Long.toString(imsi).substring(0, 3));
    // }
    //
    //
    // /**
    // * @param imsi
    // * @param mnc
    // * @return return MNC from MNC
    // */
    // public static String getMNC(Long imsi, String mnc) {
    //
    // return imsi.toString().substring(3, 3 + mnc.length());
    // }

    // /**
    // * @param imsi
    // * @return true/false if there is MCC matches for the Operator.
    // */
    // public static boolean isMCCmatches(Long imsi) {
    //
    // if (log.isDebugEnabled()) log.debug("isMCCmatches(" + imsi + "): MCC=" +
    // getMCC(imsi) + ", operatorMCC=" + RuleEngine.operatorMCCList);
    //
    // if (RuleEngine.operatorMCCList == null)
    // return false;
    //
    // for (String mcc : RuleEngine.operatorMCCList) {
    //
    // if (getMCC(imsi) == Integer.parseInt(mcc.trim())) {
    //
    // if (log.isDebugEnabled()) log.debug("isMCCmatches(" + imsi + ") returning
    // true");
    //
    // return true;
    // }
    // }
    //
    // if (log.isDebugEnabled()) log.debug("isMCCmatches(" + imsi + ") returning
    // false");
    //
    // return false;
    // }

    // public boolean isMCCmatches() {
    //
    // return ESR_event.isMCCmatches(this.imsi);
    // }

    // public static boolean isMNCmatches(Long imsi) {
    //
    // if (RuleEngine.operatorMNCList == null)
    // return false;
    //
    // for (String mnc : RuleEngine.operatorMNCList) {
    //
    // if (getMNC(imsi, mnc).equals(mnc.trim())) {
    //
    // if (log.isDebugEnabled()) log.debug("isMNCmatches(" + imsi + ") returning
    // true");
    //
    // return true;
    // }
    // }
    //
    // if (log.isDebugEnabled()) log.debug("isMNCmatches(" + imsi + ") returning
    // false");
    //
    // return false;
    // }

    // public boolean isMNCmatches() {
    //
    // return ESR_event.isMNCmatches(this.imsi);
    // }

    // public static boolean isHomeIMSI(Long imsi) {
    //
    // return (isMCCmatches(imsi) && isMNCmatches(imsi));
    // }

    // public boolean isHomeIMSI() {
    //
    // return ESR_event.isHomeIMSI(this.imsi);
    //
    // }

    // public String getUsercategory() {
    //
    // // The user category is being mapped from the customer_type from the CRM
    // return (imsi == null || crm == null || crm.isEmpty()) ? null :
    // crm.getCustomer_type();
    // }

    // To keep possibility to add some weight based on User category. This
    // should come from CRM data.
    public int getUsercategorymultiplier() {

        return 1;
    }

    // /**
    // * @param source
    // * @param name
    // * @return Aggregate KpiRecord ( filter by source, name)
    // * @throws CastingException
    // * @throws EsrFormatException
    // */
    // public KpiRecord aggregateKpiRecord(String source, String name)
    // throws CastingException, EsrFormatException {
    // KpiRecord r = super.aggregateKpiRecord(new KpiDesc(source, name));
    // return r;
    // }

    // /**
    // * @param source
    // * @param name
    // * @param cellId
    // * @return Aggregate KpiRecord ( filter by source, name, cellId)
    // * @throws CastingException
    // * @throws EsrFormatException
    // */
    // public KpiRecord aggregateKpiRecord(String source, String name,
    // CellIdentifier cellId)
    // throws CastingException, EsrFormatException {
    // KpiRecord pattern = new KpiRecord(cellId, null, null, null);
    // KpiRecord r = super.aggregateKpiRecord(new KpiDesc(source, name),
    // pattern);
    // return r;
    // }

    // /**
    // * @param source
    // * @param name
    // * @param cellId
    // * @param dimensions
    // * @return Aggregate KpiRecord ( filter by source, name, cellId,
    // dimensions)
    // * @throws CastingException
    // * @throws EsrFormatException
    // */
    // public KpiRecord aggregateKpiRecord(String source, String name,
    // CellIdentifier cellId, String dimensions) throws CastingException,
    // EsrFormatException {
    //
    // MatchableMap<String, String> m = new MatchableMap<String, String>();
    // String list1[] = dimensions.split(",");
    // for (String pair: list1) {
    // String list2[] = pair.split(":");
    // if (list2.length != 2)
    // continue;
    // String key = list2[0].trim();
    // String value = list2[1].trim();
    // m.put(key, value);
    // }
    //
    // KpiRecord pattern = new KpiRecord(cellId, null, m, null);
    // KpiRecord r = super.aggregateKpiRecord(new KpiDesc(source, name),
    // pattern);
    // return r;
    // }

    // /**
    // * @param source
    // * @param name
    // * @param dimensions a set string values of the format
    // "key:value,key:value....."
    // * @return aggregate KpiRecord for source,name,dimensions
    // * @throws CastingException
    // * @throws EsrFormatException
    // */
    // public KpiRecord aggregateKpiRecord(String source, String name, String
    // dimensions)
    // throws CastingException, EsrFormatException {
    // MatchableMap<String, String> m = new MatchableMap<String, String>();
    // String list1[] = dimensions.split(",");
    // for (String pair: list1) {
    // String list2[] = pair.split(":");
    // if (list2.length != 2)
    // continue;
    // String key = list2[0].trim();
    // String value = list2[1].trim();
    // m.put(key, value);
    // }
    //
    // KpiRecord pattern = new KpiRecord(null, null, m, null);
    // KpiRecord r = super.aggregateKpiRecord(new KpiDesc(source, name),
    // pattern);
    // return r;
    // }

    // /**
    // * @param source
    // * @param name
    // * @param dimensions
    // * @return aggregate KpiRecord for source,name,dimensions
    // * @throws CastingException
    // * @throws EsrFormatException
    // */
    // public KpiRecord aggregateKpiRecord(String source, String name,
    // Map<String , String> dimensions)
    // throws CastingException, EsrFormatException {
    // MatchableMap<String, String> m = new MatchableMap<String, String>();
    //
    // KpiRecord pattern = new KpiRecord(null, null, m, null);
    // KpiRecord r = super.aggregateKpiRecord(new KpiDesc(source, name),
    // pattern);
    // return r;
    // }

    // /**
    // * @param source
    // * @param name
    // * @param cellId
    // * @return EventTypeKpiValue which returns Double values which support
    // null.
    // * this a convenience for rule writing.
    // * @throws CastingException
    // * @throws EsrFormatException
    // */
    // public EventTypeKpiValue aggregateKpiRecordValue(String source, String
    // name, CellIdentifier cellId)
    // throws CastingException, EsrFormatException {
    // EventTypeKpiValue v = new EventTypeKpiValue();
    // KpiRecord pattern = new KpiRecord(cellId, null, null, null);
    // KpiRecord r = super.aggregateKpiRecord(new KpiDesc(source, name),
    // pattern);
    // if (r == null) {
    // return v;
    // }
    // v.setValue(r.getValue());
    // v.setWeight(r.getWeight());
    // return v;
    // }

    // /**
    // * @param source
    // * @param name
    // * @return Aggregate KpiValue for source, name. Return value could be
    // null.
    // * @throws CastingException
    // * @throws EsrFormatException
    // */
    // public KpiValue aggregateKPI(String source, String name)
    // throws CastingException, EsrFormatException {
    // KpiValue v = super.aggregateKpi(new KpiDesc(source,name));
    //
    // if (v == null) {
    // v = new KpiValue(0, 0);
    // }
    //
    // return v;
    // }

    // /**
    // * @param source
    // * @param name
    // * @param cell
    // * @return AggregationType.MIN aggregated KpiRecord filtered by source,
    // name cell
    // * @throws CastingException
    // * @throws EsrFormatException
    // */
    // public KpiRecord aggregateKpiRecordMin(String source, String name,
    // CellIdentifier cell) throws CastingException, EsrFormatException {
    // KpiRecord pattern = new KpiRecord(cell, null, null, null);
    // KpiDesc kd = new KpiDesc(source,name,AggregationType.MIN);
    // return super.aggregateKpiRecord(kd,pattern);
    // }

    // /**
    // * @param source
    // * @param name
    // * @param cell
    // * @return Max aggregated KpiRecord filtered by source, name cell
    // * @throws CastingException
    // * @throws EsrFormatException
    // */
    // public KpiRecord aggregateKpiRecorddMax(String source, String name,
    // CellIdentifier cell) throws CastingException, EsrFormatException {
    // KpiRecord pattern = new KpiRecord(cell, null, null, null);
    // KpiDesc kd = new KpiDesc(source,name,AggregationType.MAX);
    // return super.aggregateKpiRecord(kd,pattern);
    // }

    // /**
    // * @param source
    // * @param name
    // * @param dimkey
    // * @param dimvalue
    // * @return aggregated KPI for single dimension pair ( key, value)
    // * @throws CastingException
    // * @throws EsrFormatException
    // */
    // public KpiRecord aggregateKpiRecord(String source, String name, String
    // dimkey, String dimvalue)
    // throws CastingException, EsrFormatException {
    // HashMap<String, String> dim = new HashMap<String, String>();
    // dim.put(dimkey, dimvalue); // "functionality","IM"
    // KpiRecord pattern = new KpiRecord(null, null, dim, null);
    // KpiDesc kd = new KpiDesc(source,name);
    // return super.aggregateKpiRecord(kd,pattern);
    // }

    // public String getApn() throws CastingException {
    // Collection<Bearer> bearers = getBearers();
    // SortedMap<String, Integer> apnMap = new TreeMap<String, Integer>();
    // for (Bearer bearer : bearers) {
    // String apn = (String) bearer.get(Constants.APN);
    // if (apn != null) {
    // int value = 0;
    // if (apnMap.containsKey(apn)) {
    // value = apnMap.get(apn);
    // }
    // apnMap.put(apn, value + 1);
    // }
    // }
    // if (apnMap.size() > 0) {
    // if (apnMap.lastKey() != null) {
    // return apnMap.lastKey();
    // }
    // }
    // return null;
    // }

    // /**
    // * @param ruleId
    // * : rule id as defined in drl file
    // * @param paramaterKey
    // * : parameter name as defined in drl
    // * @return: the Object value defined in the rule_parameters.csv for
    // * composite key(ruleId,parameterKey)
    // */
    // public Object getRuleParamater(String ruleId, String paramaterKey) {
    // REreference reference = REreference.getInstance();
    // RuleIdParamater ruleIdParamater = reference.getRuleIdParms();
    // Object obj = ruleIdParamater.getParameterValue(ruleId, paramaterKey);
    // return obj;
    // }

    // /**
    // * @param ruleId
    // * @param paramaterKey
    // * @param type
    // * @return the parameter Key value of the requested type.
    // */
    // public <T> T getRuleParamater(String ruleId, String paramaterKey,
    // Class<T> type) {
    //
    // REreference reference = REreference.getInstance();
    // RuleIdParamater ruleIdParamater = reference.getRuleIdParms();
    // Object o = ruleIdParamater.getParameterValue(ruleId, paramaterKey);
    // if (o == null)
    // return null;
    //
    // try {
    // return EventType_Generic.getValue(o, type);
    // }
    // catch (CastingException e) {
    // //Put it just as debug as it may not be considered an error.
    // log.warn("casting exception trying to get Type value for key=" +
    // paramaterKey +
    // " type=" + type + " object=" + o) ;
    // }
    // return null;
    //
    // }

    // private void setKpiSets() {
    //
    // // OTT sources
    // List<String> otts = null;
    // String ottSources = RuleEngine.config.ottSources();
    // if (ottSources != null && !ottSources.isEmpty())
    // otts = Arrays.asList(ottSources.split("\\s*,\\s*"));
    //
    // Map<KpiDesc, List<KpiRecord>> kpiM = null;
    // Map<KpiDesc, List<KpiRecord>> ottKpiM = new HashMap<KpiDesc,
    // List<KpiRecord>>();
    // this.kpiSetBySource = new HashSet<String> ();
    // this.kpiSetBySourceAndlocId = new HashSet<String>();
    //
    // try {
    // kpiM = this.getKpiRecords();
    // } catch (CastingException | EsrFormatException e) {
    // if (log.isDebugEnabled()) log.debug("failed to get Kpi list: " +
    // e.getMessage());
    // return;
    // }
    //
    // if (kpiM == null || kpiM.isEmpty()) {
    // log.debug("failed to get Kpi list");
    // return;
    // }
    //
    // for (Entry<KpiDesc, List<KpiRecord>> entry : kpiM.entrySet()) {
    // KpiDesc kpiDesc = entry.getKey();
    // List<KpiRecord> kl = entry.getValue();
    // String ottBySource = kpiDesc.getSource() + ";" + kpiDesc.getName();
    // this.kpiSetBySource.add(ottBySource);
    // if (otts != null && otts.contains(kpiDesc.getSource())) {
    // ottKpiM.put(kpiDesc, kl);
    // }
    // for (KpiRecord kpi : kl) {
    // if (kpi.getCellIdentifier() != null
    // && kpi.getCellIdentifier().getLocationId() != null) {
    // String ottBySourceAndCell = kpiDesc.getSource() + ";"
    // + kpiDesc.getName() + ";"
    // + kpi.getCellIdentifier().getLocationId();
    // this.kpiSetBySourceAndlocId.add(ottBySourceAndCell);
    // }
    //
    // }
    // }
    //
    // this.setOttKpiM(ottKpiM);
    // }

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
