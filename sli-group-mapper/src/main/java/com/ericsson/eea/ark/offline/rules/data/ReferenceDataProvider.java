package com.ericsson.eea.ark.offline.rules.data;

//import java.util.Map;
import java.util.List;

import com.ericsson.eea.ark.common.service.model.CellLocationMapper;
import com.ericsson.eea.ark.common.service.model.CrmCustomerDeviceInfo;
import com.ericsson.eea.ark.common.service.model.DgdKpiMetaInfo;
import com.ericsson.eea.ark.common.service.model.Imeitac;
import com.ericsson.eea.ark.common.service.model.KpiMetaInfo;
//import com.ericsson.eea.ark.common.service.model.ProfilerAttributeGroup;
//import com.ericsson.eea.ark.common.service.model.ProfilerAttributeMetadata;
//import com.ericsson.eea.ark.common.service.model.ProfilerDataDictionary;
//import com.ericsson.eea.ark.common.service.model.ProfilerSliScore;
//import com.ericsson.cea.bigdata.common.service.model.ProfilerSortKey;

//import com.ericsson.eea.sli.grouping.profiler.operation.sli.SliReferenceDataProvider;

public interface ReferenceDataProvider /*extends SliReferenceDataProvider*/ {

//	Map<Long, ProfilerDataDictionary> getProfilerDataDictionary();
//
//	List<ProfilerAttributeGroup> getProfilerAttributeGroup();
//
//	List<ProfilerAttributeMetadata> getProfilerAttributeMetadata();
//
//	List<ProfilerSortKey> getProfilerSortKey();

//	List<ProfilerSliScore> getProfilerSliScore();

//	Map<String, String> getSiteInterest();

    CrmCustomerDeviceInfo getCrmCustomerDeviceInfo(String imsi)
            throws Exception;

    CrmCustomerDeviceInfo getCrmCustomerDeviceInfo(Long imsi)
            throws Exception;

    boolean doProfiling(String imsi);

    boolean doProfiling(Long imsi);

    List<Long> getImsiFromMsisdn(String msisdn);

    List<Long> getImsiFromMsisdn(Long msisdn);

    Long getImsiFromImei(String imei);

    Long getImsiFromImei(Long imei);


    Imeitac getImeitac(Long key);

    Imeitac getImeitac(String key);

    CellLocationMapper getCellLocationMapper(String locationId);

    KpiMetaInfo getKpiMetaInfo(String source, String kpiName, String rat);

    DgdKpiMetaInfo getDgdKpiMetaInfo(String source, String kpiName, String rat);

    void init();

    void close();

}
