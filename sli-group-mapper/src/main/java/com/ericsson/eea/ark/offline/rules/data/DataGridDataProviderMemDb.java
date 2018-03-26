package com.ericsson.eea.ark.offline.rules.data;

import java.util.List;
import com.ericsson.eea.ark.offline.utils.Util;
import com.ericsson.eea.ark.common.service.ReferenceData;
import com.ericsson.eea.ark.common.service.Services;
import com.ericsson.eea.ark.common.service.WhiteList;
import com.ericsson.eea.ark.common.service.WhiteListStatus;
import com.ericsson.eea.ark.common.service.model.CellLocationMapper;
import com.ericsson.eea.ark.common.service.model.CrmCustomerDeviceInfo;
import com.ericsson.eea.ark.common.service.model.DgdKpiMetaInfo;
import com.ericsson.eea.ark.common.service.model.KpiMetaInfo;
import com.ericsson.eea.ark.common.service.model.Imeitac;
//import com.ericsson.eea.ark.common.service.model.ProfilerAttributeGroup;
//import com.ericsson.eea.ark.common.service.model.ProfilerAttributeMetadata;
//import com.ericsson.eea.ark.common.service.model.ProfilerDataDictionary;
//import com.ericsson.eea.ark.common.service.model.ProfilerSliScore;
//import com.ericsson.cea.bigdata.common.service.model.ProfilerSortKey;
//import com.ericsson.eea.ark.common.service.model.SliImportance;
//import com.ericsson.eea.ark.common.service.model.SliModuleFunction;
//import com.ericsson.eea.ark.common.service.model.SliQualityScore;

public class DataGridDataProviderMemDb extends DataGridDataProvider {

    /**
     * according to MK, MR jobs are single threaded per JVM - so this code does not need to be thread-safe
     *
     * otherwise, the common-services objects should be thread-local as only one instance of this class is created
     */
    private ReferenceData refData = null;
    private WhiteList whiteList = null;

    public DataGridDataProviderMemDb() {
        super();
    }

    private ReferenceData getRefData() {

        if (refData == null) {

            try {

                refData = Services.getReferenceData();
            }
             catch (Exception e) {

                e.printStackTrace();
            }
        }
        return refData;
    }

    private WhiteList getWhiteList() {

        if (whiteList == null) {

            try {

                whiteList = Services.getWhiteList();
            }
            catch(Exception e) {

                e.printStackTrace();
            }
        }
        return whiteList;
    }

//	@Override
//	public Map<Long, ProfilerDataDictionary> getProfilerDataDictionary() {
//			return getRefData().getProfilerDataDictionary();
//	}
//
//	@Override
//	public List<ProfilerAttributeGroup> getProfilerAttributeGroup() {
//		return getRefData().getProfilerAttributeGroup();
//	}
//
//	@Override
//	public List<ProfilerAttributeMetadata> getProfilerAttributeMetadata() {
//		return getRefData().getProfilerAttributeMetadata();
//	}

//	@Override
//	public List<ProfilerSortKey> getProfilerSortKey() {
//		List<ProfilerSortKey> ret = new ArrayList<>();
//		ret.add(new ProfilerSortKey("esr","imsi","imsi",true));
//		return ret;
//	}

    @Override
    protected CrmCustomerDeviceInfo getCrmCustomerDeviceInfoImpl(String imsi) {
        try {
            CrmCustomerDeviceInfo ccdi = getRefData().getCrmCustomerDeviceInfo(Long.parseLong(imsi));
            return ccdi;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected CrmCustomerDeviceInfo getCrmCustomerDeviceInfoImpl(Long imsi) {
        return getCrmCustomerDeviceInfo(Util.encImsi(imsi));
    }

//	@Override
//	public List<ProfilerSliScore> getProfilerSliScore() {
//		return getRefData().getProfilerSliScore();
//	}
//
//	@Override
//	public Map<String, String> getSiteInterest() {
//		return getRefData().getSiteInterest();
//	}

    @Override
    protected boolean doProfilingImpl(Long imsi) {
        return doProfilingImpl(Util.encImsi(imsi));
    }

    @Override
    protected boolean doProfilingImpl(String imsi) {
        try {
            WhiteListStatus status = getWhiteList().createProfiles(Long.parseLong(imsi));
            if( status == WhiteListStatus.ALLOW || status == WhiteListStatus.NOT_FOUND)
                return true;
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    protected List<Long> getImsiFromMsisdnImpl(String msisdn) {
        try {
            return getRefData().getImsiFromMsisdn(Long.parseLong(msisdn));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected List<Long> getImsiFromMsisdnImpl(Long msisdn) {
        return getImsiFromMsisdnImpl(Util.encMsisdn(msisdn));
    }

    @Override
    protected Long getImsiFromImeiImpl(String imei) {
        try {
            return getRefData().getImsiFromImei(Long.parseLong(imei));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected Long getImsiFromImeiImpl(Long imei) {
        return getImsiFromImeiImpl(Util.encImei(imei));
    }

    @Override
    protected Imeitac getImeitacImpl(Long imeitac) {
        return getImeitacImpl(Util.encImeitac(imeitac));
    }

    @Override
    protected Imeitac getImeitacImpl(String imeitac) {
        try {
//			Long di = Util.decImeitac(imeitac);
            return getRefData().getImeitac(imeitac/*di*/ == null ? null : Integer.parseInt(imeitac)/*di.intValue()*/);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected CellLocationMapper getCellLocationMapperImpl(String locationId) {
        try {
            return getRefData().getCellLocationMapper(locationId);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected KpiMetaInfo getKpiMetaInfoImpl(String source, String kpiName,
            String rat) {
        try {
            return getRefData().getKpiMetaInfo(source, kpiName, rat);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected DgdKpiMetaInfo getDgdKpiMetaInfoImpl(String source, String kpiName,
            String rat) {
        try {
            return getRefData().getDgdKpiMetaInfo(source, kpiName, rat);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected void initImpl() {

        Services.init();
    }

    @Override
    protected void closeImpl() {

        try {
            Services.close();
            refData = null;
            whiteList = null;
        } catch (Exception e) {

            e.printStackTrace();
        }
    }

//	@Override
//	public List<SliImportance> getSliImportance() {
//		return getRefData().getSliImportance();
//	}
//
//	@Override
//	public List<SliModuleFunction> getSliModuleFunction() {
//		return getRefData().getSliModuleFunction();
//	}
//
//	@Override
//	public List<SliQualityScore> getSliQualityScore() {
//		return getRefData().getSliQualityScore();
//	}


}
