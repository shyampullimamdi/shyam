package com.ericsson.eea.ark.offline.rules.data;

import com.ericsson.eea.ark.offline.config.Config;
import com.ericsson.eea.ark.common.service.model.CellLocationMapper;
import com.ericsson.eea.ark.common.service.model.CrmCustomerDeviceInfo;
import com.ericsson.eea.ark.common.service.model.DgdKpiMetaInfo;
import com.ericsson.eea.ark.common.service.model.KpiMetaInfo;
import com.ericsson.eea.ark.common.service.model.Imeitac;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DataGridDataProvider implements ReferenceDataProvider {
    private static Logger log = LoggerFactory.getLogger(DataGridDataProvider.class);
    /**
     * according to MK, MR jobs are single threaded per JVM - so this code does
     * not need to be thread-safe otherwise, the common-services objects should
     * be thread-local as only one instance of this class is created
     */
    private volatile boolean isInit;

    private static volatile DataGridDataProvider instance;

    protected abstract boolean doProfilingImpl(String imsi);

    protected abstract boolean doProfilingImpl(Long imsi);

    protected abstract CrmCustomerDeviceInfo getCrmCustomerDeviceInfoImpl(String imsi);

    protected abstract CrmCustomerDeviceInfo getCrmCustomerDeviceInfoImpl(Long imsi);

    protected abstract List<Long> getImsiFromMsisdnImpl(String msisdn);

    protected abstract List<Long> getImsiFromMsisdnImpl(Long msisdn);

    protected abstract Long getImsiFromImeiImpl(String imei);

    protected abstract Long getImsiFromImeiImpl(Long imei);

    protected abstract Imeitac getImeitacImpl(String key);

    protected abstract Imeitac getImeitacImpl(Long imeitac);

    protected abstract CellLocationMapper getCellLocationMapperImpl(String locationId);

    protected abstract KpiMetaInfo getKpiMetaInfoImpl(String source, String kpiName, String rat);

    protected abstract DgdKpiMetaInfo getDgdKpiMetaInfoImpl(String source, String kpiName, String rat);

    protected abstract void initImpl();

    protected abstract void closeImpl();

    public static DataGridDataProvider getInstance() {
        if (Config.dataGridEnabled) {
            if (instance == null) {
                synchronized (DataGridDataProvider.class) {
                    if (instance == null) {
                        instance = new DataGridDataProviderMemDb();
                    }
                }
            }

        }
        return instance;
    }

    @Override
    public boolean doProfiling(String imsi) {
        init();
        if (instance == null) {
            return false;
        } else {
            return instance.doProfilingImpl(imsi);
        }
    }

    @Override
    public boolean doProfiling(Long imsi) {
        init();
        if (instance == null) {
            return false;
        } else {
            return instance.doProfilingImpl(imsi);
        }
    }

    @Override
    public CrmCustomerDeviceInfo getCrmCustomerDeviceInfo(String imsi) {
        init();
        if (instance == null) {
            return null;
        } else {
            return getCrmCustomerDeviceInfoImpl(imsi);
        }
    }

    @Override
    public CrmCustomerDeviceInfo getCrmCustomerDeviceInfo(Long key) {
        init();
        if (instance == null) {
            return null;
        } else {
            return getCrmCustomerDeviceInfoImpl(key);
        }
    }

    @Override
    public List<Long> getImsiFromMsisdn(String msisdn) {
        init();
        if (instance == null) {
            return null;
        } else {
            return getImsiFromMsisdnImpl(msisdn);
        }
    }

    @Override
    public List<Long> getImsiFromMsisdn(Long msisdn) {
        init();
        if (instance == null) {
            return null;
        } else {
            return getImsiFromMsisdnImpl(msisdn);
        }
    }

    @Override
    public Long getImsiFromImei(String imei) {
        init();
        if (instance == null) {
            return null;
        } else {
            return getImsiFromImeiImpl(imei);
        }
    }

    @Override
    public Long getImsiFromImei(Long imei) {
        init();
        if (instance == null) {
            return null;
        } else {
            return getImsiFromImeiImpl(imei);
        }
    }

    @Override
    public Imeitac getImeitac(Long key) {
        init();
        if (instance == null) {
            return null;
        } else {
            return getImeitacImpl(key);
        }
    }

    @Override
    public Imeitac getImeitac(String key) {
        init();
        if (instance == null) {
            return null;
        } else {
            return getImeitacImpl(key);
        }
    }

    @Override
    public CellLocationMapper getCellLocationMapper(String locationId) {
        init();
        if (instance == null) {
            return null;
        } else {
            return getCellLocationMapperImpl(locationId);
        }
    }

    @Override
    public KpiMetaInfo getKpiMetaInfo(String source, String kpiName, String rat) {
        init();
        if (instance == null) {
            return null;
        } else {
            return getKpiMetaInfoImpl(source, kpiName, rat);
        }
    }

    @Override
    public DgdKpiMetaInfo getDgdKpiMetaInfo(String source, String kpiName, String rat) {
        init();
        if (instance == null) {
            return null;
        } else {
            return getDgdKpiMetaInfoImpl(source, kpiName, rat);
        }
    }

    @Override
    public void init() {
        if (getInstance() != null) {
            if (!isInit) {
                isInit = true;
                instance.initImpl();
            }
        }
    }

    @Override
    public void close() {
        if (isInit) {
            isInit = false;
            instance.closeImpl();
        }
    }

}
