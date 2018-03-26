package com.ericsson.eea.ark.offline.rules.refdata;

import com.ericsson.eea.ark.common.service.model.DgdKpiMetaInfo;
import com.ericsson.eea.ark.common.service.model.KpiMetaInfo;
import com.ericsson.eea.ark.common.service.model.KpiMetaInfoId;

public class KpiMetaDataRecord {
    public KpiMetaDataRecord(String service, String arim, KpiMetaInfoId id) {
        super();
        this.service = service;
        this.arim = arim;
        this.id = id;
    }

    public KpiMetaDataRecord(KpiMetaInfo k) {
        if (k != null) {
            this.service = k.getService();
            this.arim = k.getArim();
            this.id = k.getKpiMetaInfoId();
        }
    }

    public KpiMetaDataRecord(DgdKpiMetaInfo d) {
        if (d != null) {
            this.service = d.getService();
            this.arim = d.getArim();
            this.id = new KpiMetaInfoId(d.getDgdKpiMetaInfoId().getSource(),
                    d.getDgdKpiMetaInfoId().getKpiName(), d.getDgdKpiMetaInfoId().getRat());
        }
    }

    public KpiMetaInfoId getId() {
        return id;
    }

    public void setId(KpiMetaInfoId id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "KpiMetaDataRecord [service=" + service + ", arim=" + arim
                + ", id=" + id + "]";
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((arim == null) ? 0 : arim.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((service == null) ? 0 : service.hashCode());
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        KpiMetaDataRecord other = (KpiMetaDataRecord) obj;
        if (arim == null) {
            if (other.arim != null)
                return false;
        } else if (!arim.equals(other.arim))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (service == null) {
            if (other.service != null)
                return false;
        } else if (!service.equals(other.service))
            return false;
        return true;
    }
    public String getService() {
        return service;
    }
    public void setService(String service) {
        this.service = service;
    }
    public String getArim() {
        return arim;
    }
    public void setArim(String arim) {
        this.arim = arim;
    }

    private String service = null;
    private String arim = null;
    private  KpiMetaInfoId id = null;

}
