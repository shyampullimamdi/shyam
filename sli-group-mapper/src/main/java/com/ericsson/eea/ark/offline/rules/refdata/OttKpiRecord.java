package com.ericsson.eea.ark.offline.rules.refdata;

import java.util.Map;
import java.util.List;
import java.util.Arrays;

import com.ericsson.bigdata.esr.dataexchange.KpiRecord;



public class OttKpiRecord extends KpiRecord {

    public OttKpiRecord(KpiRecord r) {
        super(r);
    }

    public boolean containDimensionTypes(String dimensionTypes) {

       Map<String, String> m = this.getDimensions();
        if (m == null || m.isEmpty() || dimensionTypes == null
                || dimensionTypes.isEmpty() || this.getDimensions() == null
                || this.getDimensions().isEmpty())
            return false;
        // parse dimension types
        List<String> items = Arrays.asList(dimensionTypes.split("\\s*,\\s*"));

        for (String dimensionKey : items) {
            if (!this.getDimensions().containsKey(dimensionKey))
                return false;
        }
        return true;
    }

    public String getDimensionTuples(String dimensionTypes) {
        Map<String, String> m = this.getDimensions();
        String returnStr = null;

        if (m == null || m.isEmpty() || dimensionTypes == null
                || dimensionTypes.isEmpty() || this.getDimensions() == null
                || this.getDimensions().isEmpty())
            return returnStr;
        // parse dimension types
        List<String> items = Arrays.asList(dimensionTypes.split("\\s*,\\s*"));

        StringBuffer b = new StringBuffer();
        for (String dimensionKey : items) {
            if (this.getDimensions().containsKey(dimensionKey)) {
                if (b.length() > 0)
                    b.append(",");

                b.append(dimensionKey);
                b.append(":");
                b.append(this.getDimensions().get(dimensionKey));
            }
        }
        return b.toString();
    }

}
