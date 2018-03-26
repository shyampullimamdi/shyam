package com.ericsson.eea.ark.offline.rules.refdata;

/**
 * @author ejoepao
 * Contains rule meta data
 * filename=CEA11_BASE
 * release=2.0
 * version=1.0
 */
public class RuleMetaData {
    private String filename=null;
    public String getFilename() {
        return filename;
    }
    @Override
    public String toString() {
        return "RuleMetaData [filename=" + filename + ", release=" + release
                + ", version=" + version + ", ruleId=" + ruleId + ", ruleName="
                + ruleName + ", rulecat=" + rulecat + ", kpi_name=" + kpi_name
                + ", kpi_source=" + kpi_source + "]";
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((filename == null) ? 0 : filename.hashCode());
        result = prime * result
                + ((kpi_name == null) ? 0 : kpi_name.hashCode());
        result = prime * result
                + ((kpi_source == null) ? 0 : kpi_source.hashCode());
        result = prime * result + ((release == null) ? 0 : release.hashCode());
        result = prime * result + ((ruleId == null) ? 0 : ruleId.hashCode());
        result = prime * result
                + ((ruleName == null) ? 0 : ruleName.hashCode());
        result = prime * result + ((rulecat == null) ? 0 : rulecat.hashCode());
        result = prime * result + ((version == null) ? 0 : version.hashCode());
        return result;
    }

    public RuleMetaData() {
        super();
        this.filename = null;
        this.release = null;
        this.version = null;
        this.ruleId = null;
        this.ruleName = null;
        this.rulecat = null;
        this.kpi_name = null;
        this.kpi_source = null;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RuleMetaData other = (RuleMetaData) obj;
        if (filename == null) {
            if (other.filename != null)
                return false;
        } else if (!filename.equals(other.filename))
            return false;
        if (kpi_name == null) {
            if (other.kpi_name != null)
                return false;
        } else if (!kpi_name.equals(other.kpi_name))
            return false;
        if (kpi_source == null) {
            if (other.kpi_source != null)
                return false;
        } else if (!kpi_source.equals(other.kpi_source))
            return false;
        if (release == null) {
            if (other.release != null)
                return false;
        } else if (!release.equals(other.release))
            return false;
        if (ruleId == null) {
            if (other.ruleId != null)
                return false;
        } else if (!ruleId.equals(other.ruleId))
            return false;
        if (ruleName == null) {
            if (other.ruleName != null)
                return false;
        } else if (!ruleName.equals(other.ruleName))
            return false;
        if (rulecat == null) {
            if (other.rulecat != null)
                return false;
        } else if (!rulecat.equals(other.rulecat))
            return false;
        if (version == null) {
            if (other.version != null)
                return false;
        } else if (!version.equals(other.version))
            return false;
        return true;
    }
    public void setFilename(String filename) {
        this.filename = filename;
    }
    public String getRelease() {
        return release;
    }
    public void setRelease(String release) {
        this.release = release;
    }
    public String getVersion() {
        return version;
    }
    public void setVersion(String version) {
        this.version = version;
    }
    private String release=null;
    private String version=null;
    private String ruleId=null;
    public String getRuleId() {
        return ruleId;
    }
    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }
    public String getRuleName() {
        return ruleName;
    }
    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }
    public String getRulecat() {
        return rulecat;
    }
    public void setRulecat(String rulecat) {
        this.rulecat = rulecat;
    }
    private String ruleName=null;
    public RuleMetaData(String filename, String release, String version,
            String ruleId, String ruleName, String rulecat, String kpi_name, String kpi_source) {
        super();
        this.filename = filename;
        this.release = release;
        this.version = version;
        this.ruleId = ruleId;
        this.ruleName = ruleName;
        this.rulecat = rulecat;
        this.kpi_name = kpi_name;
        this.kpi_source = kpi_source;
    }
    private String rulecat = null;
    private String kpi_name = null;
    private String kpi_source = null;
    public String getKpi_source() {
        return kpi_source;
    }
    public void setKpi_source(String kpi_source) {
        this.kpi_source = kpi_source;
    }
    public String getKpi_name() {
        return kpi_name;
    }
    public void setKpi_name(String kpi_name) {
        this.kpi_name = kpi_name;
    }

}
