package com.ericsson.eea.ark.sli.grouping.memdb;

import java.util.Set;
import java.util.TreeSet;

import com.ericsson.eea.ark.offline.utils.UserGroups;
import com.ericsson.eea.ark.offline.utils.UserGroups.UserGroup;

import net.minidev.json.JSONObject;

public class ImsiGrpMemDbPut {

    private final String group;
    private final Double percentage;

    private transient String jsonString;

    public ImsiGrpMemDbPut(String group, Double percentage) {
        this.group = group; this.percentage = percentage;
    }

    public ImsiGrpMemDbPut(String group, String percentage) {
        this(group, pctToDbl(percentage));
    }

    private static final Double pctToDbl(String percentage) {
       Double val;
       try {
           val = Double.valueOf(percentage);
       } catch (Exception e) {
           val = 1.0D;
       }
       return val;
    }

    public String getGroup() {
        return group;
    }

    public Double getPercentage() {
        return percentage;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((group == null) ? 0 : group.hashCode());
        result = prime * result
                + ((percentage == null) ? 0 : percentage.hashCode());
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
        ImsiGrpMemDbPut other = (ImsiGrpMemDbPut) obj;
        if (group == null) {
            if (other.group != null)
                return false;
        } else if (!group.equals(other.group))
            return false;
        if (percentage == null) {
            if (other.percentage != null)
                return false;
        } else if (!percentage.equals(other.percentage))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "ImsiGrpMapDbPut [group=" + group + ", percentage=" + percentage + "]";
    }

    public String toJsonString() {
        if (jsonString == null) {
            JSONObject jo = new JSONObject();
            jo.put(this.group,  this.percentage);
            jsonString = jo.toString();
        }
        return jsonString;
    }

    public UserGroups toUserGroups(String user) {
        UserGroups userGroups = new UserGroups();
        Set<UserGroup>groups = new TreeSet<>();
        groups.add(new UserGroup(group, percentage));
        userGroups.setUser(user);
        userGroups.setGroups(groups);
        return userGroups;
    }

}
