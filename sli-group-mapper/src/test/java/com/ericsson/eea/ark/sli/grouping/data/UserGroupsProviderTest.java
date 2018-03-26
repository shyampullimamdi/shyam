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

package com.ericsson.eea.ark.sli.grouping.data;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.TreeSet;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.*;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.log4j.Logger;

import com.ericsson.eea.ark.offline.utils.UserGroups;
import com.ericsson.eea.ark.offline.utils.UserGroups.UserGroup;
import com.ericsson.eea.ark.sli.grouping.data.BaseTest;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UserGroupsProviderTest extends BaseTest {

    protected final static Logger  logger = Logger.getLogger(UserGroupsProviderTest.class);
    @Override public Logger logger() { return logger; }

       @SuppressWarnings("rawtypes")
       final Triple[] data = {Triple.of("12345", "Heavy Video"   , 0.50)
      ,                   Triple.of("12345", "Heavy Data"    , 0.73)
      ,                   Triple.of("12345", "Heavy Web"     , 0.21)
      ,                   Triple.of("12345", "Non-Enterprise", 1.00)
      ,                   Triple.of("12346", "Smart Phone"   , 1.00)

      ,                   Triple.of("12346", "Heavy Video"   , 0.25)
      ,                   Triple.of("12346", "Heavy Data"    , 0.90)
      ,                   Triple.of("12346", "Heavy Web"     , 0.05)
      ,                   Triple.of("12346", "Enterprise"    , 1.00)
      ,                   Triple.of("12346", "Smart Phone"   , 1.00)

      ,                   Triple.of("12347", "Heavy Web"     , 1.00)
      ,                   Triple.of("12347", "Pre-paid"      , 1.00)
   };

   //MemDb:
   @Test
   public void test_01_MemDb_Put() throws Exception {

       String user         = "12347";

       UserGroups userGroups = refGet(user);

       gmcu.replace(userGroups);
  }

 //MemDb:
   @Test
   public void test_02_MemDb_Get() throws Exception {

       String user = "12347";

       UserGroups ug = ugp.get(user);

       assertEquals("UserGroups objects differ", refGet(user).toString(),
               ug.toString());
   }



   @SuppressWarnings("unchecked")
   private UserGroups refGet(String user) {

      // Create the returning object
      UserGroups userGroups = null;

      // Populate the user group data, throwing away historical values.
      HashMap<String,Double> userGroupEntries = new HashMap<String,Double>();

      for (Triple<String,String,Double> d : data) {

         if (!user.equals(d.getLeft())) continue;

         /*Double old_percentage =*/ userGroupEntries.put(d.getMiddle(), d.getRight());
      }

      // Convert the resulting entries into 'Set<UserGroup>'
      Set<UserGroup> groups = new TreeSet<UserGroup>(); for (Map.Entry<String,Double> v : userGroupEntries.entrySet()) {

         groups.add(new UserGroup(v.getKey(), v.getValue()));
      }

      // Populate the resulting object
      if (userGroups == null) { userGroups = new UserGroups(); {

         userGroups.setUser(user);
         userGroups.setGroups(groups);
      }}

      return userGroups;
   }

}
