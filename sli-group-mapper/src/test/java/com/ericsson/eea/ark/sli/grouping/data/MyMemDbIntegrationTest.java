package com.ericsson.eea.ark.sli.grouping.data;


import static org.junit.Assert.*;

import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.ericsson.eea.ark.offline.utils.UserGroups;
import com.ericsson.eea.ark.offline.utils.UserGroups.UserGroup;
import com.ericsson.eea.ark.sli.grouping.data.BaseTest;

public class MyMemDbIntegrationTest extends BaseTest {

    protected final static Logger  logger = Logger.getLogger(MyMemDbIntegrationTest.class);
    @Override public Logger logger() { return logger; }

   @Test
   public void testInsert_MemDb() {
       UserGroups ugs = new UserGroups();
       ugs.setUser("123456789");
       Set<UserGroup> gs = new TreeSet<>();
       gs.add(new UserGroup("default",.33333333333333333333333333334D));
       gs.add(new UserGroup("g0",.33333333333333333333333333333D));
       gs.add(new UserGroup("g1",.33333333333333333333333333333D));
       ugs.setGroups(gs);
       gmcu.replace(ugs);
       UserGroups got = ugp.get("123456789");
       assertEquals("put user group doesn't match got user group", ugs, got);
//
//      HTableInterface table = utility.createTable(Bytes.toBytes("MyTest"), Bytes.toBytes("CF"));
//      HBaseTestObj    obj   = new HBaseTestObj(); {
//
//         obj.setRowKey("ROWKEY-1");
//         obj.setData1("DATA-1");
//         obj.setData2("DATA-2");
//      }
//
//      MyHBaseDAO.insertRecord(table, obj);
//
//      Get get = new Get(Bytes.toBytes(obj.getRowKey())); {
//
//         get.addColumn(CF, CQ1);
//      }
//
//      Result result = table.get(get);
//
//      assertEquals(Bytes.toString(result.getRow()), obj.getRowKey());
//      assertEquals(Bytes.toString(result.value ()), obj.getData1 ());
   }
}
