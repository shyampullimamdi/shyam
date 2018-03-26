package com.ericsson.eea.ark.sli.grouping.data;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import com.ericsson.eea.ark.offline.config.Config;
import com.ericsson.eea.ark.offline.config.PropertyManager;
import com.ericsson.eea.ark.offline.utils.UserGroupsProvider;
import com.ericsson.eea.ark.offline.utils.UserGroupsProviderFactory;
import com.ericsson.eea.ark.sli.groups.client.data.UserGroupsProvider_MemDb;
import com.ericsson.eea.ark.sli.groups.client.util.GrpMapCliUtil;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class BaseTest {

   protected abstract Logger logger();

    //MemDb
    protected static GrpMapCliUtil gmcu;


    protected static Configuration conf;
    protected static String origDefaultFS = "file:///";
    protected static PropertyManager cfg = null;
    protected static UserGroupsProvider ugp;

    private static boolean isAllSetUp;

    @BeforeClass
    public static void setUpBeforeClass() {
        isAllSetUp = true;
        if (System.getProperty("java.security.auth.login.config") == null) {
            System.setProperty("java.security.auth.login.config",
                    "config/mapr.login.conf");
        }
        if (System.getProperty("base.dir") == null) {
            System.setProperty("base.dir", new File(".").getAbsolutePath()
                    .replaceAll("\\\\", "/").replaceAll("/\\.$", ""));
        }
        conf = checkForWindowsPatch(new Configuration());
        origDefaultFS = conf.get("fs.defaultFS", origDefaultFS);
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapred.job.tracker", "local");
        try {
            String configPath, configFile;
            String gmc = System.getenv("GROUP_MAPPER_CONF");
            if (gmc == null) {
                gmc = new File("./config/group-mapper-client.conf").getAbsoluteFile()
                        .toString().replaceAll("\\\\", "/")
                        .replaceAll("/\\./", "/");
                if (! new File(gmc).exists()) {
                    gmc = new File("./config/group-mapper.conf").getAbsoluteFile()
                            .toString().replaceAll("\\\\", "/")
                            .replaceAll("/\\./", "/");
                }
            } else {
                gmc = System.getenv("GROUP_MAPPER_CONF");
            }
            configPath = new Path(gmc).getParent().toString();
            configFile = new Path(gmc).getName();
            Config._cn_      = Config.cfg_prefix = "group-mapper";
            Config.cfgEnvVar                     = "GROUP_MAPPER_CONF";
            cfg = Config.init(configPath, configFile);
            conf.set("group-mapper.configurationDir", configPath);
            conf.set("group-mapper.configurationFile", configFile);
            conf.set("group-mapper.tableNameMemDb", Config.tableNameMemDb);
        } catch (Throwable t) {
            String msg = Config._cn_
                    + ": Cannot create/execute test.";
            Logger.getLogger(BaseTest.class).error(msg, t);
            throw new Error(t);
        }

        UserGroupsProviderFactory.setProviderClass(UserGroupsProvider_MemDb.class);
        (ugp = (UserGroupsProvider) UserGroupsProviderFactory.getUserGroupsProvider(conf)).initialize(conf);

        gmcu = GrpMapCliUtil.instance(conf);

    }

    public static Configuration checkForWindowsPatch(Configuration conf) {
        if (System.getProperty("os.name").toLowerCase().indexOf("windows") == 0)
            try {
                Class.forName("com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
                conf.set("fs.file.impl",
                        "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
            } catch (Exception e) {
                String msg = "Error: The 'HADOOP-7682' patch Jar file was not found in the Maven class-path.";
                System.err.println(msg);
                System.exit(1);
            }
        return conf;
    }

    @Before
    public void setupBefore() {
        assertYouDidEverythingRight();
    }
    @After
    public void tearDownAfter() {
        assertYouDidEverythingRight();
    }
    private void assertYouDidEverythingRight() {
        if (!isAllSetUp) throw new Error("setupBeforeClass and tearDownAfterClass should be being called automatically by the JUnit Framework");
    }

    @AfterClass
    public static void tearDownAfterClass() {
        isAllSetUp = false;
        if (gmcu != null) { try { gmcu.close(); } catch (Exception e) { e.printStackTrace(); }}
        if (ugp instanceof UserGroupsProvider_MemDb) { try { ((UserGroupsProvider_MemDb) ugp).close(); } catch (Exception e) { e.printStackTrace(); }}
        gmcu = null; ugp = null;
    }
}
