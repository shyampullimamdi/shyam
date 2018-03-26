package com.ericsson.eea.ark.sli.grouping.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Properties;
import java.util.regex.Matcher;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.ark.core.ArkContext;
import com.ericsson.ark.core.arkfile.ArkFileFactory;

public class InitUpgSliGM {

    protected static final String GROUP_MAPPER_CONF_FILE = "group-mapper.conf";

    protected static final String PROP_DATA_GRID_AVAIL = "group-mapper.dataGrid.available";
    protected static final String PROP_DESC_FILE = "group-mapper.rules.descriptionsFile";
    protected static final String PROP_PARMS_FILE = "group-mapper.rules.parametersFile";
    protected static final String PROP_BASE_INPUT_DIR = "group-mapper.baseInputDir";
    protected static final String PROP_RULES_FILE = "group-mapper.rules.file";
    protected static final String PROP_CUSTOM_DIR = "group-mapper.rules.customDir";

    protected static final String V0_DATA_GRID_AVAIL = "false";
    protected static final String V0_BASE_INPUT_DIR = "hdfs:///eea_data/profiler-services/exporter/esr/esrProfilerDefaultDatabase";
    protected static final String V0_GROUP_MAPPER_BASE_CONFIG = "hdfs:///eea_config/group-mapper";
    protected static final String V0_DESC_FILE = V0_GROUP_MAPPER_BASE_CONFIG+"/rules/rule_descriptions.csv";
    protected static final String V0_PARMS_FILE = V0_GROUP_MAPPER_BASE_CONFIG+"/rules/rule_parameters.csv";
    protected static final String V0_RULES_FILE = V0_GROUP_MAPPER_BASE_CONFIG+"/rules/SLI-user_groups-rules-base.drl";
    protected static final String V0_CUSTOM_DIR = V0_GROUP_MAPPER_BASE_CONFIG+"/rules/custom";

    protected static final String V1_DATA_GRID_AVAIL = "true";
    protected static final String V1_BASE_INPUT_DIR = "/eea_data/profiler_services/exporter/esr/esrProfilerDefaultDatabase";
    protected static final String V1_GROUP_MAPPER_BASE_CONFIG = "/eea_config/sli-group-mapper";
    protected static final String V1_DESC_FILE = V1_GROUP_MAPPER_BASE_CONFIG+"/rules/rule_descriptions.csv";
    protected static final String V1_PARMS_FILE = V1_GROUP_MAPPER_BASE_CONFIG+"/rules/rule_parameters.csv";
    protected static final String V1_RULES_FILE = V1_GROUP_MAPPER_BASE_CONFIG+"/rules/SLI-user_groups-rules-base.drl";
    protected static final String V1_CUSTOM_DIR = V1_GROUP_MAPPER_BASE_CONFIG+"/rules/custom";

    protected static final Logger log = LoggerFactory.getLogger(InitUpgSliGM.class.getName());

    protected final static boolean win = (System.getProperty("os.name").toLowerCase().indexOf("windows") == 0);

    protected final static String mainAppNamePfx = "SLI_group-mapper";
    protected final static String initAppNamePfx = "SLI_grp-mapper-init";
    protected final static String initStartupAppNamePfx = "SLI_grp-mapper-init-startup";
    protected final static String upgradeAppNamePfx = "SLI_grp-mapper-upgrade";

    protected static final ArkContext arkCtx;
    static {
        ArkContext ac;
        try {
            ac = ArkContext.create();
        } catch (Throwable t) {
            ac = null;
        }
        arkCtx = ac;
    }
    protected static final FileSystem fromFs, toFs;
    static {
        FileSystem fs1;
        try {
            fs1 = ArkFileFactory.getHdfsFileSystem();
        } catch (IOException e) {
            e.printStackTrace();
            fs1 = null;
        }
        toFs = fs1;
        if (win) {
            FileSystem fFs;
            try {
                fFs = win ? FileSystem.getLocal(fs1.getConf()) : fs1;
            } catch (IOException e) {
                fFs = null;
            }
            fromFs = fFs;
        } else {
            fromFs = fs1;
        }
    }
    protected static final String userName = System.getProperty("user.name", "root");

    protected final String[] args;
    protected final boolean upgrade;
    protected final Set<Path> copiedToUpgrade = new HashSet<>();

    protected InitUpgSliGM(String[] args, boolean upgrade) {
        this.args = args;
        this.upgrade = upgrade;
    }

    protected void exec() {
        String upgradeVersion = null;
        String oldVersion = null, newVersion = null;

        log.info("args: " + Arrays.toString(args));
        log.info("userName = "+userName);

        for (int i = 0; i < args.length-1; i++) {
            String opt = args[i];
            String arg = args[i+1];
            if ("--version".equals(opt)) {
                newVersion = arg;
            } else if (upgrade) {
                if ("--old_version".equals(opt)) {
                    oldVersion = arg;
                } else if ("--upgrade_version".equals(opt)) {
                    upgradeVersion = arg;
                }
            }
        }

        boolean valid = true;
        if (newVersion == null) {
            log.error("--version argument missing from program arguments");
            valid = false;
        } else {
            log.info("got newVersion from args: " + newVersion);
        }
        if (upgrade) {
            if (oldVersion == null) {
                log.error("--old_version argument missing from program arguments");
                valid = false;
            } else {
                log.info("got oldVersion from args: " + oldVersion);
            }
            if (upgradeVersion == null) {
                log.error("--upgrade_version argument missing from program arguments");
                valid = false;
            } else if ( ! upgradeVersion.matches("\\d+")){
                log.error("invalid --upgrade_version \""+upgradeVersion+"\" - must be strictly numeric");
                valid = false;
            } else {
                log.info("got upgradeVersion from args: " + upgradeVersion);
            }
        }
        if (arkCtx == null && !win) {
            log.error("can't get ark context");
            valid = false;
        }
        if (fromFs == null || toFs == null) {
            log.error("can't get file system handle ["+fromFs+"]->["+toFs+"]");
            valid = false;
        }

        if (!valid) {
            throw new IllegalArgumentException("one or more arguments/runtime requirements are invalid/missing");
        }

        UserGroupInformation realUser = UserGroupInformation.createRemoteUser("mapr");
        UserGroupInformation.setLoginUser(realUser);

        boolean needDeleteOldPath = false;

        if (upgrade) {
            int upgradeVersionNumber = Integer.parseInt(upgradeVersion);

            if (upgradeVersionNumber >= 1) {
                needDeleteOldPath = performUpgradeV1(oldVersion, newVersion);
            }
        }

        performInit(newVersion);

        if (needDeleteOldPath) {
            Path toDel = new Path(V0_GROUP_MAPPER_BASE_CONFIG);
            try {
                toFs.delete(toDel, true);
                log.info("deleted old path "+toDel);
            } catch (IOException e) {
                log.warn("failed to delete old path "+ toDel + ": "+e);
                e.printStackTrace();
            }
        }

        if (arkCtx != null) {
            log.info("about to close ark context...");
            try { arkCtx.close(); } catch (Throwable e) {}
        }
        if (toFs != null) {
            log.info("about to close fs...");
            try { fromFs.close(); } catch (Throwable e) {}
        }
        if (fromFs != null && ! fromFs.equals(toFs)) {
            log.info("about to close 'from' fs...");
            try { fromFs.close(); } catch (Throwable e) {}
        }

        log.info("...closed everything! complete!");
    }

    protected void performInit(String appVersion) {
        log.info("Initialize SLI Group Member ["+appVersion+"] starting...");
        List<Path> fromPaths = new ArrayList<>();
        Path upgPath = null;
        {
            Path fromPath0, fromPath1, fromPath2, fromPath3;
            if (win) {
                Path base = new Path("file:///"+new File("src/main/resources/app_config/rules").getAbsolutePath());
                fromPath0 = fromPath1 = fromPath2 = fromPath3 = base;
            } else {
                fromPath0 = new Path(String.format("hdfs:///user/%s/ark_apps/%s-%s", userName, initStartupAppNamePfx, appVersion));
                fromPath1 = new Path(String.format("hdfs:///user/%s/ark_apps/%s-%s", userName, initAppNamePfx, appVersion));
                fromPath2 = new Path(String.format("hdfs:///user/%s/ark_apps/%s-%s", userName, mainAppNamePfx, appVersion));
                fromPath3 = new Path(String.format("hdfs:///user/%s/ark_apps/%s-%s", userName, upgradeAppNamePfx, appVersion));
            }
            if (fromFs_isDirectory(fromPath0)) {
                fromPaths.add(fromPath0);
            }
            if (fromFs_isDirectory(fromPath1)) {
                fromPaths.add(fromPath1);
            }
            if (fromFs_isDirectory(fromPath2)) {
                fromPaths.add(fromPath2);
            }
            if (fromFs_isDirectory(fromPath3)) {
                upgPath = fromPath3;
                fromPaths.add(fromPath3);
            }
            if (fromPaths.isEmpty()) {
                throw new IllegalArgumentException("can't find application init/upgrade path! ["+fromPath0+"] ["+fromPath1+"] ["+fromPath2+"] ["+fromPath3+"]");
            }
        }
        Path fromConfPath = null;
        for (Path fromPath : fromPaths) {
            Path candidate = new Path(fromPath, GROUP_MAPPER_CONF_FILE);
            if (fromFs_isFile(candidate)) fromConfPath = candidate;
        }
        if (fromConfPath == null) {
            throw new RuntimeException("can't find "+GROUP_MAPPER_CONF_FILE+" in these paths: "+fromPaths);
        } else {
            log.info(GROUP_MAPPER_CONF_FILE+" found here: " + fromConfPath);
        }
        log.info("fromPaths="+fromPaths);
        Properties p = new Properties();
        FSDataInputStream confIs = null;
        try {
            confIs = fromFs.open(fromConfPath);
            p.load(confIs);
        } catch (IOException e1) {
            throw new RuntimeException("can't load "+GROUP_MAPPER_CONF_FILE+" from "+fromConfPath);
        } finally {
            if (confIs != null) { try { confIs.close(); } catch (Exception e) {} confIs = null; }
        }
        String rulesFile = p.getProperty(PROP_RULES_FILE, V1_RULES_FILE);
        String customPathS = p.getProperty(PROP_CUSTOM_DIR);
        Path toPath1 = new Path(rulesFile).getParent();
        Path toPath2 = customPathS == null ? new Path(toPath1, "custom") : new Path(customPathS);
        log.info("toPath1 = "+toPath1);
        log.info("toPath2 = "+toPath2);
        try {
            toFs.mkdirs(toPath1);
            log.info("mkdirs successful: "+toPath1);
        } catch (IllegalArgumentException | IOException e) {
            log.error("couldn't mkdirs "+toPath1+": "+e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        try {
            toFs.mkdirs(toPath2);
            log.info("mkdirs successful: "+toPath2);
        } catch (IllegalArgumentException | IOException e) {
            log.error("couldn't mkdirs "+toPath2+": "+e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        int dirs = 0, files = 0;
        for (Path fromPath : fromPaths) {
            log.info("start processing fromPath = "+fromPath);
            FileStatus[] stats = null;
            try {
                stats = fromFs.listStatus(fromPath,new PathFilter() {
                    @Override
                    public boolean accept(Path path) {
                        String name = path.getName();
                        boolean accept = fromFs_isFile(path) &&
                                (name.endsWith(".csv") ||
                                name.endsWith(".properties") ||
                                name.endsWith(".drl"));
                        log.info("accept == "+accept+" for "+path);
                        return accept;
                    }
                } );
            } catch (IOException e) {
                log.warn("couldn't list files in "+fromPath+": "+e.getMessage());
            }
            if (stats == null || stats.length == 0) {
                log.warn("couldn't find any files in "+fromPath);
            }
            dirs++;
            for (FileStatus stat : stats) {
                Path fromFile = stat.getPath();
                String name = stat.getPath().getName();
                Path toPath;
                if (name.equals("SLI-user_groups-rules-base.drl") ||
                        name.equals("rule_parameters.csv") ||
                        name.equals("rule_descriptions.csv")) {
                    toPath = toPath1;
                } else {
                    toPath = toPath2;
                }
                Path toFile = new Path(toPath, name);
                if (copiedToUpgrade.contains(fromFile)) {
                    log.info("(((skip "+fromFile+" because already processed)))");
                } else {
                    fromToFs_copy(fromFile, toFile);
                    files++;
                    if (upgPath != null) {
                        Path toUpgFile = new Path(upgPath, name);
                        fromFromFs_copy(fromFile, toUpgFile);
                        copiedToUpgrade.add(toUpgFile);
                    }
                }
            }
            log.info("end processing fromPath = "+fromPath);
        }
        if (dirs == 0) {
            String msg = "all dirs empty - fromPaths="+fromPaths;
            log.error(msg);
            throw new RuntimeException(msg);
        } else if (files == 0) {
            String msg = "no files found in dirs - fromPaths="+fromPaths;
            log.error(msg);
            throw new RuntimeException(msg);
        }
        if (upgPath != null) {
            Path toConfPath = new Path(upgPath, GROUP_MAPPER_CONF_FILE);
            fromFromFs_copy(fromConfPath, toConfPath);
        }
        log.info("...initialize SLI Group Member ["+appVersion+"] complete.");
    }

    protected boolean fromFs_isFile(Path path) {
        try {
            return fromFs.isFile(path);
        } catch (IOException e) {
            return false;
        }
    }

    protected boolean fromFs_isDirectory(Path path) {
        try {
            return fromFs.isDirectory(path);
        } catch (IOException e) {
            return false;
        }
    }

    protected void fromFromFs_copy(Path fromFile, Path toFile) {
        xxFs_Copy(fromFile, fromFs, toFile, fromFs);
    }

    protected void fromToFs_copy(Path fromFile, Path toFile) {
        xxFs_Copy(fromFile, fromFs, toFile, toFs);
    }

    protected void xxFs_Copy(Path srcFile, FileSystem srcFs, Path trgFile, FileSystem trgFs) {
        if (Path.getPathWithoutSchemeAndAuthority(srcFile).equals(Path.getPathWithoutSchemeAndAuthority(trgFile))) {
            log.info("do not copy cuz source == target. source: "+srcFile + "; target: " + trgFile);
            return;
        } else {
            log.info("copy cuz source != target. source: "+srcFile + "; target: " + trgFile);
        }
        log.info("about to copy from file \"" + srcFile + "\" to file \"" + trgFile + "\".");
        dumpFile(srcFile, srcFs, "from file before copy: "+srcFile);
        dumpFile(trgFile, trgFs, "to file before copy: "+trgFile);
        FSDataInputStream in = null;;
        InputStreamReader isr = null;
        BufferedReader r = null;
        FSDataOutputStream out = null;;
        OutputStreamWriter osw = null;
        BufferedWriter w = null;
        try {
            try {
                in = srcFs.open(srcFile);
                r = new BufferedReader(isr = new InputStreamReader(in));
            } catch (IOException e) {
                log.error("couldn't open "+srcFile+" for read: "+e.getMessage());
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            try {
                out = trgFs.create(trgFile, true);
                w = new BufferedWriter(osw = new OutputStreamWriter(out));
            } catch (IOException e) {
                log.error("couldn't open "+trgFile+" for write: "+e.getMessage());
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            try {
                String line = r.readLine();
                while (line != null) {
                    w.write(line);
                    w.newLine();
                    line = r.readLine();
                }
            } catch (IOException e) {
                log.error("couldn't copy from "+srcFile+" to "+trgFile+": "+e.getMessage());
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        } finally {
            if (r != null) { try { r.close(); } catch (Exception e) {} r = null; }
            if (isr != null) { try { isr.close(); } catch (Exception e) {} isr = null; }
            if (in != null) { try { in.close(); } catch (Exception e) {} in = null; }
            if (w != null) { try { w.close(); } catch (Exception e) {} w = null; }
            if (osw != null) { try { osw.close(); } catch (Exception e) {} osw = null; }
            if (out != null) { try { out.close(); } catch (Exception e) {} out = null; }
        }
        log.info("...copied from file \"" + srcFile + "\" to file \"" + trgFile + "\".");
        dumpFile(trgFile, trgFs, "to file AFTER copy: "+trgFile);
    }

    protected void dumpFile(Path file, FileSystem fs, String eyec) {
        log.info("dumping file \""+file+"\": "+eyec);
        try {
            String ss = null;
            FSDataInputStream is = null; BufferedReader d = null;
            try {
                is = fs.open(file);
                d = new BufferedReader(new InputStreamReader(is));
                String s = d.readLine();
                while (s != null) {
                    if (ss == null) ss = s+"\n"; else ss +=s+"\n";
                    s = d.readLine();
                }
                if (ss == null) {
                    log.info("*** FILE HAS NO CONTENT! ***");
                } else {
                    log.info(">>> FILE CONTENTS:\n"+ss);
                }
            } finally {
                if (d != null) { try { d.close(); } catch (Exception e) { } finally {  d = null; } }
                if (is != null) { try { is.close(); } catch (Exception e) { } finally { is = null; } }
            }
        } catch (Throwable t) {
            if (t instanceof FileNotFoundException) {
                log.info("(((FILE NOT FOUND)))");
            } else {
                log.error("error dumping file "+eyec + "; "+t);
                t.printStackTrace();
            }
        }
        log.info("dumped file \""+file+"\": "+eyec);
    }

    protected boolean performUpgradeV1(String oldVersion, String newVersion) {
        boolean needDeleteOldPath = false;
        log.info("Upgrade V1 SLI Group Member ["+oldVersion+"]->["+newVersion+"] starting...");
        Path fromConfPath = new Path(String.format("hdfs:///user/%s/ark_apps/%s-%s/"+GROUP_MAPPER_CONF_FILE, userName, mainAppNamePfx, newVersion));
        Path toConfPath = new Path(String.format("hdfs:///user/%s/ark_apps/%s-%s/"+GROUP_MAPPER_CONF_FILE, userName, upgradeAppNamePfx, newVersion));
        if ( ! fromFs_isFile(fromConfPath)) {
            throw new RuntimeException("can't find "+GROUP_MAPPER_CONF_FILE+" in this path: "+fromConfPath);
        } else {
            log.info(GROUP_MAPPER_CONF_FILE+" found here: " + fromConfPath);
        }
        Properties p = new Properties();
        {
            FSDataInputStream confIs = null;
            try {
                confIs = fromFs.open(fromConfPath);
                p.load(confIs);
            } catch (IOException e1) {
                throw new RuntimeException("can't load "+GROUP_MAPPER_CONF_FILE+" from "+fromConfPath);
            } finally {
                if (confIs != null) { try { confIs.close(); } catch (Exception e) {} confIs = null; }
            }
        }
        String bid = p.getProperty(PROP_BASE_INPUT_DIR, V0_BASE_INPUT_DIR);
        String rf = p.getProperty(PROP_RULES_FILE, V0_RULES_FILE);
        String cd = p.getProperty(PROP_CUSTOM_DIR, V0_CUSTOM_DIR);
        String pf = p.getProperty(PROP_PARMS_FILE, V0_PARMS_FILE);
        String df = p.getProperty(PROP_DESC_FILE, V0_DESC_FILE);
        String dga = p.getProperty(PROP_DATA_GRID_AVAIL, V0_DATA_GRID_AVAIL);
        if (bid.equals(V0_BASE_INPUT_DIR) &&
                rf.equals(V0_RULES_FILE) &&
                cd.equals(V0_CUSTOM_DIR) &&
                pf.equals(V0_PARMS_FILE) &&
                df.equals(V0_DESC_FILE) &&
                dga.equals(V0_DATA_GRID_AVAIL)) {
            FSDataInputStream confIs = null;
            FSDataOutputStream outOs = null;
            InputStreamReader isr = null;
            OutputStreamWriter osw = null;
            BufferedWriter w = null;
            BufferedReader r = null;
            try {
                log.info("gonna copy from "+fromConfPath);
                log.info("gonna copy to "+toConfPath);
                outOs = fromFs.create(toConfPath, true);
                confIs = fromFs.open(fromConfPath);
                r  = new BufferedReader(isr = new InputStreamReader(confIs));
                w  = new BufferedWriter(osw = new OutputStreamWriter(outOs));
                String line = r.readLine();
                while (line != null) {
                    log.info("read a line....: "+line);
                    String newLine = translateV0toV1(line);
                    log.info("translated line: "+newLine);
                    w.write(newLine);
                    w.newLine();
                    line = r.readLine();
                }
                log.info("done reading/writing. close now.");
            } catch (IOException e1) {
                throw new RuntimeException("can't load "+GROUP_MAPPER_CONF_FILE+" from "+fromConfPath);
            } finally {
                if (r != null) { try { r.close(); } catch (Exception e) {} r = null; }
                if (isr != null) { try { isr.close(); } catch (Exception e) {} isr = null; }
                if (confIs != null) { try { confIs.close(); } catch (Exception e) {} confIs = null; }
                if (w != null) { try { w.close(); } catch (Exception e) {} w = null; }
                if (osw != null) { try { osw.close(); } catch (Exception e) {} osw = null; }
                if (outOs != null) { try { outOs.close(); } catch (Exception e) {} outOs = null; }
            }
            dumpFile(fromConfPath, fromFs, "<<< config file before <<<");
            dumpFile(toConfPath, fromFs, ">>> config file after <<<");
            // copy it back to the orig:
            fromFromFs_copy(toConfPath, fromConfPath);
            needDeleteOldPath = true;
        } else {
            log.info("no need to upgrade - upgrade was already done or customer already custom-configed the old conf file");
        }
        log.info("...upgrade V1 SLI Group Member ["+oldVersion+"]->["+newVersion+"] complete.");
        return needDeleteOldPath;
    }

    protected String translateV0toV1(String line) {
        String newLine = line;
        newLine = translate(newLine, PROP_BASE_INPUT_DIR, V0_BASE_INPUT_DIR, V1_BASE_INPUT_DIR);
        newLine = translate(newLine, PROP_RULES_FILE, V0_RULES_FILE, V1_RULES_FILE);
        newLine = translate(newLine, PROP_CUSTOM_DIR, V0_CUSTOM_DIR, V1_CUSTOM_DIR);
        newLine = translate(newLine, PROP_PARMS_FILE, V0_PARMS_FILE, V1_PARMS_FILE);
        newLine = translate(newLine, PROP_DESC_FILE, V0_DESC_FILE, V1_DESC_FILE);
        newLine = translate(newLine, PROP_DATA_GRID_AVAIL, V0_DATA_GRID_AVAIL, V1_DATA_GRID_AVAIL);
        return newLine;
    }

    protected String translate(String oldLine, String prop, String oldVal, String newVal) {
        String propQR = Matcher.quoteReplacement(prop);
        String oldValQR = Matcher.quoteReplacement(oldVal);
        String newLine = oldLine;
        if (newLine.matches("^\\s*"+propQR+"\\s*=\\s*"+oldValQR+"\\s*$")) {
            log.info("before replacement >>>"+newLine);
            newLine = newLine.replaceFirst(oldValQR, newVal);
            log.info("after replacement >>>"+newLine);
        } else if (newLine.matches("^\\s*"+propQR+"\\s*=\\s*$")) {
            log.info("before replacement >>>"+newLine);
            newLine = newLine.replaceFirst("=\\s*$", "= "+newVal);
            log.info("after replacement >>>"+newLine);
        }
        return newLine;
    }

}
