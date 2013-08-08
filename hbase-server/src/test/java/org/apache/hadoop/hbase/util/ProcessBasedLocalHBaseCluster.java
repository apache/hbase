/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.experimental.categories.Category;

/**
 * A helper class for process-based mini-cluster tests. Unlike
 * {@link MiniHBaseCluster}, starts daemons as separate processes, allowing to
 * do real kill testing.
 */
@Category(LargeTests.class)
public class ProcessBasedLocalHBaseCluster {

  private final String hbaseHome, workDir;
  private final Configuration conf;
  private final int numMasters, numRegionServers, numDataNodes;
  private final List<Integer> rsPorts, masterPorts;

  private final int zkClientPort;

  private static final int MAX_FILE_SIZE_OVERRIDE = 10 * 1000 * 1000;

  private static final Log LOG = LogFactory.getLog(
      ProcessBasedLocalHBaseCluster.class);

  private List<String> daemonPidFiles =
      Collections.synchronizedList(new ArrayList<String>());;

  private boolean shutdownHookInstalled;

  private String hbaseDaemonScript;

  private MiniDFSCluster dfsCluster;

  private HBaseTestingUtility testUtil;

  private Thread logTailerThread;

  private List<String> logTailDirs = Collections.synchronizedList(new ArrayList<String>());

  private static enum ServerType {
    MASTER("master"),
    RS("regionserver"),
    ZK("zookeeper");

    private final String fullName;

    private ServerType(String fullName) {
      this.fullName = fullName;
    }
  }

  /**
   * Constructor. Modifies the passed configuration.
   * @param hbaseHome the top directory of the HBase source tree
   */
  public ProcessBasedLocalHBaseCluster(Configuration conf,
      int numDataNodes, int numRegionServers) {
    this.conf = conf;
    this.hbaseHome = HBaseHomePath.getHomePath();
    this.numMasters = 1;
    this.numRegionServers = numRegionServers;
    this.workDir = hbaseHome + "/target/local_cluster";
    this.numDataNodes = numDataNodes;

    hbaseDaemonScript = hbaseHome + "/bin/hbase-daemon.sh";
    zkClientPort = HBaseTestingUtility.randomFreePort();

    this.rsPorts = sortedPorts(numRegionServers);
    this.masterPorts = sortedPorts(numMasters);

    conf.set(HConstants.ZOOKEEPER_QUORUM, HConstants.LOCALHOST);
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkClientPort);
  }

  /**
   * Makes this local HBase cluster use a mini-DFS cluster. Must be called before
   * {@link #startHBase()}.
   * @throws IOException
   */
  public void startMiniDFS() throws Exception {
    if (testUtil == null) {
      testUtil = new HBaseTestingUtility(conf);
    }
    dfsCluster = testUtil.startMiniDFSCluster(numDataNodes);
  }

  /**
   * Generates a list of random port numbers in the sorted order. A sorted
   * order makes sense if we ever want to refer to these servers by their index
   * in the returned array, e.g. server #0, #1, etc.
   */
  private static List<Integer> sortedPorts(int n) {
    List<Integer> ports = new ArrayList<Integer>(n);
    for (int i = 0; i < n; ++i) {
      ports.add(HBaseTestingUtility.randomFreePort());
    }
    Collections.sort(ports);
    return ports;
  }

  public void startHBase() throws IOException {
    startDaemonLogTailer();
    cleanupOldState();

    // start ZK
    LOG.info("Starting ZooKeeper on port " + zkClientPort);
    startZK();

    HBaseTestingUtility.waitForHostPort(HConstants.LOCALHOST, zkClientPort);

    for (int masterPort : masterPorts) {
      startMaster(masterPort);
    }

    ZKUtil.waitForBaseZNode(conf);

    for (int rsPort : rsPorts) {
      startRegionServer(rsPort);
    }

    LOG.info("Waiting for HBase startup by scanning META");
    int attemptsLeft = 10;
    while (attemptsLeft-- > 0) {
      try {
        new HTable(conf, TableName.META_TABLE_NAME);
      } catch (Exception e) {
        LOG.info("Waiting for HBase to startup. Retries left: " + attemptsLeft,
            e);
        Threads.sleep(1000);
      }
    }

    LOG.info("Process-based HBase Cluster with " + numRegionServers +
        " region servers up and running... \n\n");
  }

  public void startRegionServer(int port) {
    startServer(ServerType.RS, port);
  }

  public void startMaster(int port) {
    startServer(ServerType.MASTER, port);
  }

  public void killRegionServer(int port) throws IOException {
    killServer(ServerType.RS, port);
  }

  public void killMaster() throws IOException {
    killServer(ServerType.MASTER, 0);
  }

  public void startZK() {
    startServer(ServerType.ZK, 0);
  }

  private void executeCommand(String command) {
    executeCommand(command, null);
  }

  private void executeCommand(String command, Map<String,
      String> envOverrides) {
    ensureShutdownHookInstalled();
    LOG.debug("Command : " + command);

    try {
      String [] envp = null;
      if (envOverrides != null) {
        Map<String, String> map = new HashMap<String, String>(
            System.getenv());
        map.putAll(envOverrides);
        envp = new String[map.size()];
        int idx = 0;
        for (Map.Entry<String, String> e: map.entrySet()) {
          envp[idx++] = e.getKey() + "=" + e.getValue();
        }
      }

      Process p = Runtime.getRuntime().exec(command, envp);

      BufferedReader stdInput = new BufferedReader(
          new InputStreamReader(p.getInputStream()));
      BufferedReader stdError = new BufferedReader(
          new InputStreamReader(p.getErrorStream()));

      // read the output from the command
      String s = null;
      while ((s = stdInput.readLine()) != null) {
        System.out.println(s);
      }

      // read any errors from the attempted command
      while ((s = stdError.readLine()) != null) {
        System.out.println(s);
      }
    } catch (IOException e) {
      LOG.error("Error running: " + command, e);
    }
  }

  private void shutdownAllProcesses() {
    LOG.info("Killing daemons using pid files");
    final List<String> pidFiles = new ArrayList<String>(daemonPidFiles);
    for (String pidFile : pidFiles) {
      int pid = 0;
      try {
        pid = readPidFromFile(pidFile);
      } catch (IOException ex) {
        LOG.error("Could not read pid from file " + pidFile);
      }

      if (pid > 0) {
        LOG.info("Killing pid " + pid + " (" + pidFile + ")");
        killProcess(pid);
      }
    }
  }

  private void ensureShutdownHookInstalled() {
    if (shutdownHookInstalled) {
      return;
    }

    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        shutdownAllProcesses();
      }
    }));

    shutdownHookInstalled = true;
  }

  private void cleanupOldState() {
    executeCommand("rm -rf " + workDir);
  }

  private void writeStringToFile(String s, String fileName) {
    try {
      BufferedWriter out = new BufferedWriter(new FileWriter(fileName));
      out.write(s);
      out.close();
    } catch (IOException e) {
      LOG.error("Error writing to: " + fileName, e);
    }
  }

  private String serverWorkingDir(ServerType serverType, int port) {
    return workDir + "/" + serverType + "-" + port;
  }

  private int getServerPID(ServerType serverType, int port) throws IOException {
    String pidFile = pidFilePath(serverType, port);
    return readPidFromFile(pidFile);
  }

  private static int readPidFromFile(String pidFile) throws IOException {
    Scanner scanner = new Scanner(new File(pidFile));
    try {
      return scanner.nextInt();
    } finally {
      scanner.close();
    }
  }

  private String pidFilePath(ServerType serverType, int port) {
    String dir = serverWorkingDir(serverType, port);
    String user = System.getenv("USER");
    String pidFile = String.format("%s/hbase-%s-%s.pid",
                                   dir, user, serverType.fullName);
    return pidFile;
  }

  private void killServer(ServerType serverType, int port) throws IOException {
    int pid = getServerPID(serverType, port);
    if (pid > 0) {
      LOG.info("Killing " + serverType + "; pid=" + pid);
      killProcess(pid);
    }
  }

  private void killProcess(int pid) {
    String cmd = "kill -s KILL " + pid;
    executeCommand(cmd);
  }

  private void startServer(ServerType serverType, int rsPort) {
    // create working directory for this region server.
    String dir = serverWorkingDir(serverType, rsPort);
    String confStr = generateConfig(serverType, rsPort, dir);
    LOG.debug("Creating directory " + dir);
    new File(dir).mkdirs();

    writeStringToFile(confStr, dir + "/hbase-site.xml");

    // Set debug options to an empty string so that hbase-config.sh does not configure them
    // using default ports. If we want to run remote debugging on process-based local cluster's
    // daemons, we can automatically choose non-conflicting JDWP and JMX ports for each daemon
    // and specify them here.
    writeStringToFile(
        "unset HBASE_MASTER_OPTS\n" +
        "unset HBASE_REGIONSERVER_OPTS\n" +
        "unset HBASE_ZOOKEEPER_OPTS\n" +
        "HBASE_MASTER_DBG_OPTS=' '\n" +
        "HBASE_REGIONSERVER_DBG_OPTS=' '\n" +
        "HBASE_ZOOKEEPER_DBG_OPTS=' '\n" +
        "HBASE_MASTER_JMX_OPTS=' '\n" +
        "HBASE_REGIONSERVER_JMX_OPTS=' '\n" +
        "HBASE_ZOOKEEPER_JMX_OPTS=' '\n",
        dir + "/hbase-env.sh");

    Map<String, String> envOverrides = new HashMap<String, String>();
    envOverrides.put("HBASE_LOG_DIR", dir);
    envOverrides.put("HBASE_PID_DIR", dir);
    try {
      FileUtils.copyFile(
          new File(hbaseHome, "conf/log4j.properties"),
          new File(dir, "log4j.properties"));
    } catch (IOException ex) {
      LOG.error("Could not install log4j.properties into " + dir);
    }

    executeCommand(hbaseDaemonScript + " --config " + dir +
                   " start " + serverType.fullName, envOverrides);
    daemonPidFiles.add(pidFilePath(serverType, rsPort));
    logTailDirs.add(dir);
  }

  private final String generateConfig(ServerType serverType, int rpcPort,
      String daemonDir) {
    StringBuilder sb = new StringBuilder();
    Map<String, Object> confMap = new TreeMap<String, Object>();
    confMap.put(HConstants.CLUSTER_DISTRIBUTED, true);

    if (serverType == ServerType.MASTER) {
      confMap.put(HConstants.MASTER_PORT, rpcPort);

      int masterInfoPort = HBaseTestingUtility.randomFreePort();
      reportWebUIPort("master", masterInfoPort);
      confMap.put(HConstants.MASTER_INFO_PORT, masterInfoPort);
    } else if (serverType == ServerType.RS) {
      confMap.put(HConstants.REGIONSERVER_PORT, rpcPort);

      int rsInfoPort = HBaseTestingUtility.randomFreePort();
      reportWebUIPort("region server", rsInfoPort);
      confMap.put(HConstants.REGIONSERVER_INFO_PORT, rsInfoPort);
    } else {
      confMap.put(HConstants.ZOOKEEPER_DATA_DIR, daemonDir);
    }

    confMap.put(HConstants.ZOOKEEPER_CLIENT_PORT, zkClientPort);
    confMap.put(HConstants.HREGION_MAX_FILESIZE, MAX_FILE_SIZE_OVERRIDE);

    if (dfsCluster != null) {
      String fsURL = "hdfs://" + HConstants.LOCALHOST + ":" + dfsCluster.getNameNodePort();
      confMap.put("fs.default.name", fsURL);
      confMap.put("fs.defaultFS", fsURL);
      confMap.put("hbase.rootdir", fsURL + "/hbase_test");
    }

    sb.append("<configuration>\n");
    for (Map.Entry<String, Object> entry : confMap.entrySet()) {
      sb.append("  <property>\n");
      sb.append("    <name>" + entry.getKey() + "</name>\n");
      sb.append("    <value>" + entry.getValue() + "</value>\n");
      sb.append("  </property>\n");
    }
    sb.append("</configuration>\n");
    return sb.toString();
  }

  private static void reportWebUIPort(String daemon, int port) {
    LOG.info("Local " + daemon + " web UI is at http://"
        + HConstants.LOCALHOST + ":" + port);
  }

  public Configuration getConf() {
    return conf;
  }

  public void shutdown() {
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
    shutdownAllProcesses();
  }

  private static final Pattern TO_REMOVE_FROM_LOG_LINES_RE =
      Pattern.compile("org\\.apache\\.hadoop\\.hbase\\.");

  private static final Pattern LOG_PATH_FORMAT_RE =
      Pattern.compile("^.*/([A-Z]+)-(\\d+)/[^/]+$");

  private static String processLine(String line) {
    Matcher m = TO_REMOVE_FROM_LOG_LINES_RE.matcher(line);
    return m.replaceAll("");
  }

  private final class LocalDaemonLogTailer implements Runnable {
    private final Set<String> tailedFiles = new HashSet<String>();
    private final List<String> dirList = new ArrayList<String>();
    private final Object printLock = new Object();

    private final FilenameFilter LOG_FILES = new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".out") || name.endsWith(".log");
      }
    };

    @Override
    public void run() {
      try {
        runInternal();
      } catch (IOException ex) {
        LOG.error(ex);
      }
    }

    private void runInternal() throws IOException {
      Thread.currentThread().setName(getClass().getSimpleName());
      while (true) {
        scanDirs();
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          LOG.error("Log tailer thread interrupted", e);
          break;
        }
      }
    }

    private void scanDirs() throws FileNotFoundException {
      dirList.clear();
      dirList.addAll(logTailDirs);
      for (String d : dirList) {
        for (File f : new File(d).listFiles(LOG_FILES)) {
          String filePath = f.getAbsolutePath();
          if (!tailedFiles.contains(filePath)) {
            tailedFiles.add(filePath);
            startTailingFile(filePath);
          }
        }
      }
    }

    private void startTailingFile(final String filePath) throws FileNotFoundException {
      final PrintStream dest = filePath.endsWith(".log") ? System.err : System.out;
      final ServerType serverType;
      final int serverPort;
      Matcher m = LOG_PATH_FORMAT_RE.matcher(filePath);
      if (m.matches()) {
        serverType = ServerType.valueOf(m.group(1));
        serverPort = Integer.valueOf(m.group(2));
      } else {
        LOG.error("Unrecognized log path format: " + filePath);
        return;
      }
      final String logMsgPrefix =
          "[" + serverType + (serverPort != 0 ? ":" + serverPort : "") + "] ";

      LOG.debug("Tailing " + filePath);
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            FileInputStream fis = new FileInputStream(filePath);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line;
            while (true) {
              try {
                Thread.sleep(200);
              } catch (InterruptedException e) {
                LOG.error("Tailer for " + filePath + " interrupted");
                break;
              }
              while ((line = br.readLine()) != null) {
                line = logMsgPrefix + processLine(line);
                synchronized (printLock) {
                  if (line.endsWith("\n")) {
                    dest.print(line);
                  } else {
                    dest.println(line);
                  }
                  dest.flush();
                }
              }
            }
          } catch (IOException ex) {
            LOG.error("Failed tailing " + filePath, ex);
          }
        }
      });
      t.setDaemon(true);
      t.setName("Tailer for " + filePath);
      t.start();
    }

  }

  private void startDaemonLogTailer() {
    logTailerThread = new Thread(new LocalDaemonLogTailer());
    logTailerThread.setDaemon(true);
    logTailerThread.start();
  }

}

