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
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;

/**
 * A helper class for process-based mini-cluster tests. Unlike
 * {@link MiniHBaseCluster}, starts daemons as separate processes, allowing to
 * do real kill testing.
 */
public class ProcessBasedLocalHBaseCluster {

  private static final String DEFAULT_WORKDIR =
      "/tmp/hbase-" + System.getenv("USER");

  private final String hbaseHome;
  private final String workDir;

  private int numRegionServers;
  private final int zkClientPort;
  private final int masterPort;

  private final Configuration conf;

  private static final int MAX_FILE_SIZE_OVERRIDE = 10 * 1000 * 1000;

  private static final Log LOG = LogFactory.getLog(
      ProcessBasedLocalHBaseCluster.class);

  private List<String> daemonPidFiles =
      Collections.synchronizedList(new ArrayList<String>());;

  private boolean shutdownHookInstalled;

  private String hbaseDaemonScript;

  /**
   * Constructor. Modifies the passed configuration.
   * @param hbaseHome the top directory of the HBase source tree
   */
  public ProcessBasedLocalHBaseCluster(Configuration conf, String hbaseHome,
      int numRegionServers) {
    this.conf = conf;
    this.hbaseHome = hbaseHome;
    this.numRegionServers = numRegionServers;
    this.workDir = DEFAULT_WORKDIR;

    hbaseDaemonScript = hbaseHome + "/bin/hbase-daemon.sh";
    zkClientPort = HBaseTestingUtility.randomFreePort();
    masterPort = HBaseTestingUtility.randomFreePort();

    conf.set(HConstants.ZOOKEEPER_QUORUM, HConstants.LOCALHOST);
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkClientPort);
  }

  public void start() throws IOException {
    cleanupOldState();

    // start ZK
    LOG.info("Starting ZooKeeper");
    startZK();

    HBaseTestingUtility.waitForHostPort(HConstants.LOCALHOST, zkClientPort);

    startMaster();
    ZKUtil.waitForBaseZNode(conf);

    for (int idx = 0; idx < numRegionServers; idx++) {
      startRegionServer(HBaseTestingUtility.randomFreePort());
    }

    LOG.info("Waiting for HBase startup by scanning META");
    int attemptsLeft = 10;
    while (attemptsLeft-- > 0) {
      try {
        new HTable(conf, HConstants.META_TABLE_NAME);
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
    startServer("regionserver", port);
  }

  public void startMaster() {
    startServer("master", 0);
  }

  public void killRegionServer(int port) throws IOException {
    killServer("regionserver", port);
  }

  public void killMaster() throws IOException {
    killServer("master", 0);
  }

  public void startZK() {
    startServer("zookeeper", 0);
  }

  private void executeCommand(String command) {
    ensureShutdownHookInstalled();
    executeCommand(command, null);
  }

  private void executeCommand(String command, Map<String,
      String> envOverrides) {
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
        LOG.error("Could not kill process with pid from " + pidFile);
      }

      if (pid > 0) {
        LOG.info("Killing pid " + pid + " (" + pidFile + ")");
        killProcess(pid);
      }
    }

    LOG.info("Waiting a bit to let processes terminate");
    Threads.sleep(5000);
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

  private String serverWorkingDir(String serverName, int port) {
    String dir;
    if (serverName.equals("regionserver")) {
      dir = workDir + "/" + serverName + "-" + port;
    } else {
      dir = workDir + "/" + serverName;
    }
    return dir;
  }

  private int getServerPID(String serverName, int port) throws IOException {
    String pidFile = pidFilePath(serverName, port);
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

  private String pidFilePath(String serverName, int port) {
    String dir = serverWorkingDir(serverName, port);
    String user = System.getenv("USER");
    String pidFile = String.format("%s/hbase-%s-%s.pid",
                                   dir, user, serverName);
    return pidFile;
  }

  private void killServer(String serverName, int port) throws IOException {
    int pid = getServerPID(serverName, port);
    if (pid > 0) {
      LOG.info("Killing " + serverName + "; pid=" + pid);
      killProcess(pid);
    }
  }

  private void killProcess(int pid) {
    String cmd = "kill -s KILL " + pid;
    executeCommand(cmd);
  }

  private void startServer(String serverName, int rsPort) {
    String conf = generateConfig(rsPort);

    // create working directory for this region server.
    String dir = serverWorkingDir(serverName, rsPort);
    executeCommand("mkdir -p " + dir);

    writeStringToFile(conf, dir + "/" + "hbase-site.xml");

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
                   " start " + serverName, envOverrides);
    daemonPidFiles.add(pidFilePath(serverName, rsPort));
  }

  private final String generateConfig(int rsPort) {
    StringBuilder sb = new StringBuilder();
    Map<String, Object> confMap = new TreeMap<String, Object>();
    confMap.put(HConstants.CLUSTER_DISTRIBUTED, true);
    if (rsPort > 0) {
      confMap.put(HConstants.REGIONSERVER_PORT, rsPort);
      confMap.put(HConstants.REGIONSERVER_INFO_PORT_AUTO, true);
    }

    confMap.put(HConstants.ZOOKEEPER_CLIENT_PORT, zkClientPort);
    confMap.put(HConstants.MASTER_PORT, masterPort);
    confMap.put(HConstants.HREGION_MAX_FILESIZE, MAX_FILE_SIZE_OVERRIDE);
    confMap.put("fs.file.impl", RawLocalFileSystem.class.getName());

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

  public Configuration getConf() {
    return conf;
  }

}
