package org.apache.hadoop.hbase.manual.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;

public class ProcessBasedLocalHBaseCluster {

  private static final int rsBasePort_ = 60020; // first region server port
  private static final String workDir_ = "/tmp/hbase-" + System.getenv("USER");
  private String hbaseHome_;
  private int    numRegionServers_;

  private static final Log LOG = LogFactory.getLog(ProcessBasedLocalHBaseCluster.class);


  public ProcessBasedLocalHBaseCluster(String hbaseHome,
                                       int    numRegionServers) {
    hbaseHome_ = hbaseHome;
    numRegionServers_ = numRegionServers;
  }

  public HBaseConfiguration start() {
    cleanupOldState();

    // start ZK
    startZK();

    // start master
    startMaster();

    // start region servers...
    for (int idx = 0; idx < numRegionServers_; idx++) {
      startRegionServer(rsBasePort_ + idx);
    }

    HBaseConfiguration conf = HBaseUtils.getHBaseConfFromZkNode("localhost");

    // Keep retrying until the above servers have really started.
    // We ensure this by trying to lookup the META table.
    int maxTries = 10;
    while (maxTries-- > 0) {
      try {
        new HTable(conf, HConstants.META_TABLE_NAME);
      } catch (Exception e) {
        try {
          LOG.info("Waiting for HBase to startup. Retries left.." + maxTries, e);
          Thread.sleep(1000);
        } catch (InterruptedException e1) {
        }
      }
    }
    LOG.info("###Process Based HBase Cluster with " + numRegionServers_ + " region servers up and running... \n\n");
    return conf;
  }

  public void startRegionServer(int port) {
    startServer("regionserver", port);
  }

  public void startMaster() {
    startServer("master", 0);
  }

  public void killRegionServer(int port) {
    killServer("regionserver", port);
  }

  public void killMaster() {
    killServer("master", 0);
  }

  public void startZK() {
    executeAsyncCommand(hbaseHome_ + "/bin/hbase-daemon.sh " +
                        "--config " + hbaseHome_ + "/conf " +
                        " start zookeeper");
  }

  private static void executeCommand(String command) {
    executeCommand(command, null);
  }

  private static void executeCommand(String command, Map<String, String> envOverrides) {
    LOG.debug("Command : " + command);

    try {
      String [] envp = null;
      if (envOverrides != null) {
        Map<String, String> map = new HashMap<String, String>(System.getenv());
        map.putAll(envOverrides);
        envp = new String[map.size()];
        int idx = 0;
        for (Map.Entry<String, String> e: map.entrySet()) {
          envp[idx++] = e.getKey() + "=" + e.getValue();
        }
      }

      Process p = Runtime.getRuntime().exec(command, envp);

      BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
      BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

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


  private static class CommandThread extends Thread {
    private String command_;
    private Map<String, String> envp_;

    CommandThread(String command, Map<String, String> envp) {
      command_ = command;
      envp_ = envp;
    }

    public void run() {
      executeCommand(command_, envp_);
    }
  }

  private void executeAsyncCommand(String command) {
    new CommandThread(command, null).start();
  }

  private void executeAsyncCommand(String command, Map<String, String> envOverrides) {
    new CommandThread(command, envOverrides).start();
  }

  private String generateConfig(int regionServerPortNumber) {
    return String.format(confTemplate_,
                         regionServerPortNumber);
  }

  private void cleanupOldState() {
    executeCommand("rm -rf " + workDir_);
  }

  private void writeStringToFile(String s, String fileName) {
    try {
      BufferedWriter out = new BufferedWriter(new FileWriter(fileName));
      out.write(s);
      out.close();
    }
    catch (IOException e)
    {
      LOG.error("Error writing to: " + fileName, e);
    }
  }

  private String serverWorkingDir(String serverName, int port) {
    String dir;
    if (serverName.equals("regionserver")) {
      dir = workDir_ + "/" + serverName + "-" + port;
    } else {
      dir = workDir_ + "/" + serverName;
    }
    return dir;
  }

  private int getServerPID(String serverName, int port) {
    String dir = serverWorkingDir(serverName, port);
    String user = System.getenv("USER");
    String pidFile = String.format("%s/hbase-%s-%s.pid",
                                    dir, user, serverName);
    Scanner scanner;
    try {
      scanner = new Scanner(new File(pidFile));
      int pid = scanner.nextInt();
      return pid;
    } catch (FileNotFoundException e) {
      LOG.error("Error looking up pid file: " + pidFile, e);
      return 0;
    }
  }

  private void killServer(String serverName, int port) {
    int pid = getServerPID(serverName, port);
    LOG.info("Killing " + serverName + "; pid= " + pid);
    killProcess(pid);
  }

  private void killProcess(int pid) {
    // we use abort rather than kill signal.
    String cmd = "kill -s ABRT " + pid;
    executeCommand(cmd);
  }

  // port is meaningful only for RS.
  private void startServer(String serverName, int port) {

    String conf = generateConfig(port);

    // create working directory for this region server.
    String dir = serverWorkingDir(serverName, port);
    executeCommand("mkdir -p " + dir);

    writeStringToFile(conf, dir + "/" + "hbase-site.xml");
    executeCommand("cp " + hbaseHome_ + "/conf/hbase-default.xml " + dir);

    Map<String, String> envOverrides = new HashMap<String, String>();
    envOverrides.put("HBASE_LOG_DIR", dir);
    envOverrides.put("HBASE_PID_DIR", dir);

    executeAsyncCommand(hbaseHome_ + "/bin/hbase-daemon.sh " +
                        "--config " + dir +
                        " start " + serverName,
                        envOverrides);
  }

  public static String confTemplate_ =
    "<configuration> " +
    "  <property> " +
    "    <name>hbase.cluster.distributed</name> " +
    "    <value>true</value> " +
    "    <description>The mode the cluster will be in. Possible values are " +
    "      false: standalone and pseudo-distributed setups with managed Zookeeper " +
    "      true: fully-distributed with unmanaged Zookeeper Quorum (see hbase-env.sh) " +
    "    </description> " +
    "  </property> " +
    "  <property> " +
    "    <name>hbase.regionserver.port</name> " +
    "    <value>%d</value> " +
    "    <description>The port an HBase region server binds to. " +
    "    </description> " +
    "  </property> " +
    "  <property> " +
    "    <name>hbase.regionserver.info.port.auto</name> " +
    "    <value>true</value> " +
    "    <description>Info server auto port bind. Enables automatic port " +
    "    search if hbase.regionserver.info.port is already in use. " +
    "    Useful for testing, turned off by default. " +
    "    </description> " +
    "  </property> " +
    "  <property> " +
    "    <name>hbase.hregion.max.filesize</name> " +
    "    <value>10000000</value> " +
    "    <description> " +
    "    Maximum HStoreFile size. If any one of a column families' HStoreFiles has " +
    "    grown to exceed this value, the hosting HRegion is split in two. " +
    "    Default: 256M. " +
    "    </description> " +
    "  </property> " +
    "</configuration> ";

}
