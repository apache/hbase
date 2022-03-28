/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.zookeeper;

import static org.apache.zookeeper.client.FourLetterWordMain.send4LetterWord;
import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.BindException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: Most of the code in this class is ripped from ZooKeeper tests. Instead
 * of redoing it, we should contribute updates to their code which let us more
 * easily access testing helper objects.
 */
@InterfaceAudience.Public
public class MiniZooKeeperCluster {
  private static final Logger LOG = LoggerFactory.getLogger(MiniZooKeeperCluster.class);
  private static final int TICK_TIME = 2000;
  private static final int TIMEOUT = 1000;
  private static final int DEFAULT_CONNECTION_TIMEOUT = 30000;
  private static final byte[] STATIC_BYTES = Bytes.toBytes("stat");
  private final int connectionTimeout;
  public static final String LOOPBACK_HOST = InetAddress.getLoopbackAddress().getHostName();
  public static final String HOST = LOOPBACK_HOST;

  private boolean started;

  /**
   * The default port. If zero, we use a random port.
   */
  private int defaultClientPort = 0;

  private final List<NIOServerCnxnFactory> standaloneServerFactoryList;
  private final List<ZooKeeperServer> zooKeeperServers;
  private final List<Integer> clientPortList;

  private int activeZKServerIndex;
  private int tickTime = 0;

  private final Configuration configuration;

  public MiniZooKeeperCluster() {
    this(new Configuration());
  }

  public MiniZooKeeperCluster(Configuration configuration) {
    this.started = false;
    this.configuration = configuration;
    activeZKServerIndex = -1;
    zooKeeperServers = new ArrayList<>();
    clientPortList = new ArrayList<>();
    standaloneServerFactoryList = new ArrayList<>();
    connectionTimeout = configuration
      .getInt(HConstants.ZK_SESSION_TIMEOUT + ".localHBaseCluster", DEFAULT_CONNECTION_TIMEOUT);
  }

  /**
   * Add a client port to the list.
   *
   * @param clientPort the specified port
   */
  public void addClientPort(int clientPort) {
    clientPortList.add(clientPort);
  }

  /**
   * Get the list of client ports.
   *
   * @return clientPortList the client port list
   */
  @InterfaceAudience.Private
  public List<Integer> getClientPortList() {
    return clientPortList;
  }

  /**
   * Check whether the client port in a specific position of the client port list is valid.
   *
   * @param index the specified position
   */
  private boolean hasValidClientPortInList(int index) {
    return (clientPortList.size() > index && clientPortList.get(index) > 0);
  }

  public void setDefaultClientPort(int clientPort) {
    if (clientPort <= 0) {
      throw new IllegalArgumentException("Invalid default ZK client port: " + clientPort);
    }
    this.defaultClientPort = clientPort;
  }

  /**
   * Selects a ZK client port.
   *
   * @param seedPort the seed port to start with; -1 means first time.
   * @return a valid and unused client port
   */
  private int selectClientPort(int seedPort) {
    int i;
    int returnClientPort = seedPort + 1;
    if (returnClientPort == 0) {
      // If the new port is invalid, find one - starting with the default client port.
      // If the default client port is not specified, starting with a random port.
      // The random port is selected from the range between 49152 to 65535. These ports cannot be
      // registered with IANA and are intended for dynamic allocation (see http://bit.ly/dynports).
      if (defaultClientPort > 0) {
        returnClientPort = defaultClientPort;
      } else {
        returnClientPort = 0xc000 + ThreadLocalRandom.current().nextInt(0x3f00);
      }
    }
    // Make sure that the port is unused.
    // break when an unused port is found
    do {
      for (i = 0; i < clientPortList.size(); i++) {
        if (returnClientPort == clientPortList.get(i)) {
          // Already used. Update the port and retry.
          returnClientPort++;
          break;
        }
      }
    } while (i != clientPortList.size());
    return returnClientPort;
  }

  public void setTickTime(int tickTime) {
    this.tickTime = tickTime;
  }

  public int getBackupZooKeeperServerNum() {
    return zooKeeperServers.size() - 1;
  }

  public int getZooKeeperServerNum() {
    return zooKeeperServers.size();
  }

  // / XXX: From o.a.zk.t.ClientBase
  private static void setupTestEnv() {
    // during the tests we run with 100K prealloc in the logs.
    // on windows systems prealloc of 64M was seen to take ~15seconds
    // resulting in test failure (client timeout on first session).
    // set env and directly in order to handle static init/gc issues
    System.setProperty("zookeeper.preAllocSize", "100");
    FileTxnLog.setPreallocSize(100 * 1024);
    // allow all 4 letter words
    System.setProperty("zookeeper.4lw.commands.whitelist", "*");
  }

  public int startup(File baseDir) throws IOException, InterruptedException {
    int numZooKeeperServers = clientPortList.size();
    if (numZooKeeperServers == 0) {
      numZooKeeperServers = 1; // need at least 1 ZK server for testing
    }
    return startup(baseDir, numZooKeeperServers);
  }

  /**
   * @param baseDir             the base directory to use
   * @param numZooKeeperServers the number of ZooKeeper servers
   * @return ClientPort server bound to, -1 if there was a binding problem and we couldn't pick
   *   another port.
   * @throws IOException          if an operation fails during the startup
   * @throws InterruptedException if the startup fails
   */
  public int startup(File baseDir, int numZooKeeperServers)
      throws IOException, InterruptedException {
    if (numZooKeeperServers <= 0) {
      return -1;
    }

    setupTestEnv();
    shutdown();

    int tentativePort = -1; // the seed port
    int currentClientPort;

    // running all the ZK servers
    for (int i = 0; i < numZooKeeperServers; i++) {
      File dir = new File(baseDir, "zookeeper_" + i).getAbsoluteFile();
      createDir(dir);
      int tickTimeToUse;
      if (this.tickTime > 0) {
        tickTimeToUse = this.tickTime;
      } else {
        tickTimeToUse = TICK_TIME;
      }

      // Set up client port - if we have already had a list of valid ports, use it.
      if (hasValidClientPortInList(i)) {
        currentClientPort = clientPortList.get(i);
      } else {
        tentativePort = selectClientPort(tentativePort); // update the seed
        currentClientPort = tentativePort;
      }

      ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTimeToUse);
      // Setting {min,max}SessionTimeout defaults to be the same as in Zookeeper
      server.setMinSessionTimeout(configuration.getInt("hbase.zookeeper.property.minSessionTimeout",
        -1));
      server.setMaxSessionTimeout(configuration.getInt("hbase.zookeeper.property.maxSessionTimeout",
        -1));
      NIOServerCnxnFactory standaloneServerFactory;
      while (true) {
        try {
          standaloneServerFactory = new NIOServerCnxnFactory();
          standaloneServerFactory.configure(new InetSocketAddress(LOOPBACK_HOST, currentClientPort),
            configuration.getInt(HConstants.ZOOKEEPER_MAX_CLIENT_CNXNS,
              HConstants.DEFAULT_ZOOKEEPER_MAX_CLIENT_CNXNS));
        } catch (BindException e) {
          LOG.debug("Failed binding ZK Server to client port: " + currentClientPort, e);
          // We're told to use some port but it's occupied, fail
          if (hasValidClientPortInList(i)) {
            return -1;
          }
          // This port is already in use, try to use another.
          tentativePort = selectClientPort(tentativePort);
          currentClientPort = tentativePort;
          continue;
        }
        break;
      }

      // Start up this ZK server. Dump its stats.
      standaloneServerFactory.startup(server);
      LOG.info("Started connectionTimeout={}, dir={}, {}", connectionTimeout, dir,
        getServerConfigurationOnOneLine(server));
      // Runs a 'stat' against the servers.
      if (!waitForServerUp(currentClientPort, connectionTimeout)) {
        Threads.printThreadInfo(System.out,
          "Why is zk standalone server not coming up?");
        throw new IOException("Waiting for startup of standalone server; " +
          "server isRunning=" + server.isRunning());
      }

      // We have selected a port as a client port.  Update clientPortList if necessary.
      if (clientPortList.size() <= i) { // it is not in the list, add the port
        clientPortList.add(currentClientPort);
      } else if (clientPortList.get(i) <= 0) { // the list has invalid port, update with valid port
        clientPortList.remove(i);
        clientPortList.add(i, currentClientPort);
      }

      standaloneServerFactoryList.add(standaloneServerFactory);
      zooKeeperServers.add(server);
    }

    // set the first one to be active ZK; Others are backups
    activeZKServerIndex = 0;
    started = true;
    int clientPort = clientPortList.get(activeZKServerIndex);
    LOG.info("Started MiniZooKeeperCluster and ran 'stat' on client port={}", clientPort);
    return clientPort;
  }

  private String getServerConfigurationOnOneLine(ZooKeeperServer server) {
    StringWriter sw = new StringWriter();
    try (PrintWriter pw = new PrintWriter(sw) {
      @Override public void println(int x) {
        super.print(x);
        super.print(", ");
      }

      @Override public void println(String x) {
        super.print(x);
        super.print(", ");
      }
    }) {
      server.dumpConf(pw);
    }
    return sw.toString();
  }

  private void createDir(File dir) throws IOException {
    try {
      if (!dir.exists()) {
        dir.mkdirs();
      }
    } catch (SecurityException e) {
      throw new IOException("creating dir: " + dir, e);
    }
  }

  /**
   * @throws IOException if waiting for the shutdown of a server fails
   */
  public void shutdown() throws IOException {
    // shut down all the zk servers
    for (int i = 0; i < standaloneServerFactoryList.size(); i++) {
      NIOServerCnxnFactory standaloneServerFactory = standaloneServerFactoryList.get(i);
      int clientPort = clientPortList.get(i);
      standaloneServerFactory.shutdown();
      if (!waitForServerDown(clientPort, connectionTimeout)) {
        throw new IOException("Waiting for shutdown of standalone server at port=" + clientPort +
          ", timeout=" + this.connectionTimeout);
      }
    }
    standaloneServerFactoryList.clear();

    for (ZooKeeperServer zkServer: zooKeeperServers) {
      // Explicitly close ZKDatabase since ZookeeperServer does not close them
      zkServer.getZKDatabase().close();
    }
    zooKeeperServers.clear();

    // clear everything
    if (started) {
      started = false;
      activeZKServerIndex = 0;
      clientPortList.clear();
      LOG.info("Shutdown MiniZK cluster with all ZK servers");
    }
  }

  /**
   * @return clientPort return clientPort if there is another ZK backup can run
   *         when killing the current active; return -1, if there is no backups.
   * @throws IOException if waiting for the shutdown of a server fails
   */
  public int killCurrentActiveZooKeeperServer() throws IOException, InterruptedException {
    if (!started || activeZKServerIndex < 0) {
      return -1;
    }

    // Shutdown the current active one
    NIOServerCnxnFactory standaloneServerFactory =
      standaloneServerFactoryList.get(activeZKServerIndex);
    int clientPort = clientPortList.get(activeZKServerIndex);

    standaloneServerFactory.shutdown();
    if (!waitForServerDown(clientPort, connectionTimeout)) {
      throw new IOException("Waiting for shutdown of standalone server");
    }

    zooKeeperServers.get(activeZKServerIndex).getZKDatabase().close();

    // remove the current active zk server
    standaloneServerFactoryList.remove(activeZKServerIndex);
    clientPortList.remove(activeZKServerIndex);
    zooKeeperServers.remove(activeZKServerIndex);
    LOG.info("Kill the current active ZK servers in the cluster on client port: {}", clientPort);

    if (standaloneServerFactoryList.isEmpty()) {
      // there is no backup servers;
      return -1;
    }
    clientPort = clientPortList.get(activeZKServerIndex);
    LOG.info("Activate a backup zk server in the cluster on client port: {}", clientPort);
    // return the next back zk server's port
    return clientPort;
  }

  /**
   * Kill one back up ZK servers.
   *
   * @throws IOException if waiting for the shutdown of a server fails
   */
  public void killOneBackupZooKeeperServer() throws IOException, InterruptedException {
    if (!started || activeZKServerIndex < 0 || standaloneServerFactoryList.size() <= 1) {
      return ;
    }

    int backupZKServerIndex = activeZKServerIndex+1;
    // Shutdown the current active one
    NIOServerCnxnFactory standaloneServerFactory =
      standaloneServerFactoryList.get(backupZKServerIndex);
    int clientPort = clientPortList.get(backupZKServerIndex);

    standaloneServerFactory.shutdown();
    if (!waitForServerDown(clientPort, connectionTimeout)) {
      throw new IOException("Waiting for shutdown of standalone server");
    }

    zooKeeperServers.get(backupZKServerIndex).getZKDatabase().close();

    // remove this backup zk server
    standaloneServerFactoryList.remove(backupZKServerIndex);
    clientPortList.remove(backupZKServerIndex);
    zooKeeperServers.remove(backupZKServerIndex);
    LOG.info("Kill one backup ZK servers in the cluster on client port: {}", clientPort);
  }

  // XXX: From o.a.zk.t.ClientBase. We just dropped the check for ssl/secure.
  private static boolean waitForServerDown(int port, long timeout) throws IOException {
    long start = EnvironmentEdgeManager.currentTime();
    while (true) {
      try {
        send4LetterWord(HOST, port, "stat", false, (int)timeout);
      } catch (IOException | X509Exception.SSLContextException e) {
        return true;
      }
      if (EnvironmentEdgeManager.currentTime() > start + timeout) {
        break;
      }
      try {
        Thread.sleep(TIMEOUT);
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException().initCause(e);
      }
    }
    return false;
  }

  // XXX: From o.a.zk.t.ClientBase. Its in the test jar but we don't depend on zk test jar.
  // We remove the SSL/secure bit. Not used in here.
  private static boolean waitForServerUp(int port, long timeout) throws IOException {
    long start = EnvironmentEdgeManager.currentTime();
    while (true) {
      try {
        String result = send4LetterWord(HOST, port, "stat", false, (int)timeout);
        if (result.startsWith("Zookeeper version:") && !result.contains("READ-ONLY")) {
          return true;
        } else {
          LOG.debug("Read {}", result);
        }
      } catch (ConnectException e) {
        // ignore as this is expected, do not log stacktrace
        LOG.info("{}:{} not up: {}", HOST, port, e.toString());
      } catch (IOException | X509Exception.SSLContextException e) {
        // ignore as this is expected
        LOG.info("{}:{} not up", HOST, port, e);
      }

      if (EnvironmentEdgeManager.currentTime() > start + timeout) {
        break;
      }
      try {
        Thread.sleep(TIMEOUT);
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException().initCause(e);
      }
    }
    return false;
  }

  public int getClientPort() {
    return activeZKServerIndex < 0 || activeZKServerIndex >= clientPortList.size() ? -1
        : clientPortList.get(activeZKServerIndex);
  }

  /**
   * @return Address for this  cluster instance.
   */
  public Address getAddress() {
    return Address.fromParts(HOST, getClientPort());
  }

  List<ZooKeeperServer> getZooKeeperServers() {
    return zooKeeperServers;
  }
}
