/**
 *
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

package org.apache.hadoop.hbase.util;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * Tool for loading/unloading regions to/from given regionserver This tool can be run from Command
 * line directly as a utility. Supports Ack/No Ack mode for loading/unloading operations.Ack mode
 * acknowledges if regions are online after movement while noAck mode is best effort mode that
 * improves performance but will still move on if region is stuck/not moved. Motivation behind noAck
 * mode being RS shutdown where even if a Region is stuck, upon shutdown master will move it
 * anyways. This can also be used by constructiong an Object using the builder and then calling
 * {@link #load()} or {@link #unload()} methods for the desired operations.
 */
@InterfaceAudience.Public
public class RegionMover extends AbstractHBaseTool {
  public static final String MOVE_RETRIES_MAX_KEY = "hbase.move.retries.max";
  public static final String MOVE_WAIT_MAX_KEY = "hbase.move.wait.max";
  public static final String SERVERSTART_WAIT_MAX_KEY = "hbase.serverstart.wait.max";
  public static final int DEFAULT_MOVE_RETRIES_MAX = 5;
  public static final int DEFAULT_MOVE_WAIT_MAX = 60;
  public static final int DEFAULT_SERVERSTART_WAIT_MAX = 180;
  static final Log LOG = LogFactory.getLog(RegionMover.class);
  private RegionMoverBuilder rmbuilder;
  private boolean ack = true;
  private int maxthreads = 1;
  private int timeout;
  private String loadUnload;
  private String hostname;
  private String filename;
  private String excludeFile;
  private int port;

  private RegionMover(RegionMoverBuilder builder) {
    this.hostname = builder.hostname;
    this.filename = builder.filename;
    this.excludeFile = builder.excludeFile;
    this.maxthreads = builder.maxthreads;
    this.ack = builder.ack;
    this.port = builder.port;
    this.timeout = builder.timeout;
  }

  private RegionMover() {
  }

  /**
   * Builder for Region mover. Use the {@link #build()} method to create RegionMover object. Has
   * {@link #filename(String)}, {@link #excludeFile(String)}, {@link #maxthreads(int)},
   * {@link #ack(boolean)}, {@link #timeout(int)} methods to set the corresponding options
   */
  public static class RegionMoverBuilder {
    private boolean ack = true;
    private int maxthreads = 1;
    private int timeout = Integer.MAX_VALUE;
    private String hostname;
    private String filename;
    private String excludeFile = null;
    private String defaultDir = "/tmp";
    private int port = HConstants.DEFAULT_REGIONSERVER_PORT;

    /**
     * @param hostname Hostname to unload regions from or load regions to. Can be either hostname
     *     or hostname:port.
     */
    public RegionMoverBuilder(String hostname) {
      String[] splitHostname = hostname.split(":");
      this.hostname = splitHostname[0];
      if (splitHostname.length == 2) {
        this.port = Integer.parseInt(splitHostname[1]);
      }
      setDefaultfilename(this.hostname);
    }

    private void setDefaultfilename(String hostname) {
      this.filename =
          defaultDir + "/" + System.getProperty("user.name") + this.hostname + ":"
              + Integer.toString(this.port);
    }

    /**
     * Path of file where regions will be written to during unloading/read from during loading
     * @param filename
     * @return RegionMoverBuilder object
     */
    public RegionMoverBuilder filename(String filename) {
      this.filename = filename;
      return this;
    }

    /**
     * Set the max number of threads that will be used to move regions
     */
    public RegionMoverBuilder maxthreads(int threads) {
      this.maxthreads = threads;
      return this;
    }

    /**
     * Path of file containing hostnames to be excluded during region movement. Exclude file should
     * have 'host:port' per line. Port is mandatory here as we can have many RS running on a single
     * host.
     */
    public RegionMoverBuilder excludeFile(String excludefile) {
      this.excludeFile = excludefile;
      return this;
    }

    /**
     * Set ack/noAck mode.
     * <p>
     * In ack mode regions are acknowledged before and after moving and the move is retried
     * hbase.move.retries.max times, if unsuccessful we quit with exit code 1.No Ack mode is a best
     * effort mode,each region movement is tried once.This can be used during graceful shutdown as
     * even if we have a stuck region,upon shutdown it'll be reassigned anyway.
     * <p>
     * @param ack
     * @return RegionMoverBuilder object
     */
    public RegionMoverBuilder ack(boolean ack) {
      this.ack = ack;
      return this;
    }

    /**
     * Set the timeout for Load/Unload operation in seconds.This is a global timeout,threadpool for
     * movers also have a separate time which is hbase.move.wait.max * number of regions to
     * load/unload
     * @param timeout in seconds
     * @return RegionMoverBuilder object
     */
    public RegionMoverBuilder timeout(int timeout) {
      this.timeout = timeout;
      return this;
    }

    /**
     * This method builds the appropriate RegionMover object which can then be used to load/unload
     * using load and unload methods
     * @return RegionMover object
     */
    public RegionMover build() {
      return new RegionMover(this);
    }
  }

  /**
   * Loads the specified {@link #hostname} with regions listed in the {@link #filename} RegionMover
   * Object has to be created using {@link #RegionMover(RegionMoverBuilder)}
   * @return true if loading succeeded, false otherwise
   * @throws ExecutionException
   * @throws InterruptedException if the loader thread was interrupted
   * @throws TimeoutException
   */
  public boolean load() throws ExecutionException, InterruptedException, TimeoutException {
    setConf();
    ExecutorService loadPool = Executors.newFixedThreadPool(1);
    Future<Boolean> loadTask = loadPool.submit(new Load(this));
    loadPool.shutdown();
    try {
      if (!loadPool.awaitTermination((long) this.timeout, TimeUnit.SECONDS)) {
        LOG.warn("Timed out before finishing the loading operation. Timeout:" + this.timeout
            + "sec");
        loadPool.shutdownNow();
      }
    } catch (InterruptedException e) {
      loadPool.shutdownNow();
      Thread.currentThread().interrupt();
    }
    try {
      return loadTask.get(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while loading Regions on " + this.hostname, e);
      throw e;
    } catch (ExecutionException e) {
      LOG.error("Error while loading regions on RegionServer " + this.hostname, e);
      throw e;
    }
  }

  private class Load implements Callable<Boolean> {

    private RegionMover rm;

    public Load(RegionMover rm) {
      this.rm = rm;
    }

    @Override
    public Boolean call() throws IOException {
      Connection conn = ConnectionFactory.createConnection(rm.conf);
      try {
        List<HRegionInfo> regionsToMove = readRegionsFromFile(rm.filename);
        if (regionsToMove.isEmpty()) {
          LOG.info("No regions to load.Exiting");
          return true;
        }
        Admin admin = conn.getAdmin();
        try {
          loadRegions(admin, rm.hostname, rm.port, regionsToMove, rm.ack);
        } finally {
          admin.close();
        }
      } catch (Exception e) {
        LOG.error("Error while loading regions to " + rm.hostname, e);
        return false;
      } finally {
        conn.close();
      }
      return true;
    }
  }

  /**
   * Unload regions from given {@link #hostname} using ack/noAck mode and {@link #maxthreads}.In
   * noAck mode we do not make sure that region is successfully online on the target region
   * server,hence it is best effort.We do not unload regions to hostnames given in
   * {@link #excludeFile}.
   * @return true if unloading succeeded, false otherwise
   * @throws InterruptedException if the unloader thread was interrupted
   * @throws ExecutionException
   * @throws TimeoutException
   */
  public boolean unload() throws InterruptedException, ExecutionException, TimeoutException {
    setConf();
    deleteFile(this.filename);
    ExecutorService unloadPool = Executors.newFixedThreadPool(1);
    Future<Boolean> unloadTask = unloadPool.submit(new Unload(this));
    unloadPool.shutdown();
    try {
      if (!unloadPool.awaitTermination((long) this.timeout, TimeUnit.SECONDS)) {
        LOG.warn("Timed out before finishing the unloading operation. Timeout:" + this.timeout
            + "sec");
        unloadPool.shutdownNow();
      }
    } catch (InterruptedException e) {
      unloadPool.shutdownNow();
      Thread.currentThread().interrupt();
    }
    try {
      return unloadTask.get(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while unloading Regions from " + this.hostname, e);
      throw e;
    } catch (ExecutionException e) {
      LOG.error("Error while unloading regions from RegionServer " + this.hostname, e);
      throw e;
    }
  }

  private class Unload implements Callable<Boolean> {

    List<HRegionInfo> movedRegions = Collections.synchronizedList(new ArrayList<HRegionInfo>());
    private RegionMover rm;

    public Unload(RegionMover rm) {
      this.rm = rm;
    }

    @Override
    public Boolean call() throws IOException {
      Connection conn = ConnectionFactory.createConnection(rm.conf);
      try {
        Admin admin = conn.getAdmin();
        // Get Online RegionServers
        ArrayList<String> regionServers = getServers(admin);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Online region servers:" + regionServers.toString());
        }
        // Remove the host Region server from target Region Servers list
        String server = stripServer(regionServers, hostname, port);
        // Remove RS present in the exclude file
        stripExcludes(regionServers, rm.excludeFile);
        stripMaster(regionServers, admin);
        unloadRegions(admin, server, regionServers, rm.ack, movedRegions);
      } catch (Exception e) {
        LOG.error("Error while unloading regions ", e);
        return false;
      } finally {
        try {
          conn.close();
        } catch (IOException e) {
          // ignore
        }
        if (movedRegions != null) {
          writeFile(rm.filename, movedRegions);
        }
      }
      return true;
    }
  }

  /**
   * Creates a new configuration if not already set and sets region mover specific overrides
   */
  private void setConf() {
    if (conf == null) {
      conf = HBaseConfiguration.create();
      conf.setInt("hbase.client.prefetch.limit", 1);
      conf.setInt("hbase.client.pause", 500);
      conf.setInt("hbase.client.retries.number", 100);
    }
  }

  private void loadRegions(Admin admin, String hostname, int port,
      List<HRegionInfo> regionsToMove, boolean ack) throws Exception {
    String server = null;
    List<HRegionInfo> movedRegions = Collections.synchronizedList(new ArrayList<HRegionInfo>());
    int maxWaitInSeconds =
        admin.getConfiguration().getInt(SERVERSTART_WAIT_MAX_KEY, DEFAULT_SERVERSTART_WAIT_MAX);
    long maxWait = EnvironmentEdgeManager.currentTime() + maxWaitInSeconds * 1000;
    while ((EnvironmentEdgeManager.currentTime() < maxWait) && (server == null)) {
      try {
        ArrayList<String> regionServers = getServers(admin);
        // Remove the host Region server from target Region Servers list
        server = stripServer(regionServers, hostname, port);
        if (server != null) {
          break;
        }
      } catch (IOException e) {
        LOG.warn("Could not get list of region servers", e);
      } catch (Exception e) {
        LOG.info("hostname=" + hostname + " is not up yet, waiting");
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        LOG.error("Interrupted while waiting for " + hostname + " to be up.Quitting now", e);
        throw e;
      }
    }
    if (server == null) {
      LOG.error("Host:" + hostname + " is not up.Giving up.");
      throw new Exception("Host to load regions not online");
    }
    LOG.info("Moving " + regionsToMove.size() + " regions to " + server + " using "
        + this.maxthreads + " threads.Ack mode:" + this.ack);
    ExecutorService moveRegionsPool = Executors.newFixedThreadPool(this.maxthreads);
    List<Future<Boolean>> taskList = new ArrayList<Future<Boolean>>();
    int counter = 0;
    while (counter < regionsToMove.size()) {
      HRegionInfo region = regionsToMove.get(counter);
      String currentServer = getServerNameForRegion(admin, region);
      if (currentServer == null) {
        LOG.warn("Could not get server for Region:" + region.getEncodedName() + " moving on");
        counter++;
        continue;
      } else if (server.equals(currentServer)) {
        LOG.info("Region " + region.getRegionNameAsString() + "already on target server=" + server);
        counter++;
        continue;
      }
      if (ack) {
        Future<Boolean> task =
            moveRegionsPool.submit(new MoveWithAck(admin, region, currentServer, server,
                movedRegions));
        taskList.add(task);
      } else {
        Future<Boolean> task =
            moveRegionsPool.submit(new MoveWithoutAck(admin, region, currentServer, server,
                movedRegions));
        taskList.add(task);
      }
      counter++;
    }
    moveRegionsPool.shutdown();
    long timeoutInSeconds =
        regionsToMove.size()
            * admin.getConfiguration().getInt(MOVE_WAIT_MAX_KEY, DEFAULT_MOVE_WAIT_MAX);
    try {
      if (!moveRegionsPool.awaitTermination(timeoutInSeconds, TimeUnit.SECONDS)) {
        moveRegionsPool.shutdownNow();
      }
    } catch (InterruptedException e) {
      moveRegionsPool.shutdownNow();
      Thread.currentThread().interrupt();
    }
    for (Future<Boolean> future : taskList) {
      try {
        // if even after shutdownNow threads are stuck we wait for 5 secs max
        if (!future.get(5, TimeUnit.SECONDS)) {
          LOG.error("Was Not able to move region....Exiting Now");
          throw new Exception("Could not move region Exception");
        }
      } catch (InterruptedException e) {
        LOG.error("Interrupted while waiting for Thread to Complete " + e.getMessage(), e);
        throw e;
      } catch (ExecutionException e) {
        LOG.error("Got Exception From Thread While moving region " + e.getMessage(), e);
        throw e;
      } catch (CancellationException e) {
        LOG.error("Thread for moving region cancelled. Timeout for cancellation:"
            + timeoutInSeconds + "secs", e);
        throw e;
      }
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="DLS_DEAD_LOCAL_STORE",
      justification="FB is wrong; its size is read")
  private void unloadRegions(Admin admin, String server, ArrayList<String> regionServers,
      boolean ack, List<HRegionInfo> movedRegions) throws Exception {
    List<HRegionInfo> regionsToMove = new ArrayList<HRegionInfo>();// FindBugs: DLS_DEAD_LOCAL_STORE
    regionsToMove = getRegions(this.conf, server);
    if (regionsToMove.size() == 0) {
      LOG.info("No Regions to move....Quitting now");
      return;
    } else if (regionServers.size() == 0) {
      LOG.warn("No Regions were moved - no servers available");
      throw new Exception("No online region servers");
    }
    while (true) {
      regionsToMove = getRegions(this.conf, server);
      regionsToMove.removeAll(movedRegions);
      if (regionsToMove.size() == 0) {
        break;
      }
      int counter = 0;
      LOG.info("Moving " + regionsToMove.size() + " regions from " + this.hostname + " to "
          + regionServers.size() + " servers using " + this.maxthreads + " threads .Ack Mode:"
          + ack);
      ExecutorService moveRegionsPool = Executors.newFixedThreadPool(this.maxthreads);
      List<Future<Boolean>> taskList = new ArrayList<Future<Boolean>>();
      int serverIndex = 0;
      while (counter < regionsToMove.size()) {
        if (ack) {
          Future<Boolean> task =
              moveRegionsPool.submit(new MoveWithAck(admin, regionsToMove.get(counter), server,
                  regionServers.get(serverIndex), movedRegions));
          taskList.add(task);
        } else {
          Future<Boolean> task =
              moveRegionsPool.submit(new MoveWithoutAck(admin, regionsToMove.get(counter), server,
                  regionServers.get(serverIndex), movedRegions));
          taskList.add(task);
        }
        counter++;
        serverIndex = (serverIndex + 1) % regionServers.size();
      }
      moveRegionsPool.shutdown();
      long timeoutInSeconds =
          regionsToMove.size()
              * admin.getConfiguration().getInt(MOVE_WAIT_MAX_KEY, DEFAULT_MOVE_WAIT_MAX);
      try {
        if (!moveRegionsPool.awaitTermination(timeoutInSeconds, TimeUnit.SECONDS)) {
          moveRegionsPool.shutdownNow();
        }
      } catch (InterruptedException e) {
        moveRegionsPool.shutdownNow();
        Thread.currentThread().interrupt();
      }
      for (Future<Boolean> future : taskList) {
        try {
          // if even after shutdownNow threads are stuck we wait for 5 secs max
          if (!future.get(5, TimeUnit.SECONDS)) {
            LOG.error("Was Not able to move region....Exiting Now");
            throw new Exception("Could not move region Exception");
          }
        } catch (InterruptedException e) {
          LOG.error("Interrupted while waiting for Thread to Complete " + e.getMessage(), e);
          throw e;
        } catch (ExecutionException e) {
          LOG.error("Got Exception From Thread While moving region " + e.getMessage(), e);
          throw e;
        } catch (CancellationException e) {
          LOG.error("Thread for moving region cancelled. Timeout for cancellation:"
              + timeoutInSeconds + "secs", e);
          throw e;
        }
      }
    }
  }

  /**
   * Move Regions and make sure that they are up on the target server.If a region movement fails we
   * exit as failure
   */
  private class MoveWithAck implements Callable<Boolean> {
    private Admin admin;
    private HRegionInfo region;
    private String targetServer;
    private List<HRegionInfo> movedRegions;
    private String sourceServer;

    public MoveWithAck(Admin admin, HRegionInfo regionInfo, String sourceServer,
        String targetServer, List<HRegionInfo> movedRegions) {
      this.admin = admin;
      this.region = regionInfo;
      this.targetServer = targetServer;
      this.movedRegions = movedRegions;
      this.sourceServer = sourceServer;
    }

    @Override
    public Boolean call() throws IOException, InterruptedException {
      boolean moved = false;
      int count = 0;
      int retries = admin.getConfiguration().getInt(MOVE_RETRIES_MAX_KEY, DEFAULT_MOVE_RETRIES_MAX);
      int maxWaitInSeconds =
          admin.getConfiguration().getInt(MOVE_WAIT_MAX_KEY, DEFAULT_MOVE_WAIT_MAX);
      long startTime = EnvironmentEdgeManager.currentTime();
      boolean sameServer = true;
      // Assert we can scan the region in its current location
      isSuccessfulScan(admin, region);
      LOG.info("Moving region:" + region.getEncodedName() + " from " + sourceServer + " to "
          + targetServer);
      while (count < retries && sameServer) {
        if (count > 0) {
          LOG.info("Retry " + Integer.toString(count) + " of maximum " + Integer.toString(retries));
        }
        count = count + 1;
        admin.move(region.getEncodedNameAsBytes(), Bytes.toBytes(targetServer));
        long maxWait = startTime + (maxWaitInSeconds * 1000);
        while (EnvironmentEdgeManager.currentTime() < maxWait) {
          sameServer = isSameServer(admin, region, sourceServer);
          if (!sameServer) {
            break;
          }
          Thread.sleep(100);
        }
      }
      if (sameServer) {
        LOG.error("Region: " + region.getRegionNameAsString() + " stuck on " + this.sourceServer
            + ",newServer=" + this.targetServer);
      } else {
        isSuccessfulScan(admin, region);
        LOG.info("Moved Region "
            + region.getRegionNameAsString()
            + " cost:"
            + String.format("%.3f",
              (float) (EnvironmentEdgeManager.currentTime() - startTime) / 1000));
        moved = true;
        movedRegions.add(region);
      }
      return moved;
    }
  }

  /**
   * Move Regions without Acknowledging.Usefule in case of RS shutdown as we might want to shut the
   * RS down anyways and not abort on a stuck region. Improves movement performance
   */
  private static class MoveWithoutAck implements Callable<Boolean> {
    private Admin admin;
    private HRegionInfo region;
    private String targetServer;
    private List<HRegionInfo> movedRegions;
    private String sourceServer;

    public MoveWithoutAck(Admin admin, HRegionInfo regionInfo, String sourceServer,
        String targetServer, List<HRegionInfo> movedRegions) {
      this.admin = admin;
      this.region = regionInfo;
      this.targetServer = targetServer;
      this.movedRegions = movedRegions;
      this.sourceServer = sourceServer;
    }

    @Override
    public Boolean call() {
      try {
        LOG.info("Moving region:" + region.getEncodedName() + " from " + sourceServer + " to "
            + targetServer);
        admin.move(region.getEncodedNameAsBytes(), Bytes.toBytes(targetServer));
        LOG.info("Moved " + region.getEncodedName() + " from " + sourceServer + " to "
            + targetServer);
      } catch (Exception e) {
        LOG.error("Error Moving Region:" + region.getEncodedName(), e);
      } finally {
        // we add region to the moved regions list in No Ack Mode since this is best effort
        movedRegions.add(region);
      }
      return true;
    }
  }

  private List<HRegionInfo> readRegionsFromFile(String filename) throws IOException {
    List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
    File f = new File(filename);
    if (!f.exists()) {
      return regions;
    }
    FileInputStream fis = null;
    DataInputStream dis = null;
    try {
      fis = new FileInputStream(f);
      dis = new DataInputStream(fis);
      int numRegions = dis.readInt();
      int index = 0;
      while (index < numRegions) {
        regions.add(HRegionInfo.parseFromOrNull(Bytes.readByteArray(dis)));
        index++;
      }
    } catch (IOException e) {
      LOG.error("Error while reading regions from file:" + filename, e);
      throw e;
    } finally {
      if (dis != null) {
        dis.close();
      }
      if (fis != null) {
        fis.close();
      }
    }
    return regions;
  }

  /**
   * Get online regions of the passed server
   * @param conf
   * @param server
   * @return List of Regions online on the server
   * @throws IOException
   */
  private List<HRegionInfo> getRegions(Configuration conf, String server) throws IOException {
    Connection conn = ConnectionFactory.createConnection(conf);
    try {
      return conn.getAdmin().getOnlineRegions(ServerName.valueOf(server));
    } finally {
      conn.close();
    }
  }

  /**
   * Write the number of regions moved in the first line followed by regions moved in subsequent
   * lines
   * @param filename
   * @param movedRegions
   * @throws IOException
   */
  private void writeFile(String filename, List<HRegionInfo> movedRegions) throws IOException {
    FileOutputStream fos = null;
    DataOutputStream dos = null;
    try {
      fos = new FileOutputStream(filename);
      dos = new DataOutputStream(fos);
      dos.writeInt(movedRegions.size());
      for (HRegionInfo region : movedRegions) {
        Bytes.writeByteArray(dos, region.toByteArray());
      }
    } catch (IOException e) {
      LOG.error("ERROR: Was Not able to write regions moved to output file but moved "
          + movedRegions.size() + " regions", e);
      throw e;
    } finally {
      if (dos != null) {
        dos.close();
      }
      if (fos != null) {
        fos.close();
      }
    }
  }

  /**
   * Excludes the servername whose hostname and port portion matches the list given in exclude file
   * @param regionServers
   * @param excludeFile
   * @throws IOException
   */
  private void stripExcludes(ArrayList<String> regionServers, String excludeFile)
      throws IOException {
    if (excludeFile != null) {
      ArrayList<String> excludes = readExcludes(excludeFile);
      Iterator<String> i = regionServers.iterator();
      while (i.hasNext()) {
        String rs = i.next();
        String rsPort =
            rs.split(ServerName.SERVERNAME_SEPARATOR)[0] + ":"
                + rs.split(ServerName.SERVERNAME_SEPARATOR)[1];
        if (excludes.contains(rsPort)) {
          i.remove();
        }
      }
      LOG.info("Valid Region server targets are:" + regionServers.toString());
      LOG.info("Excluded Servers are" + excludes.toString());
    }
  }

  /**
   * Exclude master from list of RSs to move regions to
   * @param regionServers
   * @param admin
   * @throws IOException
   */
  private void stripMaster(ArrayList<String> regionServers, Admin admin) throws IOException {
    String masterHostname = admin.getClusterStatus().getMaster().getHostname();
    int masterPort = admin.getClusterStatus().getMaster().getPort();
    try {
      stripServer(regionServers, masterHostname, masterPort);
    } catch (Exception e) {
      LOG.warn("Could not remove master from list of RS", e);
    }
  }

  /**
   * @return List of servers from the exclude file in format 'hostname:port'.
   */
  private ArrayList<String> readExcludes(String excludeFile) throws IOException {
    ArrayList<String> excludeServers = new ArrayList<String>();
    if (excludeFile == null) {
      return excludeServers;
    } else {
      File f = new File(excludeFile);
      String line;
      BufferedReader br = null;
      try {
        br = new BufferedReader(new FileReader(f));
        while ((line = br.readLine()) != null) {
          line = line.trim();
          if (!line.equals("")) {
            excludeServers.add(line);
          }
        }
      } catch (IOException e) {
        LOG.warn("Exception while reading excludes file,continuing anyways", e);
      } finally {
        if (br != null) {
          br.close();
        }
      }
      return excludeServers;
    }
  }

  /**
   * Remove the servername whose hostname and port portion matches from the passed array of servers.
   * Returns as side-effect the servername removed.
   * @param regionServers
   * @param hostname
   * @param port
   * @return server removed from list of Region Servers
   * @throws Exception
   */
  private String stripServer(ArrayList<String> regionServers, String hostname, int port)
      throws Exception {
    String server = null;
    String portString = Integer.toString(port);
    Iterator<String> i = regionServers.iterator();
    int noOfRs = regionServers.size();
    while (i.hasNext()) {
      server = i.next();
      String[] splitServer = server.split(ServerName.SERVERNAME_SEPARATOR);
      if (splitServer[0].equals(hostname) && splitServer[1].equals(portString)) {
        i.remove();
        return server;
      }
    }
    if (regionServers.size() >= noOfRs) {
      throw new Exception("Server " + hostname + ":" + Integer.toString(port)
          + " is not in list of online servers(Offline/Incorrect)");
    }
    return server;
  }

  /**
   * Get Arraylist of Servers in the cluster
   * @param admin
   * @return ArrayList of online region servers
   * @throws IOException
   */
  private ArrayList<String> getServers(Admin admin) throws IOException {
    ArrayList<ServerName> serverInfo =
        new ArrayList<ServerName>(admin.getClusterStatus().getServers());
    ArrayList<String> regionServers = new ArrayList<String>();
    for (ServerName server : serverInfo) {
      regionServers.add(server.getServerName());
    }
    return regionServers;
  }

  private void deleteFile(String filename) {
    File f = new File(filename);
    if (f.exists()) {
      f.delete();
    }
  }

  /**
   * Tries to scan a row from passed region
   * @param admin
   * @param region
   * @throws IOException
   */
  private void isSuccessfulScan(Admin admin, HRegionInfo region) throws IOException {
    Scan scan = new Scan(region.getStartKey());
    scan.setBatch(1);
    scan.setCaching(1);
    scan.setFilter(new FirstKeyOnlyFilter());
    try {
      Table table = admin.getConnection().getTable(region.getTable());
      try {
        ResultScanner scanner = table.getScanner(scan);
        try {
          scanner.next();
        } finally {
          scanner.close();
        }
      } finally {
        table.close();
      }
    } catch (IOException e) {
      LOG.error("Could not scan region:" + region.getEncodedName(), e);
      throw e;
    }
  }

  /**
   * Returns true if passed region is still on serverName when we look at hbase:meta.
   * @param admin
   * @param region
   * @param serverName
   * @return true if region is hosted on serverName otherwise false
   * @throws IOException
   */
  private boolean isSameServer(Admin admin, HRegionInfo region, String serverName)
      throws IOException {
    String serverForRegion = getServerNameForRegion(admin, region);
    if (serverForRegion != null && serverForRegion.equals(serverName)) {
      return true;
    }
    return false;
  }

  /**
   * Get servername that is up in hbase:meta hosting the given region. this is hostname + port +
   * startcode comma-delimited. Can return null
   * @param admin
   * @param region
   * @return regionServer hosting the given region
   * @throws IOException
   */
  private String getServerNameForRegion(Admin admin, HRegionInfo region) throws IOException {
    String server = null;
    if (!admin.isTableEnabled(region.getTable())) {
      return null;
    }
    if (region.isMetaRegion()) {
      ZooKeeperWatcher zkw = new ZooKeeperWatcher(admin.getConfiguration(), "region_mover", null);
      MetaTableLocator locator = new MetaTableLocator();
      int maxWaitInSeconds =
          admin.getConfiguration().getInt(MOVE_WAIT_MAX_KEY, DEFAULT_MOVE_WAIT_MAX);
      try {
        server = locator.waitMetaRegionLocation(zkw, maxWaitInSeconds * 1000).toString() + ",";
      } catch (InterruptedException e) {
        LOG.error("Interrupted while waiting for location of Meta", e);
      } finally {
        if (zkw != null) {
          zkw.close();
        }
      }
    } else {
      Table table = admin.getConnection().getTable(TableName.META_TABLE_NAME);
      try {
        Get get = new Get(region.getRegionName());
        get.addColumn(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
        get.addColumn(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER);
        Result result = table.get(get);
        if (result != null) {
          byte[] servername =
              result.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
          byte[] startcode =
              result.getValue(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER);
          if (servername != null) {
            server =
                Bytes.toString(servername).replaceFirst(":", ",") + "," + Bytes.toLong(startcode);
          }
        }
      } catch (IOException e) {
        LOG.error("Could not get Server Name for region:" + region.getEncodedName(), e);
        throw e;
      } finally {
        table.close();
      }
    }
    return server;
  }

  @Override
  protected void addOptions() {
    this.addRequiredOptWithArg("r", "regionserverhost", "region server <hostname>|<hostname:port>");
    this.addRequiredOptWithArg("o", "operation", "Expected: load/unload");
    this.addOptWithArg("m", "maxthreads",
      "Define the maximum number of threads to use to unload and reload the regions");
    this.addOptWithArg("x", "excludefile",
      "File with <hostname:port> per line to exclude as unload targets; default excludes only "
          + "target host; useful for rack decommisioning.");
    this.addOptWithArg("f", "filename",
      "File to save regions list into unloading, or read from loading; "
          + "default /tmp/<usernamehostname:port>");
    this.addOptNoArg("n", "noack",
      "Turn on No-Ack mode(default: false) which won't check if region is online on target "
          + "RegionServer, hence best effort. This is more performant in unloading and loading "
          + "but might lead to region being unavailable for some time till master reassigns it "
          + "in case the move failed");
    this.addOptWithArg("t", "timeout", "timeout in seconds after which the tool will exit "
        + "irrespective of whether it finished or not;default Integer.MAX_VALUE");
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    String hostname = cmd.getOptionValue("r");
    rmbuilder = new RegionMoverBuilder(hostname);
    if (cmd.hasOption('m')) {
      rmbuilder.maxthreads(Integer.parseInt(cmd.getOptionValue('m')));
    }
    if (cmd.hasOption('n')) {
      rmbuilder.ack(false);
    }
    if (cmd.hasOption('f')) {
      rmbuilder.filename(cmd.getOptionValue('f'));
    }
    if (cmd.hasOption('x')) {
      rmbuilder.excludeFile(cmd.getOptionValue('x'));
    }
    if (cmd.hasOption('t')) {
      rmbuilder.timeout(Integer.parseInt(cmd.getOptionValue('t')));
    }
    this.loadUnload = cmd.getOptionValue("o").toLowerCase(Locale.ROOT);
  }

  @Override
  protected int doWork() throws Exception {
    boolean success;
    RegionMover rm = rmbuilder.build();
    if (loadUnload.equalsIgnoreCase("load")) {
      success = rm.load();
    } else if (loadUnload.equalsIgnoreCase("unload")) {
      success = rm.unload();
    } else {
      printUsage();
      success = false;
    }
    return (success ? 0 : 1);
  }

  public static void main(String[] args) {
    new RegionMover().doStaticMain(args);
  }
}
