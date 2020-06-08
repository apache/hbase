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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

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
public class RegionMover extends AbstractHBaseTool implements Closeable {
  private static final String MOVE_RETRIES_MAX_KEY = "hbase.move.retries.max";
  private static final String MOVE_WAIT_MAX_KEY = "hbase.move.wait.max";
  static final String SERVERSTART_WAIT_MAX_KEY = "hbase.serverstart.wait.max";
  private static final int DEFAULT_MOVE_RETRIES_MAX = 5;
  private static final int DEFAULT_MOVE_WAIT_MAX = 60;
  private static final int DEFAULT_SERVERSTART_WAIT_MAX = 180;

  private static final Logger LOG = LoggerFactory.getLogger(RegionMover.class);

  private RegionMoverBuilder rmbuilder;
  private boolean ack = true;
  private int maxthreads = 1;
  private int timeout;
  private String loadUnload;
  private String hostname;
  private String filename;
  private String excludeFile;
  private int port;
  private Connection conn;
  private Admin admin;

  private RegionMover(RegionMoverBuilder builder) throws IOException {
    this.hostname = builder.hostname;
    this.filename = builder.filename;
    this.excludeFile = builder.excludeFile;
    this.maxthreads = builder.maxthreads;
    this.ack = builder.ack;
    this.port = builder.port;
    this.timeout = builder.timeout;
    setConf(builder.conf);
    this.conn = ConnectionFactory.createConnection(conf);
    this.admin = conn.getAdmin();
  }

  private RegionMover() {
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(this.admin);
    IOUtils.closeQuietly(this.conn);
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
    private String defaultDir = System.getProperty("java.io.tmpdir");
    @VisibleForTesting
    final int port;
    private final Configuration conf;

    public RegionMoverBuilder(String hostname) {
      this(hostname, createConf());
    }

    /**
     * Creates a new configuration and sets region mover specific overrides
     */
    private static Configuration createConf() {
      Configuration conf = HBaseConfiguration.create();
      conf.setInt("hbase.client.prefetch.limit", 1);
      conf.setInt("hbase.client.pause", 500);
      conf.setInt("hbase.client.retries.number", 100);
      return conf;
    }

    /**
     * @param hostname Hostname to unload regions from or load regions to. Can be either hostname
     *     or hostname:port.
     * @param conf Configuration object
     */
    public RegionMoverBuilder(String hostname, Configuration conf) {
      String[] splitHostname = hostname.toLowerCase().split(":");
      this.hostname = splitHostname[0];
      if (splitHostname.length == 2) {
        this.port = Integer.parseInt(splitHostname[1]);
      } else {
        this.port = conf.getInt(HConstants.REGIONSERVER_PORT, HConstants.DEFAULT_REGIONSERVER_PORT);
      }
      this.filename = defaultDir + File.separator + System.getProperty("user.name") + this.hostname
        + ":" + Integer.toString(this.port);
      this.conf = conf;
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
    public RegionMover build() throws IOException {
      return new RegionMover(this);
    }
  }

  /**
   * Move Regions and make sure that they are up on the target server.If a region movement fails we
   * exit as failure
   */
  private class MoveWithAck implements Callable<Boolean> {
    private RegionInfo region;
    private ServerName targetServer;
    private List<RegionInfo> movedRegions;
    private ServerName sourceServer;

    public MoveWithAck(RegionInfo regionInfo, ServerName sourceServer,
        ServerName targetServer, List<RegionInfo> movedRegions) {
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
      isSuccessfulScan(region);
      LOG.info("Moving region:" + region.getEncodedName() + " from " + sourceServer + " to "
          + targetServer);
      while (count < retries && sameServer) {
        if (count > 0) {
          LOG.info("Retry " + Integer.toString(count) + " of maximum " + Integer.toString(retries));
        }
        count = count + 1;
        admin.move(region.getEncodedNameAsBytes(), targetServer);
        long maxWait = startTime + (maxWaitInSeconds * 1000);
        while (EnvironmentEdgeManager.currentTime() < maxWait) {
          sameServer = isSameServer(region, sourceServer);
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
        isSuccessfulScan(region);
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
  private class MoveWithoutAck implements Callable<Boolean> {
    private RegionInfo region;
    private ServerName targetServer;
    private List<RegionInfo> movedRegions;
    private ServerName sourceServer;

    public MoveWithoutAck(RegionInfo regionInfo, ServerName sourceServer,
        ServerName targetServer, List<RegionInfo> movedRegions) {
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
        admin.move(region.getEncodedNameAsBytes(), targetServer);
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

  /**
   * Loads the specified {@link #hostname} with regions listed in the {@link #filename} RegionMover
   * Object has to be created using {@link #RegionMover(RegionMoverBuilder)}
   * @return true if loading succeeded, false otherwise
   */
  public boolean load() throws ExecutionException, InterruptedException, TimeoutException {
    ExecutorService loadPool = Executors.newFixedThreadPool(1);
    Future<Boolean> loadTask = loadPool.submit(() -> {
      try {
        List<RegionInfo> regionsToMove = readRegionsFromFile(filename);
        if (regionsToMove.isEmpty()) {
          LOG.info("No regions to load.Exiting");
          return true;
        }
        loadRegions(regionsToMove);
      } catch (Exception e) {
        LOG.error("Error while loading regions to " + hostname, e);
        return false;
      }
      return true;
    });
    return waitTaskToFinish(loadPool, loadTask, "loading");
  }

  private void loadRegions(List<RegionInfo> regionsToMove)
      throws Exception {
    ServerName server = getTargetServer();
    List<RegionInfo> movedRegions = Collections.synchronizedList(new ArrayList<>());
    LOG.info(
        "Moving " + regionsToMove.size() + " regions to " + server + " using " + this.maxthreads
            + " threads.Ack mode:" + this.ack);

    ExecutorService moveRegionsPool = Executors.newFixedThreadPool(this.maxthreads);
    List<Future<Boolean>> taskList = new ArrayList<>();
    int counter = 0;
    while (counter < regionsToMove.size()) {
      RegionInfo region = regionsToMove.get(counter);
      ServerName currentServer = getServerNameForRegion(region);
      if (currentServer == null) {
        LOG.warn(
            "Could not get server for Region:" + region.getRegionNameAsString() + " moving on");
        counter++;
        continue;
      } else if (server.equals(currentServer)) {
        LOG.info(
            "Region " + region.getRegionNameAsString() + " is already on target server=" + server);
        counter++;
        continue;
      }
      if (ack) {
        Future<Boolean> task =
            moveRegionsPool.submit(new MoveWithAck(region, currentServer, server, movedRegions));
        taskList.add(task);
      } else {
        Future<Boolean> task =
            moveRegionsPool.submit(new MoveWithoutAck(region, currentServer, server, movedRegions));
        taskList.add(task);
      }
      counter++;
    }

    moveRegionsPool.shutdown();
    long timeoutInSeconds = regionsToMove.size() * admin.getConfiguration()
        .getLong(MOVE_WAIT_MAX_KEY, DEFAULT_MOVE_WAIT_MAX);
    waitMoveTasksToFinish(moveRegionsPool, taskList, timeoutInSeconds);
  }

  /**
   * Unload regions from given {@link #hostname} using ack/noAck mode and {@link #maxthreads}.In
   * noAck mode we do not make sure that region is successfully online on the target region
   * server,hence it is best effort.We do not unload regions to hostnames given in
   * {@link #excludeFile}.
   * @return true if unloading succeeded, false otherwise
   */
  public boolean unload() throws InterruptedException, ExecutionException, TimeoutException {
    deleteFile(this.filename);
    ExecutorService unloadPool = Executors.newFixedThreadPool(1);
    Future<Boolean> unloadTask = unloadPool.submit(() -> {
      List<RegionInfo> movedRegions = Collections.synchronizedList(new ArrayList<>());
      try {
        // Get Online RegionServers
        List<ServerName> regionServers = new ArrayList<>();
        regionServers.addAll(admin.getRegionServers());
        // Remove the host Region server from target Region Servers list
        ServerName server = stripServer(regionServers, hostname, port);
        if (server == null) {
          LOG.info("Could not find server '{}:{}' in the set of region servers. giving up.",
              hostname, port);
          LOG.debug("List of region servers: {}", regionServers);
          return false;
        }
        // Remove RS present in the exclude file
        stripExcludes(regionServers);

        // Remove decommissioned RS
        Set<ServerName> decommissionedRS = new HashSet<>(admin.listDecommissionedRegionServers());
        if (CollectionUtils.isNotEmpty(decommissionedRS)) {
          regionServers.removeIf(decommissionedRS::contains);
          LOG.debug("Excluded RegionServers from unloading regions to because they " +
            "are marked as decommissioned. Servers: {}", decommissionedRS);
        }

        stripMaster(regionServers);
        if (regionServers.isEmpty()) {
          LOG.warn("No Regions were moved - no servers available");
          return false;
        }
        unloadRegions(server, regionServers, movedRegions);
      } catch (Exception e) {
        LOG.error("Error while unloading regions ", e);
        return false;
      } finally {
        if (movedRegions != null) {
          writeFile(filename, movedRegions);
        }
      }
      return true;
    });
    return waitTaskToFinish(unloadPool, unloadTask, "unloading");
  }

  private void unloadRegions(ServerName server, List<ServerName> regionServers,
      List<RegionInfo> movedRegions) throws Exception {
    while (true) {
      List<RegionInfo> regionsToMove = admin.getRegions(server);
      regionsToMove.removeAll(movedRegions);
      if (regionsToMove.isEmpty()) {
        LOG.info("No Regions to move....Quitting now");
        break;
      }
      int counter = 0;
      LOG.info("Moving " + regionsToMove.size() + " regions from " + this.hostname + " to "
          + regionServers.size() + " servers using " + this.maxthreads + " threads .Ack Mode:"
          + ack);
      ExecutorService moveRegionsPool = Executors.newFixedThreadPool(this.maxthreads);
      List<Future<Boolean>> taskList = new ArrayList<>();
      int serverIndex = 0;
      while (counter < regionsToMove.size()) {
        if (ack) {
          Future<Boolean> task = moveRegionsPool.submit(
              new MoveWithAck(regionsToMove.get(counter), server, regionServers.get(serverIndex),
                  movedRegions));
          taskList.add(task);
        } else {
          Future<Boolean> task = moveRegionsPool.submit(
              new MoveWithoutAck(regionsToMove.get(counter), server, regionServers.get(serverIndex),
                  movedRegions));
          taskList.add(task);
        }
        counter++;
        serverIndex = (serverIndex + 1) % regionServers.size();
      }
      moveRegionsPool.shutdown();
      long timeoutInSeconds = regionsToMove.size() * admin.getConfiguration()
          .getLong(MOVE_WAIT_MAX_KEY, DEFAULT_MOVE_WAIT_MAX);
      waitMoveTasksToFinish(moveRegionsPool, taskList, timeoutInSeconds);
    }
  }

  private boolean waitTaskToFinish(ExecutorService pool, Future<Boolean> task, String operation)
      throws TimeoutException, InterruptedException, ExecutionException {
    pool.shutdown();
    try {
      if (!pool.awaitTermination((long) this.timeout, TimeUnit.SECONDS)) {
        LOG.warn(
            "Timed out before finishing the " + operation + " operation. Timeout: " + this.timeout
                + "sec");
        pool.shutdownNow();
      }
    } catch (InterruptedException e) {
      pool.shutdownNow();
      Thread.currentThread().interrupt();
    }
    try {
      return task.get(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while " + operation + " Regions on " + this.hostname, e);
      throw e;
    } catch (ExecutionException e) {
      LOG.error("Error while " + operation + " regions on RegionServer " + this.hostname, e);
      throw e;
    }
  }

  private void waitMoveTasksToFinish(ExecutorService moveRegionsPool,
      List<Future<Boolean>> taskList, long timeoutInSeconds) throws Exception {
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
        LOG.error("Thread for moving region cancelled. Timeout for cancellation:" + timeoutInSeconds
            + "secs", e);
        throw e;
      }
    }
  }

  private ServerName getTargetServer() throws Exception {
    ServerName server = null;
    int maxWaitInSeconds =
        admin.getConfiguration().getInt(SERVERSTART_WAIT_MAX_KEY, DEFAULT_SERVERSTART_WAIT_MAX);
    long maxWait = EnvironmentEdgeManager.currentTime() + maxWaitInSeconds * 1000;
    while (EnvironmentEdgeManager.currentTime() < maxWait) {
      try {
        List<ServerName> regionServers = new ArrayList<>();
        regionServers.addAll(admin.getRegionServers());
        // Remove the host Region server from target Region Servers list
        server = stripServer(regionServers, hostname, port);
        if (server != null) {
          break;
        } else {
          LOG.warn("Server " + hostname + ":" + port + " is not up yet, waiting");
        }
      } catch (IOException e) {
        LOG.warn("Could not get list of region servers", e);
      }
      Thread.sleep(500);
    }
    if (server == null) {
      LOG.error("Server " + hostname + ":" + port + " is not up. Giving up.");
      throw new Exception("Server " + hostname + ":" + port + " to load regions not online");
    }
    return server;
  }

  private List<RegionInfo> readRegionsFromFile(String filename) throws IOException {
    List<RegionInfo> regions = new ArrayList<>();
    File f = new File(filename);
    if (!f.exists()) {
      return regions;
    }
    try (DataInputStream dis = new DataInputStream(
        new BufferedInputStream(new FileInputStream(f)))) {
      int numRegions = dis.readInt();
      int index = 0;
      while (index < numRegions) {
        regions.add(RegionInfo.parseFromOrNull(Bytes.readByteArray(dis)));
        index++;
      }
    } catch (IOException e) {
      LOG.error("Error while reading regions from file:" + filename, e);
      throw e;
    }
    return regions;
  }

  /**
   * Write the number of regions moved in the first line followed by regions moved in subsequent
   * lines
   */
  private void writeFile(String filename, List<RegionInfo> movedRegions) throws IOException {
    try (DataOutputStream dos = new DataOutputStream(
        new BufferedOutputStream(new FileOutputStream(filename)))) {
      dos.writeInt(movedRegions.size());
      for (RegionInfo region : movedRegions) {
        Bytes.writeByteArray(dos, RegionInfo.toByteArray(region));
      }
    } catch (IOException e) {
      LOG.error(
          "ERROR: Was Not able to write regions moved to output file but moved " + movedRegions
              .size() + " regions", e);
      throw e;
    }
  }

  private void deleteFile(String filename) {
    File f = new File(filename);
    if (f.exists()) {
      f.delete();
    }
  }

  /**
   * @return List of servers from the exclude file in format 'hostname:port'.
   */
  private List<String> readExcludes(String excludeFile) throws IOException {
    List<String> excludeServers = new ArrayList<>();
    if (excludeFile == null) {
      return excludeServers;
    } else {
      try {
        Files.readAllLines(Paths.get(excludeFile)).stream().map(String::trim)
            .filter(((Predicate<String>) String::isEmpty).negate()).map(String::toLowerCase)
            .forEach(excludeServers::add);
      } catch (IOException e) {
        LOG.warn("Exception while reading excludes file, continuing anyways", e);
      }
      return excludeServers;
    }
  }

  /**
   * Excludes the servername whose hostname and port portion matches the list given in exclude file
   */
  private void stripExcludes(List<ServerName> regionServers) throws IOException {
    if (excludeFile != null) {
      List<String> excludes = readExcludes(excludeFile);
      Iterator<ServerName> i = regionServers.iterator();
      while (i.hasNext()) {
        String rs = i.next().getServerName();
        String rsPort = rs.split(ServerName.SERVERNAME_SEPARATOR)[0].toLowerCase() + ":" + rs
            .split(ServerName.SERVERNAME_SEPARATOR)[1];
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
   */
  private void stripMaster(List<ServerName> regionServers) throws IOException {
    ServerName master = admin.getClusterMetrics(EnumSet.of(Option.MASTER)).getMasterName();
    stripServer(regionServers, master.getHostname(), master.getPort());
  }

  /**
   * Remove the servername whose hostname and port portion matches from the passed array of servers.
   * Returns as side-effect the servername removed.
   * @return server removed from list of Region Servers
   */
  private ServerName stripServer(List<ServerName> regionServers, String hostname, int port) {
    for (Iterator<ServerName> iter = regionServers.iterator(); iter.hasNext();) {
      ServerName server = iter.next();
      if (server.getAddress().getHostname().equalsIgnoreCase(hostname) &&
        server.getAddress().getPort() == port) {
        iter.remove();
        return server;
      }
    }
    return null;
  }

  /**
   * Tries to scan a row from passed region
   */
  private void isSuccessfulScan(RegionInfo region) throws IOException {
    Scan scan = new Scan().withStartRow(region.getStartKey()).setRaw(true).setOneRowLimit()
        .setMaxResultSize(1L).setCaching(1).setFilter(new FirstKeyOnlyFilter())
        .setCacheBlocks(false);
    try (Table table = conn.getTable(region.getTable());
        ResultScanner scanner = table.getScanner(scan)) {
      scanner.next();
    } catch (IOException e) {
      LOG.error("Could not scan region:" + region.getEncodedName(), e);
      throw e;
    }
  }

  /**
   * Returns true if passed region is still on serverName when we look at hbase:meta.
   * @return true if region is hosted on serverName otherwise false
   */
  private boolean isSameServer(RegionInfo region, ServerName serverName)
      throws IOException {
    ServerName serverForRegion = getServerNameForRegion(region);
    if (serverForRegion != null && serverForRegion.equals(serverName)) {
      return true;
    }
    return false;
  }

  /**
   * Get servername that is up in hbase:meta hosting the given region. this is hostname + port +
   * startcode comma-delimited. Can return null
   * @return regionServer hosting the given region
   */
  private ServerName getServerNameForRegion(RegionInfo region) throws IOException {
    if (!admin.isTableEnabled(region.getTable())) {
      return null;
    }
    HRegionLocation loc =
      conn.getRegionLocator(region.getTable()).getRegionLocation(region.getStartKey(),
        region.getReplicaId(),true);
    if (loc != null) {
      return loc.getServerName();
    } else {
      return null;
    }
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
    try (RegionMover rm = rmbuilder.build()) {
      if (loadUnload.equalsIgnoreCase("load")) {
        success = rm.load();
      } else if (loadUnload.equalsIgnoreCase("unload")) {
        success = rm.unload();
      } else {
        printUsage();
        success = false;
      }
    }
    return (success ? 0 : 1);
  }

  public static void main(String[] args) {
    try (RegionMover mover = new RegionMover()) {
      mover.doStaticMain(args);
    }
  }
}