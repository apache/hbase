/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PatternOptionBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Check consistency among the in-memory states of the master and the
 * region server(s) and the state of data in HDFS.
 */
public class HBaseFsck {
  private static final int MAX_NUM_THREADS = 50; // #threads to contact regions
  private static final long THREADS_KEEP_ALIVE_SECONDS = 60;
  private static Log LOG = LogFactory.getLog(HBaseFsck.class.getName());
  private Configuration conf;

  private ClusterStatus status;
  private HConnection connection;
  private TreeMap<String, HbckInfo> regionInfo = new TreeMap<String, HbckInfo>();
  private TreeMap<String, TInfo> tablesInfo = new TreeMap<String, TInfo>();
  private Set<HServerAddress> couldNotScan = Sets.newHashSet();
  ErrorReporter errors = new PrintingErrorReporter();

  private static boolean details = false; // do we display the full report
  private long timelag = 0; // tables whose modtime is older
  enum FixState {
    NONE, ERROR, ALL
  };
  FixState fix = FixState.NONE; // do we want to try fixing the errors?
  private boolean rerun = false; // if we tried to fix something rerun hbck
  private static boolean summary = false; // if we want to print less output
  private static boolean checkRegionInfo = false;
  private static boolean promptResponse = false;  // "no" to all prompt questions
  private int numThreads = MAX_NUM_THREADS;
  private final String UnkownTable = "__UnknownTable__";

  // When HMaster is processing dead server shutdown (e.g. log split), hbck detects
  // the region as unassigned and tries to fix it if fix option is on. But this process
  // cleans assignment in META and triggers a new assignment immediately. The region
  // could be assigned to a healthy node regardless of whether log split has completed.
  // Some unflushed HLog records could get stuck forever. That means the cluster "loses"
  // these data. This flag is to prevent hbck from clearing assignment in META to dead
  // region server.
  private static boolean forceCleanMeta = false;

  ThreadPoolExecutor executor;         // threads to retrieve data from regionservers
  private List<WorkItem> asyncWork = Lists.newArrayList();
  public static String json = null;

  /**
   * Constructor
   *
   * @param conf Configuration object
   * @throws MasterNotRunningException if the master is not running
   */
  public HBaseFsck(Configuration conf)
    throws MasterNotRunningException, IOException {
    this.conf = conf;

    // fetch information from master
    HBaseAdmin admin = new HBaseAdmin(conf);
    status = admin.getMaster().getClusterStatus();
    connection = admin.getConnection();

    numThreads = conf.getInt("hbasefsck.numthreads", numThreads);
    executor = new ThreadPoolExecutor(numThreads, numThreads,
          THREADS_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
          new LinkedBlockingQueue<Runnable>());
    executor.allowCoreThreadTimeOut(true);
  }

  public TreeMap<String, HbckInfo> getRegionInfo() {
    return this.regionInfo;
  }

  public int initAndScanRootMeta() throws IOException {
    // print hbase server version
    errors.print("Version: " + status.getHBaseVersion());
    LOG.debug("timelag = " + StringUtils.formatTime(this.timelag));

    // Make sure regionInfo is empty before starting
    regionInfo.clear();
    tablesInfo.clear();

    // get a list of all regions from the master. This involves
    // scanning the META table
    if (!recordRootRegion()) {
      // Will remove later if we can fix it
      errors.reportError("Encountered fatal error. Exiting...");
      return -1;
    }
    return getMetaEntries();
  }

  /**
   * Contacts the master and prints out cluster-wide information
   * @throws IOException if a remote or network exception occurs
   * @return 0 on success, non-zero on failure
   */
  int doWork() throws IOException, InterruptedException {

    if (initAndScanRootMeta() == -1) {
      return -1;
    }
    // get a list of all tables that have not changed recently.
    AtomicInteger numSkipped = new AtomicInteger(0);
    HTableDescriptor[] allTables = getTables(numSkipped);
    errors.print("Number of Tables: " + allTables.length);
    if (details) {
      if (numSkipped.get() > 0) {
        errors.detail("\n Number of Tables in flux: " + numSkipped.get());
      }
      for (HTableDescriptor td : allTables) {
        String tableName = td.getNameAsString();
        errors.detail("\t Table: " + tableName + "\t" +
                           (td.isReadOnly() ? "ro" : "rw") + "\t" +
                           (td.isRootRegion() ? "ROOT" :
                            (td.isMetaRegion() ? "META" : "    ")) + "\t" +
                           " families:" + td.getFamilies().size());
      }
    }

    // From the master, get a list of all known live region servers
    Collection<HServerInfo> regionServers = status.getServerInfo();
    errors.print("Number of live region servers:" +
                       regionServers.size());
    if (details) {
      for (HServerInfo rsinfo: regionServers) {
        errors.detail("\t RegionServer:" + rsinfo.getServerName());
      }
    }

    // From the master, get a list of all dead region servers
    Collection<String> deadRegionServers = status.getDeadServerNames();
    errors.print("Number of dead region servers:" +
                       deadRegionServers.size());
    if (details) {
      for (String name: deadRegionServers) {
        errors.detail("\t RegionServer(dead):" + name);
      }
    }

    // Determine what's deployed
    scanRegionServers(regionServers);

    // Determine what's on HDFS
    scanHdfs();

    // finish all async tasks before analyzing what we have
    finishAsyncWork();

    // Check consistency
    checkConsistency();

    // Check integrity
    checkIntegrity();

    // Check if information in .regioninfo and .META. is consistent
    if (checkRegionInfo) {
      checkRegionInfo();
    }

    // Print table summary
    printTableSummary();

    return errors.summarize();
  }

  /**
   * Read the .regioninfo for all regions of the table and compare with the
   * corresponding entry in .META.
   * Entry in .regioninfo should be consistent with the entry in .META.
   *
   */
  void checkRegionInfo() {
    Path tableDir = null;

    try {
      for (HbckInfo hbi : regionInfo.values()) {
        tableDir = HTableDescriptor.getTableDir(FSUtils.getRootDir(conf),
            hbi.metaEntry.getTableDesc().getName());

        Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
        FileSystem fs = rootDir.getFileSystem(conf);

        Path regionPath = HRegion.getRegionDir(tableDir,
                          hbi.metaEntry.getEncodedName());
        Path regionInfoPath = new Path(regionPath, HRegion.REGIONINFO_FILE);
        if (fs.exists(regionInfoPath) &&
          fs.getFileStatus(regionInfoPath).getLen() > 0) {
          FSDataInputStream in = fs.open(regionInfoPath);
          HRegionInfo f_hri = null;
          try {
            f_hri = new HRegionInfo();
            f_hri.readFields(in);
          } catch (IOException ex) {
            errors.reportError("Could not read .regioninfo file at "
                               + regionInfoPath);
          } finally {
            in.close();
          }
          HbckInfo hbckinfo = regionInfo.get(f_hri.getEncodedName());
          HRegionInfo m_hri = hbckinfo.metaEntry;
          if(!f_hri.equals(m_hri)) {
            errors.reportError("Table name: " +
                    f_hri.getTableDesc().getNameAsString() +
                    " RegionInfo for "+ f_hri.getRegionNameAsString() +
                    " inconsistent in .META. and .regioninfo");
          }
        } else {
          if (!fs.exists(regionInfoPath)) {
            errors.reportError(".regioninfo not found at "
                               + regionInfoPath.toString());
          } else if (fs.getFileStatus(regionInfoPath).getLen() <= 0) {
            errors.reportError(".regioninfo file is empty (path =  "
                               + regionInfoPath + ")");
          }
        }
      }
    } catch (IOException e) {
      errors.reportError("Error in comparing .regioninfo and .META."
                         + e.getMessage());
    }
  }


  /**
   * Scan HDFS for all regions, recording their information into
   * regionInfo
   */
  void scanHdfs() throws IOException, InterruptedException {
    Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
    FileSystem fs = rootDir.getFileSystem(conf);

    // list all tables from HDFS
    List<FileStatus> tableDirs = Lists.newArrayList();

    boolean foundVersionFile = false;
    FileStatus[] files = fs.listStatus(rootDir);
    for (FileStatus file : files) {
      if (file.getPath().getName().equals(HConstants.VERSION_FILE_NAME)) {
        foundVersionFile = true;
      } else {
        tableDirs.add(file);
      }
    }

    // verify that version file exists
    if (!foundVersionFile) {
      errors.reportError("Version file does not exist in root dir " + rootDir);
    }

    // scan all the HDFS directories in parallel
    for (FileStatus tableDir : tableDirs) {
      WorkItem work = new WorkItemHdfsDir(this, fs, errors, tableDir);
      executor.execute(work);
      asyncWork.add(work);
    }
  }

  /**
   * Record the location of the ROOT region as found in ZooKeeper,
   * as if it were in a META table. This is so that we can check
   * deployment of ROOT.
   */
  boolean recordRootRegion() throws IOException {
    HRegionLocation rootLocation = connection.locateRegion(
      HConstants.ROOT_TABLE_NAME, HConstants.EMPTY_START_ROW);

    // Check if Root region is valid and existing
    if (rootLocation == null || rootLocation.getRegionInfo() == null ||
        rootLocation.getServerAddress() == null) {
      errors.reportError("Root Region or some of its attributes is null.");
      return false;
    }

    MetaEntry m = new MetaEntry(rootLocation.getRegionInfo(),
      rootLocation.getServerAddress(), null, System.currentTimeMillis());
    HbckInfo hbInfo = new HbckInfo(m);
    regionInfo.put(rootLocation.getRegionInfo().getEncodedName(), hbInfo);
    return true;
  }

  /**
   * Contacts each regionserver and fetches metadata about regions.
   * @throws IOException if a remote or network exception occurs
   */
  void scanRegionServers() throws IOException, InterruptedException {
    Collection<HServerInfo> regionServers = status.getServerInfo();
    errors.print("Number of live region servers:" +
        regionServers.size());
    if (details) {
      for (HServerInfo rsinfo: regionServers) {
        errors.detail("\t RegionServer:" + rsinfo.getServerName());
      }
    }
    scanRegionServers(regionServers);
    // finish all async tasks before analyzing what we have
    finishAsyncWork();
  }

  /**
   * Contacts each regionserver and fetches metadata about regions.
   * @param regionServerList - the list of region servers to connect to
   * @throws IOException if a remote or network exception occurs
   */
  void scanRegionServers(Collection<HServerInfo> regionServerList)
      throws IOException, InterruptedException {

    // loop to contact each region server in parallel
    for (HServerInfo rsinfo:regionServerList) {
      WorkItem work = new WorkItemRegion(this, rsinfo, errors, connection);
      executor.execute(work);
      asyncWork.add(work);
    }
  }

  void finishAsyncWork() throws InterruptedException {
    // wait for all directories to be done
    for (WorkItem work : this.asyncWork) {
      synchronized (work) {
        while (!work.isDone()) {
          work.wait();
        }
      }
    }

  }

  /**
   * Check consistency of all regions that have been found in previous phases.
   */
  void checkConsistency() throws IOException {
    for (HbckInfo hbi : regionInfo.values()) {
      doConsistencyCheck(hbi);
    }
  }

  /**
   * Check a single region for consistency and correct deployment.
   */
  void doConsistencyCheck(HbckInfo hbi) throws IOException {
    String descriptiveName = hbi.toString();

    boolean inMeta = hbi.metaEntry != null;
    boolean inHdfs = hbi.foundRegionDir != null;
    boolean hasMetaAssignment = inMeta && hbi.metaEntry.regionServer != null;
    boolean isDeployed = !hbi.deployedOn.isEmpty();
    boolean isMultiplyDeployed = hbi.deployedOn.size() > 1;
    boolean deploymentMatchesMeta =
      hasMetaAssignment && isDeployed && !isMultiplyDeployed &&
      hbi.metaEntry.regionServer.equals(hbi.deployedOn.get(0));
    boolean shouldBeDeployed = inMeta && !hbi.metaEntry.isOffline();
    long tooRecent = System.currentTimeMillis() - timelag;
    boolean recentlyModified =
      (inHdfs && hbi.foundRegionDir.getModificationTime() > tooRecent) ||
      (inMeta && hbi.metaEntry.modTime                    > tooRecent);

    String tableName = null;
    if (inMeta) {
      tableName = hbi.metaEntry.getTableDesc().getNameAsString();
    } else {
      tableName = UnkownTable;
    }
    TInfo tableInfo = tablesInfo.get(tableName);
    if (tableInfo == null) {
      tableInfo = new TInfo(tableName);
      tablesInfo.put(tableName,  tableInfo);
    }
    tableInfo.addRegionDetails(RegionType.total, descriptiveName);
    // ========== First the healthy cases =============
    if (hbi.onlyEdits) {
      return;
    }
    if (inMeta && inHdfs && isDeployed && deploymentMatchesMeta && shouldBeDeployed) {
      if (!hbi.metaEntry.isOffline()) {
        tableInfo.addRegionDetails(RegionType.online, descriptiveName);
      } else {
        tableInfo.addRegionDetails(RegionType.offline, descriptiveName);
      }
    } else if (inMeta && !shouldBeDeployed && !isDeployed) {
      // offline regions shouldn't cause complaints
      String message = "Region " + descriptiveName + " offline, ignoring.";
      LOG.debug(message);
      tableInfo.addRegionDetails(RegionType.offline, descriptiveName);
      tableInfo.addRegionError(message);
    } else if (recentlyModified) {
      String message = "Region " + descriptiveName + " was recently modified -- skipping";
      LOG.info(message);
      tableInfo.addRegionDetails(RegionType.skipped, descriptiveName);
      tableInfo.addRegionError(message);
    }
    // ========== Cases where the region is not in META =============
    else if (!inMeta && !inHdfs && !isDeployed) {
      // We shouldn't have record of this region at all then!
      assert false : "Entry for region with no data";
    } else if (!inMeta && !inHdfs && isDeployed) {
      String message = "Region " + descriptiveName + " not on HDFS or in META but " +
        "deployed on " + Joiner.on(", ").join(hbi.deployedOn);
      errors.reportError(message);
      tableInfo.addRegionDetails(RegionType.missing, descriptiveName);
      tableInfo.addRegionError(message);
    } else if (!inMeta && inHdfs && !isDeployed) {
      String message = "Region " + descriptiveName + " on HDFS, but not listed in META " +
        "or deployed on any region server.";
      errors.reportError(message);
      tableInfo.addRegionDetails(RegionType.missing, descriptiveName);
      tableInfo.addRegionError(message);
    } else if (!inMeta && inHdfs && isDeployed) {
      String message = "Region " + descriptiveName + " not in META, but deployed on " +
        Joiner.on(", ").join(hbi.deployedOn);
      errors.reportError(message);
      tableInfo.addRegionDetails(RegionType.missing, descriptiveName);
      tableInfo.addRegionError(message);
    // ========== Cases where the region is in META =============
    } else if (inMeta && !inHdfs && !isDeployed) {
      String message = "Region " + descriptiveName + " found in META, but not in HDFS " +
          "or deployed on any region server.";
      errors.reportError(message);
      tableInfo.addRegionDetails(RegionType.unknown, descriptiveName);
      tableInfo.addRegionError(message);
    } else if (inMeta && !inHdfs && isDeployed) {
      String message = "Region " + descriptiveName + " found in META, but not in HDFS, " +
        "and deployed on " + Joiner.on(", ").join(hbi.deployedOn);
      errors.reportError(message);
      tableInfo.addRegionDetails(RegionType.unknown, descriptiveName);
      tableInfo.addRegionError(message);
    } else if (inMeta && inHdfs && !isDeployed && shouldBeDeployed) {
      if (couldNotScan.contains(hbi.metaEntry.regionServer)) {
        String message = "Could not verify region " + descriptiveName
            + " because could not scan supposed owner "
            + hbi.metaEntry.regionServer;
        LOG.info(message);
        tableInfo.addRegionDetails(RegionType.timeout, descriptiveName);
        tableInfo.addRegionError(message);
      } else {
        String message = "Region " + descriptiveName + " not deployed on any region server.";
        errors.reportWarning(message);
        // If we are trying to fix the errors
        tableInfo.addRegionDetails(RegionType.missing, descriptiveName);
        tableInfo.addRegionError(message);
        if (fix != FixState.NONE) {
          errors.print("Trying to fix unassigned region...");
          if (!hbi.deployedOn.isEmpty()) {
            String addr = hbi.deployedOn.get(0).getHostAddressWithPort();
            // Do not clean region assignment in META if a split log is ongoing for its assigned RS.
            if (status.getDeadServerNames().contains(addr) && !forceCleanMeta) {
              errors.print("Split log for dead server " + addr + " is possibly ongoing, skip unassigned region "
                  + hbi.metaEntry.getEncodedName());
              return;
            }
          }
          if (HBaseFsckRepair.fixUnassigned(this.conf, hbi.metaEntry)) {
            setShouldRerun();
          }
        }
      }
    } else if (inMeta && inHdfs && isDeployed && !shouldBeDeployed) {
      String message = "Region " + descriptiveName + " should not be deployed according " +
          "to META, but is deployed on " + Joiner.on(", ").join(hbi.deployedOn);
      errors.reportError(message);
      tableInfo.addRegionDetails(RegionType.unknown, descriptiveName);
      tableInfo.addRegionError(message);
    } else if (inMeta && inHdfs && isMultiplyDeployed) {
      String message = "Region " + descriptiveName +
          " is listed in META on region server " + hbi.metaEntry.regionServer +
          " but is multiply assigned to region servers " +
          Joiner.on(", ").join(hbi.deployedOn);
      errors.reportFixableError(message);
      tableInfo.addRegionDetails(RegionType.unknown, descriptiveName);
      tableInfo.addRegionError(message);
      // If we are trying to fix the errors
      if (fix != FixState.NONE) {
        errors.print("Trying to fix assignment error...");
        if (HBaseFsckRepair.fixDupeAssignment(this.conf, hbi.metaEntry, hbi.deployedOn)) {
          setShouldRerun();
        }
      }
    } else if (inMeta && inHdfs && isDeployed && !deploymentMatchesMeta) {
      String message = "Region " + descriptiveName +
          " listed in META on region server " + hbi.metaEntry.regionServer +
          " but found on region server " + hbi.deployedOn.get(0);
      errors.reportFixableError(message);
      tableInfo.addRegionDetails(RegionType.unknown, descriptiveName);
      tableInfo.addRegionError(message);
      // If we are trying to fix the errors
      if (fix != FixState.NONE) {
        errors.print("Trying to fix assignment error...");
        if (HBaseFsckRepair.fixDupeAssignment(this.conf, hbi.metaEntry, hbi.deployedOn)) {
          setShouldRerun();
        }
      }
    } else {
      String message = "Region " + descriptiveName + " is in an unforeseen state:" +
          " inMeta=" + inMeta +
          " inHdfs=" + inHdfs +
          " isDeployed=" + isDeployed +
          " isMultiplyDeployed=" + isMultiplyDeployed +
          " deploymentMatchesMeta=" + deploymentMatchesMeta +
          " shouldBeDeployed=" + shouldBeDeployed;
      errors.reportError(message);
      tableInfo.addRegionDetails(RegionType.unknown, descriptiveName);
      tableInfo.addRegionError(message);
    }
  }

  /**
   * Checks tables integrity. Goes over all regions and scans the tables.
   * Collects the table -> [region] mapping and checks if there are missing,
   * repeated or overlapping regions.
   */
  void checkIntegrity() {
    for (HbckInfo hbi : regionInfo.values()) {
      // Check only valid, working regions

      if (hbi.metaEntry == null) continue;
      if (hbi.metaEntry.regionServer == null) continue;
      if (hbi.foundRegionDir == null) continue;
      if (hbi.deployedOn.isEmpty()
          && !couldNotScan.contains(hbi.metaEntry.regionServer)) continue;
      if (hbi.onlyEdits) continue;

      // We should be safe here
      String tableName = hbi.metaEntry.getTableDesc().getNameAsString();
      TInfo modTInfo = tablesInfo.get(tableName);
      if (modTInfo == null) {
        modTInfo = new TInfo(tableName);
      }
      for (HServerAddress server : hbi.deployedOn) {
        modTInfo.addServer(server);
      }
      modTInfo.addEdge(hbi.metaEntry.getStartKey(), hbi.metaEntry.getEndKey());
      tablesInfo.put(tableName, modTInfo);
    }

    for (TInfo tInfo : tablesInfo.values()) {
      if (tInfo.getName().equals(UnkownTable)) continue;
      if (!tInfo.check()) {
        errors.reportError("Found inconsistency in table " + tInfo.getName() +
            ": " + tInfo.getLastError());
      }
    }
  }

  public enum RegionType {
    total,
    online,
    offline,
    missing,
    skipped,
    timeout,
    unknown
  }

  /**
   * Maintain information about a particular table.
   */
  private class TInfo {
    String tableName;
    TreeMap <byte[], byte[]> edges;
    TreeSet <HServerAddress> deployedOn;
    String lastError = null;

    private TreeMap<RegionType, ArrayList<String>> regionDetails = new TreeMap<RegionType, ArrayList<String>>();
    private ArrayList<String> regionErrors = new ArrayList<String>();

    TInfo(String name) {
      this.tableName = name;
      edges = new TreeMap <byte[], byte[]> (Bytes.BYTES_COMPARATOR);
      deployedOn = new TreeSet <HServerAddress>();
      for (RegionType regionType : RegionType.values()) {
        regionDetails.put(regionType, new ArrayList<String>());
      }
    }

    public void addEdge(byte[] fromNode, byte[] toNode) {
      this.edges.put(fromNode, toNode);
    }

    public void addServer(HServerAddress server) {
      this.deployedOn.add(server);
    }

    public String getName() {
      return tableName;
    }

    public int getNumRegions() {
      return edges.size();
    }

    public String getLastError() {
      return this.lastError;
    }

    public String posToStr(byte[] k) {
      return k.length > 0 ? Bytes.toStringBinary(k) : "0";
    }

    public String regionToStr(Map.Entry<byte[], byte []> e) {
      return posToStr(e.getKey()) + " -> " + posToStr(e.getValue());
    }

    public boolean check() {
      if (details) {
        errors.detail("Regions found in META for " + this.tableName);
        for (Map.Entry<byte[], byte []> e : edges.entrySet()) {
          errors.detail('\t' + regionToStr(e));
        }
      }

      byte[] last = new byte[0];
      byte[] next = new byte[0];
      TreeSet <byte[]> visited = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
      // Each table should start with a zero-length byte[] and end at a
      // zero-length byte[]. Just follow the edges to see if this is true
      while (true) {
        // Check if region chain is broken
        if (!edges.containsKey(last)) {
          this.lastError = "Cannot find region with start key "
            + posToStr(last);
          return false;
        }
        next = edges.get(last);
        // Found a cycle
        if (visited.contains(next)) {
          this.lastError = "Cycle found in region chain. "
            + "Current = "+ posToStr(last)
            + "; Cycle Start = " +  posToStr(next);
          return false;
        }
        // Mark next node as visited
        visited.add(next);
        // If next is zero-length byte[] we are possibly at the end of the chain
        if (next.length == 0) {
          // If we have visited all elements we are fine
          if (edges.size() != visited.size()) {
            this.lastError = "Region in-order travesal does not include "
              + "all elements found in META.  Chain=" + visited.size()
              + "; META=" + edges.size() + "; Missing=";
            for (Map.Entry<byte[], byte []> e : edges.entrySet()) {
              if (!visited.contains(e.getKey())) {
                this.lastError += regionToStr(e) + " , ";
              }
            }
            return false;
          }
          return true;
        }
        last = next;
      }
      // How did we get here?
    }

    public JSONObject toJSONObject() {
      JSONObject ret = new JSONObject();
      try {
        ret.put("table", tableName);
        for (RegionType type : RegionType.values()) {
          ret.put(type.toString(), regionDetails.get(type).size());
        }
        JSONArray arr = new JSONArray();
        for (int i=0; i<regionErrors.size(); i++) {
          arr.put(i, regionErrors.get(i));
        }
        ret.put("Errors", arr);
        ret.put("Details", this.getAllRegionDetails());
        return ret;
      } catch (JSONException ex) {
        return null;
      }
    }

    public void addRegionDetails(RegionType type, String name) {
      regionDetails.get(type).add(name);
    }

    public void addRegionError(String error) {
      regionErrors.add(error);
    }

    public ArrayList<String> getRegionDetails(RegionType type) {
      return regionDetails.get(type);
    }

    public JSONArray getRegionDetailsArray(RegionType type) {
      JSONArray arr = new JSONArray();
      ArrayList<String> regions = this.getRegionDetails(type);
      for (String s : regions) {
        arr.put(s);
      }
      return arr;
    }

    public JSONObject getAllRegionDetails() throws JSONException{
      JSONObject ret = new JSONObject();
      for (RegionType type : RegionType.values()) {
        if (type.equals(RegionType.total)) continue;
        if (type.equals(RegionType.online)) continue;
        ret.put(type.toString(), getRegionDetailsArray(type));
      }
      return ret;
    }
  }


  /**
   * Return a list of table names whose metadata have not been modified in the
   * last few milliseconds specified by timelag
   * if any of the REGIONINFO_QUALIFIER, SERVER_QUALIFIER, STARTCODE_QUALIFIER,
   * SPLITA_QUALIFIER, SPLITB_QUALIFIER have not changed in the last
   * milliseconds specified by timelag, then the table is a candidate to be returned.
   * @param regionList - all entries found in .META
   * @return tables that have not been modified recently
   * @throws IOException if an error is encountered
   */
  HTableDescriptor[] getTables(AtomicInteger numSkipped) {
    TreeSet<HTableDescriptor> uniqueTables = new TreeSet<HTableDescriptor>();
    long now = System.currentTimeMillis();

    for (HbckInfo hbi : regionInfo.values()) {
      MetaEntry info = hbi.metaEntry;

      // if the start key is zero, then we have found the first region of a table.
      // pick only those tables that were not modified in the last few milliseconds.
      if (info != null && info.getStartKey().length == 0) {
        if (info.modTime + timelag < now) {
          uniqueTables.add(info.getTableDesc());
        } else {
          numSkipped.incrementAndGet(); // one more in-flux table
        }
      }
    }
    return uniqueTables.toArray(new HTableDescriptor[uniqueTables.size()]);
  }

  private synchronized boolean addFailedServer(HServerAddress server) {
    return couldNotScan.add(server);
  }

  /**
   * Gets the entry in regionInfo corresponding to the the given encoded
   * region name. If the region has not been seen yet, a new entry is added
   * and returned.
   */
  private synchronized HbckInfo getOrCreateInfo(String name) {
    HbckInfo hbi = regionInfo.get(name);
    if (hbi == null) {
      hbi = new HbckInfo(null);
      regionInfo.put(name, hbi);
    }
    return hbi;
  }

  /**
    * Get values in regionInfo for .META.
    *
    * @return list of meta regions
    */
  private List<HbckInfo> getMetaRegions() {
    List<HbckInfo> metaRegions = Lists.newArrayList();
    for (HbckInfo value : regionInfo.values()) {
      if (value.metaEntry.isMetaTable()) {
        metaRegions.add(value);
      }
    }

    return metaRegions;
  }

  /**
   * Scan .META. and -ROOT-, adding all regions found to the regionInfo map.
   * @throws IOException if an error is encountered
   */
  int getMetaEntries() throws IOException {
    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      int countRecord = 1;

      // comparator to sort KeyValues with latest modtime
      final Comparator<KeyValue> comp = new Comparator<KeyValue>() {
        @Override
        public int compare(KeyValue k1, KeyValue k2) {
          return (int)(k1.getTimestamp() - k2.getTimestamp());
        }
      };

      @Override
      public boolean processRow(Result result) throws IOException {
        try {

          // record the latest modification of this META record
          long ts =  Collections.max(result.list(), comp).getTimestamp();

          // record region details
          byte[] value = result.getValue(HConstants.CATALOG_FAMILY,
                                         HConstants.REGIONINFO_QUALIFIER);
          HRegionInfo info = null;
          HServerAddress server = null;
          byte[] startCode = null;
          if (value != null) {
            info = Writables.getHRegionInfo(value);
          }

          // record assigned region server
          value = result.getValue(HConstants.CATALOG_FAMILY,
                                     HConstants.SERVER_QUALIFIER);
          if (value != null && value.length > 0) {
            String address = Bytes.toString(value);
            server = new HServerAddress(address);
          }

          // record region's start key
          value = result.getValue(HConstants.CATALOG_FAMILY,
                                  HConstants.STARTCODE_QUALIFIER);
          if (value != null) {
            startCode = value;
          }
          MetaEntry m = new MetaEntry(info, server, startCode, ts);
          HbckInfo hbInfo = new HbckInfo(m);
          HbckInfo previous = regionInfo.put(info.getEncodedName(), hbInfo);
          if (previous != null) {
            throw new IOException("Two entries in META are same " + previous);
          }

          // show proof of progress to the user, once for every 100 records.
          if (countRecord % 100 == 0) {
            errors.progress();
          }
          countRecord++;
          return true;
        } catch (RuntimeException e) {
          LOG.error("Result=" + result);
          throw e;
        }
      }
    };

    // Scan -ROOT- to pick up META regions
    MetaScanner.metaScan(conf, visitor,
      HConstants.ROOT_TABLE_NAME, HConstants.EMPTY_START_ROW, null,
      Integer.MAX_VALUE);

    List<HbckInfo> metaRegions = getMetaRegions();
    // If there is no region holding .META., no way to fix
    if (metaRegions.isEmpty()) {
      errors.reportError("No META record in ROOT, not able to fix");
      return -1;
    }

    // Scan .META. to pick up user regions
    try {
      MetaScanner.metaScan(conf, visitor);
    } catch (NotServingRegionException e) {
      errors.reportWarning("META is not assigned");

      boolean hasNonFixableFailure = false;

      if (fix != FixState.NONE) {
        errors.print("Trying to fix unassigned META...");
        for (HbckInfo hbi : metaRegions) {
          // Don't know which META region is not assigned (if there are more than one META regions).
          // Try to reassign all of them. We don't expect more than one META regions so
          // this un-optimal fix should be fine now.
          if (hbi.metaEntry.isMetaTable()) {
            String addr = hbi.metaEntry.regionServer.getHostAddressWithPort();
            // Do not clean META assignment if a split log is going on the RS.
            if (status.getDeadServerNames().contains(addr) && !forceCleanMeta) {
              errors.print("Split log for dead server " + addr
                  + " is possibly ongoing, skip unassigned META region "
                  + hbi.metaEntry.getEncodedName());
              hasNonFixableFailure = true;
            } else if (!HBaseFsckRepair.fixUnassigned(this.conf, hbi.metaEntry)) {
              hasNonFixableFailure = true;
            }
          }
        }
      } else {
        errors.print("Fix not enabled, won't do anything");
        throw e;
      }

      if (hasNonFixableFailure) {
        errors.reportError("Some META region assignment issue could not be fixed, exiting...");
      } else {
        errors.print("Fixed some issue, will rerun");
        setShouldRerun();
      }
      return -1;
    }

    return 0;
  }

  /**
   * Stores the entries scanned from META
   */
  public static class MetaEntry extends HRegionInfo {
    HServerAddress regionServer;   // server hosting this region
    long modTime;          // timestamp of most recent modification metadata

    public MetaEntry(HRegionInfo rinfo, HServerAddress regionServer,
                     byte[] startCode, long modTime) {
      super(rinfo);
      this.regionServer = regionServer;
      this.modTime = modTime;
    }
  }

  /**
   * Maintain information about a particular region.
   */
  static class HbckInfo {
    boolean onlyEdits = false;
    MetaEntry metaEntry = null;
    FileStatus foundRegionDir = null;
    List<HServerAddress> deployedOn = Lists.newArrayList();

    HbckInfo(MetaEntry metaEntry) {
      this.metaEntry = metaEntry;
    }

    public synchronized void addServer(HServerAddress server) {
      this.deployedOn.add(server);
    }

    @Override
    public synchronized String toString() {
      if (metaEntry != null) {
        return metaEntry.getRegionNameAsString();
      } else if (foundRegionDir != null) {
        return foundRegionDir.getPath().toString();
      } else {
        return "unknown region on " + Joiner.on(", ").join(deployedOn);
      }
    }
  }

  /**
   * Prints summary of all tables found on the system.
   */
  private void printTableSummary() {
    if (HBaseFsck.json != null) return;
    System.out.println("Summary:");
    for (TInfo tInfo : tablesInfo.values()) {
      if (tInfo.getLastError() == null) {
        System.out.println("Table " + tInfo.getName() + " is okay.");
      } else {
        System.out.println("Table " + tInfo.getName() + " is inconsistent.");
      }
      System.out.println("  -- number of regions: " + tInfo.getNumRegions());
      System.out.print("  -- deployed on:");
      for (HServerAddress server : tInfo.deployedOn) {
        System.out.print(" " + server.toString());
      }
      System.out.println("\n");
    }
  }

  interface ErrorReporter {
    public void reportWarning(String message);
    public void reportError(String message);
    public void reportFixableError(String message);
    public int summarize();
    public void detail(String details);
    public void progress();
    public void print(String message);
  }

  private static class PrintingErrorReporter implements ErrorReporter {
    public int warnCount = 0;
    public int errorCount = 0;
    public int fixableCount = 0;
    private int showProgress;

    @Override
    public synchronized void reportWarning(String message) {
      if (!summary) {
        if (HBaseFsck.json == null) {
          System.out.println("WARNING: " + message);
        }
      }
      warnCount++;
    }

    @Override
    public synchronized void reportError(String message) {
      if (!summary) {
        if (HBaseFsck.json == null) {
          System.out.println("ERROR: " + message);
        }
      }
      errorCount++;
      showProgress = 0;
    }

    @Override
    public synchronized void reportFixableError(String message) {
      if (!summary) {
        if (HBaseFsck.json == null) {
          System.out.println("ERROR (fixable): " + message);
        }
      }
      fixableCount++;
      showProgress = 0;
    }

    @Override
    public synchronized int summarize() {
      if (HBaseFsck.json == null) {
        System.out.println(Integer.toString(errorCount + fixableCount) +
                           " inconsistencies detected.");
        System.out.println(Integer.toString(fixableCount) +
        " inconsistencies are fixable.");
      }
      if (warnCount > 0) {
        if (HBaseFsck.json == null) {
          System.out.println(Integer.toString(warnCount) + " warnings.");
        }
      }
      if (errorCount + fixableCount == 0) {
        if (HBaseFsck.json == null) {
          System.out.println("Status: OK ");
        }
        return 0;
      } else if (fixableCount == 0) {
        if (HBaseFsck.json == null) {
          System.out.println("Status: INCONSISTENT");
        }
        return -1;
      } else {
        if (HBaseFsck.json == null) {
          System.out.println("Status: INCONSISTENT (fixable)");
        }
        return -2;
      }
    }

    @Override
    public synchronized void print(String message) {
      if (HBaseFsck.json != null) return;
      if (!summary) {
        System.out.println(message);
      }
    }

    @Override
    public synchronized void detail(String message) {
      if (details) {
        if (HBaseFsck.json == null){
          System.out.println(message);
        }
      }
      showProgress = 0;
    }

    @Override
    public synchronized void progress() {
      if (showProgress++ == 10) {
        if (!summary) {
          if (HBaseFsck.json == null) {
            System.out.print(".");
          }
        }
        showProgress = 0;
      }
    }
  }

  static interface WorkItem extends Runnable {
    boolean isDone();
  }

  /**
   * Contact a region server and get all information from it
   */
  static class WorkItemRegion implements WorkItem {
    private HBaseFsck hbck;
    private HServerInfo rsinfo;
    private ErrorReporter errors;
    private HConnection connection;
    private boolean done;

    WorkItemRegion(HBaseFsck hbck, HServerInfo info,
                   ErrorReporter errors, HConnection connection) {
      this.hbck = hbck;
      this.rsinfo = info;
      this.errors = errors;
      this.connection = connection;
      this.done = false;
    }

    // is this task done?
    @Override
    public synchronized boolean isDone() {
      return done;
    }

    @Override
    public synchronized void run() {
      errors.progress();
      try {
        HRegionInterface server = connection.getHRegionConnection(
                                    rsinfo.getServerAddress());

        // list all online regions from this region server
        HRegionInfo[] regions = server.getRegionsAssignment();

        if (details) {
          StringBuffer buf = new StringBuffer();
          buf.append("\nRegionServer:" + rsinfo.getServerName() +
                       " number of regions:" + regions.length);
          for (HRegionInfo rinfo: regions) {
            buf.append("\n\t name:" + rinfo.getRegionNameAsString() +
                          " id:" + rinfo.getRegionId() +
                          " encoded name:" + rinfo.getEncodedName() +
                          " start :" + Bytes.toStringBinary(rinfo.getStartKey()) +
                          " end :" + Bytes.toStringBinary(rinfo.getEndKey()));
          }
          errors.detail(buf.toString());
        }

        // check to see if the existance of this region matches the region in META
        for (HRegionInfo r:regions) {
          HbckInfo hbi = hbck.getOrCreateInfo(r.getEncodedName());
          hbi.addServer(rsinfo.getServerAddress());
        }
      } catch (IOException e) {          // unable to connect to the region server.
        errors.reportWarning("RegionServer: " + rsinfo.getServerName()
          + " Unable to fetch region information. " + e);
        hbck.addFailedServer(rsinfo.getServerAddress());
      } finally {
        done = true;
        notifyAll(); // wakeup anybody waiting for this item to be done
      }
    }
  }

  /**
   * Contact hdfs and get all information about spcified table directory.
   */
  static class WorkItemHdfsDir implements WorkItem {
    private HBaseFsck hbck;
    private FileStatus tableDir;
    private ErrorReporter errors;
    private FileSystem fs;
    private boolean done;

    WorkItemHdfsDir(HBaseFsck hbck, FileSystem fs, ErrorReporter errors,
                    FileStatus status) {
      this.hbck = hbck;
      this.fs = fs;
      this.tableDir = status;
      this.errors = errors;
      this.done = false;
    }

    @Override
    public synchronized boolean isDone() {
      return done;
    }

    @Override
    public synchronized void run() {
      try {
        String tableName = tableDir.getPath().getName();
        // ignore hidden files
        if (tableName.startsWith(".") &&
            !tableName.equals( Bytes.toString(HConstants.META_TABLE_NAME)))
          return;
        // level 2: <HBASE_DIR>/<table>/*
        FileStatus[] regionDirs = fs.listStatus(tableDir.getPath());
        for (FileStatus regionDir : regionDirs) {
          String encodedName = regionDir.getPath().getName();

          // ignore directories that aren't hexadecimal
          if (!encodedName.toLowerCase().matches("[0-9a-f]+")) continue;

          HbckInfo hbi = hbck.getOrCreateInfo(encodedName);
          synchronized (hbi) {
            if (hbi.foundRegionDir != null) {
              errors.print("Directory " + encodedName + " duplicate??" +
                           hbi.foundRegionDir);
            }
            hbi.foundRegionDir = regionDir;

            // Set a flag if this region contains only edits
            // This is special case if a region is left after split
            hbi.onlyEdits = true;
            FileStatus[] subDirs = fs.listStatus(regionDir.getPath());
            Path ePath = HLog.getRegionDirRecoveredEditsDir(regionDir.getPath());
            for (FileStatus subDir : subDirs) {
              String sdName = subDir.getPath().getName();
              if (!sdName.startsWith(".") && !sdName.equals(ePath.getName())) {
                hbi.onlyEdits = false;
                break;
              }
            }
          }
        }
      } catch (IOException e) {          // unable to connect to the region server.
        errors.reportError("Table Directory: " + tableDir.getPath().getName() +
                      " Unable to fetch region information. " + e);
      } finally {
        done = true;
        notifyAll();
      }
    }
  }

  private void WriteShortTableSummaries() {
    JSONObject ret = new JSONObject();
    JSONArray arr = new JSONArray();
    try {
      int i = 0;
      for (Entry<String, TInfo> entry : tablesInfo.entrySet()) {
        arr.put(i++, entry.getValue().toJSONObject());
      }
      ret.put("Summaries", arr);
    } catch (JSONException ex) {
      LOG.error("Problem creating the Summaries JSON");
    }
    System.out.println(ret.toString());
  }

  /**
   * Display the full report from fsck.
   * This displays all live and dead region servers, and all known regions.
   */
  void displayFullReport() {
    details = true;
  }

  /**
   * Set summary mode.
   * Print only summary of the tables and status (OK or INCONSISTENT)
   */
  void setSummary() {
    summary = true;
  }

  /**
   * Check if we should rerun fsck again. This checks if we've tried to
   * fix something and we should rerun fsck tool again.
   * Display the full report from fsck. This displays all live and dead
   * region servers, and all known regions.
   */
  void setShouldRerun() {
    rerun = true;
  }

  boolean shouldRerun() {
    return rerun;
  }

  /**
   * Fix inconsistencies found by fsck. This should try to fix errors (if any)
   * found by fsck utility.
   */
  void setFixState(FixState newVal) {
    fix = newVal;
  }

  /**
   * Let the user allow the opportunity to specify "-y" to all
   * reconfirmation questions.
   */
  static void setPromptResponse(boolean value) {
    promptResponse = value;
  }
  static boolean getPromptResponse() {
    return promptResponse;
  }

  /**
   * We are interested in only those tables that have not changed their state in
   * META during the last few seconds specified by hbase.admin.fsck.timelag
   * @param ms - the time in milliseconds
   */
  void setTimeLag(long ms) {
    timelag = ms;
  }

  /**
   * Sets the output json file where the table summaries are written to
   * @param summaryFileName - the file name
   */
  private void setJsonFlag(String jsonFlag) {
    HBaseFsck.json = jsonFlag;
  }

  /**
   * Main program
   *
   * @param args
   * @throws ParseException
   */
  public static void main(String [] args)
 throws IOException,
      MasterNotRunningException, InterruptedException, ParseException {
    Options opt = new Options();

    OptionBuilder.withArgName("property=value");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("Override HBase Configuration Settings");
    opt.addOption(OptionBuilder.create("D"));

    OptionBuilder.withArgName("timeInSeconds");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("Ignore regions with metadata updates in the last {timeInSeconds}.");
    OptionBuilder.withType(PatternOptionBuilder.NUMBER_VALUE);
    opt.addOption(OptionBuilder.create("timelag"));

    OptionBuilder.withArgName("timeInSeconds");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("Stop scan jobs after a fixed time & analyze existing data.");
    OptionBuilder.withType(PatternOptionBuilder.NUMBER_VALUE);
    OptionBuilder.create("timeout");

    opt.addOption("fix", false, "Try to fix some of the errors.");
    opt.addOption("y", false, "Do not prompt for reconfirmation from users on fix.");
    opt.addOption("w", false, "Try to fix warnings as well as errors.");
    opt.addOption("summary", false, "Print only summary of the tables and status.");
    opt.addOption("detail", false, "Display full report of all regions.");
    opt.addOption("checkRegionInfo", false, "Check if .regioninfo is consistent with .META.");
    opt.addOption("h", false, "Display this help");
    opt.addOption("json", false, "Outputs the table summaries and errors in JSON format");
    opt.addOption("forceCleanMeta", false,
        "Clean meta entry of unassigned regions regardless of possible log splitting related to this region");

    CommandLine cmd = new GnuParser().parse(opt, args);

    // any unknown args or -h
    if (!cmd.getArgList().isEmpty() || cmd.hasOption("h")) {
      new HelpFormatter().printHelp("hbck", opt);
      return;
    }

    if (cmd.hasOption("json")) {
      Logger.getLogger("org.apache.zookeeper").setLevel(Level.OFF);
      Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.OFF);
      Logger.getLogger("org.apache.hadoop.fs.FileSystem").setLevel(Level.OFF);
      Logger.getLogger("org.apache.hadoop.fs").setLevel(Level.OFF);
      Logger.getLogger("org.apache.hadoop.dfs").setLevel(Level.OFF);
      Logger.getLogger("org.apache.hadoop.hdfs").setLevel(Level.OFF);
    }

    Configuration conf = HBaseConfiguration.create();
    conf.set("fs.defaultFS", conf.get("hbase.rootdir"));

    if (cmd.hasOption("D")) {
      for (String confOpt : cmd.getOptionValues("D")) {
        String[] kv = confOpt.split("=", 2);
        if (kv.length == 2) {
          conf.set(kv[0], kv[1]);
          LOG.debug("-D configuration override: " + kv[0] + "=" + kv[1]);
        } else {
          throw new ParseException("-D option format invalid: " + confOpt);
        }
      }
    }
    if (cmd.hasOption("timeout")) {
      Object timeout = cmd.getParsedOptionValue("timeout");
      if (timeout instanceof Long) {
        conf.setLong(HConstants.HBASE_RPC_TIMEOUT_KEY, ((Long) timeout).longValue() * 1000);
      } else {
        throw new ParseException("-timeout needs a long value.");
      }
    }

    // create a fsck object
    HBaseFsck fsck = new HBaseFsck(conf);
    fsck.setTimeLag(HBaseFsckRepair.getEstimatedFixTime(conf));

    if (cmd.hasOption("details")) {
      fsck.displayFullReport();
    }
    if (cmd.hasOption("timelag")) {
      Object timelag = cmd.getParsedOptionValue("timelag");
      if (timelag instanceof Long) {
        fsck.setTimeLag(((Long) timelag).longValue() * 1000);
      } else {
        throw new ParseException("-timelag needs a long value.");
      }
    }
    if (cmd.hasOption("fix")) {
      fsck.setFixState(FixState.ERROR);
    }
    if (cmd.hasOption("w")) {
      fsck.setFixState(FixState.ALL);
    }
    if (cmd.hasOption("y")) {
      HBaseFsck.setPromptResponse(true);
    }
    if (cmd.hasOption("summary")) {
        fsck.setSummary();
    }
    if (cmd.hasOption("checkRegionInfo")) {
      checkRegionInfo = true;
    }
    if (cmd.hasOption("json")) {
      fsck.setJsonFlag("json");
    }
    if (cmd.hasOption("forceCleanMeta")) {
      forceCleanMeta = true;
    }

    int code = -1;
    try {
      // do the real work of fsck
      code = fsck.doWork();
      // If we have tried to fix the HBase state, run fsck again
      // to see if we have fixed our problems
      if (fsck.shouldRerun()) {
        fsck.setFixState(FixState.NONE);
        long fixTime = HBaseFsckRepair.getEstimatedFixTime(conf);
        if (fixTime > 0) {
          LOG.info("Waiting " + StringUtils.formatTime(fixTime) +
              " before checking to see if fixes worked...");
          Thread.sleep(fixTime);
        }
        code = fsck.doWork();
      }
    } catch (InterruptedException ie) {
      LOG.info("HBCK was interrupted by user. Exiting...");
      code = -1;
    }
    if (cmd.hasOption("json")) {
      fsck.WriteShortTableSummaries();
    }

    Runtime.getRuntime().exit(code);
  }
}
