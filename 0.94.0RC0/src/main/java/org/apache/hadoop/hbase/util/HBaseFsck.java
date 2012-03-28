/**
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HConnectionManager.HConnectable;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter.ERROR_CODE;
import org.apache.hadoop.hbase.util.hbck.TableIntegrityErrorHandler;
import org.apache.hadoop.hbase.util.hbck.TableIntegrityErrorHandlerImpl;
import org.apache.hadoop.hbase.zookeeper.RootRegionTracker;
import org.apache.hadoop.hbase.zookeeper.ZKTable;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

/**
 * HBaseFsck (hbck) is a tool for checking and repairing region consistency and
 * table integrity problems in a corrupted HBase.
 * <p>
 * Region consistency checks verify that .META., region deployment on region
 * servers and the state of data in HDFS (.regioninfo files) all are in
 * accordance.
 * <p>
 * Table integrity checks verify that all possible row keys resolve to exactly
 * one region of a table.  This means there are no individual degenerate
 * or backwards regions; no holes between regions; and that there are no
 * overlapping regions.
 * <p>
 * The general repair strategy works in two phases:
 * <ol>
 * <li> Repair Table Integrity on HDFS. (merge or fabricate regions)
 * <li> Repair Region Consistency with .META. and assignments
 * </ol>
 * <p>
 * For table integrity repairs, the tables' region directories are scanned
 * for .regioninfo files.  Each table's integrity is then verified.  If there
 * are any orphan regions (regions with no .regioninfo files) or holes, new
 * regions are fabricated.  Backwards regions are sidelined as well as empty
 * degenerate (endkey==startkey) regions.  If there are any overlapping regions,
 * a new region is created and all data is merged into the new region.
 * <p>
 * Table integrity repairs deal solely with HDFS and could potentially be done
 * offline -- the hbase region servers or master do not need to be running.
 * This phase can eventually be used to completely reconstruct the META table in
 * an offline fashion.
 * <p>
 * Region consistency requires three conditions -- 1) valid .regioninfo file
 * present in an HDFS region dir,  2) valid row with .regioninfo data in META,
 * and 3) a region is deployed only at the regionserver that was assigned to
 * with proper state in the master.
 * <p>
 * Region consistency repairs require hbase to be online so that hbck can
 * contact the HBase master and region servers.  The hbck#connect() method must
 * first be called successfully.  Much of the region consistency information
 * is transient and less risky to repair.
 * <p>
 * If hbck is run from the command line, there are a handful of arguments that
 * can be used to limit the kinds of repairs hbck will do.  See the code in
 * {@link #printUsageAndExit()} for more details.
 */
public class HBaseFsck {
  public static final long DEFAULT_TIME_LAG = 60000; // default value of 1 minute
  public static final long DEFAULT_SLEEP_BEFORE_RERUN = 10000;
  private static final int MAX_NUM_THREADS = 50; // #threads to contact regions
  private static final long THREADS_KEEP_ALIVE_SECONDS = 60;
  private static boolean rsSupportsOffline = true;
  private static final int DEFAULT_MAX_MERGE = 5;

  /**********************
   * Internal resources
   **********************/
  private static final Log LOG = LogFactory.getLog(HBaseFsck.class.getName());
  private Configuration conf;
  private ClusterStatus status;
  private HConnection connection;
  private HBaseAdmin admin;
  private HTable meta;
  private ThreadPoolExecutor executor; // threads to retrieve data from regionservers
  private int numThreads = MAX_NUM_THREADS;
  private long startMillis = System.currentTimeMillis();

  /***********
   * Options
   ***********/
  private static boolean details = false; // do we display the full report
  private long timelag = DEFAULT_TIME_LAG; // tables whose modtime is older
  private boolean fixAssignments = false; // fix assignment errors?
  private boolean fixMeta = false; // fix meta errors?
  private boolean fixHdfsHoles = false; // fix fs holes?
  private boolean fixHdfsOverlaps = false; // fix fs overlaps (risky)
  private boolean fixHdfsOrphans = false; // fix fs holes (missing .regioninfo)

  // limit fixes to listed tables, if empty atttempt to fix all
  private List<byte[]> tablesToFix = new ArrayList<byte[]>();
  private int maxMerge = DEFAULT_MAX_MERGE; // maximum number of overlapping regions to merge

  private boolean rerun = false; // if we tried to fix something, rerun hbck
  private static boolean summary = false; // if we want to print less output
  private boolean checkMetaOnly = false;

  /*********
   * State
   *********/
  private ErrorReporter errors = new PrintingErrorReporter();
  int fixes = 0;

  /**
   * This map contains the state of all hbck items.  It maps from encoded region
   * name to HbckInfo structure.  The information contained in HbckInfo is used
   * to detect and correct consistency (hdfs/meta/deployment) problems.
   */
  private TreeMap<String, HbckInfo> regionInfoMap = new TreeMap<String, HbckInfo>();
  private TreeSet<byte[]> disabledTables =
    new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
  // Empty regioninfo qualifiers in .META.
  private Set<Result> emptyRegionInfoQualifiers = new HashSet<Result>();

  /**
   * This map from Tablename -> TableInfo contains the structures necessary to
   * detect table consistency problems (holes, dupes, overlaps).  It is sorted
   * to prevent dupes.
   */
  private TreeMap<String, TableInfo> tablesInfo = new TreeMap<String, TableInfo>();

  /**
   * When initially looking at HDFS, we attempt to find any orphaned data.
   */
  private List<HbckInfo> orphanHdfsDirs = new ArrayList<HbckInfo>();

  /**
   * Constructor
   *
   * @param conf Configuration object
   * @throws MasterNotRunningException if the master is not running
   * @throws ZooKeeperConnectionException if unable to connect to ZooKeeper
   */
  public HBaseFsck(Configuration conf) throws MasterNotRunningException,
      ZooKeeperConnectionException, IOException {
    this.conf = conf;

    int numThreads = conf.getInt("hbasefsck.numthreads", Integer.MAX_VALUE);
    executor = new ThreadPoolExecutor(1, numThreads,
        THREADS_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>());
    executor.allowCoreThreadTimeOut(true);
  }

  /**
   * To repair region consistency, one must call connect() in order to repair
   * online state.
   */
  public void connect() throws IOException {
    admin = new HBaseAdmin(conf);
    meta = new HTable(conf, HConstants.META_TABLE_NAME);
    status = admin.getMaster().getClusterStatus();
    connection = admin.getConnection();
  }

  /**
   * Get deployed regions according to the region servers.
   */
  private void loadDeployedRegions() throws IOException, InterruptedException {
    // From the master, get a list of all known live region servers
    Collection<ServerName> regionServers = status.getServers();
    errors.print("Number of live region servers: " + regionServers.size());
    if (details) {
      for (ServerName rsinfo: regionServers) {
        errors.print("  " + rsinfo.getServerName());
      }
    }

    // From the master, get a list of all dead region servers
    Collection<ServerName> deadRegionServers = status.getDeadServerNames();
    errors.print("Number of dead region servers: " + deadRegionServers.size());
    if (details) {
      for (ServerName name: deadRegionServers) {
        errors.print("  " + name);
      }
    }

    // Print the current master name and state
    errors.print("Master: " + status.getMaster());

    // Print the list of all backup masters
    Collection<ServerName> backupMasters = status.getBackupMasters();
    errors.print("Number of backup masters: " + backupMasters.size());
    if (details) {
      for (ServerName name: backupMasters) {
        errors.print("  " + name);
      }
    }

    // Determine what's deployed
    processRegionServers(regionServers);
  }

  /**
   * Clear the current state of hbck.
   */
  private void clearState() {
    // Make sure regionInfo is empty before starting
    fixes = 0;
    regionInfoMap.clear();
    emptyRegionInfoQualifiers.clear();
    disabledTables.clear();
    errors.clear();
    tablesInfo.clear();
    orphanHdfsDirs.clear();
  }

  /**
   * This repair method analyzes hbase data in hdfs and repairs it to satisfy
   * the table integrity rules.  HBase doesn't need to be online for this
   * operation to work.
   */
  public void offlineHdfsIntegrityRepair() throws IOException, InterruptedException {
    // Initial pass to fix orphans.
    if (shouldFixHdfsOrphans() || shouldFixHdfsHoles() || shouldFixHdfsOverlaps()) {
      LOG.info("Loading regioninfos HDFS");
      // if nothing is happening this should always complete in two iterations.
      int maxIterations = conf.getInt("hbase.hbck.integrityrepair.iterations.max", 3);
      int curIter = 0;
      do {
        clearState(); // clears hbck state and reset fixes to 0 and.
        // repair what's on HDFS
        restoreHdfsIntegrity();
        curIter++;// limit the number of iterations.
      } while (fixes > 0 && curIter <= maxIterations);

      // Repairs should be done in the first iteration and verification in the second.
      // If there are more than 2 passes, something funny has happened.
      if (curIter > 2) {
        if (curIter == maxIterations) {
          LOG.warn("Exiting integrity repairs after max " + curIter + " iterations. "
              + "Tables integrity may not be fully repaired!");
        } else {
          LOG.info("Successfully exiting integrity repairs after " + curIter + " iterations");
        }
      }
    }
  }

  /**
   * This repair method requires the cluster to be online since it contacts
   * region servers and the masters.  It makes each region's state in HDFS, in
   * .META., and deployments consistent.
   *
   * @return If > 0 , number of errors detected, if < 0 there was an unrecoverable
   * error.  If 0, we have a clean hbase.
   */
  public int onlineConsistencyRepair() throws IOException, KeeperException,
    InterruptedException {
    clearState();

    LOG.info("Loading regionsinfo from the .META. table");
    boolean success = loadMetaEntries();
    if (!success) return -1;

    // Check if .META. is found only once and in the right place
    if (!checkMetaRegion()) {
      // Will remove later if we can fix it
      errors.reportError("Encountered fatal error. Exiting...");
      return -2;
    }

    // get a list of all tables that have not changed recently.
    if (!checkMetaOnly) {
      reportTablesInFlux();
    }

    // get regions according to what is online on each RegionServer
    loadDeployedRegions();

    // load regiondirs and regioninfos from HDFS
    loadHdfsRegionDirs();
    loadHdfsRegionInfos();

    // Empty cells in .META.?
    reportEmptyMetaCells();

    // Get disabled tables from ZooKeeper
    loadDisabledTables();

    // Check and fix consistency
    checkAndFixConsistency();

    // Check integrity (does not fix)
    checkIntegrity();
    return errors.getErrorList().size();
  }

  /**
   * Contacts the master and prints out cluster-wide information
   * @return 0 on success, non-zero on failure
   */
  public int onlineHbck() throws IOException, KeeperException, InterruptedException {
    // print hbase server version
    errors.print("Version: " + status.getHBaseVersion());
    offlineHdfsIntegrityRepair();

    // turn the balancer off
    boolean oldBalancer = admin.balanceSwitch(false);

    onlineConsistencyRepair();

    admin.balanceSwitch(oldBalancer);

    // Print table summary
    printTableSummary(tablesInfo);
    return errors.summarize();
  }

  /**
   * Iterates through the list of all orphan/invalid regiondirs.
   */
  private void adoptHdfsOrphans(Collection<HbckInfo> orphanHdfsDirs) throws IOException {
    for (HbckInfo hi : orphanHdfsDirs) {
      LOG.info("Attempting to handle orphan hdfs dir: " + hi.getHdfsRegionDir());
      adoptHdfsOrphan(hi);
    }
  }

  /**
   * Orphaned regions are regions without a .regioninfo file in them.  We "adopt"
   * these orphans by creating a new region, and moving the column families,
   * recovered edits, HLogs, into the new region dir.  We determine the region
   * startkey and endkeys by looking at all of the hfiles inside the column
   * families to identify the min and max keys. The resulting region will
   * likely violate table integrity but will be dealt with by merging
   * overlapping regions.
   */
  private void adoptHdfsOrphan(HbckInfo hi) throws IOException {
    Path p = hi.getHdfsRegionDir();
    FileSystem fs = p.getFileSystem(conf);
    FileStatus[] dirs = fs.listStatus(p);

    String tableName = Bytes.toString(hi.getTableName());
    TableInfo tableInfo = tablesInfo.get(tableName);
    Preconditions.checkNotNull("Table " + tableName + "' not present!", tableInfo);
    HTableDescriptor template = tableInfo.getHTD();

    // find min and max key values
    Pair<byte[],byte[]> orphanRegionRange = null;
    for (FileStatus cf : dirs) {
      String cfName= cf.getPath().getName();
      // TODO Figure out what the special dirs are
      if (cfName.startsWith(".") || cfName.equals("splitlog")) continue;

      FileStatus[] hfiles = fs.listStatus(cf.getPath());
      for (FileStatus hfile : hfiles) {
        byte[] start, end;
        HFile.Reader hf = null;
        try {
          CacheConfig cacheConf = new CacheConfig(conf);
          hf = HFile.createReader(fs, hfile.getPath(), cacheConf);
          hf.loadFileInfo();
          KeyValue startKv = KeyValue.createKeyValueFromKey(hf.getFirstKey());
          start = startKv.getRow();
          KeyValue endKv = KeyValue.createKeyValueFromKey(hf.getLastKey());
          end = endKv.getRow();
        } catch (IOException ioe) {
          LOG.warn("Problem reading orphan file " + hfile + ", skipping");
          continue;
        } catch (NullPointerException ioe) {
          LOG.warn("Orphan file " + hfile + " is possibly corrupted HFile, skipping");
          continue;
        } finally {
          if (hf != null) {
            hf.close();
          }
        }

        // expand the range to include the range of all hfiles
        if (orphanRegionRange == null) {
          // first range
          orphanRegionRange = new Pair<byte[], byte[]>(start, end);
        } else {
          // TODO add test

          // expand range only if the hfile is wider.
          if (Bytes.compareTo(orphanRegionRange.getFirst(), start) > 0) {
            orphanRegionRange.setFirst(start);
          }
          if (Bytes.compareTo(orphanRegionRange.getSecond(), end) < 0 ) {
            orphanRegionRange.setSecond(end);
          }
        }
      }
    }
    if (orphanRegionRange == null) {
      LOG.warn("No data in dir " + p + ", sidelining data");
      fixes++;
      sidelineRegionDir(fs, hi);
      return;
    }
    LOG.info("Min max keys are : [" + Bytes.toString(orphanRegionRange.getFirst()) + ", " +
        Bytes.toString(orphanRegionRange.getSecond()) + ")");

    // create new region on hdfs.  move data into place.
    HRegionInfo hri = new HRegionInfo(template.getName(), orphanRegionRange.getFirst(), orphanRegionRange.getSecond());
    LOG.info("Creating new region : " + hri);
    HRegion region = HBaseFsckRepair.createHDFSRegionDir(conf, hri, template);
    Path target = region.getRegionDir();

    // rename all the data to new region
    mergeRegionDirs(target, hi);
    fixes++;
  }

  /**
   * This method determines if there are table integrity errors in HDFS.  If
   * there are errors and the appropriate "fix" options are enabled, the method
   * will first correct orphan regions making them into legit regiondirs, and
   * then reload to merge potentially overlapping regions.
   *
   * @return number of table integrity errors found
   */
  private int restoreHdfsIntegrity() throws IOException, InterruptedException {
    // Determine what's on HDFS
    LOG.info("Loading HBase regioninfo from HDFS...");
    loadHdfsRegionDirs(); // populating regioninfo table.

    int errs = errors.getErrorList().size();
    // First time just get suggestions.
    tablesInfo = loadHdfsRegionInfos(); // update tableInfos based on region info in fs.
    checkHdfsIntegrity(false, false);

    if (errors.getErrorList().size() == errs) {
      LOG.info("No integrity errors.  We are done with this phase. Glorious.");
      return 0;
    }

    if (shouldFixHdfsOrphans() && orphanHdfsDirs.size() > 0) {
      adoptHdfsOrphans(orphanHdfsDirs);
      // TODO optimize by incrementally adding instead of reloading.
    }

    // Make sure there are no holes now.
    if (shouldFixHdfsHoles()) {
      clearState(); // this also resets # fixes.
      loadHdfsRegionDirs();
      tablesInfo = loadHdfsRegionInfos(); // update tableInfos based on region info in fs.
      tablesInfo = checkHdfsIntegrity(shouldFixHdfsHoles(), false);
    }

    // Now we fix overlaps
    if (shouldFixHdfsOverlaps()) {
      // second pass we fix overlaps.
      clearState(); // this also resets # fixes.
      loadHdfsRegionDirs();
      tablesInfo = loadHdfsRegionInfos(); // update tableInfos based on region info in fs.
      tablesInfo = checkHdfsIntegrity(false, shouldFixHdfsOverlaps());
    }

    return errors.getErrorList().size();
  }

  /**
   * TODO -- need to add tests for this.
   */
  private void reportEmptyMetaCells() {
    errors.print("Number of empty REGIONINFO_QUALIFIER rows in .META.: " +
      emptyRegionInfoQualifiers.size());
    if (details) {
      for (Result r: emptyRegionInfoQualifiers) {
        errors.print("  " + r);
      }
    }
  }

  /**
   * TODO -- need to add tests for this.
   */
  private void reportTablesInFlux() {
    AtomicInteger numSkipped = new AtomicInteger(0);
    HTableDescriptor[] allTables = getTables(numSkipped);
    errors.print("Number of Tables: " + allTables.length);
    if (details) {
      if (numSkipped.get() > 0) {
        errors.detail("Number of Tables in flux: " + numSkipped.get());
      }
      for (HTableDescriptor td : allTables) {
        String tableName = td.getNameAsString();
        errors.detail("  Table: " + tableName + "\t" +
                           (td.isReadOnly() ? "ro" : "rw") + "\t" +
                           (td.isRootRegion() ? "ROOT" :
                            (td.isMetaRegion() ? "META" : "    ")) + "\t" +
                           " families: " + td.getFamilies().size());
      }
    }
  }

  public ErrorReporter getErrors() {
    return errors;
  }

  /**
   * Read the .regioninfo file from the file system.  If there is no
   * .regioninfo, add it to the orphan hdfs region list.
   */
  private void loadHdfsRegioninfo(HbckInfo hbi) throws IOException {
    Path regionDir = hbi.getHdfsRegionDir();
    if (regionDir == null) {
      LOG.warn("No HDFS region dir found: " + hbi + " meta=" + hbi.metaEntry);
      return;
    }
    Path regioninfo = new Path(regionDir, HRegion.REGIONINFO_FILE);
    FileSystem fs = regioninfo.getFileSystem(conf);

    FSDataInputStream in = fs.open(regioninfo);
    HRegionInfo hri = new HRegionInfo();
    hri.readFields(in);
    in.close();
    LOG.debug("HRegionInfo read: " + hri.toString());
    hbi.hdfsEntry.hri = hri;
  }

  /**
   * Exception thrown when a integrity repair operation fails in an
   * unresolvable way.
   */
  public static class RegionRepairException extends IOException {
    private static final long serialVersionUID = 1L;
    final IOException ioe;
    public RegionRepairException(String s, IOException ioe) {
      super(s);
      this.ioe = ioe;
    }
  }

  /**
   * Populate hbi's from regionInfos loaded from file system.
   */
  private TreeMap<String, TableInfo> loadHdfsRegionInfos() throws IOException {
    tablesInfo.clear(); // regenerating the data
    // generate region split structure
    for (HbckInfo hbi : regionInfoMap.values()) {

      // only load entries that haven't been loaded yet.
      if (hbi.getHdfsHRI() == null) {
        try {
          loadHdfsRegioninfo(hbi);
        } catch (IOException ioe) {
          String msg = "Orphan region in HDFS: Unable to load .regioninfo from table "
            + Bytes.toString(hbi.getTableName()) + " in hdfs dir "
            + hbi.getHdfsRegionDir()
            + "!  It may be an invalid format or version file.  Treating as "
            + "an orphaned regiondir.";
          errors.reportError(ERROR_CODE.ORPHAN_HDFS_REGION, msg);
          debugLsr(hbi.getHdfsRegionDir());
          orphanHdfsDirs.add(hbi);
          continue;
        }
      }

      // get table name from hdfs, populate various HBaseFsck tables.
      String tableName = Bytes.toString(hbi.getTableName());
      if (tableName == null) {
        // There was an entry in META not in the HDFS?
        LOG.warn("tableName was null for: " + hbi);
        continue;
      }

      TableInfo modTInfo = tablesInfo.get(tableName);
      if (modTInfo == null) {
        // only executed once per table.
        modTInfo = new TableInfo(tableName);
        Path hbaseRoot = new Path(conf.get(HConstants.HBASE_DIR));
        HTableDescriptor htd =
          FSTableDescriptors.getTableDescriptor(hbaseRoot.getFileSystem(conf),
              hbaseRoot, tableName);
        modTInfo.htds.add(htd);
      }
      modTInfo.addRegionInfo(hbi);
      tablesInfo.put(tableName, modTInfo);
    }

    return tablesInfo;
  }

  /**
   * This borrows code from MasterFileSystem.bootstrap()
   * 
   * @return an open .META. HRegion
   */
  private HRegion createNewRootAndMeta() throws IOException {
    Path rootdir = new Path(conf.get(HConstants.HBASE_DIR));
    Configuration c = conf;
    HRegionInfo rootHRI = new HRegionInfo(HRegionInfo.ROOT_REGIONINFO);
    MasterFileSystem.setInfoFamilyCachingForRoot(false);
    HRegionInfo metaHRI = new HRegionInfo(HRegionInfo.FIRST_META_REGIONINFO);
    MasterFileSystem.setInfoFamilyCachingForMeta(false);
    HRegion root = HRegion.createHRegion(rootHRI, rootdir, c,
        HTableDescriptor.ROOT_TABLEDESC);
    HRegion meta = HRegion.createHRegion(metaHRI, rootdir, c,
        HTableDescriptor.META_TABLEDESC);
    MasterFileSystem.setInfoFamilyCachingForRoot(true);
    MasterFileSystem.setInfoFamilyCachingForMeta(true);

    // Add first region from the META table to the ROOT region.
    HRegion.addRegionToMETA(root, meta);
    root.close();
    root.getLog().closeAndDelete();
    return meta;
  }

  /**
   * Generate set of puts to add to new meta.  This expects the tables to be 
   * clean with no overlaps or holes.  If there are any problems it returns null.
   * 
   * @return An array list of puts to do in bulk, null if tables have problems
   */
  private ArrayList<Put> generatePuts(TreeMap<String, TableInfo> tablesInfo) throws IOException {
    ArrayList<Put> puts = new ArrayList<Put>();
    boolean hasProblems = false;
    for (Entry<String, TableInfo> e : tablesInfo.entrySet()) {
      String name = e.getKey();

      // skip "-ROOT-" and ".META."
      if (Bytes.compareTo(Bytes.toBytes(name), HConstants.ROOT_TABLE_NAME) == 0
          || Bytes.compareTo(Bytes.toBytes(name), HConstants.META_TABLE_NAME) == 0) {
        continue;
      }

      TableInfo ti = e.getValue();
      for (Entry<byte[], Collection<HbckInfo>> spl : ti.sc.getStarts().asMap()
          .entrySet()) {
        Collection<HbckInfo> his = spl.getValue();
        int sz = his.size();
        if (sz != 1) {
          // problem
          LOG.error("Split starting at " + Bytes.toStringBinary(spl.getKey())
              + " had " +  sz + " regions instead of exactly 1." );
          hasProblems = true;
          continue;
        }

        // add the row directly to meta.
        HbckInfo hi = his.iterator().next();
        HRegionInfo hri = hi.getHdfsHRI(); // hi.metaEntry;
        Put p = new Put(hri.getRegionName());
        p.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
            Writables.getBytes(hri));
        puts.add(p);
      }
    }
    return hasProblems ? null : puts;
  }

  /**
   * Suggest fixes for each table
   */
  private void suggestFixes(TreeMap<String, TableInfo> tablesInfo) throws IOException {
    for (TableInfo tInfo : tablesInfo.values()) {
      TableIntegrityErrorHandler handler = tInfo.new IntegrityFixSuggester(tInfo, errors);
      tInfo.checkRegionChain(handler);
    }
  }

  /**
   * Rebuilds meta from information in hdfs/fs.  Depends on configuration
   * settings passed into hbck constructor to point to a particular fs/dir.
   * 
   * @param fix flag that determines if method should attempt to fix holes
   * @return true if successful, false if attempt failed.
   */
  public boolean rebuildMeta(boolean fix) throws IOException,
      InterruptedException {

    // TODO check to make sure hbase is offline. (or at least the table
    // currently being worked on is off line)

    // Determine what's on HDFS
    LOG.info("Loading HBase regioninfo from HDFS...");
    loadHdfsRegionDirs(); // populating regioninfo table.

    int errs = errors.getErrorList().size();
    tablesInfo = loadHdfsRegionInfos(); // update tableInfos based on region info in fs.
    checkHdfsIntegrity(false, false);

    // make sure ok.
    if (errors.getErrorList().size() != errs) {
      // While in error state, iterate until no more fixes possible
      while(true) {
        fixes = 0;
        suggestFixes(tablesInfo);
        errors.clear();
        loadHdfsRegionInfos(); // update tableInfos based on region info in fs.
        checkHdfsIntegrity(shouldFixHdfsHoles(), shouldFixHdfsOverlaps());

        int errCount = errors.getErrorList().size();

        if (fixes == 0) {
          if (errCount > 0) {
            return false; // failed to fix problems.
          } else {
            break; // no fixes and no problems? drop out and fix stuff!
          }
        }
      }
    }

    // we can rebuild, move old root and meta out of the way and start
    LOG.info("HDFS regioninfo's seems good.  Sidelining old .META.");
    sidelineOldRootAndMeta();

    LOG.info("Creating new .META.");
    HRegion meta = createNewRootAndMeta();

    // populate meta
    List<Put> puts = generatePuts(tablesInfo);
    if (puts == null) {
      LOG.fatal("Problem encountered when creating new .META. entries.  " +
        "You may need to restore the previously sidelined -ROOT- and .META.");
      return false;
    }
    meta.put(puts.toArray(new Put[0]));
    meta.close();
    meta.getLog().closeAndDelete();
    LOG.info("Success! .META. table rebuilt.");
    return true;
  }

  private TreeMap<String, TableInfo> checkHdfsIntegrity(boolean fixHoles,
      boolean fixOverlaps) throws IOException {
    LOG.info("Checking HBase region split map from HDFS data...");
    for (TableInfo tInfo : tablesInfo.values()) {
      TableIntegrityErrorHandler handler;
      if (fixHoles || fixOverlaps) {
        if (shouldFixTable(Bytes.toBytes(tInfo.getName()))) {
          handler = tInfo.new HDFSIntegrityFixer(tInfo, errors, conf,
              fixHoles, fixOverlaps);
        } else {
          LOG.info("Table " + tInfo.getName() + " is not in the include table " +
            "list.  Just suggesting fixes.");
          handler = tInfo.new IntegrityFixSuggester(tInfo, errors);
        }
      } else {
        handler = tInfo.new IntegrityFixSuggester(tInfo, errors);
      }
      if (!tInfo.checkRegionChain(handler)) {
        // should dump info as well.
        errors.report("Found inconsistency in table " + tInfo.getName());
      }
    }
    return tablesInfo;
  }

  private Path getSidelineDir() throws IOException {
    Path hbaseDir = FSUtils.getRootDir(conf);
    Path backupDir = new Path(hbaseDir.getParent(), hbaseDir.getName() + "-"
        + startMillis);
    return backupDir;
  }

  /**
   * Sideline a region dir (instead of deleting it)
   */
  void sidelineRegionDir(FileSystem fs, HbckInfo hi)
    throws IOException {
    String tableName = Bytes.toString(hi.getTableName());
    Path regionDir = hi.getHdfsRegionDir();

    if (!fs.exists(regionDir)) {
      LOG.warn("No previous " + regionDir + " exists.  Continuing.");
      return;
    }

    Path sidelineTableDir= new Path(getSidelineDir(), tableName);
    Path sidelineRegionDir = new Path(sidelineTableDir, regionDir.getName());
    fs.mkdirs(sidelineRegionDir);
    boolean success = false;
    FileStatus[] cfs =  fs.listStatus(regionDir);
    if (cfs == null) {
      LOG.info("Region dir is empty: " + regionDir);
    } else {
      for (FileStatus cf : cfs) {
        Path src = cf.getPath();
        Path dst =  new Path(sidelineRegionDir, src.getName());
        if (fs.isFile(src)) {
          // simple file
          success = fs.rename(src, dst);
          if (!success) {
            String msg = "Unable to rename file " + src +  " to " + dst;
            LOG.error(msg);
            throw new IOException(msg);
          }
          continue;
        }

        // is a directory.
        fs.mkdirs(dst);

        LOG.info("Sidelining files from " + src + " into containing region " + dst);
        // FileSystem.rename is inconsistent with directories -- if the
        // dst (foo/a) exists and is a dir, and the src (foo/b) is a dir,
        // it moves the src into the dst dir resulting in (foo/a/b).  If
        // the dst does not exist, and the src a dir, src becomes dst. (foo/b)
        for (FileStatus hfile : fs.listStatus(src)) {
          success = fs.rename(hfile.getPath(), dst);
          if (!success) {
            String msg = "Unable to rename file " + src +  " to " + dst;
            LOG.error(msg);
            throw new IOException(msg);
          }
        }
        LOG.debug("Sideline directory contents:");
        debugLsr(sidelineRegionDir);
      }
    }

    LOG.info("Removing old region dir: " + regionDir);
    success = fs.delete(regionDir, true);
    if (!success) {
      String msg = "Unable to delete dir " + regionDir;
      LOG.error(msg);
      throw new IOException(msg);
    }
  }

  /**
   * Side line an entire table.
   */
  void sidelineTable(FileSystem fs, byte[] table, Path hbaseDir,
      Path backupHbaseDir) throws IOException {
    String tableName = Bytes.toString(table);
    Path tableDir = new Path(hbaseDir, tableName);
    if (fs.exists(tableDir)) {
      Path backupTableDir= new Path(backupHbaseDir, tableName);
      boolean success = fs.rename(tableDir, backupTableDir);
      if (!success) {
        throw new IOException("Failed to move  " + tableName + " from " 
            +  tableDir.getName() + " to " + backupTableDir.getName());
      }
    } else {
      LOG.info("No previous " + tableName +  " exists.  Continuing.");
    }
  }

  /**
   * @return Path to backup of original directory
   */
  Path sidelineOldRootAndMeta() throws IOException {
    // put current -ROOT- and .META. aside.
    Path hbaseDir = new Path(conf.get(HConstants.HBASE_DIR));
    FileSystem fs = hbaseDir.getFileSystem(conf);
    Path backupDir = new Path(hbaseDir.getParent(), hbaseDir.getName() + "-"
        + startMillis);
    fs.mkdirs(backupDir);

    sidelineTable(fs, HConstants.ROOT_TABLE_NAME, hbaseDir, backupDir);
    try {
      sidelineTable(fs, HConstants.META_TABLE_NAME, hbaseDir, backupDir);
    } catch (IOException e) {
      LOG.error("Attempt to sideline meta failed, attempt to revert...", e);
      try {
        // move it back.
        sidelineTable(fs, HConstants.ROOT_TABLE_NAME, backupDir, hbaseDir);
        LOG.warn("... revert succeed.  -ROOT- and .META. still in "
            + "original state.");
      } catch (IOException ioe) {
        LOG.fatal("... failed to sideline root and meta and failed to restore "
            + "prevoius state.  Currently in inconsistent state.  To restore "
            + "try to rename -ROOT- in " + backupDir.getName() + " to " 
            + hbaseDir.getName() + ".", ioe);
      }
      throw e; // throw original exception
    }
    return backupDir;
  }

  /**
   * Load the list of disabled tables in ZK into local set.
   * @throws ZooKeeperConnectionException
   * @throws IOException
   */
  private void loadDisabledTables()
  throws ZooKeeperConnectionException, IOException {
    HConnectionManager.execute(new HConnectable<Void>(conf) {
      @Override
      public Void connect(HConnection connection) throws IOException {
        ZooKeeperWatcher zkw = connection.getZooKeeperWatcher();
        try {
          for (String tableName : ZKTable.getDisabledOrDisablingTables(zkw)) {
            disabledTables.add(Bytes.toBytes(tableName));
          }
        } catch (KeeperException ke) {
          throw new IOException(ke);
        }
        return null;
      }
    });
  }

  /**
   * Check if the specified region's table is disabled.
   */
  private boolean isTableDisabled(HRegionInfo regionInfo) {
    return disabledTables.contains(regionInfo.getTableName());
  }

  /**
   * Scan HDFS for all regions, recording their information into
   * regionInfoMap
   */
  public void loadHdfsRegionDirs() throws IOException, InterruptedException {
    Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
    FileSystem fs = rootDir.getFileSystem(conf);

    // list all tables from HDFS
    List<FileStatus> tableDirs = Lists.newArrayList();

    boolean foundVersionFile = false;
    FileStatus[] files = fs.listStatus(rootDir);
    for (FileStatus file : files) {
      String dirName = file.getPath().getName();
      if (dirName.equals(HConstants.VERSION_FILE_NAME)) {
        foundVersionFile = true;
      } else {
        if (!checkMetaOnly ||
            dirName.equals("-ROOT-") ||
            dirName.equals(".META.")) {
          tableDirs.add(file);
        }
      }
    }

    // verify that version file exists
    if (!foundVersionFile) {
      errors.reportError(ERROR_CODE.NO_VERSION_FILE,
          "Version file does not exist in root dir " + rootDir);
    }

    // level 1:  <HBASE_DIR>/*
    WorkItemHdfsDir[] dirs = new WorkItemHdfsDir[tableDirs.size()];
    int num = 0;
    for (FileStatus tableDir : tableDirs) {
      LOG.debug("Loading region dirs from " +tableDir.getPath());
      dirs[num] = new WorkItemHdfsDir(this, fs, errors, tableDir);
      executor.execute(dirs[num]);
      num++;
    }

    // wait for all directories to be done
    for (int i = 0; i < num; i++) {
      WorkItemHdfsDir dir = dirs[i];
      synchronized (dir) {
        while (!dir.isDone()) {
          dir.wait();
        }
      }
    }
  }

  /**
   * Record the location of the ROOT region as found in ZooKeeper,
   * as if it were in a META table. This is so that we can check
   * deployment of ROOT.
   */
  private boolean recordRootRegion() throws IOException {
    HRegionLocation rootLocation = connection.locateRegion(
      HConstants.ROOT_TABLE_NAME, HConstants.EMPTY_START_ROW);

    // Check if Root region is valid and existing
    if (rootLocation == null || rootLocation.getRegionInfo() == null ||
        rootLocation.getHostname() == null) {
      errors.reportError(ERROR_CODE.NULL_ROOT_REGION,
        "Root Region or some of its attributes are null.");
      return false;
    }
    ServerName sn;
    try {
      sn = getRootRegionServerName();
    } catch (InterruptedException e) {
      throw new IOException("Interrupted", e);
    }
    MetaEntry m =
      new MetaEntry(rootLocation.getRegionInfo(), sn, System.currentTimeMillis());
    HbckInfo hbInfo = new HbckInfo(m);
    regionInfoMap.put(rootLocation.getRegionInfo().getEncodedName(), hbInfo);
    return true;
  }

  private ServerName getRootRegionServerName()
  throws IOException, InterruptedException {
    RootRegionTracker rootRegionTracker =
      new RootRegionTracker(this.connection.getZooKeeperWatcher(), new Abortable() {
        @Override
        public void abort(String why, Throwable e) {
          LOG.error(why, e);
          System.exit(1);
        }
        @Override
        public boolean isAborted(){
          return false;
        }
        
      });
    rootRegionTracker.start();
    ServerName sn = null;
    try {
      sn = rootRegionTracker.getRootRegionLocation();
    } finally {
      rootRegionTracker.stop();
    }
    return sn;
  }

  /**
   * Contacts each regionserver and fetches metadata about regions.
   * @param regionServerList - the list of region servers to connect to
   * @throws IOException if a remote or network exception occurs
   */
  void processRegionServers(Collection<ServerName> regionServerList)
    throws IOException, InterruptedException {

    WorkItemRegion[] work = new WorkItemRegion[regionServerList.size()];
    int num = 0;

    // loop to contact each region server in parallel
    for (ServerName rsinfo: regionServerList) {
      work[num] = new WorkItemRegion(this, rsinfo, errors, connection);
      executor.execute(work[num]);
      num++;
    }
    
    // wait for all submitted tasks to be done
    for (int i = 0; i < num; i++) {
      synchronized (work[i]) {
        while (!work[i].isDone()) {
          work[i].wait();
        }
      }
    }
  }

  /**
   * Check consistency of all regions that have been found in previous phases.
   */
  private void checkAndFixConsistency()
  throws IOException, KeeperException, InterruptedException {
    for (java.util.Map.Entry<String, HbckInfo> e: regionInfoMap.entrySet()) {
      checkRegionConsistency(e.getKey(), e.getValue());
    }
  }

  /**
   * Deletes region from meta table
   */
  private void deleteMetaRegion(HbckInfo hi) throws IOException {
    Delete d = new Delete(hi.metaEntry.getRegionName());
    meta.delete(d);
    meta.flushCommits();
    LOG.info("Deleted " + hi.metaEntry.getRegionNameAsString() + " from META" );
  }

  /**
   * This backwards-compatibility wrapper for permanently offlining a region
   * that should not be alive.  If the region server does not support the
   * "offline" method, it will use the closest unassign method instead.  This
   * will basically work until one attempts to disable or delete the affected
   * table.  The problem has to do with in-memory only master state, so
   * restarting the HMaster or failing over to another should fix this.
   */
  private void offline(byte[] regionName) throws IOException {
    String regionString = Bytes.toStringBinary(regionName);
    if (!rsSupportsOffline) {
      LOG.warn("Using unassign region " + regionString
          + " instead of using offline method, you should"
          + " restart HMaster after these repairs");
      admin.unassign(regionName, true);
      return;
    }

    // first time we assume the rs's supports #offline.
    try {
      LOG.info("Offlining region " + regionString);
      admin.getMaster().offline(regionName);
    } catch (IOException ioe) {
      String notFoundMsg = "java.lang.NoSuchMethodException: " +
        "org.apache.hadoop.hbase.master.HMaster.offline([B)";
      if (ioe.getMessage().contains(notFoundMsg)) {
        LOG.warn("Using unassign region " + regionString
            + " instead of using offline method, you should"
            + " restart HMaster after these repairs");
        rsSupportsOffline = false; // in the future just use unassign
        admin.unassign(regionName, true);
        return;
      }
      throw ioe;
    }
  }

  private void undeployRegions(HbckInfo hi) throws IOException, InterruptedException {
    for (OnlineEntry rse : hi.deployedEntries) {
      LOG.debug("Undeploy region "  + rse.hri + " from " + rse.hsa);
      try {
        HBaseFsckRepair.closeRegionSilentlyAndWait(admin, rse.hsa, rse.hri);
        offline(rse.hri.getRegionName());
      } catch (IOException ioe) {
        LOG.warn("Got exception when attempting to offline region "
            + Bytes.toString(rse.hri.getRegionName()), ioe);
      }
    }
  }

  /**
   * Attempts to undeploy a region from a region server based in information in
   * META.  Any operations that modify the file system should make sure that
   * its corresponding region is not deployed to prevent data races.
   *
   * A separate call is required to update the master in-memory region state
   * kept in the AssignementManager.  Because disable uses this state instead of
   * that found in META, we can't seem to cleanly disable/delete tables that
   * have been hbck fixed.  When used on a version of HBase that does not have
   * the offline ipc call exposed on the master (<0.90.5, <0.92.0) a master
   * restart or failover may be required.
   */
  private void closeRegion(HbckInfo hi) throws IOException, InterruptedException {
    if (hi.metaEntry == null && hi.hdfsEntry == null) {
      undeployRegions(hi);
      return;
    }

    // get assignment info and hregioninfo from meta.
    Get get = new Get(hi.getRegionName());
    get.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    get.addColumn(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
    get.addColumn(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER);
    Result r = meta.get(get);
    byte[] value = r.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
    byte[] startcodeBytes = r.getValue(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER);
    if (value == null || startcodeBytes == null) {
      errors.reportError("Unable to close region "
          + hi.getRegionNameAsString() +  " because meta does not "
          + "have handle to reach it.");
      return;
    }
    long startcode = Bytes.toLong(startcodeBytes);

    ServerName hsa = new ServerName(Bytes.toString(value), startcode);
    byte[] hriVal = r.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    HRegionInfo hri= Writables.getHRegionInfoOrNull(hriVal);
    if (hri == null) {
      LOG.warn("Unable to close region " + hi.getRegionNameAsString()
          + " because META had invalid or missing "
          + HConstants.CATALOG_FAMILY_STR + ":"
          + Bytes.toString(HConstants.REGIONINFO_QUALIFIER)
          + " qualifier value.");
      return;
    }

    // close the region -- close files and remove assignment
    HBaseFsckRepair.closeRegionSilentlyAndWait(admin, hsa, hri);
  }

  private void tryAssignmentRepair(HbckInfo hbi, String msg) throws IOException,
    KeeperException, InterruptedException {
    // If we are trying to fix the errors
    if (shouldFixAssignments()) {
      errors.print(msg);
      undeployRegions(hbi);
      setShouldRerun();
      HBaseFsckRepair.fixUnassigned(admin, hbi.getHdfsHRI());
      HBaseFsckRepair.waitUntilAssigned(admin, hbi.getHdfsHRI());
    }
  }

  /**
   * Check a single region for consistency and correct deployment.
   */
  private void checkRegionConsistency(final String key, final HbckInfo hbi)
  throws IOException, KeeperException, InterruptedException {
    String descriptiveName = hbi.toString();

    boolean inMeta = hbi.metaEntry != null;
    boolean inHdfs = hbi.getHdfsRegionDir()!= null;
    boolean hasMetaAssignment = inMeta && hbi.metaEntry.regionServer != null;
    boolean isDeployed = !hbi.deployedOn.isEmpty();
    boolean isMultiplyDeployed = hbi.deployedOn.size() > 1;
    boolean deploymentMatchesMeta =
      hasMetaAssignment && isDeployed && !isMultiplyDeployed &&
      hbi.metaEntry.regionServer.equals(hbi.deployedOn.get(0));
    boolean splitParent =
      (hbi.metaEntry == null)? false: hbi.metaEntry.isSplit() && hbi.metaEntry.isOffline();
    boolean shouldBeDeployed = inMeta && !isTableDisabled(hbi.metaEntry);
    boolean recentlyModified = hbi.getHdfsRegionDir() != null &&
      hbi.getModTime() + timelag > System.currentTimeMillis();

    // ========== First the healthy cases =============
    if (hbi.containsOnlyHdfsEdits()) {
      return;
    }
    if (inMeta && inHdfs && isDeployed && deploymentMatchesMeta && shouldBeDeployed) {
      return;
    } else if (inMeta && inHdfs && !isDeployed && splitParent) {
      LOG.warn("Region " + descriptiveName + " is a split parent in META and in HDFS");
      return;
    } else if (inMeta && inHdfs && !shouldBeDeployed && !isDeployed) {
      LOG.info("Region " + descriptiveName + " is in META, and in a disabled " +
        "tabled that is not deployed");
      return;
    } else if (recentlyModified) {
      LOG.warn("Region " + descriptiveName + " was recently modified -- skipping");
      return;
    }
    // ========== Cases where the region is not in META =============
    else if (!inMeta && !inHdfs && !isDeployed) {
      // We shouldn't have record of this region at all then!
      assert false : "Entry for region with no data";
    } else if (!inMeta && !inHdfs && isDeployed) {
      errors.reportError(ERROR_CODE.NOT_IN_META_HDFS, "Region "
          + descriptiveName + ", key=" + key + ", not on HDFS or in META but " +
          "deployed on " + Joiner.on(", ").join(hbi.deployedOn));
      if (shouldFixAssignments()) {
        undeployRegions(hbi);
      }

    } else if (!inMeta && inHdfs && !isDeployed) {
      errors.reportError(ERROR_CODE.NOT_IN_META_OR_DEPLOYED, "Region "
          + descriptiveName + " on HDFS, but not listed in META " +
          "or deployed on any region server");
      // restore region consistency of an adopted orphan
      if (shouldFixMeta()) {
        if (!hbi.isHdfsRegioninfoPresent()) {
          LOG.error("Region " + hbi.getHdfsHRI() + " could have been repaired"
              +  " in table integrity repair phase if -fixHdfsOrphans was" +
              " used.");
          return;
        }

        LOG.info("Patching .META. with .regioninfo: " + hbi.getHdfsHRI());
        HBaseFsckRepair.fixMetaHoleOnline(conf, hbi.getHdfsHRI());

        tryAssignmentRepair(hbi, "Trying to reassign region...");
      }

    } else if (!inMeta && inHdfs && isDeployed) {
      errors.reportError(ERROR_CODE.NOT_IN_META, "Region " + descriptiveName
          + " not in META, but deployed on " + Joiner.on(", ").join(hbi.deployedOn));
      debugLsr(hbi.getHdfsRegionDir());
      if (shouldFixMeta()) {
        if (!hbi.isHdfsRegioninfoPresent()) {
          LOG.error("This should have been repaired in table integrity repair phase");
          return;
        }

        LOG.info("Patching .META. with with .regioninfo: " + hbi.getHdfsHRI());
        HBaseFsckRepair.fixMetaHoleOnline(conf, hbi.getHdfsHRI());

        tryAssignmentRepair(hbi, "Trying to fix unassigned region...");
      }

    // ========== Cases where the region is in META =============
    } else if (inMeta && !inHdfs && !isDeployed) {
      errors.reportError(ERROR_CODE.NOT_IN_HDFS_OR_DEPLOYED, "Region "
          + descriptiveName + " found in META, but not in HDFS "
          + "or deployed on any region server.");
      if (shouldFixMeta()) {
        deleteMetaRegion(hbi);
      }
    } else if (inMeta && !inHdfs && isDeployed) {
      errors.reportError(ERROR_CODE.NOT_IN_HDFS, "Region " + descriptiveName
          + " found in META, but not in HDFS, " +
          "and deployed on " + Joiner.on(", ").join(hbi.deployedOn));
      // We treat HDFS as ground truth.  Any information in meta is transient
      // and equivalent data can be regenerated.  So, lets unassign and remove
      // these problems from META.
      if (shouldFixAssignments()) {
        errors.print("Trying to fix unassigned region...");
        closeRegion(hbi);// Close region will cause RS to abort.
      }
      if (shouldFixMeta()) {
        // wait for it to complete
        deleteMetaRegion(hbi);
      }
    } else if (inMeta && inHdfs && !isDeployed && shouldBeDeployed) {
      errors.reportError(ERROR_CODE.NOT_DEPLOYED, "Region " + descriptiveName
          + " not deployed on any region server.");
      tryAssignmentRepair(hbi, "Trying to fix unassigned region...");
    } else if (inMeta && inHdfs && isDeployed && !shouldBeDeployed) {
      errors.reportError(ERROR_CODE.SHOULD_NOT_BE_DEPLOYED, "UNHANDLED CASE:" +
          " Region " + descriptiveName + " should not be deployed according " +
          "to META, but is deployed on " + Joiner.on(", ").join(hbi.deployedOn));
      // TODO test and handle this case.
    } else if (inMeta && inHdfs && isMultiplyDeployed) {
      errors.reportError(ERROR_CODE.MULTI_DEPLOYED, "Region " + descriptiveName
          + " is listed in META on region server " + hbi.metaEntry.regionServer
          + " but is multiply assigned to region servers " +
          Joiner.on(", ").join(hbi.deployedOn));
      // If we are trying to fix the errors
      if (shouldFixAssignments()) {
        errors.print("Trying to fix assignment error...");
        setShouldRerun();
        HBaseFsckRepair.fixMultiAssignment(admin, hbi.metaEntry, hbi.deployedOn);
      }
    } else if (inMeta && inHdfs && isDeployed && !deploymentMatchesMeta) {
      errors.reportError(ERROR_CODE.SERVER_DOES_NOT_MATCH_META, "Region "
          + descriptiveName + " listed in META on region server " +
          hbi.metaEntry.regionServer + " but found on region server " +
          hbi.deployedOn.get(0));
      // If we are trying to fix the errors
      if (shouldFixAssignments()) {
        errors.print("Trying to fix assignment error...");
        setShouldRerun();
        HBaseFsckRepair.fixMultiAssignment(admin, hbi.metaEntry, hbi.deployedOn);
        HBaseFsckRepair.waitUntilAssigned(admin, hbi.getHdfsHRI());
      }
    } else {
      errors.reportError(ERROR_CODE.UNKNOWN, "Region " + descriptiveName +
          " is in an unforeseen state:" +
          " inMeta=" + inMeta +
          " inHdfs=" + inHdfs +
          " isDeployed=" + isDeployed +
          " isMultiplyDeployed=" + isMultiplyDeployed +
          " deploymentMatchesMeta=" + deploymentMatchesMeta +
          " shouldBeDeployed=" + shouldBeDeployed);
    }
  }

  /**
   * Checks tables integrity. Goes over all regions and scans the tables.
   * Collects all the pieces for each table and checks if there are missing,
   * repeated or overlapping ones.
   * @throws IOException
   */
  TreeMap<String, TableInfo> checkIntegrity() throws IOException {
    tablesInfo = new TreeMap<String,TableInfo> ();
    List<HbckInfo> noHDFSRegionInfos = new ArrayList<HbckInfo>();
    LOG.debug("There are " + regionInfoMap.size() + " region info entries");
    for (HbckInfo hbi : regionInfoMap.values()) {
      // Check only valid, working regions
      if (hbi.metaEntry == null) {
        // this assumes that consistency check has run loadMetaEntry
        noHDFSRegionInfos.add(hbi);
        Path p = hbi.getHdfsRegionDir();
        if (p == null) {
          errors.report("No regioninfo in Meta or HDFS. " + hbi);
        }

        // TODO test.
        continue;
      }
      if (hbi.metaEntry.regionServer == null) {
        errors.detail("Skipping region because no region server: " + hbi);
        continue;
      }
      if (hbi.metaEntry.isOffline()) {
        errors.detail("Skipping region because it is offline: " + hbi);
        continue;
      }
      if (hbi.containsOnlyHdfsEdits()) {
        errors.detail("Skipping region because it only contains edits" + hbi);
        continue;
      }

      // Missing regionDir or over-deployment is checked elsewhere. Include
      // these cases in modTInfo, so we can evaluate those regions as part of
      // the region chain in META
      //if (hbi.foundRegionDir == null) continue;
      //if (hbi.deployedOn.size() != 1) continue;
      if (hbi.deployedOn.size() == 0) continue;

      // We should be safe here
      String tableName = hbi.metaEntry.getTableNameAsString();
      TableInfo modTInfo = tablesInfo.get(tableName);
      if (modTInfo == null) {
        modTInfo = new TableInfo(tableName);
      }
      for (ServerName server : hbi.deployedOn) {
        modTInfo.addServer(server);
      }

      modTInfo.addRegionInfo(hbi);

      tablesInfo.put(tableName, modTInfo);
    }

    for (TableInfo tInfo : tablesInfo.values()) {
      TableIntegrityErrorHandler handler = tInfo.new IntegrityFixSuggester(tInfo, errors);
      if (!tInfo.checkRegionChain(handler)) {
        errors.report("Found inconsistency in table " + tInfo.getName());
      }
    }
    return tablesInfo;
  }

  /**
   * Merge hdfs data by moving from contained HbckInfo into targetRegionDir.
   * @return number of file move fixes done to merge regions.
   */
  public int mergeRegionDirs(Path targetRegionDir, HbckInfo contained) throws IOException {
    int fileMoves = 0;

    LOG.debug("Contained region dir after close and pause");
    debugLsr(contained.getHdfsRegionDir());

    // rename the contained into the container.
    FileSystem fs = targetRegionDir.getFileSystem(conf);
    FileStatus[] dirs = fs.listStatus(contained.getHdfsRegionDir());

    if (dirs == null) {
      if (!fs.exists(contained.getHdfsRegionDir())) {
        LOG.warn("HDFS region dir " + contained.getHdfsRegionDir() + " already sidelined.");
      } else {
        sidelineRegionDir(fs, contained);
      }
      return fileMoves;
    }

    for (FileStatus cf : dirs) {
      Path src = cf.getPath();
      Path dst =  new Path(targetRegionDir, src.getName());

      if (src.getName().equals(HRegion.REGIONINFO_FILE)) {
        // do not copy the old .regioninfo file.
        continue;
      }

      if (src.getName().equals(HConstants.HREGION_OLDLOGDIR_NAME)) {
        // do not copy the .oldlogs files
        continue;
      }

      LOG.info("Moving files from " + src + " into containing region " + dst);
      // FileSystem.rename is inconsistent with directories -- if the
      // dst (foo/a) exists and is a dir, and the src (foo/b) is a dir,
      // it moves the src into the dst dir resulting in (foo/a/b).  If
      // the dst does not exist, and the src a dir, src becomes dst. (foo/b)
      for (FileStatus hfile : fs.listStatus(src)) {
        boolean success = fs.rename(hfile.getPath(), dst);
        if (success) {
          fileMoves++;
        }
      }
      LOG.debug("Sideline directory contents:");
      debugLsr(targetRegionDir);
    }

    // if all success.
    sidelineRegionDir(fs, contained);
    LOG.info("Sidelined region dir "+ contained.getHdfsRegionDir() + " into " +
        getSidelineDir());
    debugLsr(contained.getHdfsRegionDir());

    return fileMoves;
  }

  /**
   * Maintain information about a particular table.
   */
  public class TableInfo {
    String tableName;
    TreeSet <ServerName> deployedOn;

    // backwards regions
    final List<HbckInfo> backwards = new ArrayList<HbckInfo>();

    // region split calculator
    final RegionSplitCalculator<HbckInfo> sc = new RegionSplitCalculator<HbckInfo>(cmp);

    // Histogram of different HTableDescriptors found.  Ideally there is only one!
    final Set<HTableDescriptor> htds = new HashSet<HTableDescriptor>();

    // key = start split, values = set of splits in problem group
    final Multimap<byte[], HbckInfo> overlapGroups =
      TreeMultimap.create(RegionSplitCalculator.BYTES_COMPARATOR, cmp);

    TableInfo(String name) {
      this.tableName = name;
      deployedOn = new TreeSet <ServerName>();
    }

    /**
     * @return descriptor common to all regions.  null if are none or multiple!
     */
    private HTableDescriptor getHTD() {
      if (htds.size() == 1) {
        return (HTableDescriptor)htds.toArray()[0];
      }
      return null;
    }

    public void addRegionInfo(HbckInfo hir) {
      if (Bytes.equals(hir.getEndKey(), HConstants.EMPTY_END_ROW)) {
        // end key is absolute end key, just add it.
        sc.add(hir);
        return;
      }

      // if not the absolute end key, check for cycle 
      if (Bytes.compareTo(hir.getStartKey(), hir.getEndKey()) > 0) {
        errors.reportError(
            ERROR_CODE.REGION_CYCLE,
            String.format("The endkey for this region comes before the "
                + "startkey, startkey=%s, endkey=%s",
                Bytes.toStringBinary(hir.getStartKey()),
                Bytes.toStringBinary(hir.getEndKey())), this, hir);
        backwards.add(hir);
        return;
      }

      // main case, add to split calculator
      sc.add(hir);
    }

    public void addServer(ServerName server) {
      this.deployedOn.add(server);
    }

    public String getName() {
      return tableName;
    }

    public int getNumRegions() {
      return sc.getStarts().size() + backwards.size();
    }

    private class IntegrityFixSuggester extends TableIntegrityErrorHandlerImpl {
      ErrorReporter errors;

      IntegrityFixSuggester(TableInfo ti, ErrorReporter errors) {
        this.errors = errors;
        setTableInfo(ti);
      }

      @Override
      public void handleRegionStartKeyNotEmpty(HbckInfo hi) throws IOException{
        errors.reportError(ERROR_CODE.FIRST_REGION_STARTKEY_NOT_EMPTY,
            "First region should start with an empty key.  You need to "
            + " create a new region and regioninfo in HDFS to plug the hole.",
            getTableInfo(), hi);
      }

      @Override
      public void handleDegenerateRegion(HbckInfo hi) throws IOException{
        errors.reportError(ERROR_CODE.DEGENERATE_REGION,
            "Region has the same start and end key.", getTableInfo(), hi);
      }

      @Override
      public void handleDuplicateStartKeys(HbckInfo r1, HbckInfo r2) throws IOException{
        byte[] key = r1.getStartKey();
        // dup start key
        errors.reportError(ERROR_CODE.DUPE_STARTKEYS,
            "Multiple regions have the same startkey: "
            + Bytes.toStringBinary(key), getTableInfo(), r1);
        errors.reportError(ERROR_CODE.DUPE_STARTKEYS,
            "Multiple regions have the same startkey: "
            + Bytes.toStringBinary(key), getTableInfo(), r2);
      }

      @Override
      public void handleOverlapInRegionChain(HbckInfo hi1, HbckInfo hi2) throws IOException{
        errors.reportError(ERROR_CODE.OVERLAP_IN_REGION_CHAIN,
            "There is an overlap in the region chain.",
            getTableInfo(), hi1, hi2);
      }

      @Override
      public void handleHoleInRegionChain(byte[] holeStart, byte[] holeStop) throws IOException{
        errors.reportError(
            ERROR_CODE.HOLE_IN_REGION_CHAIN,
            "There is a hole in the region chain between "
                + Bytes.toStringBinary(holeStart) + " and "
                + Bytes.toStringBinary(holeStop)
                + ".  You need to create a new .regioninfo and region "
                + "dir in hdfs to plug the hole.");
      }
    };

    /**
     * This handler fixes integrity errors from hdfs information.  There are
     * basically three classes of integrity problems 1) holes, 2) overlaps, and
     * 3) invalid regions.
     *
     * This class overrides methods that fix holes and the overlap group case.
     * Individual cases of particular overlaps are handled by the general
     * overlap group merge repair case.
     *
     * If hbase is online, this forces regions offline before doing merge
     * operations.
     */
    private class HDFSIntegrityFixer extends IntegrityFixSuggester {
      Configuration conf;

      boolean fixOverlaps = true;

      HDFSIntegrityFixer(TableInfo ti, ErrorReporter errors, Configuration conf,
          boolean fixHoles, boolean fixOverlaps) {
        super(ti, errors);
        this.conf = conf;
        this.fixOverlaps = fixOverlaps;
        // TODO properly use fixHoles
      }

      /**
       * This is a special case hole -- when the first region of a table is
       * missing from META, HBase doesn't acknowledge the existance of the
       * table.
       */
      public void handleRegionStartKeyNotEmpty(HbckInfo next) throws IOException {
        errors.reportError(ERROR_CODE.FIRST_REGION_STARTKEY_NOT_EMPTY,
            "First region should start with an empty key.  Creating a new " +
            "region and regioninfo in HDFS to plug the hole.",
            getTableInfo(), next);
        HTableDescriptor htd = getTableInfo().getHTD();
        // from special EMPTY_START_ROW to next region's startKey
        HRegionInfo newRegion = new HRegionInfo(htd.getName(),
            HConstants.EMPTY_START_ROW, next.getStartKey());

        // TODO test
        HRegion region = HBaseFsckRepair.createHDFSRegionDir(conf, newRegion, htd);
        LOG.info("Table region start key was not empty.  Created new empty region: "
            + newRegion + " " +region);
        fixes++;
      }

      /**
       * There is a hole in the hdfs regions that violates the table integrity
       * rules.  Create a new empty region that patches the hole.
       */
      public void handleHoleInRegionChain(byte[] holeStartKey, byte[] holeStopKey) throws IOException {
        errors.reportError(
            ERROR_CODE.HOLE_IN_REGION_CHAIN,
            "There is a hole in the region chain between "
                + Bytes.toStringBinary(holeStartKey) + " and "
                + Bytes.toStringBinary(holeStopKey)
                + ".  Creating a new regioninfo and region "
                + "dir in hdfs to plug the hole.");
        HTableDescriptor htd = getTableInfo().getHTD();
        HRegionInfo newRegion = new HRegionInfo(htd.getName(), holeStartKey, holeStopKey);
        HRegion region = HBaseFsckRepair.createHDFSRegionDir(conf, newRegion, htd);
        LOG.info("Plugged hold by creating new empty region: "+ newRegion + " " +region);
        fixes++;
      }

      /**
       * This takes set of overlapping regions and merges them into a single
       * region.  This covers cases like degenerate regions, shared start key,
       * general overlaps, duplicate ranges, and partial overlapping regions.
       *
       * Cases:
       * - Clean regions that overlap
       * - Only .oldlogs regions (can't find start/stop range, or figure out)
       */
      @Override
      public void handleOverlapGroup(Collection<HbckInfo> overlap)
          throws IOException {
        Preconditions.checkNotNull(overlap);
        Preconditions.checkArgument(overlap.size() >0);

        if (!this.fixOverlaps) {
          LOG.warn("Not attempting to repair overlaps.");
          return;
        }

        if (overlap.size() > maxMerge) {
          LOG.warn("Overlap group has " + overlap.size() + " overlapping " +
              "regions which is greater than " + maxMerge + ", the max " +
              "number of regions to merge.");
          return;
        }

        LOG.info("== Merging regions into one region: "
            + Joiner.on(",").join(overlap));
        // get the min / max range and close all concerned regions
        Pair<byte[], byte[]> range = null;
        for (HbckInfo hi : overlap) {
          if (range == null) {
            range = new Pair<byte[], byte[]>(hi.getStartKey(), hi.getEndKey());
          } else {
            if (RegionSplitCalculator.BYTES_COMPARATOR
                .compare(hi.getStartKey(), range.getFirst()) < 0) {
              range.setFirst(hi.getStartKey());
            }
            if (RegionSplitCalculator.BYTES_COMPARATOR
                .compare(hi.getEndKey(), range.getSecond()) > 0) {
              range.setSecond(hi.getEndKey());
            }
          }
          // need to close files so delete can happen.
          LOG.debug("Closing region before moving data around: " +  hi);
          LOG.debug("Contained region dir before close");
          debugLsr(hi.getHdfsRegionDir());
          try {
            closeRegion(hi);
          } catch (IOException ioe) {
            // TODO exercise this
            LOG.warn("Was unable to close region " + hi.getRegionNameAsString()
                + ".  Just continuing... ");
          } catch (InterruptedException e) {
            // TODO exercise this
            LOG.warn("Was unable to close region " + hi.getRegionNameAsString()
                + ".  Just continuing... ");
          }

          try {
            LOG.info("Offlining region: " + hi);
            offline(hi.getRegionName());
          } catch (IOException ioe) {
            LOG.warn("Unable to offline region from master: " + hi, ioe);
          }
        }

        // create new empty container region.
        HTableDescriptor htd = getTableInfo().getHTD();
        // from start key to end Key
        HRegionInfo newRegion = new HRegionInfo(htd.getName(), range.getFirst(),
            range.getSecond());
        HRegion region = HBaseFsckRepair.createHDFSRegionDir(conf, newRegion, htd);
        LOG.info("Created new empty container region: " +
            newRegion + " to contain regions: " + Joiner.on(",").join(overlap));
        debugLsr(region.getRegionDir());

        // all target regions are closed, should be able to safely cleanup.
        boolean didFix= false;
        Path target = region.getRegionDir();
        for (HbckInfo contained : overlap) {
          LOG.info("Merging " + contained  + " into " + target );
          int merges = mergeRegionDirs(target, contained);
          if (merges > 0) {
            didFix = true;
          }
        }
        if (didFix) {
          fixes++;
        }
      }
    };

    /**
     * Check the region chain (from META) of this table.  We are looking for
     * holes, overlaps, and cycles.
     * @return false if there are errors
     * @throws IOException
     */
    public boolean checkRegionChain(TableIntegrityErrorHandler handler) throws IOException {
      int originalErrorsCount = errors.getErrorList().size();
      Multimap<byte[], HbckInfo> regions = sc.calcCoverage();
      SortedSet<byte[]> splits = sc.getSplits();

      byte[] prevKey = null;
      byte[] problemKey = null;
      for (byte[] key : splits) {
        Collection<HbckInfo> ranges = regions.get(key);
        if (prevKey == null && !Bytes.equals(key, HConstants.EMPTY_BYTE_ARRAY)) {
          for (HbckInfo rng : ranges) {
            handler.handleRegionStartKeyNotEmpty(rng);
          }
        }

        // check for degenerate ranges
        for (HbckInfo rng : ranges) {
          // special endkey case converts '' to null
          byte[] endKey = rng.getEndKey();
          endKey = (endKey.length == 0) ? null : endKey;
          if (Bytes.equals(rng.getStartKey(),endKey)) {
            handler.handleDegenerateRegion(rng);
          }
        }

        if (ranges.size() == 1) {
          // this split key is ok -- no overlap, not a hole.
          if (problemKey != null) {
            LOG.warn("reached end of problem group: " + Bytes.toStringBinary(key));
          }
          problemKey = null; // fell through, no more problem.
        } else if (ranges.size() > 1) {
          // set the new problem key group name, if already have problem key, just
          // keep using it.
          if (problemKey == null) {
            // only for overlap regions.
            LOG.warn("Naming new problem group: " + Bytes.toStringBinary(key));
            problemKey = key;
          }
          overlapGroups.putAll(problemKey, ranges);

          // record errors
          ArrayList<HbckInfo> subRange = new ArrayList<HbckInfo>(ranges);
          //  this dumb and n^2 but this shouldn't happen often
          for (HbckInfo r1 : ranges) {
            subRange.remove(r1);
            for (HbckInfo r2 : subRange) {
              if (Bytes.compareTo(r1.getStartKey(), r2.getStartKey())==0) {
                handler.handleDuplicateStartKeys(r1,r2);
              } else {
                // overlap
                handler.handleOverlapInRegionChain(r1, r2);
              }
            }
          }

        } else if (ranges.size() == 0) {
          if (problemKey != null) {
            LOG.warn("reached end of problem group: " + Bytes.toStringBinary(key));
          }
          problemKey = null;

          byte[] holeStopKey = sc.getSplits().higher(key);
          // if higher key is null we reached the top.
          if (holeStopKey != null) {
            // hole
            handler.handleHoleInRegionChain(key, holeStopKey);
          }
        }
        prevKey = key;
      }

      for (Collection<HbckInfo> overlap : overlapGroups.asMap().values()) {
        handler.handleOverlapGroup(overlap);
      }

      if (details) {
        // do full region split map dump
        System.out.println("---- Table '"  +  this.tableName 
            + "': region split map");
        dump(splits, regions);
        System.out.println("---- Table '"  +  this.tableName 
            + "': overlap groups");
        dumpOverlapProblems(overlapGroups);
        System.out.println("There are " + overlapGroups.keySet().size()
            + " overlap groups with " + overlapGroups.size()
            + " overlapping regions");
      }
      return errors.getErrorList().size() == originalErrorsCount;
    }

    /**
     * This dumps data in a visually reasonable way for visual debugging
     * 
     * @param splits
     * @param regions
     */
    void dump(SortedSet<byte[]> splits, Multimap<byte[], HbckInfo> regions) {
      // we display this way because the last end key should be displayed as well.
      for (byte[] k : splits) {
        System.out.print(Bytes.toStringBinary(k) + ":\t");
        for (HbckInfo r : regions.get(k)) {
          System.out.print("[ "+ r.toString() + ", "
              + Bytes.toStringBinary(r.getEndKey())+ "]\t");
        }
        System.out.println();
      }
    }
  }

  public void dumpOverlapProblems(Multimap<byte[], HbckInfo> regions) {
    // we display this way because the last end key should be displayed as
    // well.
    for (byte[] k : regions.keySet()) {
      System.out.print(Bytes.toStringBinary(k) + ":\n");
      for (HbckInfo r : regions.get(k)) {
        System.out.print("[ " + r.toString() + ", "
            + Bytes.toStringBinary(r.getEndKey()) + "]\n");
      }
      System.out.println("----");
    }
  }

  public Multimap<byte[], HbckInfo> getOverlapGroups(
      String table) {
    TableInfo ti = tablesInfo.get(table);
    return ti.overlapGroups;
  }

  /**
   * Return a list of user-space table names whose metadata have not been
   * modified in the last few milliseconds specified by timelag
   * if any of the REGIONINFO_QUALIFIER, SERVER_QUALIFIER, STARTCODE_QUALIFIER,
   * SPLITA_QUALIFIER, SPLITB_QUALIFIER have not changed in the last
   * milliseconds specified by timelag, then the table is a candidate to be returned.
   * @return tables that have not been modified recently
   * @throws IOException if an error is encountered
   */
   HTableDescriptor[] getTables(AtomicInteger numSkipped) {
    List<String> tableNames = new ArrayList<String>();
    long now = System.currentTimeMillis();

    for (HbckInfo hbi : regionInfoMap.values()) {
      MetaEntry info = hbi.metaEntry;

      // if the start key is zero, then we have found the first region of a table.
      // pick only those tables that were not modified in the last few milliseconds.
      if (info != null && info.getStartKey().length == 0 && !info.isMetaRegion()) {
        if (info.modTime + timelag < now) {
          tableNames.add(info.getTableNameAsString());
        } else {
          numSkipped.incrementAndGet(); // one more in-flux table
        }
      }
    }
    return getHTableDescriptors(tableNames);
  }

   HTableDescriptor[] getHTableDescriptors(List<String> tableNames) {
    HTableDescriptor[] htd = null;
     try {
       LOG.info("getHTableDescriptors == tableNames => " + tableNames);
       htd = new HBaseAdmin(conf).getTableDescriptors(tableNames);
     } catch (IOException e) {
       LOG.debug("Exception getting table descriptors", e);
     }
     return htd;
  }


  /**
   * Gets the entry in regionInfo corresponding to the the given encoded
   * region name. If the region has not been seen yet, a new entry is added
   * and returned.
   */
  private synchronized HbckInfo getOrCreateInfo(String name) {
    HbckInfo hbi = regionInfoMap.get(name);
    if (hbi == null) {
      hbi = new HbckInfo(null);
      regionInfoMap.put(name, hbi);
    }
    return hbi;
  }

  /**
    * Check values in regionInfo for .META.
    * Check if zero or more than one regions with META are found.
    * If there are inconsistencies (i.e. zero or more than one regions
    * pretend to be holding the .META.) try to fix that and report an error.
    * @throws IOException from HBaseFsckRepair functions
   * @throws KeeperException
   * @throws InterruptedException
    */
  boolean checkMetaRegion()
    throws IOException, KeeperException, InterruptedException {
    List <HbckInfo> metaRegions = Lists.newArrayList();
    for (HbckInfo value : regionInfoMap.values()) {
      if (value.metaEntry.isMetaRegion()) {
        metaRegions.add(value);
      }
    }

    // If something is wrong
    if (metaRegions.size() != 1) {
      HRegionLocation rootLocation = connection.locateRegion(
        HConstants.ROOT_TABLE_NAME, HConstants.EMPTY_START_ROW);
      HbckInfo root =
          regionInfoMap.get(rootLocation.getRegionInfo().getEncodedName());

      // If there is no region holding .META.
      if (metaRegions.size() == 0) {
        errors.reportError(ERROR_CODE.NO_META_REGION, ".META. is not found on any region.");
        if (shouldFixAssignments()) {
          errors.print("Trying to fix a problem with .META...");
          setShouldRerun();
          // try to fix it (treat it as unassigned region)
          HBaseFsckRepair.fixUnassigned(admin, root.metaEntry);
          HBaseFsckRepair.waitUntilAssigned(admin, root.getHdfsHRI());
        }
      }
      // If there are more than one regions pretending to hold the .META.
      else if (metaRegions.size() > 1) {
        errors.reportError(ERROR_CODE.MULTI_META_REGION, ".META. is found on more than one region.");
        if (shouldFixAssignments()) {
          errors.print("Trying to fix a problem with .META...");
          setShouldRerun();
          // try fix it (treat is a dupe assignment)
          List <ServerName> deployedOn = Lists.newArrayList();
          for (HbckInfo mRegion : metaRegions) {
            deployedOn.add(mRegion.metaEntry.regionServer);
          }
          HBaseFsckRepair.fixMultiAssignment(admin, root.metaEntry, deployedOn);
        }
      }
      // rerun hbck with hopefully fixed META
      return false;
    }
    // no errors, so continue normally
    return true;
  }

  /**
   * Scan .META. and -ROOT-, adding all regions found to the regionInfo map.
   * @throws IOException if an error is encountered
   */
  boolean loadMetaEntries() throws IOException {

    // get a list of all regions from the master. This involves
    // scanning the META table
    if (!recordRootRegion()) {
      // Will remove later if we can fix it
      errors.reportError("Fatal error: unable to get root region location. Exiting...");
      return false;
    }

    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      int countRecord = 1;

      // comparator to sort KeyValues with latest modtime
      final Comparator<KeyValue> comp = new Comparator<KeyValue>() {
        public int compare(KeyValue k1, KeyValue k2) {
          return (int)(k1.getTimestamp() - k2.getTimestamp());
        }
      };

      public boolean processRow(Result result) throws IOException {
        try {

          // record the latest modification of this META record
          long ts =  Collections.max(result.list(), comp).getTimestamp();
          Pair<HRegionInfo, ServerName> pair = MetaReader.parseCatalogResult(result);
          if (pair == null || pair.getFirst() == null) {
            emptyRegionInfoQualifiers.add(result);
            return true;
          }
          ServerName sn = null;
          if (pair.getSecond() != null) {
            sn = pair.getSecond();
          }
          MetaEntry m = new MetaEntry(pair.getFirst(), sn, ts);
          HbckInfo hbInfo = new HbckInfo(m);
          HbckInfo previous = regionInfoMap.put(pair.getFirst().getEncodedName(), hbInfo);
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
    MetaScanner.metaScan(conf, visitor, null, null,
      Integer.MAX_VALUE, HConstants.ROOT_TABLE_NAME);

    if (!checkMetaOnly) {
      // Scan .META. to pick up user regions
      MetaScanner.metaScan(conf, visitor);
    }
    
    errors.print("");
    return true;
  }

  /**
   * Stores the regioninfo entries scanned from META
   */
  static class MetaEntry extends HRegionInfo {
    ServerName regionServer;   // server hosting this region
    long modTime;          // timestamp of most recent modification metadata

    public MetaEntry(HRegionInfo rinfo, ServerName regionServer, long modTime) {
      super(rinfo);
      this.regionServer = regionServer;
      this.modTime = modTime;
    }

    public boolean equals(Object o) {
      boolean superEq = super.equals(o);
      if (!superEq) {
        return superEq;
      }

      MetaEntry me = (MetaEntry) o;
      if (!regionServer.equals(me.regionServer)) {
        return false;
      }
      return (modTime == me.modTime);
    }
  }

  /**
   * Stores the regioninfo entries from HDFS
   */
  static class HdfsEntry {
    HRegionInfo hri;
    Path hdfsRegionDir = null;
    long hdfsRegionDirModTime  = 0;
    boolean hdfsRegioninfoFilePresent = false;
    boolean hdfsOnlyEdits = false;
  }

  /**
   * Stores the regioninfo retrieved from Online region servers.
   */
  static class OnlineEntry {
    HRegionInfo hri;
    ServerName hsa;

    public String toString() {
      return hsa.toString() + ";" + hri.getRegionNameAsString();
    }
  }

  /**
   * Maintain information about a particular region.  It gathers information
   * from three places -- HDFS, META, and region servers.
   */
  public static class HbckInfo implements KeyRange {
    private MetaEntry metaEntry = null; // info in META
    private HdfsEntry hdfsEntry = null; // info in HDFS
    private List<OnlineEntry> deployedEntries = Lists.newArrayList(); // on Region Server
    private List<ServerName> deployedOn = Lists.newArrayList(); // info on RS's

    HbckInfo(MetaEntry metaEntry) {
      this.metaEntry = metaEntry;
    }

    public synchronized void addServer(HRegionInfo hri, ServerName server) {
      OnlineEntry rse = new OnlineEntry() ;
      rse.hri = hri;
      rse.hsa = server;
      this.deployedEntries.add(rse);
      this.deployedOn.add(server);
    }

    public synchronized String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{ meta => ");
      sb.append((metaEntry != null)? metaEntry.getRegionNameAsString() : "null");
      sb.append( ", hdfs => " + getHdfsRegionDir());
      sb.append( ", deployed => " + Joiner.on(", ").join(deployedEntries));
      sb.append(" }");
      return sb.toString();
    }

    @Override
    public byte[] getStartKey() {
      if (this.metaEntry != null) {
        return this.metaEntry.getStartKey();
      } else if (this.hdfsEntry != null) {
        return this.hdfsEntry.hri.getStartKey();
      } else {
        LOG.error("Entry " + this + " has no meta or hdfs region start key.");
        return null;
      }
    }

    @Override
    public byte[] getEndKey() {
      if (this.metaEntry != null) {
        return this.metaEntry.getEndKey();
      } else if (this.hdfsEntry != null) {
        return this.hdfsEntry.hri.getEndKey();
      } else {
        LOG.error("Entry " + this + " has no meta or hdfs region start key.");
        return null;
      }
    }

    public byte[] getTableName() {
      if (this.metaEntry != null) {
        return this.metaEntry.getTableName();
      } else if (this.hdfsEntry != null) {
        // we are only guaranteed to have a path and not an HRI for hdfsEntry,
        // so we get the name from the Path
        Path tableDir = this.hdfsEntry.hdfsRegionDir.getParent();
        return Bytes.toBytes(tableDir.getName());
      } else {
        // Currently no code exercises this path, but we could add one for
        // getting table name from OnlineEntry
        return null;
      }
    }

    public String getRegionNameAsString() {
      if (metaEntry != null) {
        return metaEntry.getRegionNameAsString();
      } else if (hdfsEntry != null) {
        return hdfsEntry.hri.getRegionNameAsString();
      } else {
        return null;
      }
    }

    public byte[] getRegionName() {
      if (metaEntry != null) {
        return metaEntry.getRegionName();
      } else if (hdfsEntry != null) {
        return hdfsEntry.hri.getRegionName();
      } else {
        return null;
      }
    }

    Path getHdfsRegionDir() {
      if (hdfsEntry == null) {
        return null;
      }
      return hdfsEntry.hdfsRegionDir;
    }

    boolean containsOnlyHdfsEdits() {
      if (hdfsEntry == null) {
        return false;
      }
      return hdfsEntry.hdfsOnlyEdits;
    }

    boolean isHdfsRegioninfoPresent() {
      if (hdfsEntry == null) {
        return false;
      }
      return hdfsEntry.hdfsRegioninfoFilePresent;
    }

    long getModTime() {
      if (hdfsEntry == null) {
        return 0;
      }
      return hdfsEntry.hdfsRegionDirModTime;
    }

    HRegionInfo getHdfsHRI() {
      if (hdfsEntry == null) {
        return null;
      }
      return hdfsEntry.hri;
    }
  }

  final static Comparator<HbckInfo> cmp = new Comparator<HbckInfo>() {
    @Override
    public int compare(HbckInfo l, HbckInfo r) {
      if (l == r) {
        // same instance
        return 0;
      }

      int tableCompare = RegionSplitCalculator.BYTES_COMPARATOR.compare(
          l.getTableName(), r.getTableName());
      if (tableCompare != 0) {
        return tableCompare;
      }

      int startComparison = RegionSplitCalculator.BYTES_COMPARATOR.compare(
          l.getStartKey(), r.getStartKey());
      if (startComparison != 0) {
        return startComparison;
      }

      // Special case for absolute endkey
      byte[] endKey = r.getEndKey();
      endKey = (endKey.length == 0) ? null : endKey;
      byte[] endKey2 = l.getEndKey();
      endKey2 = (endKey2.length == 0) ? null : endKey2;
      int endComparison = RegionSplitCalculator.BYTES_COMPARATOR.compare(
          endKey2,  endKey);

      if (endComparison != 0) {
        return endComparison;
      }

      // use regionId as tiebreaker.
      // Null is considered after all possible values so make it bigger.
      if (l.hdfsEntry == null && r.hdfsEntry == null) {
        return 0;
      }
      if (l.hdfsEntry == null && r.hdfsEntry != null) {
        return 1;
      }
      // l.hdfsEntry must not be null
      if (r.hdfsEntry == null) {
        return -1;
      }
      // both l.hdfsEntry and r.hdfsEntry must not be null.
      return (int) (l.hdfsEntry.hri.getRegionId()- r.hdfsEntry.hri.getRegionId());
    }
  };

  /**
   * Prints summary of all tables found on the system.
   */
  private void printTableSummary(TreeMap<String, TableInfo> tablesInfo) {
    System.out.println("Summary:");
    for (TableInfo tInfo : tablesInfo.values()) {
      if (errors.tableHasErrors(tInfo)) {
        System.out.println("Table " + tInfo.getName() + " is inconsistent.");
      } else {
        System.out.println("  " + tInfo.getName() + " is okay.");
      }
      System.out.println("    Number of regions: " + tInfo.getNumRegions());
      System.out.print("    Deployed on: ");
      for (ServerName server : tInfo.deployedOn) {
        System.out.print(" " + server.toString());
      }
      System.out.println();
    }
  }

  public interface ErrorReporter {
    public static enum ERROR_CODE {
      UNKNOWN, NO_META_REGION, NULL_ROOT_REGION, NO_VERSION_FILE, NOT_IN_META_HDFS, NOT_IN_META,
      NOT_IN_META_OR_DEPLOYED, NOT_IN_HDFS_OR_DEPLOYED, NOT_IN_HDFS, SERVER_DOES_NOT_MATCH_META, NOT_DEPLOYED,
      MULTI_DEPLOYED, SHOULD_NOT_BE_DEPLOYED, MULTI_META_REGION, RS_CONNECT_FAILURE,
      FIRST_REGION_STARTKEY_NOT_EMPTY, DUPE_STARTKEYS,
      HOLE_IN_REGION_CHAIN, OVERLAP_IN_REGION_CHAIN, REGION_CYCLE, DEGENERATE_REGION,
      ORPHAN_HDFS_REGION
    }
    public void clear();
    public void report(String message);
    public void reportError(String message);
    public void reportError(ERROR_CODE errorCode, String message);
    public void reportError(ERROR_CODE errorCode, String message, TableInfo table, HbckInfo info);
    public void reportError(ERROR_CODE errorCode, String message, TableInfo table, HbckInfo info1, HbckInfo info2);
    public int summarize();
    public void detail(String details);
    public ArrayList<ERROR_CODE> getErrorList();
    public void progress();
    public void print(String message);
    public void resetErrors();
    public boolean tableHasErrors(TableInfo table);
  }

  private static class PrintingErrorReporter implements ErrorReporter {
    public int errorCount = 0;
    private int showProgress;

    Set<TableInfo> errorTables = new HashSet<TableInfo>();

    // for use by unit tests to verify which errors were discovered
    private ArrayList<ERROR_CODE> errorList = new ArrayList<ERROR_CODE>();

    public void clear() {
      errorTables.clear();
      errorList.clear();
      errorCount = 0;
    }

    public synchronized void reportError(ERROR_CODE errorCode, String message) {
      errorList.add(errorCode);
      if (!summary) {
        System.out.println("ERROR: " + message);
      }
      errorCount++;
      showProgress = 0;
    }

    public synchronized void reportError(ERROR_CODE errorCode, String message, TableInfo table,
                                         HbckInfo info) {
      errorTables.add(table);
      String reference = "(region " + info.getRegionNameAsString() + ")";
      reportError(errorCode, reference + " " + message);
    }

    public synchronized void reportError(ERROR_CODE errorCode, String message, TableInfo table,
                                         HbckInfo info1, HbckInfo info2) {
      errorTables.add(table);
      String reference = "(regions " + info1.getRegionNameAsString()
          + " and " + info2.getRegionNameAsString() + ")";
      reportError(errorCode, reference + " " + message);
    }

    public synchronized void reportError(String message) {
      reportError(ERROR_CODE.UNKNOWN, message);
    }

    /**
     * Report error information, but do not increment the error count.  Intended for cases
     * where the actual error would have been reported previously.
     * @param message
     */
    public synchronized void report(String message) {
      if (! summary) {
        System.out.println("ERROR: " + message);
      }
      showProgress = 0;
    }

    public synchronized int summarize() {
      System.out.println(Integer.toString(errorCount) +
                         " inconsistencies detected.");
      if (errorCount == 0) {
        System.out.println("Status: OK");
        return 0;
      } else {
        System.out.println("Status: INCONSISTENT");
        return -1;
      }
    }

    public ArrayList<ERROR_CODE> getErrorList() {
      return errorList;
    }

    public synchronized void print(String message) {
      if (!summary) {
        System.out.println(message);
      }
    }

    @Override
    public boolean tableHasErrors(TableInfo table) {
      return errorTables.contains(table);
    }

    @Override
    public void resetErrors() {
      errorCount = 0;
    }

    public synchronized void detail(String message) {
      if (details) {
        System.out.println(message);
      }
      showProgress = 0;
    }

    public synchronized void progress() {
      if (showProgress++ == 10) {
        if (!summary) {
          System.out.print(".");
        }
        showProgress = 0;
      }
    }
  }

  /**
   * Contact a region server and get all information from it
   */
  static class WorkItemRegion implements Runnable {
    private HBaseFsck hbck;
    private ServerName rsinfo;
    private ErrorReporter errors;
    private HConnection connection;
    private boolean done;

    WorkItemRegion(HBaseFsck hbck, ServerName info,
                   ErrorReporter errors, HConnection connection) {
      this.hbck = hbck;
      this.rsinfo = info;
      this.errors = errors;
      this.connection = connection;
      this.done = false;
    }

    // is this task done?
    synchronized boolean isDone() {
      return done;
    }

    @Override
    public synchronized void run() {
      errors.progress();
      try {
        HRegionInterface server =
            connection.getHRegionConnection(rsinfo.getHostname(), rsinfo.getPort());

        // list all online regions from this region server
        List<HRegionInfo> regions = server.getOnlineRegions();
        if (hbck.checkMetaOnly) {
          regions = filterOnlyMetaRegions(regions);
        }
        if (details) {
          errors.detail("RegionServer: " + rsinfo.getServerName() +
                           " number of regions: " + regions.size());
          for (HRegionInfo rinfo: regions) {
            errors.detail("  " + rinfo.getRegionNameAsString() +
                             " id: " + rinfo.getRegionId() +
                             " encoded_name: " + rinfo.getEncodedName() +
                             " start: " + Bytes.toStringBinary(rinfo.getStartKey()) +
                             " end: " + Bytes.toStringBinary(rinfo.getEndKey()));
          }
        }

        // check to see if the existence of this region matches the region in META
        for (HRegionInfo r:regions) {
          HbckInfo hbi = hbck.getOrCreateInfo(r.getEncodedName());
          hbi.addServer(r, rsinfo);
        }
      } catch (IOException e) {          // unable to connect to the region server. 
        errors.reportError(ERROR_CODE.RS_CONNECT_FAILURE, "RegionServer: " + rsinfo.getServerName() +
          " Unable to fetch region information. " + e);
      } finally {
        done = true;
        notifyAll(); // wakeup anybody waiting for this item to be done
      }
    }

    private List<HRegionInfo> filterOnlyMetaRegions(List<HRegionInfo> regions) {
      List<HRegionInfo> ret = Lists.newArrayList();
      for (HRegionInfo hri : regions) {
        if (hri.isMetaTable()) {
          ret.add(hri);
        }
      }
      return ret;
    }
  }

  /**
   * Contact hdfs and get all information about specified table directory into
   * regioninfo list.
   */
  static class WorkItemHdfsDir implements Runnable {
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

    synchronized boolean isDone() {
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

          LOG.debug("Loading region info from hdfs:"+ regionDir.getPath());
          HbckInfo hbi = hbck.getOrCreateInfo(encodedName);
          HdfsEntry he = new HdfsEntry();
          synchronized (hbi) {
            if (hbi.getHdfsRegionDir() != null) {
              errors.print("Directory " + encodedName + " duplicate??" +
                           hbi.getHdfsRegionDir());
            }

            he.hdfsRegionDir = regionDir.getPath();
            he.hdfsRegionDirModTime = regionDir.getModificationTime();
            Path regioninfoFile = new Path(he.hdfsRegionDir, HRegion.REGIONINFO_FILE);
            he.hdfsRegioninfoFilePresent = fs.exists(regioninfoFile);
            // we add to orphan list when we attempt to read .regioninfo

            // Set a flag if this region contains only edits
            // This is special case if a region is left after split
            he.hdfsOnlyEdits = true;
            FileStatus[] subDirs = fs.listStatus(regionDir.getPath());
            Path ePath = HLog.getRegionDirRecoveredEditsDir(regionDir.getPath());
            for (FileStatus subDir : subDirs) {
              String sdName = subDir.getPath().getName();
              if (!sdName.startsWith(".") && !sdName.equals(ePath.getName())) {
                he.hdfsOnlyEdits = false;
                break;
              }
            }
            hbi.hdfsEntry = he;
          }
        }
      } catch (IOException e) {
        // unable to connect to the region server.
        errors.reportError(ERROR_CODE.RS_CONNECT_FAILURE, "Table Directory: "
            + tableDir.getPath().getName()
            + " Unable to fetch region information. " + e);
      } finally {
        done = true;
        notifyAll();
      }
    }
  }

  /**
   * Display the full report from fsck. This displays all live and dead region
   * servers, and all known regions.
   */
  public void setDisplayFullReport() {
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
   * Set META check mode.
   * Print only info about META table deployment/state
   */
  void setCheckMetaOnly() {
    checkMetaOnly = true;
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
  public void setFixAssignments(boolean shouldFix) {
    fixAssignments = shouldFix;
  }

  boolean shouldFixAssignments() {
    return fixAssignments;
  }

  public void setFixMeta(boolean shouldFix) {
    fixMeta = shouldFix;
  }

  boolean shouldFixMeta() {
    return fixMeta;
  }

  public void setFixHdfsHoles(boolean shouldFix) {
    fixHdfsHoles = shouldFix;
  }

  boolean shouldFixHdfsHoles() {
    return fixHdfsHoles;
  }

  public void setFixHdfsOverlaps(boolean shouldFix) {
    fixHdfsOverlaps = shouldFix;
  }

  boolean shouldFixHdfsOverlaps() {
    return fixHdfsOverlaps;
  }

  public void setFixHdfsOrphans(boolean shouldFix) {
    fixHdfsOrphans = shouldFix;
  }

  boolean shouldFixHdfsOrphans() {
    return fixHdfsOrphans;
  }

  /**
   * @param mm maximum number of regions to merge into a single region.
   */
  public void setMaxMerge(int mm) {
    this.maxMerge = mm;
  }

  public int getMaxMerge() {
    return maxMerge;
  }

  /**
   * Only fix tables specified by the list
   */
  boolean shouldFixTable(byte[] table) {
    if (tablesToFix.size() == 0) {
      return true;
    }

    // doing this naively since byte[] equals may not do what we want.
    for (byte[] t : tablesToFix) {
      if (Bytes.equals(t, table)) return true;
    }
    return false;
  }

  void includeTable(byte[] table) {
    tablesToFix.add(table);
  }

  /**
   * We are interested in only those tables that have not changed their state in
   * META during the last few seconds specified by hbase.admin.fsck.timelag
   * @param seconds - the time in seconds
   */
  public void setTimeLag(long seconds) {
    timelag = seconds * 1000; // convert to milliseconds
  }

  protected static void printUsageAndExit() {
    System.err.println("Usage: fsck [opts] {only tables}");
    System.err.println(" where [opts] are:");
    System.err.println("   -details Display full report of all regions.");
    System.err.println("   -timelag {timeInSeconds}  Process only regions that " +
                       " have not experienced any metadata updates in the last " +
                       " {{timeInSeconds} seconds.");
    System.err.println("   -sleepBeforeRerun {timeInSeconds} Sleep this many seconds" +
        " before checking if the fix worked if run with -fix");
    System.err.println("   -summary Print only summary of the tables and status.");
    System.err.println("   -metaonly Only check the state of ROOT and META tables.");

    System.err.println("  Repair options: (expert features, use with caution!)");
    System.err.println("   -fix              Try to fix region assignments.  This is for backwards compatiblity");
    System.err.println("   -fixAssignments   Try to fix region assignments.  Replaces the old -fix");
    System.err.println("   -fixMeta          Try to fix meta problems.  This assumes HDFS region info is good.");
    System.err.println("   -fixHdfsHoles     Try to fix region holes in hdfs.");
    System.err.println("   -fixHdfsOrphans   Try to fix region dirs with no .regioninfo file in hdfs");
    System.err.println("   -fixHdfsOverlaps  Try to fix region overlaps in hdfs.");
    System.err.println("   -maxMerge <n>     When fixing region overlaps, allow at most <n> regions to merge. (n=" + DEFAULT_MAX_MERGE +" by default)");
    System.err.println("");
    System.err.println("   -repair           Shortcut for -fixAssignments -fixMeta -fixHdfsHoles -fixHdfsOrphans -fixHdfsOverlaps");
    System.err.println("   -repairHoles      Shortcut for -fixAssignments -fixMeta -fixHdfsHoles -fixHdfsOrphans");

    Runtime.getRuntime().exit(-2);
  }

  /**
   * Main program
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    // create a fsck object
    Configuration conf = HBaseConfiguration.create();
    conf.set("fs.defaultFS", conf.get(HConstants.HBASE_DIR));
    HBaseFsck fsck = new HBaseFsck(conf);
    long sleepBeforeRerun = DEFAULT_SLEEP_BEFORE_RERUN;

    // Process command-line args.
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      if (cmd.equals("-details")) {
        fsck.setDisplayFullReport();
      } else if (cmd.equals("-timelag")) {
        if (i == args.length - 1) {
          System.err.println("HBaseFsck: -timelag needs a value.");
          printUsageAndExit();
        }
        try {
          long timelag = Long.parseLong(args[i+1]);
          fsck.setTimeLag(timelag);
        } catch (NumberFormatException e) {
          System.err.println("-timelag needs a numeric value.");
          printUsageAndExit();
        }
        i++;
      } else if (cmd.equals("-sleepBeforeRerun")) {
        if (i == args.length - 1) {
          System.err.println("HBaseFsck: -sleepBeforeRerun needs a value.");
          printUsageAndExit();
        }
        try {
          sleepBeforeRerun = Long.parseLong(args[i+1]);
        } catch (NumberFormatException e) {
          System.err.println("-sleepBeforeRerun needs a numeric value.");
          printUsageAndExit();
        }
        i++;
      } else if (cmd.equals("-fix")) {
        System.err.println("This option is deprecated, please use " +
          "-fixAssignments instead.");
        fsck.setFixAssignments(true);
      } else if (cmd.equals("-fixAssignments")) {
        fsck.setFixAssignments(true);
      } else if (cmd.equals("-fixMeta")) {
        fsck.setFixMeta(true);
      } else if (cmd.equals("-fixHdfsHoles")) {
        fsck.setFixHdfsHoles(true);
      } else if (cmd.equals("-fixHdfsOrphans")) {
        fsck.setFixHdfsOrphans(true);
      } else if (cmd.equals("-fixHdfsOverlaps")) {
        fsck.setFixHdfsOverlaps(true);
      } else if (cmd.equals("-repair")) {
        // this attempts to merge overlapping hdfs regions, needs testing
        // under load
        fsck.setFixHdfsHoles(true);
        fsck.setFixHdfsOrphans(true);
        fsck.setFixMeta(true);
        fsck.setFixAssignments(true);
        fsck.setFixHdfsOverlaps(true);
      } else if (cmd.equals("-repairHoles")) {
        // this will make all missing hdfs regions available but may lose data
        fsck.setFixHdfsHoles(true);
        fsck.setFixHdfsOrphans(false);
        fsck.setFixMeta(true);
        fsck.setFixAssignments(true);
        fsck.setFixHdfsOverlaps(false);
      } else if (cmd.equals("-maxMerge")) {
        if (i == args.length - 1) {
          System.err.println("-maxMerge needs a numeric value argument.");
          printUsageAndExit();
        }
        try {
          int maxMerge = Integer.parseInt(args[i+1]);
          fsck.setMaxMerge(maxMerge);
        } catch (NumberFormatException e) {
          System.err.println("-maxMerge needs a numeric value argument.");
          printUsageAndExit();
        }
        i++;
      } else if (cmd.equals("-summary")) {
        fsck.setSummary();
      } else if (cmd.equals("-metaonly")) {
        fsck.setCheckMetaOnly();
      } else {
        byte[] table = Bytes.toBytes(cmd);
        fsck.includeTable(table);
        System.out.println("Allow fixes for table: " + cmd);
      }
    }
    // do the real work of fsck
    fsck.connect();
    int code = fsck.onlineHbck();
    // If we have changed the HBase state it is better to run fsck again
    // to see if we haven't broken something else in the process.
    // We run it only once more because otherwise we can easily fall into
    // an infinite loop.
    if (fsck.shouldRerun()) {
      try {
        LOG.info("Sleeping " + sleepBeforeRerun + "ms before re-checking after fix...");
        Thread.sleep(sleepBeforeRerun);
      } catch (InterruptedException ie) {
        Runtime.getRuntime().exit(code);
      }
      // Just report
      fsck.setFixAssignments(false);
      fsck.setFixMeta(false);
      fsck.setFixHdfsHoles(false);
      fsck.setFixHdfsOverlaps(false);
      fsck.errors.resetErrors();
      code = fsck.onlineHbck();
    }

    Runtime.getRuntime().exit(code);
  }

  /**
   * ls -r for debugging purposes
   */
  void debugLsr(Path p) throws IOException {
    debugLsr(conf, p);
  }

  /**
   * ls -r for debugging purposes
   */
  public static void debugLsr(Configuration conf, Path p) throws IOException {
    if (!LOG.isDebugEnabled()) {
      return;
    }
    FileSystem fs = p.getFileSystem(conf);

    if (!fs.exists(p)) {
      // nothing
      return;
    }
    System.out.println(p);

    if (fs.isFile(p)) {
      return;
    }

    if (fs.getFileStatus(p).isDir()) {
      FileStatus[] fss= fs.listStatus(p);
      for (FileStatus status : fss) {
        debugLsr(conf, status.getPath());
      }
    }
  }
}
