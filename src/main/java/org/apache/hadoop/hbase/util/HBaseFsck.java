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
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
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
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitorBase;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter.ERROR_CODE;
import org.apache.hadoop.hbase.util.hbck.HFileCorruptionChecker;
import org.apache.hadoop.hbase.util.hbck.TableIntegrityErrorHandler;
import org.apache.hadoop.hbase.util.hbck.TableIntegrityErrorHandlerImpl;
import org.apache.hadoop.hbase.zookeeper.RootRegionTracker;
import org.apache.hadoop.hbase.zookeeper.ZKTableReadOnly;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
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
  private static boolean rsSupportsOffline = true;
  private static final int DEFAULT_OVERLAPS_TO_SIDELINE = 2;
  private static final int DEFAULT_MAX_MERGE = 5;
  private static final String TO_BE_LOADED = "to_be_loaded";

  /**********************
   * Internal resources
   **********************/
  private static final Log LOG = LogFactory.getLog(HBaseFsck.class.getName());
  private Configuration conf;
  private ClusterStatus status;
  private HConnection connection;
  private HBaseAdmin admin;
  private HTable meta;
  protected ExecutorService executor; // threads to retrieve data from regionservers
  private long startMillis = System.currentTimeMillis();
  private HFileCorruptionChecker hfcc;
  private int retcode = 0;

  /***********
   * Options
   ***********/
  private static boolean details = false; // do we display the full report
  private long timelag = DEFAULT_TIME_LAG; // tables whose modtime is older
  private boolean fixAssignments = false; // fix assignment errors?
  private boolean fixMeta = false; // fix meta errors?
  private boolean checkHdfs = true; // load and check fs consistency?
  private boolean fixHdfsHoles = false; // fix fs holes?
  private boolean fixHdfsOverlaps = false; // fix fs overlaps (risky)
  private boolean fixHdfsOrphans = false; // fix fs holes (missing .regioninfo)
  private boolean fixTableOrphans = false; // fix fs holes (missing .tableinfo)
  private boolean fixVersionFile = false; // fix missing hbase.version file in hdfs
  private boolean fixSplitParents = false; // fix lingering split parents

  // limit checking/fixes to listed tables, if empty attempt to check/fix all
  // -ROOT- and .META. are always checked
  private Set<String> tablesIncluded = new HashSet<String>();
  private int maxMerge = DEFAULT_MAX_MERGE; // maximum number of overlapping regions to merge
  private int maxOverlapsToSideline = DEFAULT_OVERLAPS_TO_SIDELINE; // maximum number of overlapping regions to sideline
  private boolean sidelineBigOverlaps = false; // sideline overlaps with >maxMerge regions
  private Path sidelineDir = null;

  private boolean rerun = false; // if we tried to fix something, rerun hbck
  private static boolean summary = false; // if we want to print less output
  private boolean checkMetaOnly = false;
  private boolean ignorePreCheckPermission = false; // if pre-check permission

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
   *
   * If tablesIncluded is empty, this map contains all tables.
   * Otherwise, it contains only meta tables and tables in tablesIncluded,
   * unless checkMetaOnly is specified, in which case, it contains only
   * the meta tables (.META. and -ROOT-).
   */
  private SortedMap<String, TableInfo> tablesInfo = new ConcurrentSkipListMap<String,TableInfo>();

  /**
   * When initially looking at HDFS, we attempt to find any orphaned data.
   */
  private List<HbckInfo> orphanHdfsDirs = Collections.synchronizedList(new ArrayList<HbckInfo>());
  
  private Map<String, Set<String>> orphanTableDirs = new HashMap<String, Set<String>>();

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

    int numThreads = conf.getInt("hbasefsck.numthreads", MAX_NUM_THREADS);
    executor = new ScheduledThreadPoolExecutor(numThreads);
  }

  /**
   * Constructor
   *
   * @param conf
   *          Configuration object
   * @throws MasterNotRunningException
   *           if the master is not running
   * @throws ZooKeeperConnectionException
   *           if unable to connect to ZooKeeper
   */
  public HBaseFsck(Configuration conf, ExecutorService exec) throws MasterNotRunningException,
      ZooKeeperConnectionException, IOException {
    this.conf = conf;
    this.executor = exec;
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
    if (shouldCheckHdfs() && (shouldFixHdfsOrphans() || shouldFixHdfsHoles()
        || shouldFixHdfsOverlaps() || shouldFixTableOrphans())) {
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
    if (shouldCheckHdfs()) {
      loadHdfsRegionDirs();
      loadHdfsRegionInfos();
    }

    // Empty cells in .META.?
    reportEmptyMetaCells();

    // Get disabled tables from ZooKeeper
    loadDisabledTables();

    // fix the orphan tables
    fixOrphanTables();

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
    boolean oldBalancer = admin.setBalancerRunning(false, true);
    try {
      onlineConsistencyRepair();
    }
    finally {
      admin.setBalancerRunning(oldBalancer, false);
    }

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
    if (dirs == null) {
      LOG.warn("Attempt to adopt ophan hdfs region skipped becuase no files present in " +
          p + ". This dir could probably be deleted.");
      return ;
    }

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

    if (hbi.hdfsEntry.hri != null) {
      // already loaded data
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
  private SortedMap<String, TableInfo> loadHdfsRegionInfos() throws IOException, InterruptedException {
    tablesInfo.clear(); // regenerating the data
    // generate region split structure
    Collection<HbckInfo> hbckInfos = regionInfoMap.values();

    // Parallelized read of .regioninfo files.
    List<WorkItemHdfsRegionInfo> hbis = new ArrayList<WorkItemHdfsRegionInfo>(hbckInfos.size());
    List<Future<Void>> hbiFutures;

    for (HbckInfo hbi : hbckInfos) {
      WorkItemHdfsRegionInfo work = new WorkItemHdfsRegionInfo(hbi, this, errors);
      hbis.add(work);
    }

    // Submit and wait for completion
    hbiFutures = executor.invokeAll(hbis);

    for(int i=0; i<hbiFutures.size(); i++) {
      WorkItemHdfsRegionInfo work = hbis.get(i);
      Future<Void> f = hbiFutures.get(i);
      try {
        f.get();
      } catch(ExecutionException e) {
        LOG.warn("Failed to read .regioninfo file for region " +
              work.hbi.getRegionNameAsString(), e.getCause());
      }
    }

    // serialized table info gathering.
    for (HbckInfo hbi: hbckInfos) {

      if (hbi.getHdfsHRI() == null) {
        // was an orphan
        continue;
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
        Path hbaseRoot = FSUtils.getRootDir(conf);
        tablesInfo.put(tableName, modTInfo);
        try {
          HTableDescriptor htd =
              FSTableDescriptors.getTableDescriptor(hbaseRoot.getFileSystem(conf),
              hbaseRoot, tableName);
          modTInfo.htds.add(htd);
        } catch (IOException ioe) {
          if (!orphanTableDirs.containsKey(tableName)) {
            LOG.warn("Unable to read .tableinfo from " + hbaseRoot, ioe);
            //should only report once for each table
            errors.reportError(ERROR_CODE.NO_TABLEINFO_FILE, 
                "Unable to read .tableinfo from " + hbaseRoot + "/" + tableName);
            Set<String> columns = new HashSet<String>();
            orphanTableDirs.put(tableName, getColumnFamilyList(columns, hbi));
          }
        }
      }
      modTInfo.addRegionInfo(hbi);
    }

    return tablesInfo;
  }
  
  /**
   * To get the column family list according to the column family dirs
   * @param columns
   * @param hbi
   * @return
   * @throws IOException
   */
  private Set<String> getColumnFamilyList(Set<String> columns, HbckInfo hbi) throws IOException {
    Path regionDir = hbi.getHdfsRegionDir();
    FileSystem fs = regionDir.getFileSystem(conf);
    FileStatus[] subDirs = fs.listStatus(regionDir, new FSUtils.FamilyDirFilter(fs));
    for (FileStatus subdir : subDirs) {
      String columnfamily = subdir.getPath().getName();
      columns.add(columnfamily);
    }
    return columns;
  }
  
  /**
   * To fabricate a .tableinfo file with following contents<br>
   * 1. the correct tablename <br>
   * 2. the correct colfamily list<br>
   * 3. the default properties for both {@link HTableDescriptor} and {@link HColumnDescriptor}<br>
   * @param tableName
   * @throws IOException
   */
  private boolean fabricateTableInfo(String tableName, Set<String> columns) throws IOException {
    if (columns ==null || columns.isEmpty()) return false;
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (String columnfamimly : columns) {
      htd.addFamily(new HColumnDescriptor(columnfamimly));
    }
    FSTableDescriptors.createTableDescriptor(htd, conf, true);
    return true;
  }
  
  /**
   * To fix orphan table by creating a .tableinfo file under tableDir <br>
   * 1. if TableInfo is cached, to recover the .tableinfo accordingly <br>
   * 2. else create a default .tableinfo file with following items<br>
   * &nbsp;2.1 the correct tablename <br>
   * &nbsp;2.2 the correct colfamily list<br>
   * &nbsp;2.3 the default properties for both {@link HTableDescriptor} and {@link HColumnDescriptor}<br>
   * @throws IOException
   */
  public void fixOrphanTables() throws IOException {
    if (shouldFixTableOrphans() && !orphanTableDirs.isEmpty()) {

      Path hbaseRoot = FSUtils.getRootDir(conf);
      List<String> tmpList = new ArrayList<String>();
      tmpList.addAll(orphanTableDirs.keySet());
      HTableDescriptor[] htds = getHTableDescriptors(tmpList);
      Iterator<Entry<String, Set<String>>> iter = orphanTableDirs.entrySet().iterator();
      int j = 0; 
      int numFailedCase = 0;
      while (iter.hasNext()) {
        Entry<String, Set<String>> entry = (Entry<String, Set<String>>) iter.next();
        String tableName = entry.getKey();
        LOG.info("Trying to fix orphan table error: " + tableName);
        if (j < htds.length) {
          if (tableName.equals(Bytes.toString(htds[j].getName()))) {
            HTableDescriptor htd = htds[j];
            LOG.info("fixing orphan table: " + tableName + " from cache");
            FSTableDescriptors.createTableDescriptor(
                hbaseRoot.getFileSystem(conf), hbaseRoot, htd, true);
            j++;
            iter.remove();
          }
        } else {
          if (fabricateTableInfo(tableName, entry.getValue())) {
            LOG.warn("fixing orphan table: " + tableName + " with a default .tableinfo file");
            LOG.warn("Strongly recommend to modify the HTableDescriptor if necessary for: " + tableName);
            iter.remove();
          } else {
            LOG.error("Unable to create default .tableinfo for " + tableName + " while missing column family information");
            numFailedCase++;
          }
        }
        fixes++;
      }

      if (orphanTableDirs.isEmpty()) {
        // all orphanTableDirs are luckily recovered
        // re-run doFsck after recovering the .tableinfo file
        setShouldRerun();
        LOG.warn("Strongly recommend to re-run manually hfsck after all orphanTableDirs being fixed");
      } else if (numFailedCase > 0) {
        LOG.error("Failed to fix " + numFailedCase
            + " OrphanTables with default .tableinfo files");
      }

    }
    //cleanup the list
    orphanTableDirs.clear();

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
  private ArrayList<Put> generatePuts(SortedMap<String, TableInfo> tablesInfo) throws IOException {
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
  private void suggestFixes(SortedMap<String, TableInfo> tablesInfo) throws IOException {
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
    Path backupDir = sidelineOldRootAndMeta();

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
    LOG.info("Old -ROOT- and .META. are moved into " + backupDir);
    return true;
  }

  private SortedMap<String, TableInfo> checkHdfsIntegrity(boolean fixHoles,
      boolean fixOverlaps) throws IOException {
    LOG.info("Checking HBase region split map from HDFS data...");
    for (TableInfo tInfo : tablesInfo.values()) {
      TableIntegrityErrorHandler handler;
      if (fixHoles || fixOverlaps) {
        handler = tInfo.new HDFSIntegrityFixer(tInfo, errors, conf,
          fixHoles, fixOverlaps);
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
    if (sidelineDir == null) {
      Path hbaseDir = FSUtils.getRootDir(conf);
      Path hbckDir = new Path(hbaseDir, HConstants.HBCK_SIDELINEDIR_NAME);
      sidelineDir = new Path(hbckDir, hbaseDir.getName() + "-"
          + startMillis);
    }
    return sidelineDir;
  }

  /**
   * Sideline a region dir (instead of deleting it)
   */
  Path sidelineRegionDir(FileSystem fs, HbckInfo hi) throws IOException {
    return sidelineRegionDir(fs, null, hi);
  }

  /**
   * Sideline a region dir (instead of deleting it)
   *
   * @param parentDir if specified, the region will be sidelined to
   * folder like .../parentDir/<table name>/<region name>. The purpose
   * is to group together similar regions sidelined, for example, those
   * regions should be bulk loaded back later on. If null, it is ignored.
   */
  Path sidelineRegionDir(FileSystem fs,
      String parentDir, HbckInfo hi) throws IOException {
    String tableName = Bytes.toString(hi.getTableName());
    Path regionDir = hi.getHdfsRegionDir();

    if (!fs.exists(regionDir)) {
      LOG.warn("No previous " + regionDir + " exists.  Continuing.");
      return null;
    }

    Path rootDir = getSidelineDir();
    if (parentDir != null) {
      rootDir = new Path(rootDir, parentDir);
    }
    Path sidelineTableDir= new Path(rootDir, tableName);
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
        FileStatus[] hfiles = fs.listStatus(src);
        if (hfiles != null && hfiles.length > 0) {
          for (FileStatus hfile : hfiles) {
            success = fs.rename(hfile.getPath(), dst);
            if (!success) {
              String msg = "Unable to rename file " + src +  " to " + dst;
              LOG.error(msg);
              throw new IOException(msg);
            }
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
    return sidelineRegionDir;
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
    Path backupDir = getSidelineDir();
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
          for (String tableName : ZKTableReadOnly.getDisabledOrDisablingTables(zkw)) {
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
        if ((!checkMetaOnly && isTableIncluded(dirName)) ||
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
      if (shouldFixVersionFile()) {
        LOG.info("Trying to create a new " + HConstants.VERSION_FILE_NAME
            + " file.");
        setShouldRerun();
        FSUtils.setVersion(fs, rootDir, conf.getInt(
            HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000), conf.getInt(
            HConstants.VERSION_FILE_WRITE_ATTEMPTS,
            HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS));
      }
    }

    // level 1:  <HBASE_DIR>/*
    List<WorkItemHdfsDir> dirs = new ArrayList<WorkItemHdfsDir>(tableDirs.size());
    List<Future<Void>> dirsFutures;

    for (FileStatus tableDir : tableDirs) {
      LOG.debug("Loading region dirs from " +tableDir.getPath());
      dirs.add(new WorkItemHdfsDir(this, fs, errors, tableDir));
    }

    // Invoke and wait for Callables to complete
    dirsFutures = executor.invokeAll(dirs);

    for(Future<Void> f: dirsFutures) {
      try {
        f.get();
      } catch(ExecutionException e) {
        LOG.warn("Could not load region dir " , e.getCause());
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

    List<WorkItemRegion> workItems = new ArrayList<WorkItemRegion>(regionServerList.size());
    List<Future<Void>> workFutures;

    // loop to contact each region server in parallel
    for (ServerName rsinfo: regionServerList) {
      workItems.add(new WorkItemRegion(this, rsinfo, errors, connection));
    }
    
    workFutures = executor.invokeAll(workItems);

    for(int i=0; i<workFutures.size(); i++) {
      WorkItemRegion item = workItems.get(i);
      Future<Void> f = workFutures.get(i);
      try {
        f.get();
      } catch(ExecutionException e) {
        LOG.warn("Could not process regionserver " + item.rsinfo.getHostAndPort(),
            e.getCause());
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

  private void preCheckPermission() throws IOException, AccessControlException {
    if (shouldIgnorePreCheckPermission()) {
      return;
    }

    Path hbaseDir = new Path(conf.get(HConstants.HBASE_DIR));
    FileSystem fs = hbaseDir.getFileSystem(conf);
    UserGroupInformation ugi = User.getCurrent().getUGI();
    FileStatus[] files = fs.listStatus(hbaseDir);
    for (FileStatus file : files) {
      try {
        FSUtils.checkAccess(ugi, file, FsAction.WRITE);
      } catch (AccessControlException ace) {
        LOG.warn("Got AccessControlException when preCheckPermission ", ace);
        System.err.println("Current user " + ugi.getUserName() + " does not have write perms to " + file.getPath()
            + ". Please rerun hbck as hdfs user " + file.getOwner());
        throw new AccessControlException(ace);
      }
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
   * Reset the split parent region info in meta table
   */
  private void resetSplitParent(HbckInfo hi) throws IOException {
    RowMutations mutations = new RowMutations(hi.metaEntry.getRegionName());
    Delete d = new Delete(hi.metaEntry.getRegionName());
    d.deleteColumn(HConstants.CATALOG_FAMILY, HConstants.SPLITA_QUALIFIER);
    d.deleteColumn(HConstants.CATALOG_FAMILY, HConstants.SPLITB_QUALIFIER);
    mutations.add(d);

    Put p = new Put(hi.metaEntry.getRegionName());
    HRegionInfo hri = new HRegionInfo(hi.metaEntry);
    hri.setOffline(false);
    hri.setSplit(false);
    p.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
      Writables.getBytes(hri));
    mutations.add(p);

    meta.mutateRow(mutations);
    meta.flushCommits();
    LOG.info("Reset split parent " + hi.metaEntry.getRegionNameAsString() + " in META" );
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
  @SuppressWarnings("deprecation")
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
      HRegionInfo hri = hbi.getHdfsHRI();
      if (hri == null) {
        hri = hbi.metaEntry;
      }
      HBaseFsckRepair.fixUnassigned(admin, hri);
      HBaseFsckRepair.waitUntilAssigned(admin, hri);
    }
  }

  /**
   * Check a single region for consistency and correct deployment.
   */
  private void checkRegionConsistency(final String key, final HbckInfo hbi)
  throws IOException, KeeperException, InterruptedException {
    String descriptiveName = hbi.toString();

    boolean inMeta = hbi.metaEntry != null;
    // In case not checking HDFS, assume the region is on HDFS
    boolean inHdfs = !shouldCheckHdfs() || hbi.getHdfsRegionDir() != null;
    boolean hasMetaAssignment = inMeta && hbi.metaEntry.regionServer != null;
    boolean isDeployed = !hbi.deployedOn.isEmpty();
    boolean isMultiplyDeployed = hbi.deployedOn.size() > 1;
    boolean deploymentMatchesMeta =
      hasMetaAssignment && isDeployed && !isMultiplyDeployed &&
      hbi.metaEntry.regionServer.equals(hbi.deployedOn.get(0));
    boolean splitParent =
      (hbi.metaEntry == null)? false: hbi.metaEntry.isSplit() && hbi.metaEntry.isOffline();
    boolean shouldBeDeployed = inMeta && !isTableDisabled(hbi.metaEntry);
    boolean recentlyModified = inHdfs &&
      hbi.getModTime() + timelag > System.currentTimeMillis();

    // ========== First the healthy cases =============
    if (hbi.containsOnlyHdfsEdits()) {
      return;
    }
    if (inMeta && inHdfs && isDeployed && deploymentMatchesMeta && shouldBeDeployed) {
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
    } else if (inMeta && inHdfs && !isDeployed && splitParent) {
      errors.reportError(ERROR_CODE.LINGERING_SPLIT_PARENT, "Region "
          + descriptiveName + " is a split parent in META, in HDFS, "
          + "and not deployed on any region server. This could be transient.");
      if (shouldFixSplitParents()) {
        setShouldRerun();
        resetSplitParent(hbi);
      }
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
      errors.reportError(ERROR_CODE.SHOULD_NOT_BE_DEPLOYED,
          "Region " + descriptiveName + " should not be deployed according " +
          "to META, but is deployed on " + Joiner.on(", ").join(hbi.deployedOn));
      if (shouldFixAssignments()) {
        errors.print("Trying to close the region " + descriptiveName);
        setShouldRerun();
        HBaseFsckRepair.fixMultiAssignment(admin, hbi.metaEntry, hbi.deployedOn);
      }
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
  SortedMap<String, TableInfo> checkIntegrity() throws IOException {
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

    // sidelined big overlapped regions
    final Map<Path, HbckInfo> sidelinedRegions = new HashMap<Path, HbckInfo>();

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
      } else {
        LOG.error("None/Multiple table descriptors found for table '"
          + tableName + "' regions: " + htds);
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
      public void handleRegionEndKeyNotEmpty(byte[] curEndKey) throws IOException {
        errors.reportError(ERROR_CODE.LAST_REGION_ENDKEY_NOT_EMPTY,
            "Last region should end with an empty key. You need to "
                + "create a new region and regioninfo in HDFS to plug the hole.", getTableInfo());
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

      public void handleRegionEndKeyNotEmpty(byte[] curEndKey) throws IOException {
        errors.reportError(ERROR_CODE.LAST_REGION_ENDKEY_NOT_EMPTY,
            "Last region should end with an empty key. Creating a new "
                + "region and regioninfo in HDFS to plug the hole.", getTableInfo());
        HTableDescriptor htd = getTableInfo().getHTD();
        // from curEndKey to EMPTY_START_ROW
        HRegionInfo newRegion = new HRegionInfo(htd.getName(), curEndKey,
            HConstants.EMPTY_START_ROW);

        HRegion region = HBaseFsckRepair.createHDFSRegionDir(conf, newRegion, htd);
        LOG.info("Table region end key was not empty. Created new empty region: " + newRegion
            + " " + region);
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
            "regions which is greater than " + maxMerge + ", the max number of regions to merge");
          if (sidelineBigOverlaps) {
            // we only sideline big overlapped groups that exceeds the max number of regions to merge
            sidelineBigOverlaps(overlap);
          }
          return;
        }

        mergeOverlaps(overlap);
      }

      void mergeOverlaps(Collection<HbckInfo> overlap)
          throws IOException {
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
            LOG.info("Closing region: " + hi);
            closeRegion(hi);
          } catch (IOException ioe) {
            LOG.warn("Was unable to close region " + hi
              + ".  Just continuing... ", ioe);
          } catch (InterruptedException e) {
            LOG.warn("Was unable to close region " + hi
              + ".  Just continuing... ", e);
          }

          try {
            LOG.info("Offlining region: " + hi);
            offline(hi.getRegionName());
          } catch (IOException ioe) {
            LOG.warn("Unable to offline region from master: " + hi
              + ".  Just continuing... ", ioe);
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

      /**
       * Sideline some regions in a big overlap group so that it
       * will have fewer regions, and it is easier to merge them later on.
       *
       * @param bigOverlap the overlapped group with regions more than maxMerge
       * @throws IOException
       */
      void sidelineBigOverlaps(
          Collection<HbckInfo> bigOverlap) throws IOException {
        int overlapsToSideline = bigOverlap.size() - maxMerge;
        if (overlapsToSideline > maxOverlapsToSideline) {
          overlapsToSideline = maxOverlapsToSideline;
        }
        List<HbckInfo> regionsToSideline =
          RegionSplitCalculator.findBigRanges(bigOverlap, overlapsToSideline);
        FileSystem fs = FileSystem.get(conf);
        for (HbckInfo regionToSideline: regionsToSideline) {
          try {
            LOG.info("Closing region: " + regionToSideline);
            closeRegion(regionToSideline);
          } catch (IOException ioe) {
            LOG.warn("Was unable to close region " + regionToSideline
              + ".  Just continuing... ", ioe);
          } catch (InterruptedException e) {
            LOG.warn("Was unable to close region " + regionToSideline
              + ".  Just continuing... ", e);
          }

          try {
            LOG.info("Offlining region: " + regionToSideline);
            offline(regionToSideline.getRegionName());
          } catch (IOException ioe) {
            LOG.warn("Unable to offline region from master: " + regionToSideline
              + ".  Just continuing... ", ioe);
          }

          LOG.info("Before sideline big overlapped region: " + regionToSideline.toString());
          Path sidelineRegionDir = sidelineRegionDir(fs, TO_BE_LOADED, regionToSideline);
          if (sidelineRegionDir != null) {
            sidelinedRegions.put(sidelineRegionDir, regionToSideline);
            LOG.info("After sidelined big overlapped region: "
              + regionToSideline.getRegionNameAsString()
              + " to " + sidelineRegionDir.toString());
            fixes++;
          }
        }
      }
    }

    /**
     * Check the region chain (from META) of this table.  We are looking for
     * holes, overlaps, and cycles.
     * @return false if there are errors
     * @throws IOException
     */
    public boolean checkRegionChain(TableIntegrityErrorHandler handler) throws IOException {
      // When table is disabled no need to check for the region chain. Some of the regions
      // accidently if deployed, this below code might report some issues like missing start
      // or end regions or region hole in chain and may try to fix which is unwanted.
      if (disabledTables.contains(this.tableName.getBytes())) {
        return true;
      }
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

      // When the last region of a table is proper and having an empty end key, 'prevKey'
      // will be null.
      if (prevKey != null) {
        handler.handleRegionEndKeyNotEmpty(prevKey);
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
      if (!sidelinedRegions.isEmpty()) {
        LOG.warn("Sidelined big overlapped regions, please bulk load them!");
        System.out.println("---- Table '"  +  this.tableName
            + "': sidelined big overlapped regions");
        dumpSidelinedRegions(sidelinedRegions);
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

  public void dumpSidelinedRegions(Map<Path, HbckInfo> regions) {
    for (Map.Entry<Path, HbckInfo> entry: regions.entrySet()) {
      String tableName = Bytes.toStringBinary(entry.getValue().getTableName());
      Path path = entry.getKey();
      System.out.println("This sidelined region dir should be bulk loaded: "
        + path.toString());
      System.out.println("Bulk load command looks like: "
        + "hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles "
        + path.toUri().getPath() + " "+ tableName);
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
    HTableDescriptor[] htd = new HTableDescriptor[0];
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

    MetaScannerVisitor visitor = new MetaScannerVisitorBase() {
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
          HRegionInfo hri = pair.getFirst();
          if (!(isTableIncluded(hri.getTableNameAsString())
              || hri.isMetaRegion() || hri.isRootRegion())) {
            return true;
          }
          MetaEntry m = new MetaEntry(hri, sn, ts);
          HbckInfo hbInfo = new HbckInfo(m);
          HbckInfo previous = regionInfoMap.put(hri.getEncodedName(), hbInfo);
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
        if (hdfsEntry.hri != null) {
          return hdfsEntry.hri.getRegionNameAsString();
        }
      }
      return null;
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
  private void printTableSummary(SortedMap<String, TableInfo> tablesInfo) {
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
      FIRST_REGION_STARTKEY_NOT_EMPTY, LAST_REGION_ENDKEY_NOT_EMPTY, DUPE_STARTKEYS,
      HOLE_IN_REGION_CHAIN, OVERLAP_IN_REGION_CHAIN, REGION_CYCLE, DEGENERATE_REGION,
      ORPHAN_HDFS_REGION, LINGERING_SPLIT_PARENT, NO_TABLEINFO_FILE
    }
    public void clear();
    public void report(String message);
    public void reportError(String message);
    public void reportError(ERROR_CODE errorCode, String message);
    public void reportError(ERROR_CODE errorCode, String message, TableInfo table);
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

    public synchronized void reportError(ERROR_CODE errorCode, String message, TableInfo table) {
      errorTables.add(table);
      reportError(errorCode, message);
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
  static class WorkItemRegion implements Callable<Void> {
    private HBaseFsck hbck;
    private ServerName rsinfo;
    private ErrorReporter errors;
    private HConnection connection;

    WorkItemRegion(HBaseFsck hbck, ServerName info,
                   ErrorReporter errors, HConnection connection) {
      this.hbck = hbck;
      this.rsinfo = info;
      this.errors = errors;
      this.connection = connection;
    }

    @Override
    public synchronized Void call() throws IOException {
      errors.progress();
      try {
        HRegionInterface server =
            connection.getHRegionConnection(rsinfo.getHostname(), rsinfo.getPort());

        // list all online regions from this region server
        List<HRegionInfo> regions = server.getOnlineRegions();
        regions = filterRegions(regions);
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
        throw e;
      }
      return null;
    }

    private List<HRegionInfo> filterRegions(List<HRegionInfo> regions) {
      List<HRegionInfo> ret = Lists.newArrayList();
      for (HRegionInfo hri : regions) {
        if (hri.isMetaTable() || (!hbck.checkMetaOnly
            && hbck.isTableIncluded(hri.getTableNameAsString()))) {
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
  static class WorkItemHdfsDir implements Callable<Void> {
    private HBaseFsck hbck;
    private FileStatus tableDir;
    private ErrorReporter errors;
    private FileSystem fs;

    WorkItemHdfsDir(HBaseFsck hbck, FileSystem fs, ErrorReporter errors, 
                    FileStatus status) {
      this.hbck = hbck;
      this.fs = fs;
      this.tableDir = status;
      this.errors = errors;
    }

    @Override
    public synchronized Void call() throws IOException {
      try {
        String tableName = tableDir.getPath().getName();
        // ignore hidden files
        if (tableName.startsWith(".") &&
            !tableName.equals( Bytes.toString(HConstants.META_TABLE_NAME))) {
          return null;
        }
        // level 2: <HBASE_DIR>/<table>/*
        FileStatus[] regionDirs = fs.listStatus(tableDir.getPath());
        for (FileStatus regionDir : regionDirs) {
          String encodedName = regionDir.getPath().getName();
          // ignore directories that aren't hexadecimal
          if (!encodedName.toLowerCase().matches("[0-9a-f]+")) {
            continue;
          }

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
        throw e;
      }
      return null;
    }
  }

  /**
   * Contact hdfs and get all information about specified table directory into
   * regioninfo list.
   */
  static class WorkItemHdfsRegionInfo implements Callable<Void> {
    private HbckInfo hbi;
    private HBaseFsck hbck;
    private ErrorReporter errors;

    WorkItemHdfsRegionInfo(HbckInfo hbi, HBaseFsck hbck, ErrorReporter errors) {
      this.hbi = hbi;
      this.hbck = hbck;
      this.errors = errors;
    }

    @Override
    public synchronized Void call() throws IOException {
      // only load entries that haven't been loaded yet.
      if (hbi.getHdfsHRI() == null) {
        try {
          hbck.loadHdfsRegioninfo(hbi);
        } catch (IOException ioe) {
          String msg = "Orphan region in HDFS: Unable to load .regioninfo from table "
              + Bytes.toString(hbi.getTableName()) + " in hdfs dir "
              + hbi.getHdfsRegionDir()
              + "!  It may be an invalid format or version file.  Treating as "
              + "an orphaned regiondir.";
          errors.reportError(ERROR_CODE.ORPHAN_HDFS_REGION, msg);
          try {
            hbck.debugLsr(hbi.getHdfsRegionDir());
          } catch (IOException ioe2) {
            LOG.error("Unable to read directory " + hbi.getHdfsRegionDir(), ioe2);
            throw ioe2;
          }
          hbck.orphanHdfsDirs.add(hbi);
          throw ioe;
        }
      }
      return null;
    }
  };

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

  public void setCheckHdfs(boolean checking) {
    checkHdfs = checking;
  }

  boolean shouldCheckHdfs() {
    return checkHdfs;
  }

  public void setFixHdfsHoles(boolean shouldFix) {
    fixHdfsHoles = shouldFix;
  }

  boolean shouldFixHdfsHoles() {
    return fixHdfsHoles;
  }
  
  public void setFixTableOrphans(boolean shouldFix) {
    fixTableOrphans = shouldFix;
  }
   
  boolean shouldFixTableOrphans() {
    return fixTableOrphans;
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

  public void setFixVersionFile(boolean shouldFix) {
    fixVersionFile = shouldFix;
  }

  public boolean shouldFixVersionFile() {
    return fixVersionFile;
  }

  public void setSidelineBigOverlaps(boolean sbo) {
    this.sidelineBigOverlaps = sbo;
  }

  public boolean shouldSidelineBigOverlaps() {
    return sidelineBigOverlaps;
  }

  public void setFixSplitParents(boolean shouldFix) {
    fixSplitParents = shouldFix;
  }

  boolean shouldFixSplitParents() {
    return fixSplitParents;
  }

  public boolean shouldIgnorePreCheckPermission() {
    return ignorePreCheckPermission;
  }

  public void setIgnorePreCheckPermission(boolean ignorePreCheckPermission) {
    this.ignorePreCheckPermission = ignorePreCheckPermission;
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

  public void setMaxOverlapsToSideline(int mo) {
    this.maxOverlapsToSideline = mo;
  }

  public int getMaxOverlapsToSideline() {
    return maxOverlapsToSideline;
  }

  /**
   * Only check/fix tables specified by the list,
   * Empty list means all tables are included.
   */
  boolean isTableIncluded(String table) {
    return (tablesIncluded.size() == 0) || tablesIncluded.contains(table);
  }

  public void includeTable(String table) {
    tablesIncluded.add(table);
  }

  Set<String> getIncludedTables() {
    return new HashSet<String>(tablesIncluded);
  }

  /**
   * We are interested in only those tables that have not changed their state in
   * META during the last few seconds specified by hbase.admin.fsck.timelag
   * @param seconds - the time in seconds
   */
  public void setTimeLag(long seconds) {
    timelag = seconds * 1000; // convert to milliseconds
  }

  /**
   * 
   * @param sidelineDir - HDFS path to sideline data
   */
  public void setSidelineDir(String sidelineDir) {
    this.sidelineDir = new Path(sidelineDir);
  }

  protected HFileCorruptionChecker createHFileCorruptionChecker(boolean sidelineCorruptHFiles) throws IOException {
    return new HFileCorruptionChecker(conf, executor, sidelineCorruptHFiles);
  }

  public HFileCorruptionChecker getHFilecorruptionChecker() {
    return hfcc;
  }

  public void setHFileCorruptionChecker(HFileCorruptionChecker hfcc) {
    this.hfcc = hfcc;
  }

  public void setRetCode(int code) {
    this.retcode = code;
  }

  public int getRetCode() {
    return retcode;
  }

  protected HBaseFsck printUsageAndExit() {
    System.err.println("Usage: fsck [opts] {only tables}");
    System.err.println(" where [opts] are:");
    System.err.println("   -help Display help options (this)");
    System.err.println("   -details Display full report of all regions.");
    System.err.println("   -timelag <timeInSeconds>  Process only regions that " +
                       " have not experienced any metadata updates in the last " +
                       " <timeInSeconds> seconds.");
    System.err.println("   -sleepBeforeRerun <timeInSeconds> Sleep this many seconds" +
        " before checking if the fix worked if run with -fix");
    System.err.println("   -summary Print only summary of the tables and status.");
    System.err.println("   -metaonly Only check the state of ROOT and META tables.");
    System.err.println("   -sidelineDir <hdfs://> HDFS path to backup existing meta and root.");

    System.err.println("");
    System.err.println("  Metadata Repair options: (expert features, use with caution!)");
    System.err.println("   -fix              Try to fix region assignments.  This is for backwards compatiblity");
    System.err.println("   -fixAssignments   Try to fix region assignments.  Replaces the old -fix");
    System.err.println("   -fixMeta          Try to fix meta problems.  This assumes HDFS region info is good.");
    System.err.println("   -noHdfsChecking   Don't load/check region info from HDFS."
        + " Assumes META region info is good. Won't check/fix any HDFS issue, e.g. hole, orphan, or overlap");
    System.err.println("   -fixHdfsHoles     Try to fix region holes in hdfs.");
    System.err.println("   -fixHdfsOrphans   Try to fix region dirs with no .regioninfo file in hdfs");
    System.err.println("   -fixTableOrphans  Try to fix table dirs with no .tableinfo file in hdfs (online mode only)");
    System.err.println("   -fixHdfsOverlaps  Try to fix region overlaps in hdfs.");
    System.err.println("   -fixVersionFile   Try to fix missing hbase.version file in hdfs.");
    System.err.println("   -maxMerge <n>     When fixing region overlaps, allow at most <n> regions to merge. (n=" + DEFAULT_MAX_MERGE +" by default)");
    System.err.println("   -sidelineBigOverlaps  When fixing region overlaps, allow to sideline big overlaps");
    System.err.println("   -maxOverlapsToSideline <n>  When fixing region overlaps, allow at most <n> regions to sideline per group. (n=" + DEFAULT_OVERLAPS_TO_SIDELINE +" by default)");
    System.err.println("   -fixSplitParents  Try to force offline split parents to be online.");
    System.err.println("   -ignorePreCheckPermission  ignore filesystem permission pre-check");

    System.err.println("");
    System.err.println("  Datafile Repair options: (expert features, use with caution!)");
    System.err.println("   -checkCorruptHFiles     Check all Hfiles by opening them to make sure they are valid");
    System.err.println("   -sidelineCorruptHfiles  Quarantine corrupted HFiles.  implies -checkCorruptHfiles");

    System.err.println("");
    System.err.println("  Metadata Repair shortcuts");
    System.err.println("   -repair           Shortcut for -fixAssignments -fixMeta -fixHdfsHoles " +
        "-fixHdfsOrphans -fixHdfsOverlaps -fixVersionFile -sidelineBigOverlaps");
    System.err.println("   -repairHoles      Shortcut for -fixAssignments -fixMeta -fixHdfsHoles");

    setRetCode(-2);
    return this;
  }

  /**
   * Main program
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    // create a fsck object
    Configuration conf = HBaseConfiguration.create();
    Path hbasedir = new Path(conf.get(HConstants.HBASE_DIR));
    URI defaultFs = hbasedir.getFileSystem(conf).getUri();
    conf.set("fs.defaultFS", defaultFs.toString());     // for hadoop 0.21+
    conf.set("fs.default.name", defaultFs.toString());  // for hadoop 0.20

    int numThreads = conf.getInt("hbasefsck.numthreads", MAX_NUM_THREADS);
    ExecutorService exec = new ScheduledThreadPoolExecutor(numThreads);
    HBaseFsck hbck = new HBaseFsck(conf, exec);
    hbck.exec(exec, args);
    int retcode = hbck.getRetCode();
    Runtime.getRuntime().exit(retcode);
  }

  public HBaseFsck exec(ExecutorService exec, String[] args) throws KeeperException, IOException,
    InterruptedException {
    long sleepBeforeRerun = DEFAULT_SLEEP_BEFORE_RERUN;

    boolean checkCorruptHFiles = false;
    boolean sidelineCorruptHFiles = false;

    // Process command-line args.
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      if (cmd.equals("-help") || cmd.equals("-h")) {
        return printUsageAndExit();
      } else if (cmd.equals("-details")) {
        setDisplayFullReport();
      } else if (cmd.equals("-timelag")) {
        if (i == args.length - 1) {
          System.err.println("HBaseFsck: -timelag needs a value.");
          return printUsageAndExit();
        }
        try {
          long timelag = Long.parseLong(args[i+1]);
          setTimeLag(timelag);
        } catch (NumberFormatException e) {
          System.err.println("-timelag needs a numeric value.");
          return printUsageAndExit();
        }
        i++;
      } else if (cmd.equals("-sleepBeforeRerun")) {
        if (i == args.length - 1) {
          System.err.println("HBaseFsck: -sleepBeforeRerun needs a value.");
          return printUsageAndExit();
        }
        try {
          sleepBeforeRerun = Long.parseLong(args[i+1]);
        } catch (NumberFormatException e) {
          System.err.println("-sleepBeforeRerun needs a numeric value.");
          return printUsageAndExit();
        }
        i++;
      } else if (cmd.equals("-sidelineDir")) {
        if (i == args.length - 1) {
          System.err.println("HBaseFsck: -sidelineDir needs a value.");
          return printUsageAndExit();
        }
        i++;
        setSidelineDir(args[i]);
      } else if (cmd.equals("-fix")) {
        System.err.println("This option is deprecated, please use " +
          "-fixAssignments instead.");
        setFixAssignments(true);
      } else if (cmd.equals("-fixAssignments")) {
        setFixAssignments(true);
      } else if (cmd.equals("-fixMeta")) {
        setFixMeta(true);
      } else if (cmd.equals("-noHdfsChecking")) {
        setCheckHdfs(false);
      } else if (cmd.equals("-fixHdfsHoles")) {
        setFixHdfsHoles(true);
      } else if (cmd.equals("-fixHdfsOrphans")) {
        setFixHdfsOrphans(true);
      } else if (cmd.equals("-fixTableOrphans")) {
        setFixTableOrphans(true);
      } else if (cmd.equals("-fixHdfsOverlaps")) {
        setFixHdfsOverlaps(true);
      } else if (cmd.equals("-fixVersionFile")) {
        setFixVersionFile(true);
      } else if (cmd.equals("-sidelineBigOverlaps")) {
        setSidelineBigOverlaps(true);
      } else if (cmd.equals("-fixSplitParents")) {
        setFixSplitParents(true);
      } else if (cmd.equals("-ignorePreCheckPermission")) {
        setIgnorePreCheckPermission(true);
      } else if (cmd.equals("-checkCorruptHFiles")) {
        checkCorruptHFiles = true;
      } else if (cmd.equals("-sidelineCorruptHFiles")) {
        sidelineCorruptHFiles = true;
      } else if (cmd.equals("-repair")) {
        // this attempts to merge overlapping hdfs regions, needs testing
        // under load
        setFixHdfsHoles(true);
        setFixHdfsOrphans(true);
        setFixMeta(true);
        setFixAssignments(true);
        setFixHdfsOverlaps(true);
        setFixVersionFile(true);
        setSidelineBigOverlaps(true);
        setFixSplitParents(false);
        setCheckHdfs(true);
      } else if (cmd.equals("-repairHoles")) {
        // this will make all missing hdfs regions available but may lose data
        setFixHdfsHoles(true);
        setFixHdfsOrphans(false);
        setFixMeta(true);
        setFixAssignments(true);
        setFixHdfsOverlaps(false);
        setSidelineBigOverlaps(false);
        setFixSplitParents(false);
        setCheckHdfs(true);
      } else if (cmd.equals("-maxOverlapsToSideline")) {
        if (i == args.length - 1) {
          System.err.println("-maxOverlapsToSideline needs a numeric value argument.");
          return printUsageAndExit();
        }
        try {
          int maxOverlapsToSideline = Integer.parseInt(args[i+1]);
          setMaxOverlapsToSideline(maxOverlapsToSideline);
        } catch (NumberFormatException e) {
          System.err.println("-maxOverlapsToSideline needs a numeric value argument.");
          return printUsageAndExit();
        }
        i++;
      } else if (cmd.equals("-maxMerge")) {
        if (i == args.length - 1) {
          System.err.println("-maxMerge needs a numeric value argument.");
          return printUsageAndExit();
        }
        try {
          int maxMerge = Integer.parseInt(args[i+1]);
          setMaxMerge(maxMerge);
        } catch (NumberFormatException e) {
          System.err.println("-maxMerge needs a numeric value argument.");
          return printUsageAndExit();
        }
        i++;
      } else if (cmd.equals("-summary")) {
        setSummary();
      } else if (cmd.equals("-metaonly")) {
        setCheckMetaOnly();
      } else if (cmd.startsWith("-")) {
        System.err.println("Unrecognized option:" + cmd);
        return printUsageAndExit();
      } else {
        includeTable(cmd);
        System.out.println("Allow checking/fixes for table: " + cmd);
      }
    }

    // pre-check current user has FS write permission or not
    try {
      preCheckPermission();
    } catch (AccessControlException ace) {
      Runtime.getRuntime().exit(-1);
    } catch (IOException ioe) {
      Runtime.getRuntime().exit(-1);
    }

    // do the real work of hbck
    connect();

    // if corrupt file mode is on, first fix them since they may be opened later
    if (checkCorruptHFiles || sidelineCorruptHFiles) {
      LOG.info("Checking all hfiles for corruption");
      HFileCorruptionChecker hfcc = createHFileCorruptionChecker(sidelineCorruptHFiles);
      setHFileCorruptionChecker(hfcc); // so we can get result
      Collection<String> tables = getIncludedTables();
      Collection<Path> tableDirs = new ArrayList<Path>();
      Path rootdir = FSUtils.getRootDir(conf);
      if (tables.size() > 0) {
        for (String t : tables) {
          tableDirs.add(FSUtils.getTablePath(rootdir, t));
        }
      } else {
        tableDirs = FSUtils.getTableDirs(FSUtils.getCurrentFileSystem(conf), rootdir);
      }
      hfcc.checkTables(tableDirs);
      PrintWriter out = new PrintWriter(System.out);
      hfcc.report(out);
      out.flush();
    }

    // check and fix table integrity, region consistency.
    int code = onlineHbck();
    setRetCode(code);
    // If we have changed the HBase state it is better to run hbck again
    // to see if we haven't broken something else in the process.
    // We run it only once more because otherwise we can easily fall into
    // an infinite loop.
    if (shouldRerun()) {
      try {
        LOG.info("Sleeping " + sleepBeforeRerun + "ms before re-checking after fix...");
        Thread.sleep(sleepBeforeRerun);
      } catch (InterruptedException ie) {
        return this;
      }
      // Just report
      setFixAssignments(false);
      setFixMeta(false);
      setFixHdfsHoles(false);
      setFixHdfsOverlaps(false);
      setFixVersionFile(false);
      setFixTableOrphans(false);
      errors.resetErrors();
      code = onlineHbck();
      setRetCode(code);
    }
    return this;
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
    if (!LOG.isDebugEnabled() || p == null) {
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
