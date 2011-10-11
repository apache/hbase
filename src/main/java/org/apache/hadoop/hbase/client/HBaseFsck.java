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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * Check consistency among the in-memory states of the master and the
 * region server(s) and the state of data in HDFS.
 */
public class HBaseFsck {
  public static final long DEFAULT_TIME_LAG = 60000; // default value of 1 minute

  private static final Log LOG = LogFactory.getLog(HBaseFsck.class.getName());
  private Configuration conf;

  private ClusterStatus status;
  private HConnection connection;
  private TreeMap<String, HbckInfo> regionInfo = new TreeMap<String, HbckInfo>();
  ErrorReporter errors = new PrintingErrorReporter();

  private boolean details = false; // do we display the full report?
  private long timelag = DEFAULT_TIME_LAG; // tables whose modtime is older
  private boolean fix = false; // do we want to try fixing the errors?
  private boolean rerun = false; // if we tried to fix something rerun hbck

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
  }

  /**
   * Contacts the master and prints out cluster-wide information
   * @throws IOException if a remote or network exception occurs
   * @return 0 on success, non-zero on failure
   */
  int doWork() throws IOException {
    // print hbase server version
    System.out.println("Version: " + status.getHBaseVersion());

    // get a list of all regions from the master. This involves
    // scanning the META table
    recordRootRegion();
    getMetaEntries();

    // get a list of all tables that have not changed recently.
    AtomicInteger numSkipped = new AtomicInteger(0);
    HTableDescriptor[] allTables = getTables(numSkipped);
    System.out.println("Number of Tables: " + allTables.length);
    if (details) {
      if (numSkipped.get() > 0) {
        System.out.println("\n Number of Tables in flux: " + numSkipped.get());
      }
      for (HTableDescriptor td : allTables) {
        String tableName = td.getNameAsString();
        System.out.println("\t Table: " + tableName + "\t" +
                           (td.isReadOnly() ? "ro" : "rw") + "\t" +
                           (td.isRootRegion() ? "ROOT" :
                            (td.isMetaRegion() ? "META" : "    ")) + "\t" +
                           " families:" + td.getFamilies().size());
      }
    }

    // From the master, get a list of all known live region servers
    Collection<HServerInfo> regionServers = status.getServerInfo();
    System.out.println("Number of live region servers:" +
                       regionServers.size());
    if (details) {
      for (HServerInfo rsinfo: regionServers) {
        errors.detail("\t RegionServer:" + rsinfo.getServerName());
      }
    }

    // From the master, get a list of all dead region servers
    Collection<String> deadRegionServers = status.getDeadServerNames();
    System.out.println("Number of dead region servers:" +
                       deadRegionServers.size());
    if (details) {
      for (String name: deadRegionServers) {
        errors.detail("\t RegionServer(dead):" + name);
      }
    }

    // Determine what's deployed
    processRegionServers(regionServers);

    // Determine what's on HDFS
    checkHdfs();

    // Check consistency
    checkConsistency();

    return errors.summarize();
  }

  /**
   * Scan HDFS for all regions, recording their information into
   * regionInfo
   */
  void checkHdfs() throws IOException {
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

    // level 1:  <HBASE_DIR>/*
    for (FileStatus tableDir : tableDirs) {
      String tableName = tableDir.getPath().getName();
      // ignore hidden files
      if (tableName.startsWith(".") &&
          !tableName.equals( Bytes.toString(HConstants.META_TABLE_NAME)))
        continue;
      // level 2: <HBASE_DIR>/<table>/*
      FileStatus[] regionDirs = fs.listStatus(tableDir.getPath());
      for (FileStatus regionDir : regionDirs) {
        String encodedName = regionDir.getPath().getName();

        // ignore directories that aren't numeric
        if (!encodedName.matches("^\\d+$")) continue;

        HbckInfo hbi = getOrCreateInfo(encodedName);
        hbi.foundRegionDir = regionDir;
      }
    }
  }

  /**
   * Record the location of the ROOT region as found in ZooKeeper,
   * as if it were in a META table. This is so that we can check
   * deployment of ROOT.
   */
  void recordRootRegion() throws IOException {
    HRegionLocation rootLocation = connection.locateRegion(
      HConstants.ROOT_TABLE_NAME, HConstants.EMPTY_START_ROW);

    MetaEntry m = new MetaEntry(rootLocation.getRegionInfo(),
      rootLocation.getServerAddress(), null, System.currentTimeMillis());
    HbckInfo hbInfo = new HbckInfo(m);
    regionInfo.put(rootLocation.getRegionInfo().getEncodedName(), hbInfo);
  }


  /**
   * Contacts each regionserver and fetches metadata about regions.
   * @param regionServerList - the list of region servers to connect to
   * @throws IOException if a remote or network exception occurs
   */
  void processRegionServers(Collection<HServerInfo> regionServerList)
    throws IOException {

    // loop to contact each region server
    for (HServerInfo rsinfo:regionServerList) {
      errors.progress();
      try {
        HRegionInterface server = connection.getHRegionConnection(
                                    rsinfo.getServerAddress());

        // list all online regions from this region server
        HRegionInfo[] regions = server.getRegionsAssignment();

        if (details) {
          errors.detail("\nRegionServer:" + rsinfo.getServerName() +
                        " number of regions:" + regions.length);
          for (HRegionInfo rinfo: regions) {
            errors.detail("\n\t name:" + rinfo.getRegionNameAsString() +
                          " id:" + rinfo.getRegionId() +
                          " encoded name:" + rinfo.getEncodedName() +
                          " start :" + Bytes.toStringBinary(rinfo.getStartKey()) +
                          " end :" + Bytes.toStringBinary(rinfo.getEndKey()));
          }
        }

        // check to see if the existance of this region matches the region in META
        for (HRegionInfo r:regions) {
          HbckInfo hbi = getOrCreateInfo(r.getEncodedName());
          hbi.deployedOn.add(rsinfo.getServerAddress());
        }
      } catch (IOException e) {          // unable to connect to the region server.
        errors.reportError("RegionServer: " + rsinfo.getServerName() +
                      " Unable to fetch region information. " + e);
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
    boolean recentlyModified = hbi.foundRegionDir != null &&
      hbi.foundRegionDir.getModificationTime() + timelag > System.currentTimeMillis();

    // ========== First the healthy cases =============
    if (inMeta && inHdfs && isDeployed && deploymentMatchesMeta && shouldBeDeployed) {
      LOG.debug("Region " + descriptiveName + " healthy");
      return;
    } else if (inMeta && !shouldBeDeployed && !isDeployed) {
      // offline regions shouldn't cause complaints
      LOG.debug("Region " + descriptiveName + " offline, ignoring.");
      return;
    } else if (recentlyModified) {
      LOG.info("Region " + descriptiveName + " was recently modified -- skipping");
      return;
    }
    // ========== Cases where the region is not in META =============
    else if (!inMeta && !inHdfs && !isDeployed) {
      // We shouldn't have record of this region at all then!
      assert false : "Entry for region with no data";
    } else if (!inMeta && !inHdfs && isDeployed) {
      errors.reportError("Region " + descriptiveName + " not on HDFS or in META but " +
        "deployed on " + Joiner.on(", ").join(hbi.deployedOn));
    } else if (!inMeta && inHdfs && !isDeployed) {
      errors.reportError("Region " + descriptiveName + " on HDFS, but not listed in META " +
        "or deployed on any region server.");
    } else if (!inMeta && inHdfs && isDeployed) {
      errors.reportError("Region " + descriptiveName + " not in META, but deployed on " +
        Joiner.on(", ").join(hbi.deployedOn));

    // ========== Cases where the region is in META =============
    } else if (inMeta && !inHdfs && !isDeployed) {
      errors.reportError("Region " + descriptiveName + " found in META, but not in HDFS " +
        "or deployed on any region server.");
    } else if (inMeta && !inHdfs && isDeployed) {
      errors.reportError("Region " + descriptiveName + " found in META, but not in HDFS, " +
        "and deployed on " + Joiner.on(", ").join(hbi.deployedOn));
    } else if (inMeta && inHdfs && !isDeployed) {
      errors.reportError("Region " + descriptiveName + " not deployed on any region server.");
      // If we are trying to fix the errors
      if (shouldFix()) {
        System.out.println("Trying to fix unassigned region...");
        setShouldRerun();
        HBaseFsckRepair.fixUnassigned(this.conf, hbi.metaEntry);
      }
    } else if (inMeta && inHdfs && isDeployed && !shouldBeDeployed) {
      errors.reportError("Region " + descriptiveName + " has should not be deployed according " +
        "to META, but is deployed on " + Joiner.on(", ").join(hbi.deployedOn));
    } else if (inMeta && inHdfs && isMultiplyDeployed) {
      errors.reportError("Region " + descriptiveName + " is listed in META on region server " +
        hbi.metaEntry.regionServer + " but is multiply assigned to region servers " +
        Joiner.on(", ").join(hbi.deployedOn));
      // If we are trying to fix the errors
      if (shouldFix()) {
        System.out.println("Trying to fix assignment error...");
        setShouldRerun();
        HBaseFsckRepair.fixDupeAssignment(this.conf, hbi.metaEntry, hbi.deployedOn);
      }
    } else if (inMeta && inHdfs && isDeployed && !deploymentMatchesMeta) {
      errors.reportError("Region " + descriptiveName + " listed in META on region server " +
        hbi.metaEntry.regionServer + " but found on region server " +
        hbi.deployedOn.get(0));
      // If we are trying to fix the errors
      if (shouldFix()) {
        System.out.println("Trying to fix assignment error...");
        setShouldRerun();
        HBaseFsckRepair.fixDupeAssignment(this.conf, hbi.metaEntry, hbi.deployedOn);
      }
    } else {
      errors.reportError("Region " + descriptiveName + " is in an unforeseen state:" +
        " inMeta=" + inMeta +
        " inHdfs=" + inHdfs +
        " isDeployed=" + isDeployed +
        " isMultiplyDeployed=" + isMultiplyDeployed +
        " deploymentMatchesMeta=" + deploymentMatchesMeta +
        " shouldBeDeployed=" + shouldBeDeployed);
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

  /**
   * Gets the entry in regionInfo corresponding to the the given encoded
   * region name. If the region has not been seen yet, a new entry is added
   * and returned.
   */
  private HbckInfo getOrCreateInfo(String name) {
    HbckInfo hbi = regionInfo.get(name);
    if (hbi == null) {
      hbi = new HbckInfo(null);
      regionInfo.put(name, hbi);
    }
    return hbi;
  }

  /**
   * Scan .META. and -ROOT-, adding all regions found to the regionInfo map.
   * @throws IOException if an error is encountered
   */
  void getMetaEntries() throws IOException {
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
            System.out.print(".");
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
    // Scan .META. to pick up user regions
    MetaScanner.metaScan(conf, visitor);
    System.out.println("");
  }

  /**
   * Stores the entries scanned from META
   */
  private static class MetaEntry extends HRegionInfo {
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
    MetaEntry metaEntry = null;
    FileStatus foundRegionDir = null;
    List<HServerAddress> deployedOn = Lists.newArrayList();

    HbckInfo(MetaEntry metaEntry) {
      this.metaEntry = metaEntry;
    }

    public String toString() {
      if (metaEntry != null) {
        return metaEntry.getRegionNameAsString();
      } else if (foundRegionDir != null) {
        return foundRegionDir.getPath().toString();
      } else {
        return "unknown region on " + Joiner.on(", ").join(deployedOn);
      }
    }
  }

  interface ErrorReporter {
    public void reportError(String message);
    public int summarize();
    public void detail(String details);
    public void progress();
  }

  private static class PrintingErrorReporter implements ErrorReporter {
    public int errorCount = 0;
    private int showProgress;

    public void reportError(String message) {
      System.out.println("ERROR: " + message);
      errorCount++;
      showProgress = 0;
    }

    public int summarize() {
      if (errorCount == 0) {
        System.out.println("\nRest easy, buddy! HBase is clean. ");
        return 0;
      } else {
        System.out.println("\n" + Integer.toString(errorCount) + " inconsistencies detected.");
        return -1;
      }
    }

    public void detail(String details) {
      System.out.println(details);
      showProgress = 0;
    }

    public void progress() {
      if (showProgress++ == 10) {
        System.out.print(".");
        showProgress = 0;
      }
    }
  }

  /**
   * Display the full report from fsck. This displays all live and dead region servers ,
   * and all known regions.
   */
  void displayFullReport() {
    details = true;
  }

  /**
   * Check if we should rerun fsck again. This checks if we've tried to fix
   * something and we should rerun fsck tool again.
   * Display the full report from fsck. This displays all live and dead region servers ,
   * and all known regions.
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
  void setFixErrors() {
    fix = true;
  }

  boolean shouldFix() {
    return fix;
  }

  /**
   * We are interested in only those tables that have not changed their state in
   * META during the last few seconds specified by hbase.admin.fsck.timelag
   * @param seconds - the time in seconds
   */
  void setTimeLag(long seconds) {
    timelag = seconds * 1000; // convert to milliseconds
  }

  protected static void printUsageAndExit() {
    System.err.println("Usage: fsck [opts] ");
    System.err.println(" where [opts] are:");
    System.err.println("   -details Display full report of all regions.");
    System.err.println("   -timelag {timeInSeconds}  Process only regions that " +
                       " have not experienced any metadata updates in the last " +
                       " {{timeInSeconds} seconds.");
    Runtime.getRuntime().exit(-2);
  }

  /**
   * Main program
   * @param args
   */
  public static void main(String [] args)
    throws IOException, MasterNotRunningException {

    // create a fsck object
    Configuration conf = HBaseConfiguration.create();
    conf.set("fs.defaultFS", conf.get("hbase.rootdir"));
    HBaseFsck fsck = new HBaseFsck(conf);

    // Process command-line args.
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      if (cmd.equals("-details")) {
        fsck.displayFullReport();
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
      } else if (cmd.equals("-fix")) {
        fsck.setFixErrors();
      } else {
        String str = "Unknown command line option : " + cmd;
        LOG.info(str);
        System.out.println(str);
        printUsageAndExit();
      }
    }
    // do the real work of fsck
    int code = fsck.doWork();
    // If we have changed the HBase state it is better to run fsck again
    // to see if we haven't broken something else in the process.
    // We run it only once more because otherwise we can easily fall into
    // an infinite loop.
    if (fsck.shouldRerun()) {
      code = fsck.doWork();
    }

    Runtime.getRuntime().exit(code);
  }
}
