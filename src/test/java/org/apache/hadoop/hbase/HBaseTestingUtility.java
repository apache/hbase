/**
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Jdk14Logger;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.NoServerForRegionException;
import org.apache.hadoop.hbase.client.PreemptiveFastFailException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ServerConnectionManager;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.ZooKeeper;

import com.google.common.base.Preconditions;

/**
 * Facility for testing HBase. Added as tool to abet junit4 testing.  Replaces
 * old HBaseTestCase and HBaseCluserTestCase functionality.
 * Create an instance and keep it around doing HBase testing.  This class is
 * meant to be your one-stop shop for anything you might need testing.  Manages
 * one cluster at a time only.  Depends on log4j being on classpath and
 * hbase-site.xml for logging and test-run configuration.  It does not set
 * logging levels nor make changes to configuration parameters.
 */
public class HBaseTestingUtility {
  private final static Log LOG = LogFactory.getLog(HBaseTestingUtility.class);
  private final Configuration conf;
  private final CacheConfig cacheConf;
  private MiniZooKeeperCluster zkCluster = null;
  private static int USERNAME_SUFFIX = 0;

  /**
   * The default number of regions per regionserver when creating a pre-split
   * table.
   */
  private static int DEFAULT_REGIONS_PER_SERVER = 5;

  private MiniDFSCluster dfsCluster = null;
  private MiniHBaseCluster hbaseCluster = null;
  private MiniMRCluster mrCluster = null;

  /** The root directory for all mini-cluster test data for this testing utility instance. */
  private File clusterTestBuildDir = null;
  
  /** If there is a mini cluster running for this testing utility instance. */
  private boolean miniClusterRunning;

  private HBaseAdmin hbaseAdmin = null;

  private String hadoopLogDir;

  /**
   * System property key to get test directory value.
   */
  public static final String TEST_DIRECTORY_KEY = "test.build.data";

  /**
   * Default parent directory for test output.
   */
  public static final String DEFAULT_TEST_DIRECTORY = "target/build/data";
  
  /** Filesystem URI used for map-reduce mini-cluster setup */
  private static String fsURI;

  /** A set of ports that have been claimed using {@link #randomFreePort()}. */
  private static final Set<Integer> takenRandomPorts = new HashSet<Integer>();

  /** Compression algorithms to use in parameterized JUnit 4 tests */
  public static final List<Object[]> COMPRESSION_ALGORITHMS_PARAMETERIZED =
    Arrays.asList(new Object[][] {
      { Compression.Algorithm.NONE },
      { Compression.Algorithm.GZ }
    });

  /** This is for unit tests parameterized with a single boolean. */
  public static final List<Object[]> BOOLEAN_PARAMETERIZED =
      Arrays.asList(new Object[][] {
          { new Boolean(false) },
          { new Boolean(true) }
      });

  /** Compression algorithms to use in testing */
  public static final Compression.Algorithm[] COMPRESSION_ALGORITHMS =
      new Compression.Algorithm[] {
        Compression.Algorithm.NONE, Compression.Algorithm.GZ
      };

  /**
   * Create all combinations of Bloom filters and compression algorithms for
   * testing.
   */
  private static List<Object[]> bloomAndCompressionCombinations() {
    List<Object[]> configurations = new ArrayList<Object[]>();
    for (Compression.Algorithm comprAlgo :
         HBaseTestingUtility.COMPRESSION_ALGORITHMS) {
      for (StoreFile.BloomType bloomType : StoreFile.BloomType.values()) {
        configurations.add(new Object[] { comprAlgo, bloomType });
      }
    }
    return Collections.unmodifiableList(configurations);
  }

  public static final Collection<Object[]> BLOOM_AND_COMPRESSION_COMBINATIONS =
      bloomAndCompressionCombinations();

  public HBaseTestingUtility() {
    this(HBaseConfiguration.create());
  }

  public HBaseTestingUtility(Configuration conf) {
    this.conf = conf;
    cacheConf = new CacheConfig(conf);
  }

  /**
   * @return Instance of Configuration.
   */
  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * @return Where to write test data on local filesystem; usually
   * {@link #DEFAULT_TEST_DIRECTORY}
   * @see #setupClusterTestBuildDir()
   */
  public Path getTestDir() {
    setupClusterTestBuildDir();
    return new Path(clusterTestBuildDir.getPath());
  }

  /**
   * @param subdirName
   * @return Path to a subdirectory named <code>subdirName</code> under
   * {@link #getTestDir()}.
   * @see #setupClusterTestBuildDir()
   */
  public Path getTestDir(final String subdirName) {
    return new Path(getTestDir(), subdirName);
  }

  /**
   * Home our cluster in a dir under target/test.  Give it a random name
   * so can have many concurrent clusters running if we need to.  Need to
   * amend the test.build.data System property.  Its what minidfscluster bases
   * it data dir on.  Moding a System property is not the way to do concurrent
   * instances -- another instance could grab the temporary
   * value unintentionally -- but not anything can do about it at moment;
   * single instance only is how the minidfscluster works.
   * @return The calculated cluster test build directory.
   */
  public void setupClusterTestBuildDir() {
    if (clusterTestBuildDir != null) {
      return;
    }

    String randomStr = UUID.randomUUID().toString();
    String dirStr = new Path(DEFAULT_TEST_DIRECTORY, randomStr).toString();
    clusterTestBuildDir = new File(dirStr).getAbsoluteFile();
    clusterTestBuildDir.mkdirs();
    clusterTestBuildDir.deleteOnExit();
    conf.set(TEST_DIRECTORY_KEY, clusterTestBuildDir.getPath());
    LOG.info("Created new mini-cluster data directory: " + clusterTestBuildDir);
  }

  /**
   * Start a minidfscluster.
   * Can only create one.
   * @param dir Where to home your dfs cluster.
   * @param servers How many DNs to start.
   * @see {@link #shutdownMiniDFSCluster()}
   * @return The mini dfs cluster created.
   */
  public MiniDFSCluster startMiniDFSCluster(int servers) throws IOException {
    createDirsAndSetProperties();
    this.dfsCluster = new MiniDFSCluster(0, this.conf, servers, true, true,
      true, null, null, null, null);
    return this.dfsCluster;
  }

  public MiniDFSCluster startMiniDFSClusterForTestHLog(int namenodePort) throws IOException {
    createDirsAndSetProperties();
    dfsCluster = new MiniDFSCluster(namenodePort, conf, 5, false, true, true, null,
        null, null, null);
    return dfsCluster;
  }

  /** This is used before starting HDFS and map-reduce mini-clusters */
  private void createDirsAndSetProperties() {
    setupClusterTestBuildDir();
    System.setProperty(TEST_DIRECTORY_KEY, clusterTestBuildDir.getPath());
    createDirAndSetProperty("cache_data", "test.cache.data");
    createDirAndSetProperty("hadoop_tmp", "hadoop.tmp.dir");
    hadoopLogDir = createDirAndSetProperty("hadoop_logs", "hadoop.log.dir");
    createDirAndSetProperty("mapred_output", "mapred.output.dir");
    createDirAndSetProperty("mapred_local", "mapred.local.dir");
    createDirAndSetProperty("mapred_system", "mapred.system.dir");
    createDirAndSetProperty("fsimage", "dfs.name.dir");
    createDirAndSetProperty("fsedit", "dfs.name.edits.dir");
  }

  /**
   * Shuts down instance created by call to {@link #startMiniDFSCluster(int)}
   * or does nothing.
   * @throws IOException 
   * @throws Exception
   */
  public void shutdownMiniDFSCluster() throws IOException {
    if (this.dfsCluster != null) {
      // The below throws an exception per dn, AsynchronousCloseException.
      this.dfsCluster.shutdown();
      dfsCluster = null;
    }
  }

  /**
   * Call this if you only want a zk cluster.
   * @see #startMiniZKCluster() if you want zk + dfs + hbase mini cluster.
   * @throws Exception
   * @see #shutdownMiniZKCluster()
   * @return zk cluster started.
   */
  public MiniZooKeeperCluster startMiniZKCluster() throws Exception {
    setupClusterTestBuildDir();
    return startMiniZKCluster(clusterTestBuildDir);
  }

  private MiniZooKeeperCluster startMiniZKCluster(final File dir)
      throws IOException, InterruptedException {
    if (this.zkCluster != null) {
      throw new IOException("Cluster already running at " + dir);
    }
    this.zkCluster = new MiniZooKeeperCluster();
    int clientPort = this.zkCluster.startup(dir);
    this.conf.set(HConstants.ZOOKEEPER_CLIENT_PORT,
        Integer.toString(clientPort));
    return this.zkCluster;
  }

  /**
   * Shuts down zk cluster created by call to {@link #startMiniZKCluster(File)}
   * or does nothing.
   * @throws IOException
   * @see #startMiniZKCluster()
   */
  public void shutdownMiniZKCluster() throws IOException {
    if (this.zkCluster != null) {
      this.zkCluster.shutdown();
      zkCluster = null;
    }
  }

  /**
   * Start up a minicluster of hbase, dfs, and zookeeper.
   * @throws Exception
   * @return Mini hbase cluster instance created.
   * @see {@link #shutdownMiniDFSCluster()}
   */
  public MiniHBaseCluster startMiniCluster() throws Exception {
    return startMiniCluster(1, 1);
  }

  /**
   * Start up a minicluster of hbase, optionally dfs, and zookeeper.
   * Modifies Configuration.  Homes the cluster data directory under a random
   * subdirectory in a directory under System property test.build.data.
   * Directory is cleaned up on exit.
   * @param numSlaves Number of slaves to start up.  We'll start this many
   * datanodes and regionservers.  If numSlaves is > 1, then make sure
   * hbase.regionserver.info.port is -1 (i.e. no ui per regionserver) otherwise
   * bind errors.
   * @throws Exception
   * @see {@link #shutdownMiniCluster()}
   * @return Mini hbase cluster instance created.
   */
  public MiniHBaseCluster startMiniCluster(final int numSlaves)
      throws IOException, InterruptedException {
    return startMiniCluster(1, numSlaves);
  }

  public MiniHBaseCluster startMiniCluster(final int numMasters,
      final int numSlaves) throws IOException, InterruptedException {
    return startMiniCluster(numMasters, numSlaves, MiniHBaseCluster.MiniHBaseClusterRegionServer.class);
  }

  /**
   * Start up a minicluster of hbase, optionally dfs, and zookeeper.
   * Modifies Configuration.  Homes the cluster data directory under a random
   * subdirectory in a directory under System property test.build.data.
   * Directory is cleaned up on exit.
   * @param numMasters Number of masters to start up.  We'll start this many
   * hbase masters.  If numMasters > 1, you can find the active/primary master
   * with {@link MiniHBaseCluster#getMaster()}.
   * @param numSlaves Number of slave servers to start up.  We'll start this
   * many datanodes and regionservers.  If servers is > 1, then make sure
   * hbase.regionserver.info.port is -1 (i.e. no ui per regionserver) otherwise
   * bind errors.
   * @throws Exception
   * @see {@link #shutdownMiniCluster()}
   * @return Mini hbase cluster instance created.
   */
  public MiniHBaseCluster startMiniCluster(final int numMasters,
      final int numSlaves, final Class<? extends HRegionServer> regionServerClass) throws IOException, InterruptedException {
    LOG.info("Starting up minicluster");
    // If we already put up a cluster, fail.
    if (miniClusterRunning) {
      throw new IllegalStateException("A mini-cluster is already running");
    }
    miniClusterRunning = true;

    // Make a new random dir to home everything in.  Set it as system property.
    // minidfs reads home from system property.
    setupClusterTestBuildDir();
    System.setProperty(TEST_DIRECTORY_KEY, this.clusterTestBuildDir.getPath());
    // Bring up mini dfs cluster. This spews a bunch of warnings about missing
    // scheme. Complaints are 'Scheme is undefined for build/test/data/dfs/name1'.
    startMiniDFSCluster(numSlaves);

    // Mangle conf so fs parameter points to minidfs we just started up
    FileSystem fs = this.dfsCluster.getFileSystem();
    this.conf.set("fs.defaultFS", fs.getUri().toString());
    // Do old style too just to be safe.
    this.conf.set("fs.default.name", fs.getUri().toString());
    this.dfsCluster.waitClusterUp();

    // Start up a zk cluster.
    if (this.zkCluster == null) {
      startMiniZKCluster(this.clusterTestBuildDir);
    }

    // Now do the mini hbase cluster.  Set the hbase.rootdir in config.
    Path hbaseRootdir = fs.makeQualified(fs.getHomeDirectory());
    this.conf.set(HConstants.HBASE_DIR, hbaseRootdir.toString());
    fs.mkdirs(hbaseRootdir);
    FSUtils.setVersion(fs, hbaseRootdir);
    startMiniHBaseCluster(numMasters, numSlaves, regionServerClass);

    // Don't leave here till we've done a successful scan of the .META.
    HTable t = null;
    for (int i = 0; i < 10; ++i) { 
      try {
        t = new HTable(this.conf, HConstants.META_TABLE_NAME);
        for (Result result : t.getScanner(new Scan())) {
          LOG.debug("Successfully read meta entry: " + result);
        }
        break;
      } catch (NoServerForRegionException ex) {
        LOG.error("META is not online, sleeping");
        Threads.sleepWithoutInterrupt(2000);
      }
    }
    if (t == null) {
      throw new IOException("Could not open META on cluster startup");
    }

    ResultScanner s = t.getScanner(new Scan());
    while (s.next() != null) continue;
    LOG.info("Minicluster is up");
    return this.hbaseCluster;
  }


  public void startMiniHBaseCluster(final int numMasters, final int numSlaves)
    throws IOException, InterruptedException {
    startMiniHBaseCluster(numMasters, numSlaves, MiniHBaseCluster.MiniHBaseClusterRegionServer.class);
  }

  public void startMiniHBaseCluster(final int numMasters, final int numSlaves,
    Class<? extends HRegionServer> regionServerClass)
  throws IOException, InterruptedException {
    this.hbaseCluster = new MiniHBaseCluster(this.conf, numMasters, numSlaves, regionServerClass);
  }

  /**
   * @return Current mini hbase cluster. Only has something in it after a call
   * to {@link #startMiniCluster()}.
   * @see #startMiniCluster()
   */
  public MiniHBaseCluster getMiniHBaseCluster() {
    return this.hbaseCluster;
  }

  /**
   * @throws IOException
   * @see {@link #startMiniCluster(int)}
   */
  public void shutdownMiniCluster() throws IOException {
    LOG.info("Shutting down minicluster");
    shutdownMiniHBaseCluster();
    shutdownMiniZKCluster();
    shutdownMiniDFSCluster();

    cleanupTestDir();
    miniClusterRunning = false;
    LOG.info("Minicluster is down");
  }

  /**
   * Shutdown HBase mini cluster.  Does not shutdown zk or dfs if running.
   * @throws IOException
   */
  public void shutdownMiniHBaseCluster() throws IOException {
    if (this.hbaseCluster != null) {
      this.hbaseCluster.shutdown();
      // Wait till hbase is down before going on to shutdown zk.
      this.hbaseCluster.join();
      this.hbaseCluster = null;
    }
  }

  /**
   * Flushes all caches in the mini hbase cluster
   * @throws IOException
   */
  public void flush() throws IOException {
    this.hbaseCluster.flushcache();
  }

  /**
   * Flushes all caches in the mini hbase cluster
   * @throws IOException
   */
  public void flush(byte [] tableName) throws IOException {
    this.hbaseCluster.flushcache(tableName);
  }


  /**
   * Create a table.
   * @param tableName
   * @param family
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(byte[] tableName, byte[] family)
  throws IOException{
    return createTable(tableName, new byte[][]{family});
  }


  public HTable createTable(byte[] tableName, byte[][] families,
      int numVersions, byte[] startKey, byte[] endKey, int numRegions)
  throws IOException{
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for (byte[] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family)
          .setMaxVersions(numVersions);
      desc.addFamily(hcd);
    }
    (new HBaseAdmin(getConfiguration())).createTable(desc, startKey,
        endKey, numRegions);
    return new HTable(getConfiguration(), tableName);
  }


  /**
   * Create a table.
   * @param tableName
   * @param families
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(byte[] tableName, byte[][] families)
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for(byte[] family : families) {
      desc.addFamily(new HColumnDescriptor(family));
    }
    (new HBaseAdmin(getConfiguration())).createTable(desc);
    return new HTable(getConfiguration(), tableName);
  }

  /**
   * Create a table
   *
   * @param tableName
   * @param columnDescriptors
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(byte[] tableName,
      HColumnDescriptor[] columnDescriptors) throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for (HColumnDescriptor columnDescriptor : columnDescriptors) {
      desc.addFamily(columnDescriptor);
    }
    HBaseAdmin admin = new HBaseAdmin(getConfiguration());
    try {
      admin.createTable(desc);
      return new HTable(getConfiguration(), tableName);
    } finally {
      admin.close();
    }
  }

  /**
   * Create a table.
   * @param tableName
   * @param family
   * @param numVersions
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(byte[] tableName, byte[] family, int numVersions)
  throws IOException {
    return createTable(tableName, new byte[][]{family}, numVersions);
  }

  /**
   * Create a table.
   * @param tableName
   * @param families
   * @param numVersions
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(byte[] tableName, byte[][] families,
      int numVersions)
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for (byte[] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family)
          .setMaxVersions(numVersions);
      desc.addFamily(hcd);
    }
    getHBaseAdmin().createTable(desc);
    return new HTable(new Configuration(getConfiguration()), tableName);
  }

  /**
   * Create a table.
   * @param tableName
   * @param families
   * @param numVersions
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(byte[] tableName, byte[][] families,
    int numVersions, int blockSize) throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for (byte[] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family)
          .setMaxVersions(numVersions)
          .setBlocksize(blockSize);
      desc.addFamily(hcd);
    }
    (new HBaseAdmin(getConfiguration())).createTable(desc);
    return new HTable(getConfiguration(), tableName);
  }

  /**
   * Create a table.
   * @param tableName
   * @param families
   * @param numVersions
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(byte[] tableName, byte[][] families,
      int[] numVersions)
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    int i = 0;
    for (byte[] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family)
          .setMaxVersions(numVersions[i]);
      desc.addFamily(hcd);
      i++;
    }
    (new HBaseAdmin(getConfiguration())).createTable(desc);
    return new HTable(getConfiguration(), tableName);
  }

  public void deleteTable(byte[] tableName) throws IOException {
    HBaseAdmin hba = new HBaseAdmin(getConfiguration());
    hba.disableTable(tableName);
    hba.deleteTable(tableName);
  }

  /**
   * Provide an existing table name to truncate
   * @param tableName existing table
   * @return HTable to that new table
   * @throws IOException
   */
  public HTable truncateTable(byte [] tableName) throws IOException {
    HTable table = new HTable(getConfiguration(), tableName);
    Scan scan = new Scan();
    ResultScanner resScan = table.getScanner(scan);
    for(Result res : resScan) {
      Delete del = new Delete(res.getRow());
      table.delete(del);
    }
    return table;
  }

  /**
   * Load table with rows from 'aaa' to 'zzz'.
   * @param t Table
   * @param f Family
   * @return Count of rows loaded.
   * @throws IOException
   */
  public int loadTable(final HTable t, final byte[] f) throws IOException {
    t.setAutoFlush(false);
    byte[] k = new byte[3];
    int rowCount = 0;
    for (byte b1 = 'a'; b1 <= 'z'; b1++) {
      for (byte b2 = 'a'; b2 <= 'z'; b2++) {
        for (byte b3 = 'a'; b3 <= 'z'; b3++) {
          k[0] = b1;
          k[1] = b2;
          k[2] = b3;
          Put put = new Put(k);
          put.add(f, null, k);
          t.put(put);
          rowCount++;
        }
      }
    }
    t.flushCommits();
    return rowCount;
  }

  public int loadTable2(final HTable t, final byte[] f) throws IOException {
    t.setAutoFlush(false);
    byte[] k = new byte[4];
    int rowCount = 0;
    for (byte b0 = 'a'; b0 <= 'z'; b0++) {
      for (byte b1 = 'a'; b1 <= 'z'; b1++) {
        for (byte b2 = 'a'; b2 <= 'z'; b2++) {
          for (byte b3 = 'a'; b3 <= 'z'; b3++) {
            k[0] = b0;
            k[1] = b1;
            k[2] = b2;
            k[3] = b3;
            Put put = new Put(k);
            put.add(f, null, k);
            t.put(put);
            rowCount++;
            if (rowCount % 6000 == 0) {
              t.flushCommits();
              this.flush(t.getTableName());
            }
          }
        }
      }
    }
    t.flushCommits();
    return rowCount;
  }

  /**
   * Return the number of rows in the given table.
   */
  public int countRows(final HTable table) throws IOException {
    Scan scan = new Scan();
    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (@SuppressWarnings("unused") Result res : results) {
      count++;
    }
    results.close();
    return count;
  }

  /**
   * Return an md5 digest of the entire contents of a table.
   */
  public String checksumRows(final HTable table) throws Exception {
    Scan scan = new Scan();
    ResultScanner results = table.getScanner(scan);
    MessageDigest digest = MessageDigest.getInstance("MD5");
    for (Result res : results) {
      digest.update(res.getRow());
    }
    results.close();
    return digest.toString();
  }

  /**
   * Creates many regions names "aaa" to "zzz".
   *
   * @param table  The table to use for the data.
   * @param columnFamily  The family to insert the data into.
   * @return count of regions created.
   * @throws IOException When creating the regions fails.
   */
  public int createMultiRegions(HTable table, byte[] columnFamily)
  throws IOException {
    return createMultiRegions(getConfiguration(), table, columnFamily);
  }

  /**
   * Creates many regions names "aaa" to "zzz".
   * @param c Configuration to use.
   * @param table  The table to use for the data.
   * @param columnFamily  The family to insert the data into.
   * @return count of regions created.
   * @throws IOException When creating the regions fails.
   */
  public int createMultiRegions(final Configuration c, final HTable table,
      final byte[] columnFamily)
  throws IOException {
    return createMultiRegions(c, table, columnFamily, getTmpKeys());
  }

  public byte[][] getTmpKeys() {
    byte[][] KEYS = {
        HConstants.EMPTY_BYTE_ARRAY, Bytes.toBytes("bbb"),
        Bytes.toBytes("ccc"), Bytes.toBytes("ddd"), Bytes.toBytes("eee"),
        Bytes.toBytes("fff"), Bytes.toBytes("ggg"), Bytes.toBytes("hhh"),
        Bytes.toBytes("iii"), Bytes.toBytes("jjj"), Bytes.toBytes("kkk"),
        Bytes.toBytes("lll"), Bytes.toBytes("mmm"), Bytes.toBytes("nnn"),
        Bytes.toBytes("ooo"), Bytes.toBytes("ppp"), Bytes.toBytes("qqq"),
        Bytes.toBytes("rrr"), Bytes.toBytes("sss"), Bytes.toBytes("ttt"),
        Bytes.toBytes("uuu"), Bytes.toBytes("vvv"), Bytes.toBytes("www"),
        Bytes.toBytes("xxx"), Bytes.toBytes("yyy")
      };
    return KEYS;
  }

  public int createMultiRegions(final Configuration c, final HTable table,
      final byte[] columnFamily, byte [][] startKeys)
  throws IOException {
    return createMultiRegionsWithFavoredNodes(c,table,columnFamily,
        new Pair<byte[][], byte[][]>(startKeys, null));
  }

  public int createMultiRegionsWithFavoredNodes(final Configuration c, final HTable table,
      final byte[] columnFamily, Pair<byte[][], byte[][]> startKeysAndFavNodes)
  throws IOException {
    byte[][] startKeys = startKeysAndFavNodes.getFirst();
    byte[][] favNodes = startKeysAndFavNodes.getSecond();
    Arrays.sort(startKeys, Bytes.BYTES_COMPARATOR);
    HTable meta = new HTable(c, HConstants.META_TABLE_NAME);
    HTableDescriptor htd = table.getTableDescriptor();
    if(!htd.hasFamily(columnFamily)) {
      HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
      htd.addFamily(hcd);
    }
    // remove empty region - this is tricky as the mini cluster during the test
    // setup already has the "<tablename>,,123456789" row with an empty start
    // and end key. Adding the custom regions below adds those blindly,
    // including the new start region from empty to "bbb". lg
    List<byte[]> rows = getMetaTableRows(htd.getName());
    // add custom ones
    int count = 0;
    for (int i = 0; i < startKeys.length; i++) {
      int j = (i + 1) % startKeys.length;
      HRegionInfo hri = new HRegionInfo(table.getTableDescriptor(),
        startKeys[i], startKeys[j]);
      Put put = new Put(hri.getRegionName());
      put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(hri));
      if (favNodes != null) {
        put.add(HConstants.CATALOG_FAMILY, HConstants.FAVOREDNODES_QUALIFIER,
            favNodes[i]);
      }
      meta.put(put);
      LOG.info("createMultiRegions: inserted " + hri.toString());
      count++;
    }
    // see comment above, remove "old" (or previous) single region
    for (byte[] row : rows) {
      LOG.info("createMultiRegions: deleting meta row -> " +
        Bytes.toStringBinary(row));
      meta.delete(new Delete(row));
    }
    // flush cache of regions
    HConnection conn = table.getConnectionAndResetOperationContext();
    conn.clearRegionCache();
    return count;
  }

  /**
   * Returns all rows from the .META. table.
   *
   * @throws IOException When reading the rows fails.
   */
  public List<byte[]> getMetaTableRows() throws IOException {
    HTable t = new HTable(this.conf, HConstants.META_TABLE_NAME);
    List<byte[]> rows = new ArrayList<byte[]>();
    ResultScanner s = t.getScanner(new Scan());
    for (Result result : s) {
      LOG.info("getMetaTableRows: row -> " +
        Bytes.toStringBinary(result.getRow()));
      rows.add(result.getRow());
    }
    s.close();
    return rows;
  }

  /**
   * Returns all rows from the .META. table for a given user table
   *
   * @throws IOException When reading the rows fails.
   */
  public List<byte[]> getMetaTableRows(byte[] tableName) throws IOException {
    HTable t = new HTable(this.conf, HConstants.META_TABLE_NAME);
    List<byte[]> rows = new ArrayList<byte[]>();
    ResultScanner s = t.getScanner(new Scan());
    for (Result result : s) {
      HRegionInfo info = Writables.getHRegionInfo(
          result.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER));
      HTableDescriptor desc = info.getTableDesc();
      if (Bytes.compareTo(desc.getName(), tableName) == 0) {
        LOG.info("getMetaTableRows: row -> " +
            Bytes.toStringBinary(result.getRow()));
        rows.add(result.getRow());
      }
    }
    s.close();
    return rows;
  }

  /**
   * Tool to get the reference to the region server object that holds the
   * region of the specified user table.
   * It first searches for the meta rows that contain the region of the
   * specified table, then gets the index of that RS, and finally retrieves
   * the RS's reference.
   * @param tableName user table to lookup in .META.
   * @return region server that holds it, null if the row doesn't exist
   * @throws IOException
   */
  public HRegionServer getRSForFirstRegionInTable(byte[] tableName)
      throws IOException {
    List<byte[]> metaRows = getMetaTableRows(tableName);
    if (metaRows == null || metaRows.isEmpty()) {
      return null;
    }
    LOG.debug("Found " + metaRows.size() + " rows for table " +
      Bytes.toString(tableName));
    byte [] firstrow = metaRows.get(0);
    LOG.debug("FirstRow=" + Bytes.toString(firstrow));
    int index = hbaseCluster.getServerWith(firstrow);
    return hbaseCluster.getRegionServerThreads().get(index).getRegionServer();
  }

  /**
   * Get the RegionServer which hosts a region with the given region name.
   * @param regionName
   * @return
   */
  public HRegionServer getRSWithRegion(byte[] regionName) {
    int index = hbaseCluster.getServerWith(regionName);
    if (index == -1) {
      return null;
    }
    return hbaseCluster.getRegionServerThreads().get(index).getRegionServer();
  }

  /**
   * Starts a <code>MiniMRCluster</code> with a default number of
   * <code>TaskTracker</code>'s.
   *
   * @throws IOException When starting the cluster fails.
   */
  public MiniMRCluster startMiniMapReduceCluster() throws IOException {
    startMiniMapReduceCluster(2);
    return mrCluster;
  }

  /**
   * Tasktracker has a bug where changing the hadoop.log.dir system property
   * will not change its internal static LOG_DIR variable.
   */
  private void forceChangeTaskLogDir() {
    Field logDirField;
    try {
      logDirField = TaskLog.class.getDeclaredField("LOG_DIR");
      logDirField.setAccessible(true);

      Field modifiersField = Field.class.getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      modifiersField.setInt(logDirField, logDirField.getModifiers() & ~Modifier.FINAL);

      logDirField.set(null, new File(hadoopLogDir, "userlogs"));
    } catch (SecurityException e) {
      throw new RuntimeException(e);
    } catch (NoSuchFieldException e) {
      // TODO Auto-generated catch block
      throw new RuntimeException(e);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Starts a <code>MiniMRCluster</code>. Call {@link #setFileSystemURI(String)} to use a different
   * filesystem.
   *
   * @param servers  The number of <code>TaskTracker</code>'s to start.
   * @throws IOException When starting the cluster fails.
   */
  private void startMiniMapReduceCluster(final int servers) throws IOException {
    if (mrCluster != null) {
      throw new IllegalStateException("MiniMRCluster is already running");
    }
    LOG.info("Starting mini mapreduce cluster...");
    // These are needed for the new and improved Map/Reduce framework
    Configuration c = getConfiguration();
    
    setupClusterTestBuildDir();
    createDirsAndSetProperties();

    forceChangeTaskLogDir();

    // Allow the user to override FS URI for this map-reduce cluster to use.
    mrCluster = new MiniMRCluster(servers,
        fsURI != null ? fsURI : FileSystem.get(c).getUri().toString(), 1);

    LOG.info("Mini mapreduce cluster started");
    c.set("mapred.job.tracker",
        mrCluster.createJobConf().get("mapred.job.tracker"));
  }

  private String createDirAndSetProperty(final String relPath, String property) {
    String path = clusterTestBuildDir.getPath() + "/" + relPath;
    System.setProperty(property, path);
    conf.set(property, path);
    new File(path).mkdirs();
    LOG.info("Setting " + property + " to " + path + " in system properties and HBase conf");
    return path;
  }

  /**
   * Stops the previously started <code>MiniMRCluster</code>.
   */
  public void shutdownMiniMapReduceCluster() {
    LOG.info("Stopping mini mapreduce cluster...");
    if (mrCluster != null) {
      mrCluster.shutdown();
      mrCluster = null;
    }
    // Restore configuration to point to local jobtracker
    conf.set("mapred.job.tracker", "local");
    LOG.info("Mini mapreduce cluster stopped");
  }

  /**
   * Switches the logger for the given class to DEBUG level.
   *
   * @param clazz  The class for which to switch to debug logging.
   */
  public void enableDebug(Class<?> clazz) {
    Log l = LogFactory.getLog(clazz);
    if (l instanceof Log4JLogger) {
      ((Log4JLogger) l).getLogger().setLevel(org.apache.log4j.Level.DEBUG);
    } else if (l instanceof Jdk14Logger) {
      ((Jdk14Logger) l).getLogger().setLevel(java.util.logging.Level.ALL);
    }
  }

  /**
   * Expire the Master's session
   * @throws Exception
   */
  public void expireMasterSession() throws Exception {
    HMaster master = hbaseCluster.getMaster();
    expireSession(master.getZooKeeperWrapper());
  }

  /**
   * Expire a region server's session
   * @param index which RS
   * @throws Exception
   */
  public void expireRegionServerSession(int index) throws Exception {
    HRegionServer rs = hbaseCluster.getRegionServer(index);
    expireSession(rs.getZooKeeperWrapper());
  }

  public void expireSession(ZooKeeperWrapper nodeZK) throws Exception{
    nodeZK.registerListener(EmptyWatcher.instance);
    String quorumServers = nodeZK.getQuorumServers();
    int sessionTimeout = nodeZK.getSessionTimeout();
    byte[] password = nodeZK.getSessionPassword();
    long sessionID = nodeZK.getSessionID();
    final int sleep = 1000;
    final int maxRetryNum = 60 * 1000 / sleep;  // Wait for a total of up to one minute.
    int retryNum = maxRetryNum;

    ZooKeeper zk = new ZooKeeper(quorumServers,
        sessionTimeout, EmptyWatcher.instance, sessionID, password);
    LOG.debug("Closing ZK");
    zk.close();

    while (!nodeZK.isAborted() && retryNum != 0) {
      Thread.sleep(sleep);
      retryNum--;
    }
    if (retryNum == 0) {
      fail("ZooKeeper is not aborted after " + maxRetryNum + " attempts.");
    }
    LOG.debug("ZooKeeper is closed");
  }

  /**
   * Get the HBase cluster.
   *
   * @return hbase cluster
   */
  public MiniHBaseCluster getHBaseCluster() {
    return hbaseCluster;
  }

  /**
   * Returns a HBaseAdmin instance.
   *
   * @return The HBaseAdmin instance.
   * @throws MasterNotRunningException
   */
  public HBaseAdmin getHBaseAdmin() throws MasterNotRunningException {
    if (hbaseAdmin == null) {
      hbaseAdmin = new HBaseAdmin(getConfiguration());
    }
    return hbaseAdmin;
  }

  /**
   * Closes the named region.
   *
   * @param regionName  The region to close.
   * @throws IOException
   */
  public void closeRegion(String regionName) throws IOException {
    closeRegion(Bytes.toBytes(regionName));
  }

  /**
   * Closes the named region.
   *
   * @param regionName  The region to close.
   * @throws IOException
   */
  public void closeRegion(byte[] regionName) throws IOException {
    HBaseAdmin admin = getHBaseAdmin();
    admin.closeRegion(regionName, (Object[]) null);
  }

  /**
   * Closes the region containing the given row.
   *
   * @param row  The row to find the containing region.
   * @param table  The table to find the region.
   * @throws IOException
   */
  public void closeRegionByRow(String row, HTable table) throws IOException {
    closeRegionByRow(Bytes.toBytes(row), table);
  }

  /**
   * Closes the region containing the given row.
   *
   * @param row  The row to find the containing region.
   * @param table  The table to find the region.
   * @throws IOException
   */
  public void closeRegionByRow(byte[] row, HTable table) throws IOException {
    HRegionLocation hrl = table.getRegionLocation(row);
    closeRegion(hrl.getRegionInfo().getRegionName());
  }

  public MiniZooKeeperCluster getZkCluster() {
    return zkCluster;
  }

  public void setZkCluster(MiniZooKeeperCluster zkCluster) {
    this.zkCluster = zkCluster;
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkCluster.getClientPort());
  }

  public MiniDFSCluster getDFSCluster() {
    return dfsCluster;
  }

  public FileSystem getTestFileSystem() throws IOException {
    return FileSystem.get(conf);
  }

  public void waitTableAvailable(byte[] table, long timeoutMillis)
  throws InterruptedException, IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    long startWait = System.currentTimeMillis();
    while (!admin.isTableAvailable(table)) {
      assertTrue("Timed out waiting for table " + Bytes.toStringBinary(table),
          System.currentTimeMillis() - startWait < timeoutMillis);
      Thread.sleep(500);
    }
  }

  /**
   * Make sure that at least the specified number of region servers
   * are running
   * @param num minimum number of region servers that should be running
   * @throws IOException
   */
  public void ensureSomeRegionServersAvailable(final int num)
      throws IOException {
    if (this.getHBaseCluster().getLiveRegionServerThreads().size() < num) {
      // Need at least "num" servers.
      LOG.info("Started new server=" +
        this.getHBaseCluster().startRegionServer());

    }
  }

  /**
   * This method clones the passed <code>c</code> configuration setting a new
   * user into the clone.  Use it getting new instances of FileSystem.  Only
   * works for DistributedFileSystem.
   * @param c Initial configuration
   * @param differentiatingSuffix Suffix to differentiate this user from others.
   * @return A new configuration instance with a different user set into it.
   * @throws IOException
   */
  public static Configuration setDifferentUser(final Configuration c)
  throws IOException {
    FileSystem currentfs = FileSystem.get(c);
    Preconditions.checkArgument(currentfs instanceof DistributedFileSystem);
    // Else distributed filesystem.  Make a new instance per daemon.  Below
    // code is taken from the AppendTestUtil over in hdfs.
    Configuration c2 = new Configuration(c);
    String username = UserGroupInformation.getCurrentUGI().getUserName() + (USERNAME_SUFFIX++);
    UnixUserGroupInformation.saveToConf(c2,
      UnixUserGroupInformation.UGI_PROPERTY_NAME,
      new UnixUserGroupInformation(username, new String[]{"supergroup"}));
    return c2;
  }

  /**
   * Set soft and hard limits in namenode.
   * You'll get a NPE if you call before you've started a minidfscluster.
   * @param soft Soft limit
   * @param hard Hard limit
   * @throws NoSuchFieldException
   * @throws SecurityException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   */
  public void setNameNodeNameSystemLeasePeriod(final int soft, final int hard)
  throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
    // TODO: If 0.20 hadoop do one thing, if 0.21 hadoop do another.
    // Not available in 0.20 hdfs.  Use reflection to make it happen.

    // private NameNode nameNode;
    NameNode nn = null;
    try {
      Field field = this.dfsCluster.getClass().getDeclaredField("nameNode");
      field.setAccessible(true);
      nn = (NameNode)field.get(this.dfsCluster);
    } catch (NoSuchFieldException ne) {
      // The latest version of HDFS has a nice API for this.
      nn = dfsCluster.getNameNode();
    }
    nn.namesystem.leaseManager.setLeasePeriod(100, 50000);
  }

  /**
   * Set maxRecoveryErrorCount in DFSClient.  In 0.20 pre-append its hard-coded to 5 and
   * makes tests linger.  Here is the exception you'll see:
   * <pre>
   * 2010-06-15 11:52:28,511 WARN  [DataStreamer for file /hbase/.logs/hlog.1276627923013 block blk_928005470262850423_1021] hdfs.DFSClient$DFSOutputStream(2657): Error Recovery for block blk_928005470262850423_1021 failed  because recovery from primary datanode 127.0.0.1:53683 failed 4 times.  Pipeline was 127.0.0.1:53687, 127.0.0.1:53683. Will retry...
   * </pre>
   * @param stream A DFSClient.DFSOutputStream.
   * @param max
   * @throws NoSuchFieldException
   * @throws SecurityException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   */
  public static void setMaxRecoveryErrorCount(final OutputStream stream,
      final int max) {
    try {
      Class<?> [] clazzes = DFSClient.class.getDeclaredClasses();
      for (Class<?> clazz: clazzes) {
        String className = clazz.getSimpleName();
        if (className.equals("DFSOutputStream")) {
          if (clazz.isInstance(stream)) {
            Field maxRecoveryErrorCountField =
              stream.getClass().getDeclaredField("maxRecoveryErrorCount");
            maxRecoveryErrorCountField.setAccessible(true);
            maxRecoveryErrorCountField.setInt(stream, max);
            break;
          }
        }
      }
    } catch (Exception e) {
      LOG.info("Could not set max recovery field", e);
    }
  }

  /**
   *
   * @param expectedUserRegions The number of regions which are not META or ROOT
   */
  public void waitForOnlineRegionsToBeAssigned(int expectedUserRegions) {
    int actualRegions;

    do {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {} // Ignore

      actualRegions = 0;

      for (HRegionServer server : getOnlineRegionServers()) {
        for (HRegion region : server.getOnlineRegions()) {
          if (!region.getRegionInfo().isMetaRegion() && !region.getRegionInfo().isRootRegion()) {
            actualRegions += 1;
          }
        }
      }
    } while (actualRegions != expectedUserRegions);

  }

  public List<HRegionServer> getOnlineRegionServers() {
    List<HRegionServer> list = new ArrayList<HRegionServer>();
    for (JVMClusterUtil.RegionServerThread rst : this.getMiniHBaseCluster().getRegionServerThreads()) {
      if (rst.getRegionServer().isOnline() && !rst.getRegionServer().isStopped()) {
        list.add(rst.getRegionServer());
      }
    }
    return list;
  }


  /**
   * Wait until <code>countOfRegion</code> in .META. have a non-empty
   * info:server.  This means all regions have been deployed, master has been
   * informed and updated .META. with the regions deployed server.
   * @param countOfRegions How many regions in .META.
   * @throws IOException
   */
  public void waitUntilAllRegionsAssigned(final int countOfRegions)
  throws IOException {
    HTable meta = new HTable(getConfiguration(), HConstants.META_TABLE_NAME);
    HConnection connection = ServerConnectionManager.getConnection(conf);
TOP_LOOP:
    while (true) {
      int rows = 0;
      Scan scan = new Scan();
      scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
      ResultScanner s;
      try {
        s = meta.getScanner(scan);
      } catch (RetriesExhaustedException ex) {
        // This function has infinite patience.
        Threads.sleepWithoutInterrupt(2000);
        continue;
      } catch (PreemptiveFastFailException ex) {
        // Be more patient
        Threads.sleepWithoutInterrupt(2000);
        continue;
      }
      Map<String, HRegionInfo[]> regionAssignment =
          new HashMap<String, HRegionInfo[]>();
REGION_LOOP:
      for (Result r = null; (r = s.next()) != null;) {
        byte [] b =
          r.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
        if (b == null || b.length <= 0) break;
        // Make sure the regionserver really has this region.
        String serverAddress = Bytes.toString(b);
        if (!regionAssignment.containsKey(serverAddress)) {
          HRegionInterface hri =
            connection.getHRegionConnection(new HServerAddress(serverAddress),
                false);
          HRegionInfo[] regions;
          try {
            regions = hri.getRegionsAssignment();
          } catch (IOException ex) {
            LOG.info("Could not contact regionserver " + serverAddress);
            Threads.sleepWithoutInterrupt(1000);
            continue TOP_LOOP;
          }
          regionAssignment.put(serverAddress, regions);
        }
        String regionName = Bytes.toString(r.getRow());
        for (HRegionInfo regInfo : regionAssignment.get(serverAddress)) {
          String regNameOnRS = Bytes.toString(regInfo.getRegionName());
          if (regNameOnRS.equals(regionName)) {
            rows++;
            continue REGION_LOOP;
          }
        }
      }
      s.close();
      // If I get to here and all rows have a Server, then all have been assigned.
      if (rows >= countOfRegions)
        break;
      LOG.info("Found " + rows + " open regions, waiting for " +
          countOfRegions);
      Threads.sleepWithoutInterrupt(1000);
    }
  }

  /**
   * Do a small get/scan against one store. This is required because store
   * has no actual methods of querying itself, and relies on StoreScanner.
   */
  public static List<KeyValue> getFromStoreFile(Store store,
                                                Get get) throws IOException {
    MultiVersionConsistencyControl.resetThreadReadPoint();
    Scan scan = new Scan(get);
    InternalScanner scanner = (InternalScanner) store.getScanner(scan,
        scan.getFamilyMap().get(store.getFamily().getName()));

    List<KeyValue> result = new ArrayList<KeyValue>();
    scanner.next(result);
    if (!result.isEmpty()) {
      // verify that we are on the row we want:
      KeyValue kv = result.get(0);
      if (!Bytes.equals(kv.getRow(), get.getRow())) {
        result.clear();
      }
    }
    return result;
  }

  /**
   * Do a small get/scan against one store. This is required because store
   * has no actual methods of querying itself, and relies on StoreScanner.
   */
  public static List<KeyValue> getFromStoreFile(Store store,
                                                byte [] row,
                                                NavigableSet<byte[]> columns
                                                ) throws IOException {
    Get get = new Get(row);
    Map<byte[], NavigableSet<byte[]>> s = get.getFamilyMap();
    s.put(store.getFamily().getName(), columns);

    return getFromStoreFile(store,get);
  }

  public static void assertKVListsEqual(String additionalMsg,
      final List<KeyValue> expected,
      final List<KeyValue> actual) {
    final int eLen = expected.size();
    final int aLen = actual.size();
    final int minLen = Math.min(eLen, aLen);

    int i;
    for (i = 0; i < minLen
        && KeyValue.COMPARATOR.compare(expected.get(i), actual.get(i)) == 0;
        ++i) {}

    if (additionalMsg == null) {
      additionalMsg = "";
    }
    if (!additionalMsg.isEmpty()) {
      additionalMsg = ". " + additionalMsg;
    }

    if (eLen != aLen || i != minLen) {
      throw new AssertionError(
          "Expected and actual KV arrays differ at position " + i + ": " +
          safeGetAsStr(expected, i) + " (length " + eLen +") vs. " +
          safeGetAsStr(actual, i) + " (length " + aLen + ")" + additionalMsg);
    }
  }

  private static <T> String safeGetAsStr(List<T> lst, int i) {
    if (0 <= i && i < lst.size()) {
      return lst.get(i).toString();
    } else {
      return "<out_of_range>";
    }
  }

  /** Creates a random table with the given parameters */
  public HTable createRandomTable(String tableName,
      final Collection<String> families,
      final int maxVersions,
      final int numColsPerRow,
      final int numFlushes,
      final int numRegions,
      final int numRowsPerFlush)
      throws IOException, InterruptedException {

    LOG.info("\n\nCreating random table " + tableName + " with " + numRegions +
        " regions, " + numFlushes + " storefiles per region, " +
        numRowsPerFlush + " rows per flush, maxVersions=" +  maxVersions +
        "\n");

    final Random rand = new Random(tableName.hashCode() * 17L + 12938197137L);
    final int numCF = families.size();
    final byte[][] cfBytes = new byte[numCF][];
    final byte[] tableNameBytes = Bytes.toBytes(tableName);

    {
      int cfIndex = 0;
      for (String cf : families) {
        cfBytes[cfIndex++] = Bytes.toBytes(cf);
      }
    }

    final int actualStartKey = 0;
    final int actualEndKey = Integer.MAX_VALUE;
    final int keysPerRegion = (actualEndKey - actualStartKey) / numRegions;
    final int splitStartKey = actualStartKey + keysPerRegion;
    final int splitEndKey = actualEndKey - keysPerRegion;
    final String keyFormat = "%08x";
    final HTable table = createTable(tableNameBytes, cfBytes,
        maxVersions,
        Bytes.toBytes(String.format(keyFormat, splitStartKey)),
        Bytes.toBytes(String.format(keyFormat, splitEndKey)),
        numRegions);

    if (hbaseCluster != null) {
      hbaseCluster.flushcache(HConstants.META_TABLE_NAME);
    }

    for (int iFlush = 0; iFlush < numFlushes; ++iFlush) {
      for (int iRow = 0; iRow < numRowsPerFlush; ++iRow) {
        final byte[] row = Bytes.toBytes(String.format(keyFormat,
            actualStartKey + rand.nextInt(actualEndKey - actualStartKey)));

        Put put = new Put(row);
        Delete del = new Delete(row);
        for (int iCol = 0; iCol < numColsPerRow; ++iCol) {
          final byte[] cf = cfBytes[rand.nextInt(numCF)];
          final long ts = rand.nextInt();
          final byte[] qual = Bytes.toBytes("col" + iCol);
          if (rand.nextBoolean()) {
            final byte[] value = Bytes.toBytes("value_for_row_" + iRow +
                "_cf_" + Bytes.toStringBinary(cf) + "_col_" + iCol + "_ts_" +
                ts + "_random_" + rand.nextLong());
            put.add(cf, qual, ts, value);
          } else if (rand.nextDouble() < 0.8) {
            del.deleteColumn(cf, qual, ts);
          } else {
            del.deleteColumns(cf, qual, ts);
          }
        }

        if (!put.isEmpty()) {
          table.put(put);
        }

        if (!del.isEmpty()) {
          table.delete(del);
        }
      }
      LOG.info("Initiating flush #" + iFlush + " for table " + tableName);
      table.flushCommits();
      if (hbaseCluster != null) {
        hbaseCluster.flushcache(tableNameBytes);
      }
    }

    return table;
  }

  private static final int MIN_RANDOM_PORT = 0xc000;
  private static final int MAX_RANDOM_PORT = 0xfffe;

  /**
   * Returns a random port. These ports cannot be registered with IANA and are
   * intended for dynamic allocation (see http://bit.ly/dynports).
   */
  public static int randomPort() {
    return MIN_RANDOM_PORT
        + new Random().nextInt(MAX_RANDOM_PORT - MIN_RANDOM_PORT);
  }

  /** 
   * Returns a random free port and marks that port as taken. Not thread-safe. Expected to be
   * called from single-threaded test setup code/
   */
  public static int randomFreePort() {
    int port = 0;
    do {
      port = randomPort();
      if (takenRandomPorts.contains(port)) {
        continue;
      }
      takenRandomPorts.add(port);

      try {
        ServerSocket sock = new ServerSocket(port);
        sock.close();
      } catch (IOException ex) {
        port = 0;
      }
    } while (port == 0);
    return port;
  }

  public static void waitForHostPort(String host, int port)
      throws IOException {
    final int maxTimeMs = 10000;
    final int maxNumAttempts = maxTimeMs / HConstants.SOCKET_RETRY_WAIT_MS;
    IOException savedException = null;
    LOG.info("Waiting for server at " + host + ":" + port);
    for (int attempt = 0; attempt < maxNumAttempts; ++attempt) {
      try {
        Socket sock = new Socket(InetAddress.getByName(host), port);
        sock.close();
        savedException = null;
        LOG.info("Server at " + host + ":" + port + " is available");
        break;
      } catch (UnknownHostException e) {
        throw new IOException("Failed to look up " + host, e);
      } catch (IOException e) {
        savedException = e;
      }
      Threads.sleepWithoutInterrupt(HConstants.SOCKET_RETRY_WAIT_MS);
    }

    if (savedException != null) {
      throw savedException;
    }
  }

  /**
   * Creates a pre-split table for load testing. If the table already exists,
   * logs a warning and continues.
   * @return the number of regions the table was split into
   */
  public static int createPreSplitLoadTestTable(Configuration conf,
      byte[] tableName, byte[] columnFamily, Algorithm compression,
      DataBlockEncoding dataBlockEncoding) throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
    hcd.setDataBlockEncoding(dataBlockEncoding);
    hcd.setCompressionType(compression);
    desc.addFamily(hcd);

    int totalNumberOfRegions = 0;
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);

      // create a table a pre-splits regions.
      // The number of splits is set as:
      //    region servers * regions per region server
      int numberOfServers = admin.getClusterStatus().getServers();
      if (numberOfServers == 0) {
        throw new IllegalStateException("No live regionservers");
      }

      totalNumberOfRegions = numberOfServers * DEFAULT_REGIONS_PER_SERVER;
      LOG.info("Number of live regionservers: " + numberOfServers + ", " +
          "pre-splitting table into " + totalNumberOfRegions + " regions " +
          "(default regions per server: " + DEFAULT_REGIONS_PER_SERVER + ")");

      byte[][] splits = new RegionSplitter.HexStringSplit().split(
          totalNumberOfRegions);

      admin.createTable(desc, splits);
      admin.close();
    } catch (MasterNotRunningException e) {
      LOG.error("Master not running", e);
      throw new IOException(e);
    } catch (TableExistsException e) {
      LOG.warn("Table " + Bytes.toStringBinary(tableName) +
          " already exists, continuing");
    }
    return totalNumberOfRegions;
  }

  public static int getMetaRSPort(Configuration conf) throws IOException {
    HTable table = new HTable(conf, HConstants.META_TABLE_NAME);
    HRegionLocation hloc = table.getRegionLocation(Bytes.toBytes(""));
    table.close();
    return hloc.getServerAddress().getPort();
  }

  public HRegion createTestRegion(String tableName, HColumnDescriptor hcd)
      throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(hcd);
    HRegionInfo info =
        new HRegionInfo(htd, null, null, false);
    HRegion region =
        HRegion.createHRegion(info, getTestDir("test_region_" +
            tableName), getConfiguration());
    return region;
  }

  /**
   * Useful for tests that do not start a full cluster, e.g. those that use a mini MR cluster only.
   */
  public void cleanupTestDir() throws IOException {
    if (this.clusterTestBuildDir != null && this.clusterTestBuildDir.exists()) {
      // Need to use deleteDirectory because File.delete required dir is empty.
      if (!FSUtils.deleteDirectory(FileSystem.getLocal(this.conf),
          new Path(this.clusterTestBuildDir.toString()))) {
        LOG.warn("Failed delete of " + this.clusterTestBuildDir.toString());
      }
    }
  }

  public static void setFileSystemURI(String fsURI) {
    HBaseTestingUtility.fsURI = fsURI;
  }

  private static void logMethodEntryAndSetThreadName(String methodName) {
    LOG.info("\nStarting " + methodName + "\n");
    Thread.currentThread().setName(methodName);
  }

  /**
   * Sets the current thread name to the caller's method name. 
   */
  public static void setThreadNameFromMethod() {
    logMethodEntryAndSetThreadName(new Throwable().getStackTrace()[1].getMethodName());
  }

  /**
   * Sets the current thread name to the caller's caller's method name. 
   */
  public static void setThreadNameFromCallerMethod() {
    logMethodEntryAndSetThreadName(new Throwable().getStackTrace()[2].getMethodName());
  }

  public void killMiniHBaseCluster() {
    for (RegionServerThread rst : hbaseCluster.getRegionServerThreads()) {
      rst.getRegionServer().kill();
    }
    for (HMaster master : hbaseCluster.getMasters()) {
      master.stop("killMiniHBaseCluster");
    }
  }

  public void dropDefaultTable() throws Exception {
    HBaseAdmin admin = new HBaseAdmin(getConfiguration());
    if (admin.tableExists(HTestConst.DEFAULT_TABLE_BYTES)) {
      admin.disableTable(HTestConst.DEFAULT_TABLE_BYTES);
      admin.deleteTable(HTestConst.DEFAULT_TABLE_BYTES);
    }
    admin.close();
  }

  public BlockCache getBlockCache() {
    return cacheConf.getBlockCache();
  }
}
