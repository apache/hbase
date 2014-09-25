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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Jdk14Logger;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.ChecksumUtil;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.mapreduce.MapreduceTestingShim;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.tool.Canary;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.EmptyWatcher;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;

/**
 * Facility for testing HBase. Replacement for
 * old HBaseTestCase and HBaseClusterTestCase functionality.
 * Create an instance and keep it around testing HBase.  This class is
 * meant to be your one-stop shop for anything you might need testing.  Manages
 * one cluster at a time only. Managed cluster can be an in-process
 * {@link MiniHBaseCluster}, or a deployed cluster of type {@link DistributedHBaseCluster}.
 * Not all methods work with the real cluster.
 * Depends on log4j being on classpath and
 * hbase-site.xml for logging and test-run configuration.  It does not set
 * logging levels nor make changes to configuration parameters.
 * <p>To preserve test data directories, pass the system property "hbase.testing.preserve.testdir"
 * setting it to true.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HBaseTestingUtility extends HBaseCommonTestingUtility {
   private MiniZooKeeperCluster zkCluster = null;

  public static final String REGIONS_PER_SERVER_KEY = "hbase.test.regions-per-server";
  /**
   * The default number of regions per regionserver when creating a pre-split
   * table.
   */
  public static final int DEFAULT_REGIONS_PER_SERVER = 5;

  /**
   * Set if we were passed a zkCluster.  If so, we won't shutdown zk as
   * part of general shutdown.
   */
  private boolean passedZkCluster = false;
  private MiniDFSCluster dfsCluster = null;

  private HBaseCluster hbaseCluster = null;
  private MiniMRCluster mrCluster = null;

  /** If there is a mini cluster running for this testing utility instance. */
  private boolean miniClusterRunning;

  private String hadoopLogDir;

  /** Directory (a subdirectory of dataTestDir) used by the dfs cluster if any */
  private File clusterTestDir = null;

  /** Directory on test filesystem where we put the data for this instance of
    * HBaseTestingUtility*/
  private Path dataTestDirOnTestFS = null;

  /**
   * System property key to get test directory value.
   * Name is as it is because mini dfs has hard-codings to put test data here.
   * It should NOT be used directly in HBase, as it's a property used in
   *  mini dfs.
   *  @deprecated can be used only with mini dfs
   */
  @Deprecated
  private static final String TEST_DIRECTORY_KEY = "test.build.data";

  /** Filesystem URI used for map-reduce mini-cluster setup */
  private static String FS_URI;

  /** A set of ports that have been claimed using {@link #randomFreePort()}. */
  private static final Set<Integer> takenRandomPorts = new HashSet<Integer>();

  /** Compression algorithms to use in parameterized JUnit 4 tests */
  public static final List<Object[]> COMPRESSION_ALGORITHMS_PARAMETERIZED =
    Arrays.asList(new Object[][] {
      { Compression.Algorithm.NONE },
      { Compression.Algorithm.GZ }
    });

  /** This is for unit tests parameterized with a two booleans. */
  public static final List<Object[]> BOOLEAN_PARAMETERIZED =
      Arrays.asList(new Object[][] {
          { new Boolean(false) },
          { new Boolean(true) }
      });

  /** This is for unit tests parameterized with a single boolean. */
  public static final List<Object[]> MEMSTORETS_TAGS_PARAMETRIZED = memStoreTSAndTagsCombination()  ;
  /** Compression algorithms to use in testing */
  public static final Compression.Algorithm[] COMPRESSION_ALGORITHMS ={
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
      for (BloomType bloomType : BloomType.values()) {
        configurations.add(new Object[] { comprAlgo, bloomType });
      }
    }
    return Collections.unmodifiableList(configurations);
  }

  /**
   * Create combination of memstoreTS and tags
   */
  private static List<Object[]> memStoreTSAndTagsCombination() {
    List<Object[]> configurations = new ArrayList<Object[]>();
    configurations.add(new Object[] { false, false });
    configurations.add(new Object[] { false, true });
    configurations.add(new Object[] { true, false });
    configurations.add(new Object[] { true, true });
    return Collections.unmodifiableList(configurations);
  }

  public static final Collection<Object[]> BLOOM_AND_COMPRESSION_COMBINATIONS =
      bloomAndCompressionCombinations();

  public HBaseTestingUtility() {
    this(HBaseConfiguration.create());
  }

  public HBaseTestingUtility(Configuration conf) {
    super(conf);

    // a hbase checksum verification failure will cause unit tests to fail
    ChecksumUtil.generateExceptionForChecksumFailureForTest(true);
  }

  /**
   * Create an HBaseTestingUtility where all tmp files are written to the local test data dir.
   * It is needed to properly base FSUtil.getRootDirs so that they drop temp files in the proper
   * test dir.  Use this when you aren't using an Mini HDFS cluster.
   * @return HBaseTestingUtility that use local fs for temp files.
   */
  public static HBaseTestingUtility createLocalHTU() {
    Configuration c = HBaseConfiguration.create();
    return createLocalHTU(c);
  }

  /**
   * Create an HBaseTestingUtility where all tmp files are written to the local test data dir.
   * It is needed to properly base FSUtil.getRootDirs so that they drop temp files in the proper
   * test dir.  Use this when you aren't using an Mini HDFS cluster.
   * @param c Configuration (will be modified)
   * @return HBaseTestingUtility that use local fs for temp files.
   */
  public static HBaseTestingUtility createLocalHTU(Configuration c) {
    HBaseTestingUtility htu = new HBaseTestingUtility(c);
    String dataTestDir = htu.getDataTestDir().toString();
    htu.getConfiguration().set(HConstants.HBASE_DIR, dataTestDir);
    LOG.debug("Setting " + HConstants.HBASE_DIR + " to " + dataTestDir);
    return htu;
  }

  /**
   * Controls how many attempts we will make in the face of failures in HDFS.
   * @deprecated to be removed with Hadoop 1.x support
   */
  @Deprecated
  public void setHDFSClientRetry(final int retries) {
    this.conf.setInt("hdfs.client.retries.number", retries);
    if (0 == retries) {
      makeDFSClientNonRetrying();
    }
  }

  /**
   * Returns this classes's instance of {@link Configuration}.  Be careful how
   * you use the returned Configuration since {@link HConnection} instances
   * can be shared.  The Map of HConnections is keyed by the Configuration.  If
   * say, a Connection was being used against a cluster that had been shutdown,
   * see {@link #shutdownMiniCluster()}, then the Connection will no longer
   * be wholesome.  Rather than use the return direct, its usually best to
   * make a copy and use that.  Do
   * <code>Configuration c = new Configuration(INSTANCE.getConfiguration());</code>
   * @return Instance of Configuration.
   */
  @Override
  public Configuration getConfiguration() {
    return super.getConfiguration();
  }

  public void setHBaseCluster(HBaseCluster hbaseCluster) {
    this.hbaseCluster = hbaseCluster;
  }

  /**
   * Home our data in a dir under {@link #DEFAULT_BASE_TEST_DIRECTORY}.
   * Give it a random name so can have many concurrent tests running if
   * we need to.  It needs to amend the {@link #TEST_DIRECTORY_KEY}
   * System property, as it's what minidfscluster bases
   * it data dir on.  Moding a System property is not the way to do concurrent
   * instances -- another instance could grab the temporary
   * value unintentionally -- but not anything can do about it at moment;
   * single instance only is how the minidfscluster works.
   *
   * We also create the underlying directory for
   *  hadoop.log.dir, mapred.local.dir and hadoop.tmp.dir, and set the values
   *  in the conf, and as a system property for hadoop.tmp.dir
   *
   * @return The calculated data test build directory, if newly-created.
   */
  @Override
  protected Path setupDataTestDir() {
    Path testPath = super.setupDataTestDir();
    if (null == testPath) {
      return null;
    }

    createSubDirAndSystemProperty(
      "hadoop.log.dir",
      testPath, "hadoop-log-dir");

    // This is defaulted in core-default.xml to /tmp/hadoop-${user.name}, but
    //  we want our own value to ensure uniqueness on the same machine
    createSubDirAndSystemProperty(
      "hadoop.tmp.dir",
      testPath, "hadoop-tmp-dir");

    // Read and modified in org.apache.hadoop.mapred.MiniMRCluster
    createSubDir(
      "mapred.local.dir",
      testPath, "mapred-local-dir");

    return testPath;
  }

  private void createSubDirAndSystemProperty(
    String propertyName, Path parent, String subDirName){

    String sysValue = System.getProperty(propertyName);

    if (sysValue != null) {
      // There is already a value set. So we do nothing but hope
      //  that there will be no conflicts
      LOG.info("System.getProperty(\""+propertyName+"\") already set to: "+
        sysValue + " so I do NOT create it in " + parent);
      String confValue = conf.get(propertyName);
      if (confValue != null && !confValue.endsWith(sysValue)){
       LOG.warn(
         propertyName + " property value differs in configuration and system: "+
         "Configuration="+confValue+" while System="+sysValue+
         " Erasing configuration value by system value."
       );
      }
      conf.set(propertyName, sysValue);
    } else {
      // Ok, it's not set, so we create it as a subdirectory
      createSubDir(propertyName, parent, subDirName);
      System.setProperty(propertyName, conf.get(propertyName));
    }
  }

  /**
   * @return Where to write test data on the test filesystem; Returns working directory
   * for the test filesystem by default
   * @see #setupDataTestDirOnTestFS()
   * @see #getTestFileSystem()
   */
  private Path getBaseTestDirOnTestFS() throws IOException {
    FileSystem fs = getTestFileSystem();
    return new Path(fs.getWorkingDirectory(), "test-data");
  }

  /**
   * @return Where the DFS cluster will write data on the local subsystem.
   * Creates it if it does not exist already.  A subdir of {@link #getBaseTestDir()}
   * @see #getTestFileSystem()
   */
  Path getClusterTestDir() {
    if (clusterTestDir == null){
      setupClusterTestDir();
    }
    return new Path(clusterTestDir.getAbsolutePath());
  }

  /**
   * Creates a directory for the DFS cluster, under the test data
   */
  private void setupClusterTestDir() {
    if (clusterTestDir != null) {
      return;
    }

    // Using randomUUID ensures that multiple clusters can be launched by
    //  a same test, if it stops & starts them
    Path testDir = getDataTestDir("dfscluster_" + UUID.randomUUID().toString());
    clusterTestDir = new File(testDir.toString()).getAbsoluteFile();
    // Have it cleaned up on exit
    boolean b = deleteOnExit();
    if (b) clusterTestDir.deleteOnExit();
    conf.set(TEST_DIRECTORY_KEY, clusterTestDir.getPath());
    LOG.info("Created new mini-cluster data directory: " + clusterTestDir + ", deleteOnExit=" + b);
  }

  /**
   * Returns a Path in the test filesystem, obtained from {@link #getTestFileSystem()}
   * to write temporary test data. Call this method after setting up the mini dfs cluster
   * if the test relies on it.
   * @return a unique path in the test filesystem
   */
  public Path getDataTestDirOnTestFS() throws IOException {
    if (dataTestDirOnTestFS == null) {
      setupDataTestDirOnTestFS();
    }

    return dataTestDirOnTestFS;
  }

  /**
   * Returns a Path in the test filesystem, obtained from {@link #getTestFileSystem()}
   * to write temporary test data. Call this method after setting up the mini dfs cluster
   * if the test relies on it.
   * @return a unique path in the test filesystem
   * @param subdirName name of the subdir to create under the base test dir
   */
  public Path getDataTestDirOnTestFS(final String subdirName) throws IOException {
    return new Path(getDataTestDirOnTestFS(), subdirName);
  }

  /**
   * Sets up a path in test filesystem to be used by tests
   */
  private void setupDataTestDirOnTestFS() throws IOException {
    if (dataTestDirOnTestFS != null) {
      LOG.warn("Data test on test fs dir already setup in "
          + dataTestDirOnTestFS.toString());
      return;
    }

    //The file system can be either local, mini dfs, or if the configuration
    //is supplied externally, it can be an external cluster FS. If it is a local
    //file system, the tests should use getBaseTestDir, otherwise, we can use
    //the working directory, and create a unique sub dir there
    FileSystem fs = getTestFileSystem();
    if (fs.getUri().getScheme().equals(FileSystem.getLocal(conf).getUri().getScheme())) {
      File dataTestDir = new File(getDataTestDir().toString());
      if (deleteOnExit()) dataTestDir.deleteOnExit();
      dataTestDirOnTestFS = new Path(dataTestDir.getAbsolutePath());
    } else {
      Path base = getBaseTestDirOnTestFS();
      String randomStr = UUID.randomUUID().toString();
      dataTestDirOnTestFS = new Path(base, randomStr);
      if (deleteOnExit()) fs.deleteOnExit(dataTestDirOnTestFS);
    }
  }

  /**
   * Cleans the test data directory on the test filesystem.
   * @return True if we removed the test dirs
   * @throws IOException
   */
  public boolean cleanupDataTestDirOnTestFS() throws IOException {
    boolean ret = getTestFileSystem().delete(dataTestDirOnTestFS, true);
    if (ret)
      dataTestDirOnTestFS = null;
    return ret;
  }

  /**
   * Cleans a subdirectory under the test data directory on the test filesystem.
   * @return True if we removed child
   * @throws IOException
   */
  public boolean cleanupDataTestDirOnTestFS(String subdirName) throws IOException {
    Path cpath = getDataTestDirOnTestFS(subdirName);
    return getTestFileSystem().delete(cpath, true);
  }

  /**
   * Start a minidfscluster.
   * @param servers How many DNs to start.
   * @throws Exception
   * @see {@link #shutdownMiniDFSCluster()}
   * @return The mini dfs cluster created.
   */
  public MiniDFSCluster startMiniDFSCluster(int servers) throws Exception {
    return startMiniDFSCluster(servers, null);
  }

  /**
   * Start a minidfscluster.
   * This is useful if you want to run datanode on distinct hosts for things
   * like HDFS block location verification.
   * If you start MiniDFSCluster without host names, all instances of the
   * datanodes will have the same host name.
   * @param hosts hostnames DNs to run on.
   * @throws Exception
   * @see {@link #shutdownMiniDFSCluster()}
   * @return The mini dfs cluster created.
   */
  public MiniDFSCluster startMiniDFSCluster(final String hosts[])
  throws Exception {
    if ( hosts != null && hosts.length != 0) {
      return startMiniDFSCluster(hosts.length, hosts);
    } else {
      return startMiniDFSCluster(1, null);
    }
  }

  /**
   * Start a minidfscluster.
   * Can only create one.
   * @param servers How many DNs to start.
   * @param hosts hostnames DNs to run on.
   * @throws Exception
   * @see {@link #shutdownMiniDFSCluster()}
   * @return The mini dfs cluster created.
   */
  public MiniDFSCluster startMiniDFSCluster(int servers, final String hosts[])
  throws Exception {
    createDirsAndSetProperties();
    try {
      Method m = Class.forName("org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream")
          .getMethod("setShouldSkipFsyncForTesting", new Class<?> []{ boolean.class });
      m.invoke(null, new Object[] {true});
    } catch (ClassNotFoundException e) {
      LOG.info("EditLogFileOutputStream not found");
    }

    // Error level to skip some warnings specific to the minicluster. See HBASE-4709
    org.apache.log4j.Logger.getLogger(org.apache.hadoop.metrics2.util.MBeans.class).
        setLevel(org.apache.log4j.Level.ERROR);
    org.apache.log4j.Logger.getLogger(org.apache.hadoop.metrics2.impl.MetricsSystemImpl.class).
        setLevel(org.apache.log4j.Level.ERROR);


    this.dfsCluster = new MiniDFSCluster(0, this.conf, servers, true, true,
      true, null, null, hosts, null);

    // Set this just-started cluster as our filesystem.
    FileSystem fs = this.dfsCluster.getFileSystem();
    FSUtils.setFsDefault(this.conf, new Path(fs.getUri()));

    // Wait for the cluster to be totally up
    this.dfsCluster.waitClusterUp();

    //reset the test directory for test file system
    dataTestDirOnTestFS = null;

    return this.dfsCluster;
  }


  public MiniDFSCluster startMiniDFSCluster(int servers, final  String racks[], String hosts[])
      throws Exception {
    createDirsAndSetProperties();
    this.dfsCluster = new MiniDFSCluster(0, this.conf, servers, true, true,
        true, null, racks, hosts, null);

    // Set this just-started cluster as our filesystem.
    FileSystem fs = this.dfsCluster.getFileSystem();
    FSUtils.setFsDefault(this.conf, new Path(fs.getUri()));

    // Wait for the cluster to be totally up
    this.dfsCluster.waitClusterUp();

    //reset the test directory for test file system
    dataTestDirOnTestFS = null;

    return this.dfsCluster;
  }

  public MiniDFSCluster startMiniDFSClusterForTestHLog(int namenodePort) throws IOException {
    createDirsAndSetProperties();
    dfsCluster = new MiniDFSCluster(namenodePort, conf, 5, false, true, true, null,
        null, null, null);
    return dfsCluster;
  }

  /** This is used before starting HDFS and map-reduce mini-clusters */
  private void createDirsAndSetProperties() throws IOException {
    setupClusterTestDir();
    System.setProperty(TEST_DIRECTORY_KEY, clusterTestDir.getPath());
    createDirAndSetProperty("cache_data", "test.cache.data");
    createDirAndSetProperty("hadoop_tmp", "hadoop.tmp.dir");
    hadoopLogDir = createDirAndSetProperty("hadoop_logs", "hadoop.log.dir");
    createDirAndSetProperty("mapred_local", "mapred.local.dir");
    createDirAndSetProperty("mapred_temp", "mapred.temp.dir");
    enableShortCircuit();

    Path root = getDataTestDirOnTestFS("hadoop");
    conf.set(MapreduceTestingShim.getMROutputDirProp(),
      new Path(root, "mapred-output-dir").toString());
    conf.set("mapred.system.dir", new Path(root, "mapred-system-dir").toString());
    conf.set("mapreduce.jobtracker.staging.root.dir",
      new Path(root, "mapreduce-jobtracker-staging-root-dir").toString());
    conf.set("mapred.working.dir", new Path(root, "mapred-working-dir").toString());
  }


  /**
   *  Get the HBase setting for dfs.client.read.shortcircuit from the conf or a system property.
   *  This allows to specify this parameter on the command line.
   *   If not set, default is true.
   */
  public boolean isReadShortCircuitOn(){
    final String propName = "hbase.tests.use.shortcircuit.reads";
    String readOnProp = System.getProperty(propName);
    if (readOnProp != null){
      return  Boolean.parseBoolean(readOnProp);
    } else {
      return conf.getBoolean(propName, false);
    }
  }

  /** Enable the short circuit read, unless configured differently.
   * Set both HBase and HDFS settings, including skipping the hdfs checksum checks.
   */
  private void enableShortCircuit() {
    if (isReadShortCircuitOn()) {
      String curUser = System.getProperty("user.name");
      LOG.info("read short circuit is ON for user " + curUser);
      // read short circuit, for hdfs
      conf.set("dfs.block.local-path-access.user", curUser);
      // read short circuit, for hbase
      conf.setBoolean("dfs.client.read.shortcircuit", true);
      // Skip checking checksum, for the hdfs client and the datanode
      conf.setBoolean("dfs.client.read.shortcircuit.skip.checksum", true);
    } else {
      LOG.info("read short circuit is OFF");
    }
  }

  private String createDirAndSetProperty(final String relPath, String property) {
    String path = getDataTestDir(relPath).toString();
    System.setProperty(property, path);
    conf.set(property, path);
    new File(path).mkdirs();
    LOG.info("Setting " + property + " to " + path + " in system properties and HBase conf");
    return path;
  }

  /**
   * Shuts down instance created by call to {@link #startMiniDFSCluster(int)}
   * or does nothing.
   * @throws IOException
   */
  public void shutdownMiniDFSCluster() throws IOException {
    if (this.dfsCluster != null) {
      // The below throws an exception per dn, AsynchronousCloseException.
      this.dfsCluster.shutdown();
      dfsCluster = null;
      dataTestDirOnTestFS = null;
      FSUtils.setFsDefault(this.conf, new Path("file:///"));
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
    return startMiniZKCluster(1);
  }

  /**
   * Call this if you only want a zk cluster.
   * @param zooKeeperServerNum
   * @see #startMiniZKCluster() if you want zk + dfs + hbase mini cluster.
   * @throws Exception
   * @see #shutdownMiniZKCluster()
   * @return zk cluster started.
   */
  public MiniZooKeeperCluster startMiniZKCluster(int zooKeeperServerNum)
      throws Exception {
    setupClusterTestDir();
    return startMiniZKCluster(clusterTestDir, zooKeeperServerNum);
  }

  private MiniZooKeeperCluster startMiniZKCluster(final File dir)
    throws Exception {
    return startMiniZKCluster(dir,1);
  }

  /**
   * Start a mini ZK cluster. If the property "test.hbase.zookeeper.property.clientPort" is set
   *  the port mentionned is used as the default port for ZooKeeper.
   */
  private MiniZooKeeperCluster startMiniZKCluster(final File dir,
      int zooKeeperServerNum)
  throws Exception {
    if (this.zkCluster != null) {
      throw new IOException("Cluster already running at " + dir);
    }
    this.passedZkCluster = false;
    this.zkCluster = new MiniZooKeeperCluster(this.getConfiguration());
    final int defPort = this.conf.getInt("test.hbase.zookeeper.property.clientPort", 0);
    if (defPort > 0){
      // If there is a port in the config file, we use it.
      this.zkCluster.setDefaultClientPort(defPort);
    }
    int clientPort =   this.zkCluster.startup(dir,zooKeeperServerNum);
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
      this.zkCluster = null;
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
  throws Exception {
    return startMiniCluster(1, numSlaves);
  }


  /**
   * start minicluster
   * @throws Exception
   * @see {@link #shutdownMiniCluster()}
   * @return Mini hbase cluster instance created.
   */
  public MiniHBaseCluster startMiniCluster(final int numMasters,
    final int numSlaves)
  throws Exception {
    return startMiniCluster(numMasters, numSlaves, null);
  }

  /**
   * Start up a minicluster of hbase, optionally dfs, and zookeeper.
   * Modifies Configuration.  Homes the cluster data directory under a random
   * subdirectory in a directory under System property test.build.data.
   * Directory is cleaned up on exit.
   * @param numMasters Number of masters to start up.  We'll start this many
   * hbase masters.  If numMasters > 1, you can find the active/primary master
   * with {@link MiniHBaseCluster#getMaster()}.
   * @param numSlaves Number of slaves to start up.  We'll start this many
   * regionservers. If dataNodeHosts == null, this also indicates the number of
   * datanodes to start. If dataNodeHosts != null, the number of datanodes is
   * based on dataNodeHosts.length.
   * If numSlaves is > 1, then make sure
   * hbase.regionserver.info.port is -1 (i.e. no ui per regionserver) otherwise
   * bind errors.
   * @param dataNodeHosts hostnames DNs to run on.
   * This is useful if you want to run datanode on distinct hosts for things
   * like HDFS block location verification.
   * If you start MiniDFSCluster without host names,
   * all instances of the datanodes will have the same host name.
   * @throws Exception
   * @see {@link #shutdownMiniCluster()}
   * @return Mini hbase cluster instance created.
   */
  public MiniHBaseCluster startMiniCluster(final int numMasters,
      final int numSlaves, final String[] dataNodeHosts) throws Exception {
    return startMiniCluster(numMasters, numSlaves, numSlaves, dataNodeHosts, null, null);
  }

  /**
   * Same as {@link #startMiniCluster(int, int)}, but with custom number of datanodes.
   * @param numDataNodes Number of data nodes.
   */
  public MiniHBaseCluster startMiniCluster(final int numMasters,
      final int numSlaves, final int numDataNodes) throws Exception {
    return startMiniCluster(numMasters, numSlaves, numDataNodes, null, null, null);
  }

  /**
   * Start up a minicluster of hbase, optionally dfs, and zookeeper.
   * Modifies Configuration.  Homes the cluster data directory under a random
   * subdirectory in a directory under System property test.build.data.
   * Directory is cleaned up on exit.
   * @param numMasters Number of masters to start up.  We'll start this many
   * hbase masters.  If numMasters > 1, you can find the active/primary master
   * with {@link MiniHBaseCluster#getMaster()}.
   * @param numSlaves Number of slaves to start up.  We'll start this many
   * regionservers. If dataNodeHosts == null, this also indicates the number of
   * datanodes to start. If dataNodeHosts != null, the number of datanodes is
   * based on dataNodeHosts.length.
   * If numSlaves is > 1, then make sure
   * hbase.regionserver.info.port is -1 (i.e. no ui per regionserver) otherwise
   * bind errors.
   * @param dataNodeHosts hostnames DNs to run on.
   * This is useful if you want to run datanode on distinct hosts for things
   * like HDFS block location verification.
   * If you start MiniDFSCluster without host names,
   * all instances of the datanodes will have the same host name.
   * @param masterClass The class to use as HMaster, or null for default
   * @param regionserverClass The class to use as HRegionServer, or null for
   * default
   * @throws Exception
   * @see {@link #shutdownMiniCluster()}
   * @return Mini hbase cluster instance created.
   */
  public MiniHBaseCluster startMiniCluster(final int numMasters,
      final int numSlaves, final String[] dataNodeHosts, Class<? extends HMaster> masterClass,
      Class<? extends MiniHBaseCluster.MiniHBaseClusterRegionServer> regionserverClass)
          throws Exception {
    return startMiniCluster(
        numMasters, numSlaves, numSlaves, dataNodeHosts, masterClass, regionserverClass);
  }

  /**
   * Same as {@link #startMiniCluster(int, int, String[], Class, Class)}, but with custom
   * number of datanodes.
   * @param numDataNodes Number of data nodes.
   */
  public MiniHBaseCluster startMiniCluster(final int numMasters,
    final int numSlaves, int numDataNodes, final String[] dataNodeHosts,
    Class<? extends HMaster> masterClass,
    Class<? extends MiniHBaseCluster.MiniHBaseClusterRegionServer> regionserverClass)
  throws Exception {
    if (dataNodeHosts != null && dataNodeHosts.length != 0) {
      numDataNodes = dataNodeHosts.length;
    }

    LOG.info("Starting up minicluster with " + numMasters + " master(s) and " +
        numSlaves + " regionserver(s) and " + numDataNodes + " datanode(s)");

    // If we already put up a cluster, fail.
    if (miniClusterRunning) {
      throw new IllegalStateException("A mini-cluster is already running");
    }
    miniClusterRunning = true;

    setupClusterTestDir();
    System.setProperty(TEST_DIRECTORY_KEY, this.clusterTestDir.getPath());

    // Bring up mini dfs cluster. This spews a bunch of warnings about missing
    // scheme. Complaints are 'Scheme is undefined for build/test/data/dfs/name1'.
    startMiniDFSCluster(numDataNodes, dataNodeHosts);

    // Start up a zk cluster.
    if (this.zkCluster == null) {
      startMiniZKCluster(clusterTestDir);
    }

    // Start the MiniHBaseCluster
    return startMiniHBaseCluster(numMasters, numSlaves, masterClass, regionserverClass);
  }

  public MiniHBaseCluster startMiniHBaseCluster(final int numMasters, final int numSlaves)
      throws IOException, InterruptedException{
    return startMiniHBaseCluster(numMasters, numSlaves, null, null);
  }

  /**
   * Starts up mini hbase cluster.  Usually used after call to
   * {@link #startMiniCluster(int, int)} when doing stepped startup of clusters.
   * Usually you won't want this.  You'll usually want {@link #startMiniCluster()}.
   * @param numMasters
   * @param numSlaves
   * @return Reference to the hbase mini hbase cluster.
   * @throws IOException
   * @throws InterruptedException
   * @see {@link #startMiniCluster()}
   */
  public MiniHBaseCluster startMiniHBaseCluster(final int numMasters,
        final int numSlaves, Class<? extends HMaster> masterClass,
        Class<? extends MiniHBaseCluster.MiniHBaseClusterRegionServer> regionserverClass)
  throws IOException, InterruptedException {
    // Now do the mini hbase cluster.  Set the hbase.rootdir in config.
    createRootDir();

    // These settings will make the server waits until this exact number of
    // regions servers are connected.
    if (conf.getInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, -1) == -1) {
      conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, numSlaves);
    }
    if (conf.getInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, -1) == -1) {
      conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, numSlaves);
    }

    Configuration c = new Configuration(this.conf);
    this.hbaseCluster =
        new MiniHBaseCluster(c, numMasters, numSlaves, masterClass, regionserverClass);
    // Don't leave here till we've done a successful scan of the hbase:meta
    HTable t = new HTable(c, TableName.META_TABLE_NAME);
    ResultScanner s = t.getScanner(new Scan());
    while (s.next() != null) {
      continue;
    }
    s.close();
    t.close();

    getHBaseAdmin(); // create immediately the hbaseAdmin
    LOG.info("Minicluster is up");
    return (MiniHBaseCluster)this.hbaseCluster;
  }

  /**
   * Starts the hbase cluster up again after shutting it down previously in a
   * test.  Use this if you want to keep dfs/zk up and just stop/start hbase.
   * @param servers number of region servers
   * @throws IOException
   */
  public void restartHBaseCluster(int servers) throws IOException, InterruptedException {
    this.hbaseCluster = new MiniHBaseCluster(this.conf, servers);
    // Don't leave here till we've done a successful scan of the hbase:meta
    HTable t = new HTable(new Configuration(this.conf), TableName.META_TABLE_NAME);
    ResultScanner s = t.getScanner(new Scan());
    while (s.next() != null) {
      // do nothing
    }
    LOG.info("HBase has been restarted");
    s.close();
    t.close();
  }

  /**
   * @return Current mini hbase cluster. Only has something in it after a call
   * to {@link #startMiniCluster()}.
   * @see #startMiniCluster()
   */
  public MiniHBaseCluster getMiniHBaseCluster() {
    if (this.hbaseCluster == null || this.hbaseCluster instanceof MiniHBaseCluster) {
      return (MiniHBaseCluster)this.hbaseCluster;
    }
    throw new RuntimeException(hbaseCluster + " not an instance of " +
                               MiniHBaseCluster.class.getName());
  }

  /**
   * Stops mini hbase, zk, and hdfs clusters.
   * @throws IOException
   * @see {@link #startMiniCluster(int)}
   */
  public void shutdownMiniCluster() throws Exception {
    LOG.info("Shutting down minicluster");
    shutdownMiniHBaseCluster();
    if (!this.passedZkCluster){
      shutdownMiniZKCluster();
    }
    shutdownMiniDFSCluster();

    cleanupTestDir();
    miniClusterRunning = false;
    LOG.info("Minicluster is down");
  }

  /**
   * @return True if we removed the test dirs
   * @throws IOException
   */
  @Override
  public boolean cleanupTestDir() throws IOException {
    boolean ret = super.cleanupTestDir();
    if (deleteDir(this.clusterTestDir)) {
      this.clusterTestDir = null;
      return ret & true;
    }
    return false;
  }

  /**
   * Shutdown HBase mini cluster.  Does not shutdown zk or dfs if running.
   * @throws IOException
   */
  public void shutdownMiniHBaseCluster() throws IOException {
    if (hbaseAdmin != null) {
      hbaseAdmin.close0();
      hbaseAdmin = null;
    }

    // unset the configuration for MIN and MAX RS to start
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, -1);
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, -1);
    if (this.hbaseCluster != null) {
      this.hbaseCluster.shutdown();
      // Wait till hbase is down before going on to shutdown zk.
      this.hbaseCluster.waitUntilShutDown();
      this.hbaseCluster = null;
    }

    if (zooKeeperWatcher != null) {
      zooKeeperWatcher.close();
      zooKeeperWatcher = null;
    }
  }

  /**
   * Returns the path to the default root dir the minicluster uses.
   * Note: this does not cause the root dir to be created.
   * @return Fully qualified path for the default hbase root dir
   * @throws IOException
   */
  public Path getDefaultRootDirPath() throws IOException {
	FileSystem fs = FileSystem.get(this.conf);
	return new Path(fs.makeQualified(fs.getHomeDirectory()),"hbase");
  }

  /**
   * Creates an hbase rootdir in user home directory.  Also creates hbase
   * version file.  Normally you won't make use of this method.  Root hbasedir
   * is created for you as part of mini cluster startup.  You'd only use this
   * method if you were doing manual operation.
   * @return Fully qualified path to hbase root dir
   * @throws IOException
   */
  public Path createRootDir() throws IOException {
    FileSystem fs = FileSystem.get(this.conf);
    Path hbaseRootdir = getDefaultRootDirPath();
    FSUtils.setRootDir(this.conf, hbaseRootdir);
    fs.mkdirs(hbaseRootdir);
    FSUtils.setVersion(fs, hbaseRootdir);
    return hbaseRootdir;
  }

  /**
   * Flushes all caches in the mini hbase cluster
   * @throws IOException
   */
  public void flush() throws IOException {
    getMiniHBaseCluster().flushcache();
  }

  /**
   * Flushes all caches in the mini hbase cluster
   * @throws IOException
   */
  public void flush(TableName tableName) throws IOException {
    getMiniHBaseCluster().flushcache(tableName);
  }

  /**
   * Compact all regions in the mini hbase cluster
   * @throws IOException
   */
  public void compact(boolean major) throws IOException {
    getMiniHBaseCluster().compact(major);
  }

  /**
   * Compact all of a table's reagion in the mini hbase cluster
   * @throws IOException
   */
  public void compact(TableName tableName, boolean major) throws IOException {
    getMiniHBaseCluster().compact(tableName, major);
  }

  /**
   * Create a table.
   * @param tableName
   * @param family
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(String tableName, String family)
  throws IOException{
    return createTable(TableName.valueOf(tableName), new String[]{family});
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
    return createTable(TableName.valueOf(tableName), new byte[][]{family});
  }

  /**
   * Create a table.
   * @param tableName
   * @param families
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(TableName tableName, String[] families)
  throws IOException {
    List<byte[]> fams = new ArrayList<byte[]>(families.length);
    for (String family : families) {
      fams.add(Bytes.toBytes(family));
    }
    return createTable(tableName, fams.toArray(new byte[0][]));
  }

  /**
   * Create a table.
   * @param tableName
   * @param family
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(TableName tableName, byte[] family)
  throws IOException{
    return createTable(tableName, new byte[][]{family});
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
    return createTable(tableName, families,
        new Configuration(getConfiguration()));
  }

  /**
   * Create a table.
   * @param tableName
   * @param families
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(TableName tableName, byte[][] families)
  throws IOException {
    return createTable(tableName, families,
        new Configuration(getConfiguration()));
  }

  public HTable createTable(byte[] tableName, byte[][] families,
      int numVersions, byte[] startKey, byte[] endKey, int numRegions) throws IOException {
    return createTable(TableName.valueOf(tableName), families, numVersions,
        startKey, endKey, numRegions);
  }

  public HTable createTable(String tableName, byte[][] families,
      int numVersions, byte[] startKey, byte[] endKey, int numRegions) throws IOException {
    return createTable(TableName.valueOf(tableName), families, numVersions,
        startKey, endKey, numRegions);
  }

  public HTable createTable(TableName tableName, byte[][] families,
      int numVersions, byte[] startKey, byte[] endKey, int numRegions)
  throws IOException{
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for (byte[] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family)
          .setMaxVersions(numVersions);
      desc.addFamily(hcd);
    }
    getHBaseAdmin().createTable(desc, startKey, endKey, numRegions);
    // HBaseAdmin only waits for regions to appear in hbase:meta we should wait until they are assigned
    waitUntilAllRegionsAssigned(tableName);
    return new HTable(getConfiguration(), tableName);
  }

  /**
   * Create a table.
   * @param htd
   * @param families
   * @param c Configuration to use
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(HTableDescriptor htd, byte[][] families, Configuration c)
  throws IOException {
    for(byte[] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      // Disable blooms (they are on by default as of 0.95) but we disable them here because
      // tests have hard coded counts of what to expect in block cache, etc., and blooms being
      // on is interfering.
      hcd.setBloomFilterType(BloomType.NONE);
      htd.addFamily(hcd);
    }
    getHBaseAdmin().createTable(htd);
    // HBaseAdmin only waits for regions to appear in hbase:meta we should wait until they are assigned
    waitUntilAllRegionsAssigned(htd.getTableName());
    return new HTable(c, htd.getTableName());
  }

  /**
   * Create a table.
   * @param tableName
   * @param families
   * @param c Configuration to use
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(TableName tableName, byte[][] families,
      final Configuration c)
  throws IOException {
    return createTable(new HTableDescriptor(tableName), families, c);
  }

  /**
   * Create a table.
   * @param tableName
   * @param families
   * @param c Configuration to use
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(byte[] tableName, byte[][] families,
      final Configuration c)
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    for(byte[] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      // Disable blooms (they are on by default as of 0.95) but we disable them here because
      // tests have hard coded counts of what to expect in block cache, etc., and blooms being
      // on is interfering.
      hcd.setBloomFilterType(BloomType.NONE);
      desc.addFamily(hcd);
    }
    getHBaseAdmin().createTable(desc);
    return new HTable(c, tableName);
  }

  /**
   * Create a table.
   * @param tableName
   * @param families
   * @param c Configuration to use
   * @param numVersions
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(TableName tableName, byte[][] families,
      final Configuration c, int numVersions)
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for(byte[] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family)
          .setMaxVersions(numVersions);
      desc.addFamily(hcd);
    }
    getHBaseAdmin().createTable(desc);
    // HBaseAdmin only waits for regions to appear in hbase:meta we should wait until they are assigned
    waitUntilAllRegionsAssigned(tableName);
    return new HTable(c, tableName);
  }

  /**
   * Create a table.
   * @param tableName
   * @param families
   * @param c Configuration to use
   * @param numVersions
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(byte[] tableName, byte[][] families,
      final Configuration c, int numVersions)
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    for(byte[] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family)
          .setMaxVersions(numVersions);
      desc.addFamily(hcd);
    }
    getHBaseAdmin().createTable(desc);
    return new HTable(c, tableName);
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
   * @param family
   * @param numVersions
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(TableName tableName, byte[] family, int numVersions)
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
    return createTable(TableName.valueOf(tableName), families, numVersions);
  }

  /**
   * Create a table.
   * @param tableName
   * @param families
   * @param numVersions
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(TableName tableName, byte[][] families,
      int numVersions)
  throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for (byte[] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family).setMaxVersions(numVersions);
      desc.addFamily(hcd);
    }
    getHBaseAdmin().createTable(desc);
    // HBaseAdmin only waits for regions to appear in hbase:meta we should wait until they are assigned
    waitUntilAllRegionsAssigned(tableName);
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
    return createTable(TableName.valueOf(tableName),
        families, numVersions, blockSize);
  }

  /**
   * Create a table.
   * @param tableName
   * @param families
   * @param numVersions
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(TableName tableName, byte[][] families,
    int numVersions, int blockSize) throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for (byte[] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family)
          .setMaxVersions(numVersions)
          .setBlocksize(blockSize);
      desc.addFamily(hcd);
    }
    getHBaseAdmin().createTable(desc);
    // HBaseAdmin only waits for regions to appear in hbase:meta we should wait until they are assigned
    waitUntilAllRegionsAssigned(tableName);
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
      int[] numVersions)
  throws IOException {
    return createTable(TableName.valueOf(tableName), families, numVersions);
  }

  /**
   * Create a table.
   * @param tableName
   * @param families
   * @param numVersions
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(TableName tableName, byte[][] families,
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
    getHBaseAdmin().createTable(desc);
    // HBaseAdmin only waits for regions to appear in hbase:meta we should wait until they are assigned
    waitUntilAllRegionsAssigned(tableName);
    return new HTable(new Configuration(getConfiguration()), tableName);
  }

  /**
   * Create a table.
   * @param tableName
   * @param family
   * @param splitRows
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(byte[] tableName, byte[] family, byte[][] splitRows)
    throws IOException{
    return createTable(TableName.valueOf(tableName), family, splitRows);
  }

  /**
   * Create a table.
   * @param tableName
   * @param family
   * @param splitRows
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(TableName tableName, byte[] family, byte[][] splitRows)
      throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    desc.addFamily(hcd);
    getHBaseAdmin().createTable(desc, splitRows);
    // HBaseAdmin only waits for regions to appear in hbase:meta we should wait until they are assigned
    waitUntilAllRegionsAssigned(tableName);
    return new HTable(getConfiguration(), tableName);
  }

  /**
   * Create a table.
   * @param tableName
   * @param families
   * @param splitRows
   * @return An HTable instance for the created table.
   * @throws IOException
   */
  public HTable createTable(byte[] tableName, byte[][] families, byte[][] splitRows)
      throws IOException {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    for(byte[] family:families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      desc.addFamily(hcd);
    }
    getHBaseAdmin().createTable(desc, splitRows);
    // HBaseAdmin only waits for regions to appear in hbase:meta we should wait until they are assigned
    waitUntilAllRegionsAssigned(TableName.valueOf(tableName));
    return new HTable(getConfiguration(), tableName);
  }

  /**
   * Drop an existing table
   * @param tableName existing table
   */
  public void deleteTable(String tableName) throws IOException {
    deleteTable(TableName.valueOf(tableName));
  }

  /**
   * Drop an existing table
   * @param tableName existing table
   */
  public void deleteTable(byte[] tableName) throws IOException {
    deleteTable(TableName.valueOf(tableName));
  }

  /**
   * Drop an existing table
   * @param tableName existing table
   */
  public void deleteTable(TableName tableName) throws IOException {
    try {
      getHBaseAdmin().disableTable(tableName);
    } catch (TableNotEnabledException e) {
      LOG.debug("Table: " + tableName + " already disabled, so just deleting it.");
    }
    getHBaseAdmin().deleteTable(tableName);
  }

  // ==========================================================================
  // Canned table and table descriptor creation
  // TODO replace HBaseTestCase

  public final static byte [] fam1 = Bytes.toBytes("colfamily11");
  public final static byte [] fam2 = Bytes.toBytes("colfamily21");
  public final static byte [] fam3 = Bytes.toBytes("colfamily31");
  public static final byte[][] COLUMNS = {fam1, fam2, fam3};
  private static final int MAXVERSIONS = 3;

  public static final char FIRST_CHAR = 'a';
  public static final char LAST_CHAR = 'z';
  public static final byte [] START_KEY_BYTES = {FIRST_CHAR, FIRST_CHAR, FIRST_CHAR};
  public static final String START_KEY = new String(START_KEY_BYTES, HConstants.UTF8_CHARSET);

  /**
   * Create a table of name <code>name</code> with {@link COLUMNS} for
   * families.
   * @param name Name to give table.
   * @param versions How many versions to allow per column.
   * @return Column descriptor.
   */
  public HTableDescriptor createTableDescriptor(final String name,
      final int minVersions, final int versions, final int ttl, boolean keepDeleted) {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name));
    for (byte[] cfName : new byte[][]{ fam1, fam2, fam3 }) {
      htd.addFamily(new HColumnDescriptor(cfName)
          .setMinVersions(minVersions)
          .setMaxVersions(versions)
          .setKeepDeletedCells(keepDeleted)
          .setBlockCacheEnabled(false)
          .setTimeToLive(ttl)
      );
    }
    return htd;
  }

  /**
   * Create a table of name <code>name</code> with {@link COLUMNS} for
   * families.
   * @param name Name to give table.
   * @return Column descriptor.
   */
  public HTableDescriptor createTableDescriptor(final String name) {
    return createTableDescriptor(name,  HColumnDescriptor.DEFAULT_MIN_VERSIONS,
        MAXVERSIONS, HConstants.FOREVER, HColumnDescriptor.DEFAULT_KEEP_DELETED);
  }

  /**
   * Create an HRegion that writes to the local tmp dirs
   * @param desc
   * @param startKey
   * @param endKey
   * @return
   * @throws IOException
   */
  public HRegion createLocalHRegion(HTableDescriptor desc, byte [] startKey,
      byte [] endKey)
  throws IOException {
    HRegionInfo hri = new HRegionInfo(desc.getTableName(), startKey, endKey);
    return createLocalHRegion(hri, desc);
  }

  /**
   * Create an HRegion that writes to the local tmp dirs
   * @param info
   * @param desc
   * @return
   * @throws IOException
   */
  public HRegion createLocalHRegion(HRegionInfo info, HTableDescriptor desc) throws IOException {
    return HRegion.createHRegion(info, getDataTestDir(), getConfiguration(), desc);
  }

  /**
   * Create an HRegion that writes to the local tmp dirs with specified hlog
   * @param info regioninfo
   * @param desc table descriptor
   * @param hlog hlog for this region.
   * @return created hregion
   * @throws IOException
   */
  public HRegion createLocalHRegion(HRegionInfo info, HTableDescriptor desc, HLog hlog) throws IOException {
    return HRegion.createHRegion(info, getDataTestDir(), getConfiguration(), desc, hlog);
  }


  /**
   * @param tableName
   * @param startKey
   * @param stopKey
   * @param callingMethod
   * @param conf
   * @param isReadOnly
   * @param families
   * @throws IOException
   * @return A region on which you must call
   *         {@link HRegion#closeHRegion(HRegion)} when done.
   */
  public HRegion createLocalHRegion(byte[] tableName, byte[] startKey, byte[] stopKey,
      String callingMethod, Configuration conf, boolean isReadOnly, Durability durability,
      HLog hlog, byte[]... families) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    htd.setReadOnly(isReadOnly);
    for (byte[] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      // Set default to be three versions.
      hcd.setMaxVersions(Integer.MAX_VALUE);
      htd.addFamily(hcd);
    }
    htd.setDurability(durability);
    HRegionInfo info = new HRegionInfo(htd.getTableName(), startKey, stopKey, false);
    return createLocalHRegion(info, htd, hlog);
  }
  //
  // ==========================================================================

  /**
   * Provide an existing table name to truncate
   * @param tableName existing table
   * @return HTable to that new table
   * @throws IOException
   */
  public HTable truncateTable(byte[] tableName) throws IOException {
    return truncateTable(TableName.valueOf(tableName));
  }

  /**
   * Provide an existing table name to truncate
   * @param tableName existing table
   * @return HTable to that new table
   * @throws IOException
   */
  public HTable truncateTable(TableName tableName) throws IOException {
    HTable table = new HTable(getConfiguration(), tableName);
    Scan scan = new Scan();
    ResultScanner resScan = table.getScanner(scan);
    for(Result res : resScan) {
      Delete del = new Delete(res.getRow());
      table.delete(del);
    }
    resScan = table.getScanner(scan);
    resScan.close();
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
    return loadTable(t, new byte[][] {f});
  }

  /**
   * Load table with rows from 'aaa' to 'zzz'.
   * @param t Table
   * @param f Family
   * @return Count of rows loaded.
   * @throws IOException
   */
  public int loadTable(final HTable t, final byte[] f, boolean writeToWAL) throws IOException {
    return loadTable(t, new byte[][] {f}, null, writeToWAL);
  }

  /**
   * Load table of multiple column families with rows from 'aaa' to 'zzz'.
   * @param t Table
   * @param f Array of Families to load
   * @return Count of rows loaded.
   * @throws IOException
   */
  public int loadTable(final HTable t, final byte[][] f) throws IOException {
    return loadTable(t, f, null);
  }

  /**
   * Load table of multiple column families with rows from 'aaa' to 'zzz'.
   * @param t Table
   * @param f Array of Families to load
   * @param value the values of the cells. If null is passed, the row key is used as value
   * @return Count of rows loaded.
   * @throws IOException
   */
  public int loadTable(final HTable t, final byte[][] f, byte[] value) throws IOException {
    return loadTable(t, f, value, true);
  }

  /**
   * Load table of multiple column families with rows from 'aaa' to 'zzz'.
   * @param t Table
   * @param f Array of Families to load
   * @param value the values of the cells. If null is passed, the row key is used as value
   * @return Count of rows loaded.
   * @throws IOException
   */
  public int loadTable(final HTable t, final byte[][] f, byte[] value, boolean writeToWAL) throws IOException {
    t.setAutoFlush(false);
    int rowCount = 0;
    for (byte[] row : HBaseTestingUtility.ROWS) {
      Put put = new Put(row);
      put.setDurability(writeToWAL ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
      for (int i = 0; i < f.length; i++) {
        put.add(f[i], null, value != null ? value : row);
      }
      t.put(put);
      rowCount++;
    }
    t.flushCommits();
    return rowCount;
  }

  /** A tracker for tracking and validating table rows
   * generated with {@link HBaseTestingUtility#loadTable(HTable, byte[])}
   */
  public static class SeenRowTracker {
    int dim = 'z' - 'a' + 1;
    int[][][] seenRows = new int[dim][dim][dim]; //count of how many times the row is seen
    byte[] startRow;
    byte[] stopRow;

    public SeenRowTracker(byte[] startRow, byte[] stopRow) {
      this.startRow = startRow;
      this.stopRow = stopRow;
    }

    void reset() {
      for (byte[] row : ROWS) {
        seenRows[i(row[0])][i(row[1])][i(row[2])] = 0;
      }
    }

    int i(byte b) {
      return b - 'a';
    }

    public void addRow(byte[] row) {
      seenRows[i(row[0])][i(row[1])][i(row[2])]++;
    }

    /** Validate that all the rows between startRow and stopRow are seen exactly once, and
     * all other rows none
     */
    public void validate() {
      for (byte b1 = 'a'; b1 <= 'z'; b1++) {
        for (byte b2 = 'a'; b2 <= 'z'; b2++) {
          for (byte b3 = 'a'; b3 <= 'z'; b3++) {
            int count = seenRows[i(b1)][i(b2)][i(b3)];
            int expectedCount = 0;
            if (Bytes.compareTo(new byte[] {b1,b2,b3}, startRow) >= 0
                && Bytes.compareTo(new byte[] {b1,b2,b3}, stopRow) < 0) {
              expectedCount = 1;
            }
            if (count != expectedCount) {
              String row = new String(new byte[] {b1,b2,b3});
              throw new RuntimeException("Row:" + row + " has a seen count of " + count + " instead of " + expectedCount);
            }
          }
        }
      }
    }
  }

  public int loadRegion(final HRegion r, final byte[] f) throws IOException {
    return loadRegion(r, f, false);
  }

  /**
   * Load region with rows from 'aaa' to 'zzz'.
   * @param r Region
   * @param f Family
   * @param flush flush the cache if true
   * @return Count of rows loaded.
   * @throws IOException
   */
  public int loadRegion(final HRegion r, final byte[] f, final boolean flush)
  throws IOException {
    byte[] k = new byte[3];
    int rowCount = 0;
    for (byte b1 = 'a'; b1 <= 'z'; b1++) {
      for (byte b2 = 'a'; b2 <= 'z'; b2++) {
        for (byte b3 = 'a'; b3 <= 'z'; b3++) {
          k[0] = b1;
          k[1] = b2;
          k[2] = b3;
          Put put = new Put(k);
          put.setDurability(Durability.SKIP_WAL);
          put.add(f, null, k);
          if (r.getLog() == null) put.setDurability(Durability.SKIP_WAL);

          int preRowCount = rowCount;
          int pause = 10;
          int maxPause = 1000;
          while (rowCount == preRowCount) {
            try {
              r.put(put);
              rowCount++;
            } catch (RegionTooBusyException e) {
              pause = (pause * 2 >= maxPause) ? maxPause : pause * 2;
              Threads.sleep(pause);
            }
          }
        }
      }
      if (flush) {
        r.flushcache();
      }
    }
    return rowCount;
  }

  public void loadNumericRows(final HTable t, final byte[] f, int startRow, int endRow) throws IOException {
    for (int i = startRow; i < endRow; i++) {
      byte[] data = Bytes.toBytes(String.valueOf(i));
      Put put = new Put(data);
      put.add(f, null, data);
      t.put(put);
    }
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

  public int countRows(final HTable table, final byte[]... families) throws IOException {
    Scan scan = new Scan();
    for (byte[] family: families) {
      scan.addFamily(family);
    }
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

  /** All the row values for the data loaded by {@link #loadTable(HTable, byte[])} */
  public static final byte[][] ROWS = new byte[(int) Math.pow('z' - 'a' + 1, 3)][3]; // ~52KB
  static {
    int i = 0;
    for (byte b1 = 'a'; b1 <= 'z'; b1++) {
      for (byte b2 = 'a'; b2 <= 'z'; b2++) {
        for (byte b3 = 'a'; b3 <= 'z'; b3++) {
          ROWS[i][0] = b1;
          ROWS[i][1] = b2;
          ROWS[i][2] = b3;
          i++;
        }
      }
    }
  }

  public static final byte[][] KEYS = {
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

  public static final byte[][] KEYS_FOR_HBA_CREATE_TABLE = {
      Bytes.toBytes("bbb"),
      Bytes.toBytes("ccc"), Bytes.toBytes("ddd"), Bytes.toBytes("eee"),
      Bytes.toBytes("fff"), Bytes.toBytes("ggg"), Bytes.toBytes("hhh"),
      Bytes.toBytes("iii"), Bytes.toBytes("jjj"), Bytes.toBytes("kkk"),
      Bytes.toBytes("lll"), Bytes.toBytes("mmm"), Bytes.toBytes("nnn"),
      Bytes.toBytes("ooo"), Bytes.toBytes("ppp"), Bytes.toBytes("qqq"),
      Bytes.toBytes("rrr"), Bytes.toBytes("sss"), Bytes.toBytes("ttt"),
      Bytes.toBytes("uuu"), Bytes.toBytes("vvv"), Bytes.toBytes("www"),
      Bytes.toBytes("xxx"), Bytes.toBytes("yyy"), Bytes.toBytes("zzz")
  };

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
    return createMultiRegions(c, table, columnFamily, KEYS);
  }

  void makeDFSClientNonRetrying() {
    if (null == this.dfsCluster) {
      LOG.debug("dfsCluster has not started, can't make client non-retrying.");
      return;
    }
    try {
      final FileSystem filesystem = this.dfsCluster.getFileSystem();
      if (!(filesystem instanceof DistributedFileSystem)) {
        LOG.debug("dfsCluster is not backed by a DistributedFileSystem, can't make client non-retrying.");
        return;
      }
      // rely on FileSystem.CACHE to alter how we talk via DFSClient
      final DistributedFileSystem fs = (DistributedFileSystem)filesystem;
      // retrieve the backing DFSClient instance
      final Field dfsField = fs.getClass().getDeclaredField("dfs");
      dfsField.setAccessible(true);
      final Class<?> dfsClazz = dfsField.getType();
      final DFSClient dfs = DFSClient.class.cast(dfsField.get(fs));

      // expose the method for creating direct RPC connections.
      final Method createRPCNamenode = dfsClazz.getDeclaredMethod("createRPCNamenode", InetSocketAddress.class, Configuration.class, UserGroupInformation.class);
      createRPCNamenode.setAccessible(true);

      // grab the DFSClient instance's backing connection information
      final Field nnField = dfsClazz.getDeclaredField("nnAddress");
      nnField.setAccessible(true);
      final InetSocketAddress nnAddress = InetSocketAddress.class.cast(nnField.get(dfs));
      final Field confField = dfsClazz.getDeclaredField("conf");
      confField.setAccessible(true);
      final Configuration conf = Configuration.class.cast(confField.get(dfs));
      final Field ugiField = dfsClazz.getDeclaredField("ugi");
      ugiField.setAccessible(true);
      final UserGroupInformation ugi = UserGroupInformation.class.cast(ugiField.get(dfs));

      // replace the proxy for the namenode rpc with a direct instance
      final Field namenodeField = dfsClazz.getDeclaredField("namenode");
      namenodeField.setAccessible(true);
      namenodeField.set(dfs, createRPCNamenode.invoke(null, nnAddress, conf, ugi));
      LOG.debug("Set DSFClient namenode to bare RPC");
    } catch (Exception exception) {
      LOG.info("Could not alter DFSClient to be non-retrying.", exception);
    }
  }

  /**
   * Creates the specified number of regions in the specified table.
   * @param c
   * @param table
   * @param family
   * @param numRegions
   * @return
   * @throws IOException
   */
  public int createMultiRegions(final Configuration c, final HTable table,
      final byte [] family, int numRegions)
  throws IOException {
    if (numRegions < 3) throw new IOException("Must create at least 3 regions");
    byte [] startKey = Bytes.toBytes("aaaaa");
    byte [] endKey = Bytes.toBytes("zzzzz");
    byte [][] splitKeys = Bytes.split(startKey, endKey, numRegions - 3);
    byte [][] regionStartKeys = new byte[splitKeys.length+1][];
    System.arraycopy(splitKeys, 0, regionStartKeys, 1, splitKeys.length);
    regionStartKeys[0] = HConstants.EMPTY_BYTE_ARRAY;
    return createMultiRegions(c, table, family, regionStartKeys);
  }

  @SuppressWarnings("deprecation")
  public int createMultiRegions(final Configuration c, final HTable table,
      final byte[] columnFamily, byte [][] startKeys)
  throws IOException {
    Arrays.sort(startKeys, Bytes.BYTES_COMPARATOR);
    HTable meta = new HTable(c, TableName.META_TABLE_NAME);
    HTableDescriptor htd = table.getTableDescriptor();
    if(!htd.hasFamily(columnFamily)) {
      HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
      htd.addFamily(hcd);
    }
    // remove empty region - this is tricky as the mini cluster during the test
    // setup already has the "<tablename>,,123456789" row with an empty start
    // and end key. Adding the custom regions below adds those blindly,
    // including the new start region from empty to "bbb". lg
    List<byte[]> rows = getMetaTableRows(htd.getTableName());
    String regionToDeleteInFS = table
        .getRegionsInRange(Bytes.toBytes(""), Bytes.toBytes("")).get(0)
        .getRegionInfo().getEncodedName();
    List<HRegionInfo> newRegions = new ArrayList<HRegionInfo>(startKeys.length);
    // add custom ones
    int count = 0;
    for (int i = 0; i < startKeys.length; i++) {
      int j = (i + 1) % startKeys.length;
      HRegionInfo hri = new HRegionInfo(table.getName(),
        startKeys[i], startKeys[j]);
      MetaEditor.addRegionToMeta(meta, hri);
      newRegions.add(hri);
      count++;
    }
    // see comment above, remove "old" (or previous) single region
    for (byte[] row : rows) {
      LOG.info("createMultiRegions: deleting meta row -> " +
        Bytes.toStringBinary(row));
      meta.delete(new Delete(row));
    }
    // remove the "old" region from FS
    Path tableDir = new Path(getDefaultRootDirPath().toString()
        + System.getProperty("file.separator") + htd.getTableName()
        + System.getProperty("file.separator") + regionToDeleteInFS);
    FileSystem.get(c).delete(tableDir);
    // flush cache of regions
    HConnection conn = table.getConnection();
    conn.clearRegionCache();
    // assign all the new regions IF table is enabled.
    HBaseAdmin admin = getHBaseAdmin();
    if (admin.isTableEnabled(table.getTableName())) {
      for(HRegionInfo hri : newRegions) {
        admin.assign(hri.getRegionName());
      }
    }

    meta.close();

    return count;
  }

  /**
   * Create rows in hbase:meta for regions of the specified table with the specified
   * start keys.  The first startKey should be a 0 length byte array if you
   * want to form a proper range of regions.
   * @param conf
   * @param htd
   * @param startKeys
   * @return list of region info for regions added to meta
   * @throws IOException
   */
  public List<HRegionInfo> createMultiRegionsInMeta(final Configuration conf,
      final HTableDescriptor htd, byte [][] startKeys)
  throws IOException {
    HTable meta = new HTable(conf, TableName.META_TABLE_NAME);
    Arrays.sort(startKeys, Bytes.BYTES_COMPARATOR);
    List<HRegionInfo> newRegions = new ArrayList<HRegionInfo>(startKeys.length);
    // add custom ones
    for (int i = 0; i < startKeys.length; i++) {
      int j = (i + 1) % startKeys.length;
      HRegionInfo hri = new HRegionInfo(htd.getTableName(), startKeys[i],
          startKeys[j]);
      MetaEditor.addRegionToMeta(meta, hri);
      newRegions.add(hri);
    }

    meta.close();
    return newRegions;
  }

  /**
   * Returns all rows from the hbase:meta table.
   *
   * @throws IOException When reading the rows fails.
   */
  public List<byte[]> getMetaTableRows() throws IOException {
    // TODO: Redo using MetaReader class
    HTable t = new HTable(new Configuration(this.conf), TableName.META_TABLE_NAME);
    List<byte[]> rows = new ArrayList<byte[]>();
    ResultScanner s = t.getScanner(new Scan());
    for (Result result : s) {
      LOG.info("getMetaTableRows: row -> " +
        Bytes.toStringBinary(result.getRow()));
      rows.add(result.getRow());
    }
    s.close();
    t.close();
    return rows;
  }

  /**
   * Returns all rows from the hbase:meta table for a given user table
   *
   * @throws IOException When reading the rows fails.
   */
  public List<byte[]> getMetaTableRows(TableName tableName) throws IOException {
    // TODO: Redo using MetaReader.
    HTable t = new HTable(new Configuration(this.conf), TableName.META_TABLE_NAME);
    List<byte[]> rows = new ArrayList<byte[]>();
    ResultScanner s = t.getScanner(new Scan());
    for (Result result : s) {
      HRegionInfo info = HRegionInfo.getHRegionInfo(result);
      if (info == null) {
        LOG.error("No region info for row " + Bytes.toString(result.getRow()));
        // TODO figure out what to do for this new hosed case.
        continue;
      }

      if (info.getTable().equals(tableName)) {
        LOG.info("getMetaTableRows: row -> " +
            Bytes.toStringBinary(result.getRow()) + info);
        rows.add(result.getRow());
      }
    }
    s.close();
    t.close();
    return rows;
  }

  /**
   * Tool to get the reference to the region server object that holds the
   * region of the specified user table.
   * It first searches for the meta rows that contain the region of the
   * specified table, then gets the index of that RS, and finally retrieves
   * the RS's reference.
   * @param tableName user table to lookup in hbase:meta
   * @return region server that holds it, null if the row doesn't exist
   * @throws IOException
   * @throws InterruptedException
   */
  public HRegionServer getRSForFirstRegionInTable(byte[] tableName)
      throws IOException, InterruptedException {
    return getRSForFirstRegionInTable(TableName.valueOf(tableName));
  }
  /**
   * Tool to get the reference to the region server object that holds the
   * region of the specified user table.
   * It first searches for the meta rows that contain the region of the
   * specified table, then gets the index of that RS, and finally retrieves
   * the RS's reference.
   * @param tableName user table to lookup in hbase:meta
   * @return region server that holds it, null if the row doesn't exist
   * @throws IOException
   */
  public HRegionServer getRSForFirstRegionInTable(TableName tableName)
      throws IOException, InterruptedException {
    List<byte[]> metaRows = getMetaTableRows(tableName);
    if (metaRows == null || metaRows.isEmpty()) {
      return null;
    }
    LOG.debug("Found " + metaRows.size() + " rows for table " +
      tableName);
    byte [] firstrow = metaRows.get(0);
    LOG.debug("FirstRow=" + Bytes.toString(firstrow));
    long pause = getConfiguration().getLong(HConstants.HBASE_CLIENT_PAUSE,
      HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    int numRetries = getConfiguration().getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    RetryCounter retrier = new RetryCounter(numRetries+1, (int)pause, TimeUnit.MICROSECONDS);
    while(retrier.shouldRetry()) {
      int index = getMiniHBaseCluster().getServerWith(firstrow);
      if (index != -1) {
        return getMiniHBaseCluster().getRegionServerThreads().get(index).getRegionServer();
      }
      // Came back -1.  Region may not be online yet.  Sleep a while.
      retrier.sleepUntilNextRetry();
    }
    return null;
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
   * @param servers  The number of <code>TaskTracker</code>'s to start.
   * @throws IOException When starting the cluster fails.
   */
  private void startMiniMapReduceCluster(final int servers) throws IOException {
    if (mrCluster != null) {
      throw new IllegalStateException("MiniMRCluster is already running");
    }
    LOG.info("Starting mini mapreduce cluster...");
    setupClusterTestDir();
    createDirsAndSetProperties();

    forceChangeTaskLogDir();

    //// hadoop2 specific settings
    // Tests were failing because this process used 6GB of virtual memory and was getting killed.
    // we up the VM usable so that processes don't get killed.
    conf.setFloat("yarn.nodemanager.vmem-pmem-ratio", 8.0f);

    // Tests were failing due to MAPREDUCE-4880 / MAPREDUCE-4607 against hadoop 2.0.2-alpha and
    // this avoids the problem by disabling speculative task execution in tests.
    conf.setBoolean("mapreduce.map.speculative", false);
    conf.setBoolean("mapreduce.reduce.speculative", false);
    ////

    // Allow the user to override FS URI for this map-reduce cluster to use.
    mrCluster = new MiniMRCluster(servers,
      FS_URI != null ? FS_URI : FileSystem.get(conf).getUri().toString(), 1,
      null, null, new JobConf(this.conf));
    JobConf jobConf = MapreduceTestingShim.getJobConf(mrCluster);
    if (jobConf == null) {
      jobConf = mrCluster.createJobConf();
    }

    jobConf.set("mapred.local.dir",
      conf.get("mapred.local.dir")); //Hadoop MiniMR overwrites this while it should not
    LOG.info("Mini mapreduce cluster started");

    // In hadoop2, YARN/MR2 starts a mini cluster with its own conf instance and updates settings.
    // Our HBase MR jobs need several of these settings in order to properly run.  So we copy the
    // necessary config properties here.  YARN-129 required adding a few properties.
    conf.set("mapred.job.tracker", jobConf.get("mapred.job.tracker"));
    // this for mrv2 support; mr1 ignores this
    conf.set("mapreduce.framework.name", "yarn");
    conf.setBoolean("yarn.is.minicluster", true);
    String rmAddress = jobConf.get("yarn.resourcemanager.address");
    if (rmAddress != null) {
      conf.set("yarn.resourcemanager.address", rmAddress);
    }
    String historyAddress = jobConf.get("mapreduce.jobhistory.address");
    if (historyAddress != null) {
      conf.set("mapreduce.jobhistory.address", historyAddress);
    }
    String schedulerAddress =
      jobConf.get("yarn.resourcemanager.scheduler.address");
    if (schedulerAddress != null) {
      conf.set("yarn.resourcemanager.scheduler.address", schedulerAddress);
    }
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
   * Create a stubbed out RegionServerService, mainly for getting FS.
   */
  public RegionServerServices createMockRegionServerService() throws IOException {
    return createMockRegionServerService((ServerName)null);
  }

  /**
   * Create a stubbed out RegionServerService, mainly for getting FS.
   * This version is used by TestTokenAuthentication
   */
  public RegionServerServices createMockRegionServerService(RpcServerInterface rpc) throws IOException {
    final MockRegionServerServices rss = new MockRegionServerServices(getZooKeeperWatcher());
    rss.setFileSystem(getTestFileSystem());
    rss.setRpcServer(rpc);
    return rss;
  }

  /**
   * Create a stubbed out RegionServerService, mainly for getting FS.
   * This version is used by TestOpenRegionHandler
   */
  public RegionServerServices createMockRegionServerService(ServerName name) throws IOException {
    final MockRegionServerServices rss = new MockRegionServerServices(getZooKeeperWatcher(), name);
    rss.setFileSystem(getTestFileSystem());
    return rss;
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
    HMaster master = getMiniHBaseCluster().getMaster();
    expireSession(master.getZooKeeper(), false);
  }

  /**
   * Expire a region server's session
   * @param index which RS
   * @throws Exception
   */
  public void expireRegionServerSession(int index) throws Exception {
    HRegionServer rs = getMiniHBaseCluster().getRegionServer(index);
    expireSession(rs.getZooKeeper(), false);
    decrementMinRegionServerCount();
  }

  private void decrementMinRegionServerCount() {
    // decrement the count for this.conf, for newly spwaned master
    // this.hbaseCluster shares this configuration too
    decrementMinRegionServerCount(getConfiguration());

    // each master thread keeps a copy of configuration
    for (MasterThread master : getHBaseCluster().getMasterThreads()) {
      decrementMinRegionServerCount(master.getMaster().getConfiguration());
    }
  }

  private void decrementMinRegionServerCount(Configuration conf) {
    int currentCount = conf.getInt(
        ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, -1);
    if (currentCount != -1) {
      conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART,
          Math.max(currentCount - 1, 1));
    }
  }

  public void expireSession(ZooKeeperWatcher nodeZK) throws Exception {
   expireSession(nodeZK, false);
  }

  @Deprecated
  public void expireSession(ZooKeeperWatcher nodeZK, Server server)
    throws Exception {
    expireSession(nodeZK, false);
  }

  /**
   * Expire a ZooKeeper session as recommended in ZooKeeper documentation
   * http://wiki.apache.org/hadoop/ZooKeeper/FAQ#A4
   * There are issues when doing this:
   * [1] http://www.mail-archive.com/dev@zookeeper.apache.org/msg01942.html
   * [2] https://issues.apache.org/jira/browse/ZOOKEEPER-1105
   *
   * @param nodeZK - the ZK watcher to expire
   * @param checkStatus - true to check if we can create an HTable with the
   *                    current configuration.
   */
  public void expireSession(ZooKeeperWatcher nodeZK, boolean checkStatus)
    throws Exception {
    Configuration c = new Configuration(this.conf);
    String quorumServers = ZKConfig.getZKQuorumServersString(c);
    ZooKeeper zk = nodeZK.getRecoverableZooKeeper().getZooKeeper();
    byte[] password = zk.getSessionPasswd();
    long sessionID = zk.getSessionId();

    // Expiry seems to be asynchronous (see comment from P. Hunt in [1]),
    //  so we create a first watcher to be sure that the
    //  event was sent. We expect that if our watcher receives the event
    //  other watchers on the same machine will get is as well.
    // When we ask to close the connection, ZK does not close it before
    //  we receive all the events, so don't have to capture the event, just
    //  closing the connection should be enough.
    ZooKeeper monitor = new ZooKeeper(quorumServers,
      1000, new org.apache.zookeeper.Watcher(){
      @Override
      public void process(WatchedEvent watchedEvent) {
        LOG.info("Monitor ZKW received event="+watchedEvent);
      }
    } , sessionID, password);

    // Making it expire
    ZooKeeper newZK = new ZooKeeper(quorumServers,
        1000, EmptyWatcher.instance, sessionID, password);

    //ensure that we have connection to the server before closing down, otherwise
    //the close session event will be eaten out before we start CONNECTING state
    long start = System.currentTimeMillis();
    while (newZK.getState() != States.CONNECTED
         && System.currentTimeMillis() - start < 1000) {
       Thread.sleep(1);
    }
    newZK.close();
    LOG.info("ZK Closed Session 0x" + Long.toHexString(sessionID));

    // Now closing & waiting to be sure that the clients get it.
    monitor.close();

    if (checkStatus) {
      new HTable(new Configuration(conf), TableName.META_TABLE_NAME).close();
    }
  }

  /**
   * Get the Mini HBase cluster.
   *
   * @return hbase cluster
   * @see #getHBaseClusterInterface()
   */
  public MiniHBaseCluster getHBaseCluster() {
    return getMiniHBaseCluster();
  }

  /**
   * Returns the HBaseCluster instance.
   * <p>Returned object can be any of the subclasses of HBaseCluster, and the
   * tests referring this should not assume that the cluster is a mini cluster or a
   * distributed one. If the test only works on a mini cluster, then specific
   * method {@link #getMiniHBaseCluster()} can be used instead w/o the
   * need to type-cast.
   */
  public HBaseCluster getHBaseClusterInterface() {
    //implementation note: we should rename this method as #getHBaseCluster(),
    //but this would require refactoring 90+ calls.
    return hbaseCluster;
  }

  /**
   * Returns a HBaseAdmin instance.
   * This instance is shared between HBaseTestingUtility instance users.
   * Closing it has no effect, it will be closed automatically when the
   * cluster shutdowns
   *
   * @return The HBaseAdmin instance.
   * @throws IOException
   */
  public synchronized HBaseAdmin getHBaseAdmin()
  throws IOException {
    if (hbaseAdmin == null){
      hbaseAdmin = new HBaseAdminForTests(getConfiguration());
    }
    return hbaseAdmin;
  }

  private HBaseAdminForTests hbaseAdmin = null;
  private static class HBaseAdminForTests extends HBaseAdmin {
    public HBaseAdminForTests(Configuration c) throws MasterNotRunningException,
        ZooKeeperConnectionException, IOException {
      super(c);
    }

    @Override
    public synchronized void close() throws IOException {
      LOG.warn("close() called on HBaseAdmin instance returned from HBaseTestingUtility.getHBaseAdmin()");
    }

    private synchronized void close0() throws IOException {
      super.close();
    }
  }

  /**
   * Returns a ZooKeeperWatcher instance.
   * This instance is shared between HBaseTestingUtility instance users.
   * Don't close it, it will be closed automatically when the
   * cluster shutdowns
   *
   * @return The ZooKeeperWatcher instance.
   * @throws IOException
   */
  public synchronized ZooKeeperWatcher getZooKeeperWatcher()
    throws IOException {
    if (zooKeeperWatcher == null) {
      zooKeeperWatcher = new ZooKeeperWatcher(conf, "testing utility",
        new Abortable() {
        @Override public void abort(String why, Throwable e) {
          throw new RuntimeException("Unexpected abort in HBaseTestingUtility:"+why, e);
        }
        @Override public boolean isAborted() {return false;}
      });
    }
    return zooKeeperWatcher;
  }
  private ZooKeeperWatcher zooKeeperWatcher;



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
    getHBaseAdmin().closeRegion(regionName, null);
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

  /*
   * Retrieves a splittable region randomly from tableName
   *
   * @param tableName name of table
   * @param maxAttempts maximum number of attempts, unlimited for value of -1
   * @return the HRegion chosen, null if none was found within limit of maxAttempts
   */
  public HRegion getSplittableRegion(TableName tableName, int maxAttempts) {
    List<HRegion> regions = getHBaseCluster().getRegions(tableName);
    int regCount = regions.size();
    Set<Integer> attempted = new HashSet<Integer>();
    int idx;
    int attempts = 0;
    do {
      regions = getHBaseCluster().getRegions(tableName);
      if (regCount != regions.size()) {
        // if there was region movement, clear attempted Set
        attempted.clear();
      }
      regCount = regions.size();
      // There are chances that before we get the region for the table from an RS the region may
      // be going for CLOSE.  This may be because online schema change is enabled
      if (regCount > 0) {
        idx = random.nextInt(regCount);
        // if we have just tried this region, there is no need to try again
        if (attempted.contains(idx))
          continue;
        try {
          regions.get(idx).checkSplit();
          return regions.get(idx);
        } catch (Exception ex) {
          LOG.warn("Caught exception", ex);
          attempted.add(idx);
        }
      }
      attempts++;
    } while (maxAttempts == -1 || attempts < maxAttempts);
    return null;
  }

  public MiniZooKeeperCluster getZkCluster() {
    return zkCluster;
  }

  public void setZkCluster(MiniZooKeeperCluster zkCluster) {
    this.passedZkCluster = true;
    this.zkCluster = zkCluster;
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkCluster.getClientPort());
  }

  public MiniDFSCluster getDFSCluster() {
    return dfsCluster;
  }

  public void setDFSCluster(MiniDFSCluster cluster) throws IOException {
    if (dfsCluster != null && dfsCluster.isClusterUp()) {
      throw new IOException("DFSCluster is already running! Shut it down first.");
    }
    this.dfsCluster = cluster;
  }

  public FileSystem getTestFileSystem() throws IOException {
    return HFileSystem.get(conf);
  }

  /**
   * Wait until all regions in a table have been assigned.  Waits default timeout before giving up
   * (30 seconds).
   * @param table Table to wait on.
   * @throws InterruptedException
   * @throws IOException
   */
  public void waitTableAvailable(byte[] table)
      throws InterruptedException, IOException {
    waitTableAvailable(getHBaseAdmin(), table, 30000);
  }

  public void waitTableAvailable(HBaseAdmin admin, byte[] table)
      throws InterruptedException, IOException {
    waitTableAvailable(admin, table, 30000);
  }

  /**
   * Wait until all regions in a table have been assigned
   * @param table Table to wait on.
   * @param timeoutMillis Timeout.
   * @throws InterruptedException
   * @throws IOException
   */
  public void waitTableAvailable(byte[] table, long timeoutMillis)
  throws InterruptedException, IOException {
    waitTableAvailable(getHBaseAdmin(), table, timeoutMillis);
  }

  public void waitTableAvailable(HBaseAdmin admin, byte[] table, long timeoutMillis)
  throws InterruptedException, IOException {
    long startWait = System.currentTimeMillis();
    while (!admin.isTableAvailable(table)) {
      assertTrue("Timed out waiting for table to become available " +
        Bytes.toStringBinary(table),
        System.currentTimeMillis() - startWait < timeoutMillis);
      Thread.sleep(200);
    }
  }

  /**
   * Waits for a table to be 'enabled'.  Enabled means that table is set as 'enabled' and the
   * regions have been all assigned.  Will timeout after default period (30 seconds)
   * @see #waitTableAvailable(byte[])
   * @param table Table to wait on.
   * @param table
   * @throws InterruptedException
   * @throws IOException
   */
  public void waitTableEnabled(byte[] table)
      throws InterruptedException, IOException {
    waitTableEnabled(getHBaseAdmin(), table, 30000);
  }

  public void waitTableEnabled(HBaseAdmin admin, byte[] table)
      throws InterruptedException, IOException {
    waitTableEnabled(admin, table, 30000);
  }

  /**
   * Waits for a table to be 'enabled'.  Enabled means that table is set as 'enabled' and the
   * regions have been all assigned.
   * @see #waitTableAvailable(byte[])
   * @param table Table to wait on.
   * @param timeoutMillis Time to wait on it being marked enabled.
   * @throws InterruptedException
   * @throws IOException
   */
  public void waitTableEnabled(byte[] table, long timeoutMillis)
  throws InterruptedException, IOException {
    waitTableEnabled(getHBaseAdmin(), table, timeoutMillis);
  }

  public void waitTableEnabled(HBaseAdmin admin, byte[] table, long timeoutMillis)
  throws InterruptedException, IOException {
    long startWait = System.currentTimeMillis();
    waitTableAvailable(admin, table, timeoutMillis);
    while (!admin.isTableEnabled(table)) {
      assertTrue("Timed out waiting for table to become available and enabled " +
         Bytes.toStringBinary(table),
         System.currentTimeMillis() - startWait < timeoutMillis);
      Thread.sleep(200);
    }
    // Finally make sure all regions are fully open and online out on the cluster. Regions may be
    // in the hbase:meta table and almost open on all regionservers but there setting the region
    // online in the regionserver is the very last thing done and can take a little while to happen.
    // Below we do a get.  The get will retry if a NotServeringRegionException or a
    // RegionOpeningException.  It is crass but when done all will be online.
    try {
      Canary.sniff(admin, TableName.valueOf(table));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Make sure that at least the specified number of region servers
   * are running
   * @param num minimum number of region servers that should be running
   * @return true if we started some servers
   * @throws IOException
   */
  public boolean ensureSomeRegionServersAvailable(final int num)
      throws IOException {
    boolean startedServer = false;
    MiniHBaseCluster hbaseCluster = getMiniHBaseCluster();
    for (int i=hbaseCluster.getLiveRegionServerThreads().size(); i<num; ++i) {
      LOG.info("Started new server=" + hbaseCluster.startRegionServer());
      startedServer = true;
    }

    return startedServer;
  }


  /**
   * Make sure that at least the specified number of region servers
   * are running. We don't count the ones that are currently stopping or are
   * stopped.
   * @param num minimum number of region servers that should be running
   * @return true if we started some servers
   * @throws IOException
   */
  public boolean ensureSomeNonStoppedRegionServersAvailable(final int num)
    throws IOException {
    boolean startedServer = ensureSomeRegionServersAvailable(num);

    int nonStoppedServers = 0;
    for (JVMClusterUtil.RegionServerThread rst :
      getMiniHBaseCluster().getRegionServerThreads()) {

      HRegionServer hrs = rst.getRegionServer();
      if (hrs.isStopping() || hrs.isStopped()) {
        LOG.info("A region server is stopped or stopping:"+hrs);
      } else {
        nonStoppedServers++;
      }
    }
    for (int i=nonStoppedServers; i<num; ++i) {
      LOG.info("Started new server=" + getMiniHBaseCluster().startRegionServer());
      startedServer = true;
    }
    return startedServer;
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
  public static User getDifferentUser(final Configuration c,
    final String differentiatingSuffix)
  throws IOException {
    FileSystem currentfs = FileSystem.get(c);
    if (!(currentfs instanceof DistributedFileSystem)) {
      return User.getCurrent();
    }
    // Else distributed filesystem.  Make a new instance per daemon.  Below
    // code is taken from the AppendTestUtil over in hdfs.
    String username = User.getCurrent().getName() +
      differentiatingSuffix;
    User user = User.createUserForTesting(c, username,
        new String[]{"supergroup"});
    return user;
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
   * Wait until all regions for a table in hbase:meta have a non-empty
   * info:server, up to 60 seconds. This means all regions have been deployed,
   * master has been informed and updated hbase:meta with the regions deployed
   * server.
   * @param tableName the table name
   * @throws IOException
   */
  public void waitUntilAllRegionsAssigned(final TableName tableName) throws IOException {
    waitUntilAllRegionsAssigned(tableName, 60000);
  }

  /**
   * Wait until all regions for a table in hbase:meta have a non-empty
   * info:server, or until timeout.  This means all regions have been deployed,
   * master has been informed and updated hbase:meta with the regions deployed
   * server.
   * @param tableName the table name
   * @param timeout timeout, in milliseconds
   * @throws IOException
   */
  public void waitUntilAllRegionsAssigned(final TableName tableName, final long timeout)
      throws IOException {
    final HTable meta = new HTable(getConfiguration(), TableName.META_TABLE_NAME);
    try {
      waitFor(timeout, 200, true, new Predicate<IOException>() {
        @Override
        public boolean evaluate() throws IOException {
          boolean allRegionsAssigned = true;
          Scan scan = new Scan();
          scan.addFamily(HConstants.CATALOG_FAMILY);
          ResultScanner s = meta.getScanner(scan);
          try {
            Result r;
            while ((r = s.next()) != null) {
              byte [] b = r.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
              HRegionInfo info = HRegionInfo.parseFromOrNull(b);
              if (info != null && info.getTable().equals(tableName)) {
                b = r.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
                allRegionsAssigned &= (b != null);
              }
            }
          } finally {
            s.close();
          }
          return allRegionsAssigned;
        }
      });
    } finally {
      meta.close();
    }
  }

  /**
   * Do a small get/scan against one store. This is required because store
   * has no actual methods of querying itself, and relies on StoreScanner.
   */
  public static List<Cell> getFromStoreFile(HStore store,
                                                Get get) throws IOException {
    Scan scan = new Scan(get);
    InternalScanner scanner = (InternalScanner) store.getScanner(scan,
        scan.getFamilyMap().get(store.getFamily().getName()),
        // originally MultiVersionConsistencyControl.resetThreadReadPoint() was called to set
        // readpoint 0.
        0);

    List<Cell> result = new ArrayList<Cell>();
    scanner.next(result);
    if (!result.isEmpty()) {
      // verify that we are on the row we want:
      Cell kv = result.get(0);
      if (!CellUtil.matchingRow(kv, get.getRow())) {
        result.clear();
      }
    }
    scanner.close();
    return result;
  }

  /**
   * Create region split keys between startkey and endKey
   *
   * @param startKey
   * @param endKey
   * @param numRegions the number of regions to be created. it has to be greater than 3.
   * @return
   */
  public byte[][] getRegionSplitStartKeys(byte[] startKey, byte[] endKey, int numRegions){
    assertTrue(numRegions>3);
    byte [][] tmpSplitKeys = Bytes.split(startKey, endKey, numRegions - 3);
    byte [][] result = new byte[tmpSplitKeys.length+1][];
    System.arraycopy(tmpSplitKeys, 0, result, 1, tmpSplitKeys.length);
    result[0] = HConstants.EMPTY_BYTE_ARRAY;
    return result;
  }

  /**
   * Do a small get/scan against one store. This is required because store
   * has no actual methods of querying itself, and relies on StoreScanner.
   */
  public static List<Cell> getFromStoreFile(HStore store,
                                                byte [] row,
                                                NavigableSet<byte[]> columns
                                                ) throws IOException {
    Get get = new Get(row);
    Map<byte[], NavigableSet<byte[]>> s = get.getFamilyMap();
    s.put(store.getFamily().getName(), columns);

    return getFromStoreFile(store,get);
  }

  /**
   * Gets a ZooKeeperWatcher.
   * @param TEST_UTIL
   */
  public static ZooKeeperWatcher getZooKeeperWatcher(
      HBaseTestingUtility TEST_UTIL) throws ZooKeeperConnectionException,
      IOException {
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
        "unittest", new Abortable() {
          boolean aborted = false;

          @Override
          public void abort(String why, Throwable e) {
            aborted = true;
            throw new RuntimeException("Fatal ZK error, why=" + why, e);
          }

          @Override
          public boolean isAborted() {
            return aborted;
          }
        });
    return zkw;
  }

  /**
   * Creates a znode with OPENED state.
   * @param TEST_UTIL
   * @param region
   * @param serverName
   * @return
   * @throws IOException
   * @throws org.apache.hadoop.hbase.ZooKeeperConnectionException
   * @throws KeeperException
   * @throws NodeExistsException
   */
  public static ZooKeeperWatcher createAndForceNodeToOpenedState(
      HBaseTestingUtility TEST_UTIL, HRegion region,
      ServerName serverName) throws ZooKeeperConnectionException,
      IOException, KeeperException, NodeExistsException {
    ZooKeeperWatcher zkw = getZooKeeperWatcher(TEST_UTIL);
    ZKAssign.createNodeOffline(zkw, region.getRegionInfo(), serverName);
    int version = ZKAssign.transitionNodeOpening(zkw, region
        .getRegionInfo(), serverName);
    ZKAssign.transitionNodeOpened(zkw, region.getRegionInfo(), serverName,
        version);
    return zkw;
  }

  public static void assertKVListsEqual(String additionalMsg,
      final List<? extends Cell> expected,
      final List<? extends Cell> actual) {
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

  public String getClusterKey() {
    return conf.get(HConstants.ZOOKEEPER_QUORUM) + ":"
        + conf.get(HConstants.ZOOKEEPER_CLIENT_PORT) + ":"
        + conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
            HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
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
    final HTable table = createTable(tableName, cfBytes,
        maxVersions,
        Bytes.toBytes(String.format(keyFormat, splitStartKey)),
        Bytes.toBytes(String.format(keyFormat, splitEndKey)),
        numRegions);

    if (hbaseCluster != null) {
      getMiniHBaseCluster().flushcache(TableName.META_TABLE_NAME);
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
        getMiniHBaseCluster().flushcache(table.getName());
      }
    }

    return table;
  }

  private static final int MIN_RANDOM_PORT = 0xc000;
  private static final int MAX_RANDOM_PORT = 0xfffe;
  private static Random random = new Random();

  /**
   * Returns a random port. These ports cannot be registered with IANA and are
   * intended for dynamic allocation (see http://bit.ly/dynports).
   */
  public static int randomPort() {
    return MIN_RANDOM_PORT
        + random.nextInt(MAX_RANDOM_PORT - MIN_RANDOM_PORT);
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


  public static String randomMultiCastAddress() {
    return "226.1.1." + random.nextInt(254);
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
      TableName tableName, byte[] columnFamily, Algorithm compression,
      DataBlockEncoding dataBlockEncoding) throws IOException {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
    hcd.setDataBlockEncoding(dataBlockEncoding);
    hcd.setCompressionType(compression);
    return createPreSplitLoadTestTable(conf, desc, hcd);
  }

  /**
   * Creates a pre-split table for load testing. If the table already exists,
   * logs a warning and continues.
   * @return the number of regions the table was split into
   */
  public static int createPreSplitLoadTestTable(Configuration conf,
      HTableDescriptor desc, HColumnDescriptor hcd) throws IOException {
    if (!desc.hasFamily(hcd.getName())) {
      desc.addFamily(hcd);
    }

    int totalNumberOfRegions = 0;
    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      // create a table a pre-splits regions.
      // The number of splits is set as:
      //    region servers * regions per region server).
      int numberOfServers = admin.getClusterStatus().getServers().size();
      if (numberOfServers == 0) {
        throw new IllegalStateException("No live regionservers");
      }

      int regionsPerServer = conf.getInt(REGIONS_PER_SERVER_KEY, DEFAULT_REGIONS_PER_SERVER);
      totalNumberOfRegions = numberOfServers * regionsPerServer;
      LOG.info("Number of live regionservers: " + numberOfServers + ", " +
          "pre-splitting table into " + totalNumberOfRegions + " regions " +
          "(default regions per server: " + regionsPerServer + ")");

      byte[][] splits = new RegionSplitter.HexStringSplit().split(
          totalNumberOfRegions);

      admin.createTable(desc, splits);
    } catch (MasterNotRunningException e) {
      LOG.error("Master not running", e);
      throw new IOException(e);
    } catch (TableExistsException e) {
      LOG.warn("Table " + desc.getTableName() +
          " already exists, continuing");
    } finally {
      admin.close();
    }
    return totalNumberOfRegions;
  }

  public static int getMetaRSPort(Configuration conf) throws IOException {
    HTable table = new HTable(conf, TableName.META_TABLE_NAME);
    HRegionLocation hloc = table.getRegionLocation(Bytes.toBytes(""));
    table.close();
    return hloc.getPort();
  }

  /**
   *  Due to async racing issue, a region may not be in
   *  the online region list of a region server yet, after
   *  the assignment znode is deleted and the new assignment
   *  is recorded in master.
   */
  public void assertRegionOnServer(
      final HRegionInfo hri, final ServerName server,
      final long timeout) throws IOException, InterruptedException {
    long timeoutTime = System.currentTimeMillis() + timeout;
    while (true) {
      List<HRegionInfo> regions = getHBaseAdmin().getOnlineRegions(server);
      if (regions.contains(hri)) return;
      long now = System.currentTimeMillis();
      if (now > timeoutTime) break;
      Thread.sleep(10);
    }
    fail("Could not find region " + hri.getRegionNameAsString()
      + " on server " + server);
  }

  /**
   * Check to make sure the region is open on the specified
   * region server, but not on any other one.
   */
  public void assertRegionOnlyOnServer(
      final HRegionInfo hri, final ServerName server,
      final long timeout) throws IOException, InterruptedException {
    long timeoutTime = System.currentTimeMillis() + timeout;
    while (true) {
      List<HRegionInfo> regions = getHBaseAdmin().getOnlineRegions(server);
      if (regions.contains(hri)) {
        List<JVMClusterUtil.RegionServerThread> rsThreads =
          getHBaseCluster().getLiveRegionServerThreads();
        for (JVMClusterUtil.RegionServerThread rsThread: rsThreads) {
          HRegionServer rs = rsThread.getRegionServer();
          if (server.equals(rs.getServerName())) {
            continue;
          }
          Collection<HRegion> hrs = rs.getOnlineRegionsLocalContext();
          for (HRegion r: hrs) {
            assertTrue("Region should not be double assigned",
              r.getRegionId() != hri.getRegionId());
          }
        }
        return; // good, we are happy
      }
      long now = System.currentTimeMillis();
      if (now > timeoutTime) break;
      Thread.sleep(10);
    }
    fail("Could not find region " + hri.getRegionNameAsString()
      + " on server " + server);
  }

  public HRegion createTestRegion(String tableName, HColumnDescriptor hcd)
      throws IOException {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    htd.addFamily(hcd);
    HRegionInfo info =
        new HRegionInfo(TableName.valueOf(tableName), null, null, false);
    HRegion region =
        HRegion.createHRegion(info, getDataTestDir(), getConfiguration(), htd);
    return region;
  }

  public void setFileSystemURI(String fsURI) {
    FS_URI = fsURI;
  }

  /**
   * Wrapper method for {@link Waiter#waitFor(Configuration, long, Predicate)}.
   */
  public <E extends Exception> long waitFor(long timeout, Predicate<E> predicate)
      throws E {
    return Waiter.waitFor(this.conf, timeout, predicate);
  }

  /**
   * Wrapper method for {@link Waiter#waitFor(Configuration, long, long, Predicate)}.
   */
  public <E extends Exception> long waitFor(long timeout, long interval, Predicate<E> predicate)
      throws E {
    return Waiter.waitFor(this.conf, timeout, interval, predicate);
  }

  /**
   * Wrapper method for {@link Waiter#waitFor(Configuration, long, long, boolean, Predicate)}.
   */
  public <E extends Exception> long waitFor(long timeout, long interval,
      boolean failIfTimeout, Predicate<E> predicate) throws E {
    return Waiter.waitFor(this.conf, timeout, interval, failIfTimeout, predicate);
  }

  /**
   * Returns a {@link Predicate} for checking that there are no regions in transition in master
   */
  public Waiter.Predicate<Exception> predicateNoRegionsInTransition() {
    return new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        final RegionStates regionStates = getMiniHBaseCluster().getMaster()
            .getAssignmentManager().getRegionStates();
        return !regionStates.isRegionsInTransition();
      }
    };
  }

  /**
   * Returns a {@link Predicate} for checking that table is enabled
   */
  public Waiter.Predicate<Exception> predicateTableEnabled(final TableName tableName) {
    return new Waiter.Predicate<Exception>() {
     @Override
     public boolean evaluate() throws Exception {
       return getHBaseAdmin().isTableEnabled(tableName);
      }
    };
  }

  /**
   * Create a set of column descriptors with the combination of compression,
   * encoding, bloom codecs available.
   * @return the list of column descriptors
   */
  public static List<HColumnDescriptor> generateColumnDescriptors() {
    return generateColumnDescriptors("");
  }

  /**
   * Create a set of column descriptors with the combination of compression,
   * encoding, bloom codecs available.
   * @param prefix family names prefix
   * @return the list of column descriptors
   */
  public static List<HColumnDescriptor> generateColumnDescriptors(final String prefix) {
    List<HColumnDescriptor> htds = new ArrayList<HColumnDescriptor>();
    long familyId = 0;
    for (Compression.Algorithm compressionType: getSupportedCompressionAlgorithms()) {
      for (DataBlockEncoding encodingType: DataBlockEncoding.values()) {
        for (BloomType bloomType: BloomType.values()) {
          String name = String.format("%s-cf-!@#&-%d!@#", prefix, familyId);
          HColumnDescriptor htd = new HColumnDescriptor(name);
          htd.setCompressionType(compressionType);
          htd.setDataBlockEncoding(encodingType);
          htd.setBloomFilterType(bloomType);
          htds.add(htd);
          familyId++;
        }
      }
    }
    return htds;
  }

  /**
   * Get supported compression algorithms.
   * @return supported compression algorithms.
   */
  public static Compression.Algorithm[] getSupportedCompressionAlgorithms() {
    String[] allAlgos = HFile.getSupportedCompressionAlgorithms();
    List<Compression.Algorithm> supportedAlgos = new ArrayList<Compression.Algorithm>();
    for (String algoName : allAlgos) {
      try {
        Compression.Algorithm algo = Compression.getCompressionAlgorithmByName(algoName);
        algo.getCompressor();
        supportedAlgos.add(algo);
      } catch (Throwable t) {
        // this algo is not available
      }
    }
    return supportedAlgos.toArray(new Algorithm[supportedAlgos.size()]);
  }
}
