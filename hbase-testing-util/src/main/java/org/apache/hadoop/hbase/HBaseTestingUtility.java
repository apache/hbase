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
package org.apache.hadoop.hbase;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.BindException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.ClusterConnectionFactory;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Hbck;
import org.apache.hadoop.hbase.client.MasterRegistry;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.ChecksumUtil;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.logging.Log4jUtils;
import org.apache.hadoop.hbase.mapreduce.MapreduceTestingShim;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.AssignmentTestingUtil;
import org.apache.hadoop.hbase.master.assignment.RegionStateStore;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.mob.MobFileCache;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.ChunkCreator;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.visibility.VisibilityLabelsCache;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.util.RegionSplitter.SplitAlgorithm;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.zookeeper.EmptyWatcher;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * Facility for testing HBase. Replacement for
 * old HBaseTestCase and HBaseClusterTestCase functionality.
 * Create an instance and keep it around testing HBase.  This class is
 * meant to be your one-stop shop for anything you might need testing.  Manages
 * one cluster at a time only. Managed cluster can be an in-process
 * {@link MiniHBaseCluster}, or a deployed cluster of type {@code DistributedHBaseCluster}.
 * Not all methods work with the real cluster.
 * Depends on log4j being on classpath and
 * hbase-site.xml for logging and test-run configuration.  It does not set
 * logging levels.
 * In the configuration properties, default values for master-info-port and
 * region-server-port are overridden such that a random port will be assigned (thus
 * avoiding port contention if another local HBase instance is already running).
 * <p>To preserve test data directories, pass the system property "hbase.testing.preserve.testdir"
 * setting it to true.
 * @deprecated since 3.0.0, will be removed in 4.0.0. Use
 *             {@link org.apache.hadoop.hbase.testing.TestingHBaseCluster} instead.
 */
@InterfaceAudience.Public
@Deprecated
public class HBaseTestingUtility extends HBaseZKTestingUtility {

  /**
   * System property key to get test directory value. Name is as it is because mini dfs has
   * hard-codings to put test data here. It should NOT be used directly in HBase, as it's a property
   * used in mini dfs.
   * @deprecated since 2.0.0 and will be removed in 3.0.0. Can be used only with mini dfs.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-19410">HBASE-19410</a>
   */
  @Deprecated
  private static final String TEST_DIRECTORY_KEY = "test.build.data";

  public static final String REGIONS_PER_SERVER_KEY = "hbase.test.regions-per-server";
  /**
   * The default number of regions per regionserver when creating a pre-split
   * table.
   */
  public static final int DEFAULT_REGIONS_PER_SERVER = 3;


  public static final String PRESPLIT_TEST_TABLE_KEY = "hbase.test.pre-split-table";
  public static final boolean PRESPLIT_TEST_TABLE = true;

  private MiniDFSCluster dfsCluster = null;

  private volatile HBaseCluster hbaseCluster = null;
  private MiniMRCluster mrCluster = null;

  /** If there is a mini cluster running for this testing utility instance. */
  private volatile boolean miniClusterRunning;

  private String hadoopLogDir;

  /** Directory on test filesystem where we put the data for this instance of
    * HBaseTestingUtility*/
  private Path dataTestDirOnTestFS = null;

  private final AtomicReference<AsyncClusterConnection> asyncConnection = new AtomicReference<>();

  /** Filesystem URI used for map-reduce mini-cluster setup */
  private static String FS_URI;

  /** This is for unit tests parameterized with a single boolean. */
  public static final List<Object[]> MEMSTORETS_TAGS_PARAMETRIZED = memStoreTSAndTagsCombination();

  /**
   * Checks to see if a specific port is available.
   *
   * @param port the port number to check for availability
   * @return <tt>true</tt> if the port is available, or <tt>false</tt> if not
   */
  public static boolean available(int port) {
    ServerSocket ss = null;
    DatagramSocket ds = null;
    try {
      ss = new ServerSocket(port);
      ss.setReuseAddress(true);
      ds = new DatagramSocket(port);
      ds.setReuseAddress(true);
      return true;
    } catch (IOException e) {
      // Do nothing
    } finally {
      if (ds != null) {
        ds.close();
      }

      if (ss != null) {
        try {
          ss.close();
        } catch (IOException e) {
          /* should not be thrown */
        }
      }
    }

    return false;
  }

  /**
   * Create all combinations of Bloom filters and compression algorithms for
   * testing.
   */
  private static List<Object[]> bloomAndCompressionCombinations() {
    List<Object[]> configurations = new ArrayList<>();
    for (Compression.Algorithm comprAlgo :
         HBaseCommonTestingUtility.COMPRESSION_ALGORITHMS) {
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
    List<Object[]> configurations = new ArrayList<>();
    configurations.add(new Object[] { false, false });
    configurations.add(new Object[] { false, true });
    configurations.add(new Object[] { true, false });
    configurations.add(new Object[] { true, true });
    return Collections.unmodifiableList(configurations);
  }

  public static List<Object[]> memStoreTSTagsAndOffheapCombination() {
    List<Object[]> configurations = new ArrayList<>();
    configurations.add(new Object[] { false, false, true });
    configurations.add(new Object[] { false, false, false });
    configurations.add(new Object[] { false, true, true });
    configurations.add(new Object[] { false, true, false });
    configurations.add(new Object[] { true, false, true });
    configurations.add(new Object[] { true, false, false });
    configurations.add(new Object[] { true, true, true });
    configurations.add(new Object[] { true, true, false });
    return Collections.unmodifiableList(configurations);
  }

  public static final Collection<Object[]> BLOOM_AND_COMPRESSION_COMBINATIONS =
      bloomAndCompressionCombinations();


  /**
   * <p>Create an HBaseTestingUtility using a default configuration.
   *
   * <p>Initially, all tmp files are written to a local test data directory.
   * Once {@link #startMiniDFSCluster} is called, either directly or via
   * {@link #startMiniCluster()}, tmp data will be written to the DFS directory instead.
   */
  public HBaseTestingUtility() {
    this(HBaseConfiguration.create());
  }

  /**
   * <p>Create an HBaseTestingUtility using a given configuration.
   *
   * <p>Initially, all tmp files are written to a local test data directory.
   * Once {@link #startMiniDFSCluster} is called, either directly or via
   * {@link #startMiniCluster()}, tmp data will be written to the DFS directory instead.
   *
   * @param conf The configuration to use for further operations
   */
  public HBaseTestingUtility(Configuration conf) {
    super(conf);

    // a hbase checksum verification failure will cause unit tests to fail
    ChecksumUtil.generateExceptionForChecksumFailureForTest(true);

    // Save this for when setting default file:// breaks things
    if (this.conf.get("fs.defaultFS") != null) {
      this.conf.set("original.defaultFS", this.conf.get("fs.defaultFS"));
    }
    if (this.conf.get(HConstants.HBASE_DIR) != null) {
      this.conf.set("original.hbase.dir", this.conf.get(HConstants.HBASE_DIR));
    }
    // Every cluster is a local cluster until we start DFS
    // Note that conf could be null, but this.conf will not be
    String dataTestDir = getDataTestDir().toString();
    this.conf.set("fs.defaultFS","file:///");
    this.conf.set(HConstants.HBASE_DIR, "file://" + dataTestDir);
    LOG.debug("Setting {} to {}", HConstants.HBASE_DIR, dataTestDir);
    this.conf.setBoolean(CommonFSUtils.UNSAFE_STREAM_CAPABILITY_ENFORCE,false);
    // If the value for random ports isn't set set it to true, thus making
    // tests opt-out for random port assignment
    this.conf.setBoolean(LocalHBaseCluster.ASSIGN_RANDOM_PORTS,
        this.conf.getBoolean(LocalHBaseCluster.ASSIGN_RANDOM_PORTS, true));
  }

  /**
   * Close both the region {@code r} and it's underlying WAL. For use in tests.
   */
  public static void closeRegionAndWAL(final Region r) throws IOException {
    closeRegionAndWAL((HRegion)r);
  }

  /**
   * Close both the HRegion {@code r} and it's underlying WAL. For use in tests.
   */
  public static void closeRegionAndWAL(final HRegion r) throws IOException {
    if (r == null) return;
    r.close();
    if (r.getWAL() == null) return;
    r.getWAL().close();
  }

  /**
   * Returns this classes's instance of {@link Configuration}.  Be careful how
   * you use the returned Configuration since {@link Connection} instances
   * can be shared.  The Map of Connections is keyed by the Configuration.  If
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
   * We also create the underlying directory names for
   *  hadoop.log.dir, mapreduce.cluster.local.dir and hadoop.tmp.dir, and set the values
   *  in the conf, and as a system property for hadoop.tmp.dir (We do not create them!).
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
      "mapreduce.cluster.local.dir",
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
   * Sets up a path in test filesystem to be used by tests.
   * Creates a new directory if not already setup.
   */
  private void setupDataTestDirOnTestFS() throws IOException {
    if (dataTestDirOnTestFS != null) {
      LOG.warn("Data test on test fs dir already setup in "
          + dataTestDirOnTestFS.toString());
      return;
    }
    dataTestDirOnTestFS = getNewDataTestDirOnTestFS();
  }

  /**
   * Sets up a new path in test filesystem to be used by tests.
   */
  private Path getNewDataTestDirOnTestFS() throws IOException {
    //The file system can be either local, mini dfs, or if the configuration
    //is supplied externally, it can be an external cluster FS. If it is a local
    //file system, the tests should use getBaseTestDir, otherwise, we can use
    //the working directory, and create a unique sub dir there
    FileSystem fs = getTestFileSystem();
    Path newDataTestDir;
    String randomStr = getRandomUUID().toString();
    if (fs.getUri().getScheme().equals(FileSystem.getLocal(conf).getUri().getScheme())) {
      newDataTestDir = new Path(getDataTestDir(), randomStr);
      File dataTestDir = new File(newDataTestDir.toString());
      if (deleteOnExit()) dataTestDir.deleteOnExit();
    } else {
      Path base = getBaseTestDirOnTestFS();
      newDataTestDir = new Path(base, randomStr);
      if (deleteOnExit()) fs.deleteOnExit(newDataTestDir);
    }
    return newDataTestDir;
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
   * @see #shutdownMiniDFSCluster()
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
   * @see #shutdownMiniDFSCluster()
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
   * @see #shutdownMiniDFSCluster()
   * @return The mini dfs cluster created.
   */
  public MiniDFSCluster startMiniDFSCluster(int servers, final String hosts[])
  throws Exception {
    return startMiniDFSCluster(servers, null, hosts);
  }

  private void setFs() throws IOException {
    if(this.dfsCluster == null){
      LOG.info("Skipping setting fs because dfsCluster is null");
      return;
    }
    FileSystem fs = this.dfsCluster.getFileSystem();
    CommonFSUtils.setFsDefault(this.conf, new Path(fs.getUri()));

    // re-enable this check with dfs
    conf.unset(CommonFSUtils.UNSAFE_STREAM_CAPABILITY_ENFORCE);
  }

  public MiniDFSCluster startMiniDFSCluster(int servers, final  String racks[], String hosts[])
      throws Exception {
    createDirsAndSetProperties();
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);

    // Error level to skip some warnings specific to the minicluster. See HBASE-4709
    Log4jUtils.setLogLevel(org.apache.hadoop.metrics2.util.MBeans.class.getName(), "ERROR");
    Log4jUtils.setLogLevel(org.apache.hadoop.metrics2.impl.MetricsSystemImpl.class.getName(),
      "ERROR");

    this.dfsCluster = new MiniDFSCluster(0, this.conf, servers, true, true,
        true, null, racks, hosts, null);

    // Set this just-started cluster as our filesystem.
    setFs();

    // Wait for the cluster to be totally up
    this.dfsCluster.waitClusterUp();

    //reset the test directory for test file system
    dataTestDirOnTestFS = null;
    String dataTestDir = getDataTestDir().toString();
    conf.set(HConstants.HBASE_DIR, dataTestDir);
    LOG.debug("Setting {} to {}", HConstants.HBASE_DIR, dataTestDir);

    return this.dfsCluster;
  }

  public MiniDFSCluster startMiniDFSClusterForTestWAL(int namenodePort) throws IOException {
    createDirsAndSetProperties();
    // Error level to skip some warnings specific to the minicluster. See HBASE-4709
    Log4jUtils.setLogLevel(org.apache.hadoop.metrics2.util.MBeans.class.getName(), "ERROR");
    Log4jUtils.setLogLevel(org.apache.hadoop.metrics2.impl.MetricsSystemImpl.class.getName(),
      "ERROR");
    dfsCluster = new MiniDFSCluster(namenodePort, conf, 5, false, true, true, null,
        null, null, null);
    return dfsCluster;
  }

  /**
   * This is used before starting HDFS and map-reduce mini-clusters Run something like the below to
   * check for the likes of '/tmp' references -- i.e. references outside of the test data dir -- in
   * the conf.
   * <pre>
   * Configuration conf = TEST_UTIL.getConfiguration();
   * for (Iterator&lt;Map.Entry&lt;String, String&gt;&gt; i = conf.iterator(); i.hasNext();) {
   *   Map.Entry&lt;String, String&gt; e = i.next();
   *   assertFalse(e.getKey() + " " + e.getValue(), e.getValue().contains("/tmp"));
   * }
   * </pre>
   */
  private void createDirsAndSetProperties() throws IOException {
    setupClusterTestDir();
    conf.set(TEST_DIRECTORY_KEY, clusterTestDir.getPath());
    System.setProperty(TEST_DIRECTORY_KEY, clusterTestDir.getPath());
    createDirAndSetProperty("test.cache.data");
    createDirAndSetProperty("hadoop.tmp.dir");
    hadoopLogDir = createDirAndSetProperty("hadoop.log.dir");
    createDirAndSetProperty("mapreduce.cluster.local.dir");
    createDirAndSetProperty("mapreduce.cluster.temp.dir");
    enableShortCircuit();

    Path root = getDataTestDirOnTestFS("hadoop");
    conf.set(MapreduceTestingShim.getMROutputDirProp(),
      new Path(root, "mapred-output-dir").toString());
    conf.set("mapreduce.jobtracker.system.dir", new Path(root, "mapred-system-dir").toString());
    conf.set("mapreduce.jobtracker.staging.root.dir",
      new Path(root, "mapreduce-jobtracker-staging-root-dir").toString());
    conf.set("mapreduce.job.working.dir", new Path(root, "mapred-working-dir").toString());
    conf.set("yarn.app.mapreduce.am.staging-dir",
      new Path(root, "mapreduce-am-staging-root-dir").toString());

    // Frustrate yarn's and hdfs's attempts at writing /tmp.
    // Below is fragile. Make it so we just interpolate any 'tmp' reference.
    createDirAndSetProperty("yarn.node-labels.fs-store.root-dir");
    createDirAndSetProperty("yarn.node-attribute.fs-store.root-dir");
    createDirAndSetProperty("yarn.nodemanager.log-dirs");
    createDirAndSetProperty("yarn.nodemanager.remote-app-log-dir");
    createDirAndSetProperty("yarn.timeline-service.entity-group-fs-store.active-dir");
    createDirAndSetProperty("yarn.timeline-service.entity-group-fs-store.done-dir");
    createDirAndSetProperty("yarn.nodemanager.remote-app-log-dir");
    createDirAndSetProperty("dfs.journalnode.edits.dir");
    createDirAndSetProperty("dfs.datanode.shared.file.descriptor.paths");
    createDirAndSetProperty("nfs.dump.dir");
    createDirAndSetProperty("java.io.tmpdir");
    createDirAndSetProperty("dfs.journalnode.edits.dir");
    createDirAndSetProperty("dfs.provided.aliasmap.inmemory.leveldb.dir");
    createDirAndSetProperty("fs.s3a.committer.staging.tmp.path");
  }

  /**
   *  Check whether the tests should assume NEW_VERSION_BEHAVIOR when creating
   *  new column families. Default to false.
   */
  public boolean isNewVersionBehaviorEnabled(){
    final String propName = "hbase.tests.new.version.behavior";
    String v = System.getProperty(propName);
    if (v != null){
      return Boolean.parseBoolean(v);
    }
    return false;
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

  private String createDirAndSetProperty(final String property) {
    return createDirAndSetProperty(property, property);
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
      CommonFSUtils.setFsDefault(this.conf, new Path("file:///"));
    }
  }

  /**
   * Start up a minicluster of hbase, dfs, and zookeeper where WAL's walDir is created separately.
   * All other options will use default values, defined in {@link StartMiniClusterOption.Builder}.
   * @param createWALDir Whether to create a new WAL directory.
   * @return The mini HBase cluster created.
   * @see #shutdownMiniCluster()
   * @deprecated since 2.2.0 and will be removed in 4.0.0. Use
   *   {@link #startMiniCluster(StartMiniClusterOption)} instead.
   * @see #startMiniCluster(StartMiniClusterOption)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-21071">HBASE-21071</a>
   */
  @Deprecated
  public MiniHBaseCluster startMiniCluster(boolean createWALDir) throws Exception {
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .createWALDir(createWALDir).build();
    return startMiniCluster(option);
  }

  /**
   * Start up a minicluster of hbase, dfs, and zookeeper.
   * All other options will use default values, defined in {@link StartMiniClusterOption.Builder}.
   * @param numSlaves Slave node number, for both HBase region server and HDFS data node.
   * @param createRootDir Whether to create a new root or data directory path.
   * @return The mini HBase cluster created.
   * @see #shutdownMiniCluster()
   * @deprecated since 2.2.0 and will be removed in 4.0.0. Use
   *   {@link #startMiniCluster(StartMiniClusterOption)} instead.
   * @see #startMiniCluster(StartMiniClusterOption)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-21071">HBASE-21071</a>
   */
  @Deprecated
  public MiniHBaseCluster startMiniCluster(int numSlaves, boolean createRootDir)
  throws Exception {
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numRegionServers(numSlaves).numDataNodes(numSlaves).createRootDir(createRootDir).build();
    return startMiniCluster(option);
  }

  /**
   * Start up a minicluster of hbase, dfs, and zookeeper.
   * All other options will use default values, defined in {@link StartMiniClusterOption.Builder}.
   * @param numSlaves Slave node number, for both HBase region server and HDFS data node.
   * @param createRootDir Whether to create a new root or data directory path.
   * @param createWALDir Whether to create a new WAL directory.
   * @return The mini HBase cluster created.
   * @see #shutdownMiniCluster()
   * @deprecated since 2.2.0 and will be removed in 4.0.0. Use
   *   {@link #startMiniCluster(StartMiniClusterOption)} instead.
   * @see #startMiniCluster(StartMiniClusterOption)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-21071">HBASE-21071</a>
   */
  @Deprecated
  public MiniHBaseCluster startMiniCluster(int numSlaves, boolean createRootDir,
      boolean createWALDir) throws Exception {
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numRegionServers(numSlaves).numDataNodes(numSlaves).createRootDir(createRootDir)
        .createWALDir(createWALDir).build();
    return startMiniCluster(option);
  }

  /**
   * Start up a minicluster of hbase, dfs, and zookeeper.
   * All other options will use default values, defined in {@link StartMiniClusterOption.Builder}.
   * @param numMasters Master node number.
   * @param numSlaves Slave node number, for both HBase region server and HDFS data node.
   * @param createRootDir Whether to create a new root or data directory path.
   * @return The mini HBase cluster created.
   * @see #shutdownMiniCluster()
   * @deprecated since 2.2.0 and will be removed in 4.0.0. Use
   *  {@link #startMiniCluster(StartMiniClusterOption)} instead.
   * @see #startMiniCluster(StartMiniClusterOption)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-21071">HBASE-21071</a>
   */
  @Deprecated
  public MiniHBaseCluster startMiniCluster(int numMasters, int numSlaves, boolean createRootDir)
    throws Exception {
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(numMasters).numRegionServers(numSlaves).createRootDir(createRootDir)
        .numDataNodes(numSlaves).build();
    return startMiniCluster(option);
  }

  /**
   * Start up a minicluster of hbase, dfs, and zookeeper.
   * All other options will use default values, defined in {@link StartMiniClusterOption.Builder}.
   * @param numMasters Master node number.
   * @param numSlaves Slave node number, for both HBase region server and HDFS data node.
   * @return The mini HBase cluster created.
   * @see #shutdownMiniCluster()
   * @deprecated since 2.2.0 and will be removed in 4.0.0. Use
   *   {@link #startMiniCluster(StartMiniClusterOption)} instead.
   * @see #startMiniCluster(StartMiniClusterOption)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-21071">HBASE-21071</a>
   */
  @Deprecated
  public MiniHBaseCluster startMiniCluster(int numMasters, int numSlaves) throws Exception {
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(numMasters).numRegionServers(numSlaves).numDataNodes(numSlaves).build();
    return startMiniCluster(option);
  }

  /**
   * Start up a minicluster of hbase, dfs, and zookeeper.
   * All other options will use default values, defined in {@link StartMiniClusterOption.Builder}.
   * @param numMasters Master node number.
   * @param numSlaves Slave node number, for both HBase region server and HDFS data node.
   * @param dataNodeHosts The hostnames of DataNodes to run on. If not null, its size will overwrite
   *                      HDFS data node number.
   * @param createRootDir Whether to create a new root or data directory path.
   * @return The mini HBase cluster created.
   * @see #shutdownMiniCluster()
   * @deprecated since 2.2.0 and will be removed in 4.0.0. Use
   *   {@link #startMiniCluster(StartMiniClusterOption)} instead.
   * @see #startMiniCluster(StartMiniClusterOption)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-21071">HBASE-21071</a>
   */
  @Deprecated
  public MiniHBaseCluster startMiniCluster(int numMasters, int numSlaves, String[] dataNodeHosts,
      boolean createRootDir) throws Exception {
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(numMasters).numRegionServers(numSlaves).createRootDir(createRootDir)
        .numDataNodes(numSlaves).dataNodeHosts(dataNodeHosts).build();
    return startMiniCluster(option);
  }

  /**
   * Start up a minicluster of hbase, dfs, and zookeeper.
   * All other options will use default values, defined in {@link StartMiniClusterOption.Builder}.
   * @param numMasters Master node number.
   * @param numSlaves Slave node number, for both HBase region server and HDFS data node.
   * @param dataNodeHosts The hostnames of DataNodes to run on. If not null, its size will overwrite
   *                      HDFS data node number.
   * @return The mini HBase cluster created.
   * @see #shutdownMiniCluster()
   * @deprecated since 2.2.0 and will be removed in 4.0.0. Use
   *   {@link #startMiniCluster(StartMiniClusterOption)} instead.
   * @see #startMiniCluster(StartMiniClusterOption)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-21071">HBASE-21071</a>
   */
  @Deprecated
  public MiniHBaseCluster startMiniCluster(int numMasters, int numSlaves, String[] dataNodeHosts)
      throws Exception {
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(numMasters).numRegionServers(numSlaves)
        .numDataNodes(numSlaves).dataNodeHosts(dataNodeHosts).build();
    return startMiniCluster(option);
  }

  /**
   * Start up a minicluster of hbase, dfs, and zookeeper.
   * All other options will use default values, defined in {@link StartMiniClusterOption.Builder}.
   * @param numMasters Master node number.
   * @param numRegionServers Number of region servers.
   * @param numDataNodes Number of datanodes.
   * @return The mini HBase cluster created.
   * @see #shutdownMiniCluster()
   * @deprecated since 2.2.0 and will be removed in 4.0.0. Use
   *   {@link #startMiniCluster(StartMiniClusterOption)} instead.
   * @see #startMiniCluster(StartMiniClusterOption)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-21071">HBASE-21071</a>
   */
  @Deprecated
  public MiniHBaseCluster startMiniCluster(int numMasters, int numRegionServers, int numDataNodes)
      throws Exception {
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(numMasters).numRegionServers(numRegionServers).numDataNodes(numDataNodes)
        .build();
    return startMiniCluster(option);
  }

  /**
   * Start up a minicluster of hbase, dfs, and zookeeper.
   * All other options will use default values, defined in {@link StartMiniClusterOption.Builder}.
   * @param numMasters Master node number.
   * @param numSlaves Slave node number, for both HBase region server and HDFS data node.
   * @param dataNodeHosts The hostnames of DataNodes to run on. If not null, its size will overwrite
   *                      HDFS data node number.
   * @param masterClass The class to use as HMaster, or null for default.
   * @param rsClass The class to use as HRegionServer, or null for default.
   * @return The mini HBase cluster created.
   * @see #shutdownMiniCluster()
   * @deprecated since 2.2.0 and will be removed in 4.0.0. Use
   *   {@link #startMiniCluster(StartMiniClusterOption)} instead.
   * @see #startMiniCluster(StartMiniClusterOption)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-21071">HBASE-21071</a>
   */
  @Deprecated
  public MiniHBaseCluster startMiniCluster(int numMasters, int numSlaves, String[] dataNodeHosts,
      Class<? extends HMaster> masterClass,
      Class<? extends MiniHBaseCluster.MiniHBaseClusterRegionServer> rsClass)
      throws Exception {
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(numMasters).masterClass(masterClass)
        .numRegionServers(numSlaves).rsClass(rsClass)
        .numDataNodes(numSlaves).dataNodeHosts(dataNodeHosts)
        .build();
    return startMiniCluster(option);
  }

  /**
   * Start up a minicluster of hbase, dfs, and zookeeper.
   * All other options will use default values, defined in {@link StartMiniClusterOption.Builder}.
   * @param numMasters Master node number.
   * @param numRegionServers Number of region servers.
   * @param numDataNodes Number of datanodes.
   * @param dataNodeHosts The hostnames of DataNodes to run on. If not null, its size will overwrite
   *                      HDFS data node number.
   * @param masterClass The class to use as HMaster, or null for default.
   * @param rsClass The class to use as HRegionServer, or null for default.
   * @return The mini HBase cluster created.
   * @see #shutdownMiniCluster()
   * @deprecated since 2.2.0 and will be removed in 4.0.0. Use
   *   {@link #startMiniCluster(StartMiniClusterOption)} instead.
   * @see #startMiniCluster(StartMiniClusterOption)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-21071">HBASE-21071</a>
   */
  @Deprecated
  public MiniHBaseCluster startMiniCluster(int numMasters, int numRegionServers, int numDataNodes,
      String[] dataNodeHosts, Class<? extends HMaster> masterClass,
      Class<? extends MiniHBaseCluster.MiniHBaseClusterRegionServer> rsClass)
    throws Exception {
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(numMasters).masterClass(masterClass)
        .numRegionServers(numRegionServers).rsClass(rsClass)
        .numDataNodes(numDataNodes).dataNodeHosts(dataNodeHosts)
        .build();
    return startMiniCluster(option);
  }

  /**
   * Start up a minicluster of hbase, dfs, and zookeeper.
   * All other options will use default values, defined in {@link StartMiniClusterOption.Builder}.
   * @param numMasters Master node number.
   * @param numRegionServers Number of region servers.
   * @param numDataNodes Number of datanodes.
   * @param dataNodeHosts The hostnames of DataNodes to run on. If not null, its size will overwrite
   *                      HDFS data node number.
   * @param masterClass The class to use as HMaster, or null for default.
   * @param rsClass The class to use as HRegionServer, or null for default.
   * @param createRootDir Whether to create a new root or data directory path.
   * @param createWALDir Whether to create a new WAL directory.
   * @return The mini HBase cluster created.
   * @see #shutdownMiniCluster()
   * @deprecated since 2.2.0 and will be removed in 4.0.0. Use
   *   {@link #startMiniCluster(StartMiniClusterOption)} instead.
   * @see #startMiniCluster(StartMiniClusterOption)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-21071">HBASE-21071</a>
   */
  @Deprecated
  public MiniHBaseCluster startMiniCluster(int numMasters, int numRegionServers, int numDataNodes,
      String[] dataNodeHosts, Class<? extends HMaster> masterClass,
      Class<? extends MiniHBaseCluster.MiniHBaseClusterRegionServer> rsClass, boolean createRootDir,
      boolean createWALDir) throws Exception {
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(numMasters).masterClass(masterClass)
        .numRegionServers(numRegionServers).rsClass(rsClass)
        .numDataNodes(numDataNodes).dataNodeHosts(dataNodeHosts)
        .createRootDir(createRootDir).createWALDir(createWALDir)
        .build();
    return startMiniCluster(option);
  }

  /**
   * Start up a minicluster of hbase, dfs and zookeeper clusters with given slave node number.
   * All other options will use default values, defined in {@link StartMiniClusterOption.Builder}.
   * @param numSlaves slave node number, for both HBase region server and HDFS data node.
   * @see #startMiniCluster(StartMiniClusterOption option)
   * @see #shutdownMiniDFSCluster()
   */
  public MiniHBaseCluster startMiniCluster(int numSlaves) throws Exception {
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numRegionServers(numSlaves).numDataNodes(numSlaves).build();
    return startMiniCluster(option);
  }

  /**
   * Start up a minicluster of hbase, dfs and zookeeper all using default options.
   * Option default value can be found in {@link StartMiniClusterOption.Builder}.
   * @see #startMiniCluster(StartMiniClusterOption option)
   * @see #shutdownMiniDFSCluster()
   */
  public MiniHBaseCluster startMiniCluster() throws Exception {
    return startMiniCluster(StartMiniClusterOption.builder().build());
  }

  /**
   * Start up a mini cluster of hbase, optionally dfs and zookeeper if needed.
   * It modifies Configuration.  It homes the cluster data directory under a random
   * subdirectory in a directory under System property test.build.data, to be cleaned up on exit.
   * @see #shutdownMiniDFSCluster()
   */
  public MiniHBaseCluster startMiniCluster(StartMiniClusterOption option) throws Exception {
    LOG.info("Starting up minicluster with option: {}", option);

    // If we already put up a cluster, fail.
    if (miniClusterRunning) {
      throw new IllegalStateException("A mini-cluster is already running");
    }
    miniClusterRunning = true;

    setupClusterTestDir();
    System.setProperty(TEST_DIRECTORY_KEY, this.clusterTestDir.getPath());

    // Bring up mini dfs cluster. This spews a bunch of warnings about missing
    // scheme. Complaints are 'Scheme is undefined for build/test/data/dfs/name1'.
    if (dfsCluster == null) {
      LOG.info("STARTING DFS");
      dfsCluster = startMiniDFSCluster(option.getNumDataNodes(), option.getDataNodeHosts());
    } else {
      LOG.info("NOT STARTING DFS");
    }

    // Start up a zk cluster.
    if (getZkCluster() == null) {
      startMiniZKCluster(option.getNumZkServers());
    }

    // Start the MiniHBaseCluster
    return startMiniHBaseCluster(option);
  }

  /**
   * Starts up mini hbase cluster. Usually you won't want this. You'll usually want
   * {@link #startMiniCluster()}. This is useful when doing stepped startup of clusters.
   * @return Reference to the hbase mini hbase cluster.
   * @see #startMiniCluster(StartMiniClusterOption)
   * @see #shutdownMiniHBaseCluster()
   */
  public MiniHBaseCluster startMiniHBaseCluster(StartMiniClusterOption option)
    throws IOException, InterruptedException {
    // Now do the mini hbase cluster. Set the hbase.rootdir in config.
    createRootDir(option.isCreateRootDir());
    if (option.isCreateWALDir()) {
      createWALRootDir();
    }
    // Set the hbase.fs.tmp.dir config to make sure that we have some default value. This is
    // for tests that do not read hbase-defaults.xml
    setHBaseFsTmpDir();

    // These settings will make the server waits until this exact number of
    // regions servers are connected.
    if (conf.getInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, -1) == -1) {
      conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, option.getNumRegionServers());
    }
    if (conf.getInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, -1) == -1) {
      conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, option.getNumRegionServers());
    }

    // Avoid log flooded with chore execution time, see HBASE-24646 for more details.
    Log4jUtils.setLogLevel(org.apache.hadoop.hbase.ScheduledChore.class.getName(), "INFO");

    Configuration c = new Configuration(this.conf);
    this.hbaseCluster = new MiniHBaseCluster(c, option.getNumMasters(),
      option.getNumAlwaysStandByMasters(), option.getNumRegionServers(), option.getRsPorts(),
      option.getMasterClass(), option.getRsClass());
    // Populate the master address configuration from mini cluster configuration.
    conf.set(HConstants.MASTER_ADDRS_KEY, MasterRegistry.getMasterAddr(c));
    // Don't leave here till we've done a successful scan of the hbase:meta
    try (Table t = getConnection().getTable(TableName.META_TABLE_NAME);
      ResultScanner s = t.getScanner(new Scan())) {
      for (;;) {
        if (s.next() == null) {
          break;
        }
      }
    }


    getAdmin(); // create immediately the hbaseAdmin
    LOG.info("Minicluster is up; activeMaster={}", getHBaseCluster().getMaster());

    return (MiniHBaseCluster) hbaseCluster;
  }

  /**
   * Starts up mini hbase cluster using default options.
   * Default options can be found in {@link StartMiniClusterOption.Builder}.
   * @see #startMiniHBaseCluster(StartMiniClusterOption)
   * @see #shutdownMiniHBaseCluster()
   */
  public MiniHBaseCluster startMiniHBaseCluster() throws IOException, InterruptedException {
    return startMiniHBaseCluster(StartMiniClusterOption.builder().build());
  }

  /**
   * Starts up mini hbase cluster.
   * Usually you won't want this.  You'll usually want {@link #startMiniCluster()}.
   * All other options will use default values, defined in {@link StartMiniClusterOption.Builder}.
   * @param numMasters Master node number.
   * @param numRegionServers Number of region servers.
   * @return The mini HBase cluster created.
   * @see #shutdownMiniHBaseCluster()
   * @deprecated since 2.2.0 and will be removed in 4.0.0. Use
   *   {@link #startMiniHBaseCluster(StartMiniClusterOption)} instead.
   * @see #startMiniHBaseCluster(StartMiniClusterOption)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-21071">HBASE-21071</a>
   */
  @Deprecated
  public MiniHBaseCluster startMiniHBaseCluster(int numMasters, int numRegionServers)
      throws IOException, InterruptedException {
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(numMasters).numRegionServers(numRegionServers).build();
    return startMiniHBaseCluster(option);
  }

  /**
   * Starts up mini hbase cluster.
   * Usually you won't want this.  You'll usually want {@link #startMiniCluster()}.
   * All other options will use default values, defined in {@link StartMiniClusterOption.Builder}.
   * @param numMasters Master node number.
   * @param numRegionServers Number of region servers.
   * @param rsPorts Ports that RegionServer should use.
   * @return The mini HBase cluster created.
   * @see #shutdownMiniHBaseCluster()
   * @deprecated since 2.2.0 and will be removed in 4.0.0. Use
   *   {@link #startMiniHBaseCluster(StartMiniClusterOption)} instead.
   * @see #startMiniHBaseCluster(StartMiniClusterOption)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-21071">HBASE-21071</a>
   */
  @Deprecated
  public MiniHBaseCluster startMiniHBaseCluster(int numMasters, int numRegionServers,
      List<Integer> rsPorts) throws IOException, InterruptedException {
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(numMasters).numRegionServers(numRegionServers).rsPorts(rsPorts).build();
    return startMiniHBaseCluster(option);
  }

  /**
   * Starts up mini hbase cluster.
   * Usually you won't want this.  You'll usually want {@link #startMiniCluster()}.
   * All other options will use default values, defined in {@link StartMiniClusterOption.Builder}.
   * @param numMasters Master node number.
   * @param numRegionServers Number of region servers.
   * @param rsPorts Ports that RegionServer should use.
   * @param masterClass The class to use as HMaster, or null for default.
   * @param rsClass The class to use as HRegionServer, or null for default.
   * @param createRootDir Whether to create a new root or data directory path.
   * @param createWALDir Whether to create a new WAL directory.
   * @return The mini HBase cluster created.
   * @see #shutdownMiniHBaseCluster()
   * @deprecated since 2.2.0 and will be removed in 4.0.0. Use
   *   {@link #startMiniHBaseCluster(StartMiniClusterOption)} instead.
   * @see #startMiniHBaseCluster(StartMiniClusterOption)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-21071">HBASE-21071</a>
   */
  @Deprecated
  public MiniHBaseCluster startMiniHBaseCluster(int numMasters, int numRegionServers,
      List<Integer> rsPorts, Class<? extends HMaster> masterClass,
      Class<? extends MiniHBaseCluster.MiniHBaseClusterRegionServer> rsClass,
      boolean createRootDir, boolean createWALDir) throws IOException, InterruptedException {
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(numMasters).masterClass(masterClass)
        .numRegionServers(numRegionServers).rsClass(rsClass).rsPorts(rsPorts)
        .createRootDir(createRootDir).createWALDir(createWALDir).build();
    return startMiniHBaseCluster(option);
  }

  /**
   * Starts the hbase cluster up again after shutting it down previously in a
   * test.  Use this if you want to keep dfs/zk up and just stop/start hbase.
   * @param servers number of region servers
   */
  public void restartHBaseCluster(int servers) throws IOException, InterruptedException {
    this.restartHBaseCluster(servers, null);
  }

  public void restartHBaseCluster(int servers, List<Integer> ports)
      throws IOException, InterruptedException {
    StartMiniClusterOption option =
        StartMiniClusterOption.builder().numRegionServers(servers).rsPorts(ports).build();
    restartHBaseCluster(option);
    invalidateConnection();
  }

  public void restartHBaseCluster(StartMiniClusterOption option)
      throws IOException, InterruptedException {
    closeConnection();
    this.hbaseCluster =
        new MiniHBaseCluster(this.conf, option.getNumMasters(), option.getNumAlwaysStandByMasters(),
            option.getNumRegionServers(), option.getRsPorts(), option.getMasterClass(),
            option.getRsClass());
    // Don't leave here till we've done a successful scan of the hbase:meta
    Connection conn = ConnectionFactory.createConnection(this.conf);
    Table t = conn.getTable(TableName.META_TABLE_NAME);
    ResultScanner s = t.getScanner(new Scan());
    while (s.next() != null) {
      // do nothing
    }
    LOG.info("HBase has been restarted");
    s.close();
    t.close();
    conn.close();
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
   * @see #startMiniCluster(int)
   */
  public void shutdownMiniCluster() throws IOException {
    LOG.info("Shutting down minicluster");
    shutdownMiniHBaseCluster();
    shutdownMiniDFSCluster();
    shutdownMiniZKCluster();

    cleanupTestDir();
    miniClusterRunning = false;
    LOG.info("Minicluster is down");
  }

  /**
   * Shutdown HBase mini cluster.Does not shutdown zk or dfs if running.
   * @throws java.io.IOException in case command is unsuccessful
   */
  public void shutdownMiniHBaseCluster() throws IOException {
    cleanup();
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
   * Abruptly Shutdown HBase mini cluster. Does not shutdown zk or dfs if running.
   * @throws java.io.IOException throws in case command is unsuccessful
   */
  public void killMiniHBaseCluster() throws IOException {
    cleanup();
    if (this.hbaseCluster != null) {
      getMiniHBaseCluster().killAll();
      this.hbaseCluster = null;
    }
    if (zooKeeperWatcher != null) {
      zooKeeperWatcher.close();
      zooKeeperWatcher = null;
    }
  }

  // close hbase admin, close current connection and reset MIN MAX configs for RS.
  private void cleanup() throws IOException {
    closeConnection();
    // unset the configuration for MIN and MAX RS to start
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, -1);
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, -1);
  }

  /**
   * Returns the path to the default root dir the minicluster uses. If <code>create</code>
   * is true, a new root directory path is fetched irrespective of whether it has been fetched
   * before or not. If false, previous path is used.
   * Note: this does not cause the root dir to be created.
   * @return Fully qualified path for the default hbase root dir
   * @throws IOException
   */
  public Path getDefaultRootDirPath(boolean create) throws IOException {
    if (!create) {
      return getDataTestDirOnTestFS();
    } else {
      return getNewDataTestDirOnTestFS();
    }
  }

  /**
   * Same as {{@link HBaseTestingUtility#getDefaultRootDirPath(boolean create)}
   * except that <code>create</code> flag is false.
   * Note: this does not cause the root dir to be created.
   * @return Fully qualified path for the default hbase root dir
   * @throws IOException
   */
  public Path getDefaultRootDirPath() throws IOException {
    return getDefaultRootDirPath(false);
  }

  /**
   * Creates an hbase rootdir in user home directory.  Also creates hbase
   * version file.  Normally you won't make use of this method.  Root hbasedir
   * is created for you as part of mini cluster startup.  You'd only use this
   * method if you were doing manual operation.
   * @param create This flag decides whether to get a new
   * root or data directory path or not, if it has been fetched already.
   * Note : Directory will be made irrespective of whether path has been fetched or not.
   * If directory already exists, it will be overwritten
   * @return Fully qualified path to hbase root dir
   * @throws IOException
   */
  public Path createRootDir(boolean create) throws IOException {
    FileSystem fs = FileSystem.get(this.conf);
    Path hbaseRootdir = getDefaultRootDirPath(create);
    CommonFSUtils.setRootDir(this.conf, hbaseRootdir);
    fs.mkdirs(hbaseRootdir);
    FSUtils.setVersion(fs, hbaseRootdir);
    return hbaseRootdir;
  }

  /**
   * Same as {@link HBaseTestingUtility#createRootDir(boolean create)}
   * except that <code>create</code> flag is false.
   * @return Fully qualified path to hbase root dir
   * @throws IOException
   */
  public Path createRootDir() throws IOException {
    return createRootDir(false);
  }

  /**
   * Creates a hbase walDir in the user's home directory.
   * Normally you won't make use of this method. Root hbaseWALDir
   * is created for you as part of mini cluster startup. You'd only use this
   * method if you were doing manual operation.
   *
   * @return Fully qualified path to hbase root dir
   * @throws IOException
  */
  public Path createWALRootDir() throws IOException {
    FileSystem fs = FileSystem.get(this.conf);
    Path walDir = getNewDataTestDirOnTestFS();
    CommonFSUtils.setWALRootDir(this.conf, walDir);
    fs.mkdirs(walDir);
    return walDir;
  }

  private void setHBaseFsTmpDir() throws IOException {
    String hbaseFsTmpDirInString = this.conf.get("hbase.fs.tmp.dir");
    if (hbaseFsTmpDirInString == null) {
      this.conf.set("hbase.fs.tmp.dir",  getDataTestDirOnTestFS("hbase-staging").toString());
      LOG.info("Setting hbase.fs.tmp.dir to " + this.conf.get("hbase.fs.tmp.dir"));
    } else {
      LOG.info("The hbase.fs.tmp.dir is set to " + hbaseFsTmpDirInString);
    }
  }

  /**
   * Flushes all caches in the mini hbase cluster
   */
  public void flush() throws IOException {
    getMiniHBaseCluster().flushcache();
  }

  /**
   * Flushes all caches in the mini hbase cluster
   */
  public void flush(TableName tableName) throws IOException {
    getMiniHBaseCluster().flushcache(tableName);
  }

  /**
   * Compact all regions in the mini hbase cluster
   */
  public void compact(boolean major) throws IOException {
    getMiniHBaseCluster().compact(major);
  }

  /**
   * Compact all of a table's reagion in the mini hbase cluster
   */
  public void compact(TableName tableName, boolean major) throws IOException {
    getMiniHBaseCluster().compact(tableName, major);
  }

  /**
   * Create a table.
   * @param tableName
   * @param family
   * @return A Table instance for the created table.
   * @throws IOException
   */
  public Table createTable(TableName tableName, String family)
  throws IOException{
    return createTable(tableName, new String[]{family});
  }

  /**
   * Create a table.
   * @param tableName
   * @param families
   * @return A Table instance for the created table.
   * @throws IOException
   */
  public Table createTable(TableName tableName, String[] families)
  throws IOException {
    List<byte[]> fams = new ArrayList<>(families.length);
    for (String family : families) {
      fams.add(Bytes.toBytes(family));
    }
    return createTable(tableName, fams.toArray(new byte[0][]));
  }

  /**
   * Create a table.
   * @param tableName
   * @param family
   * @return A Table instance for the created table.
   * @throws IOException
   */
  public Table createTable(TableName tableName, byte[] family)
  throws IOException{
    return createTable(tableName, new byte[][]{family});
  }

  /**
   * Create a table with multiple regions.
   * @param tableName
   * @param family
   * @param numRegions
   * @return A Table instance for the created table.
   * @throws IOException
   */
  public Table createMultiRegionTable(TableName tableName, byte[] family, int numRegions)
      throws IOException {
    if (numRegions < 3) throw new IOException("Must create at least 3 regions");
    byte[] startKey = Bytes.toBytes("aaaaa");
    byte[] endKey = Bytes.toBytes("zzzzz");
    byte[][] splitKeys = Bytes.split(startKey, endKey, numRegions - 3);

    return createTable(tableName, new byte[][] { family }, splitKeys);
  }

  /**
   * Create a table.
   * @param tableName
   * @param families
   * @return A Table instance for the created table.
   * @throws IOException
   */
  public Table createTable(TableName tableName, byte[][] families)
  throws IOException {
    return createTable(tableName, families, (byte[][]) null);
  }

  /**
   * Create a table with multiple regions.
   * @param tableName
   * @param families
   * @return A Table instance for the created table.
   * @throws IOException
   */
  public Table createMultiRegionTable(TableName tableName, byte[][] families) throws IOException {
    return createTable(tableName, families, KEYS_FOR_HBA_CREATE_TABLE);
  }

  /**
   * Create a table with multiple regions.
   * @param tableName
   * @param replicaCount replica count.
   * @param families
   * @return A Table instance for the created table.
   * @throws IOException
   */
  public Table createMultiRegionTable(TableName tableName, int replicaCount, byte[][] families)
    throws IOException {
    return createTable(tableName, families, KEYS_FOR_HBA_CREATE_TABLE, replicaCount);
  }

  /**
   * Create a table.
   * @param tableName
   * @param families
   * @param splitKeys
   * @return A Table instance for the created table.
   * @throws IOException
   */
  public Table createTable(TableName tableName, byte[][] families, byte[][] splitKeys)
      throws IOException {
    return createTable(tableName, families, splitKeys, 1, new Configuration(getConfiguration()));
  }

  /**
   * Create a table.
   * @param tableName the table name
   * @param families the families
   * @param splitKeys the splitkeys
   * @param replicaCount the region replica count
   * @return A Table instance for the created table.
   * @throws IOException throws IOException
   */
  public Table createTable(TableName tableName, byte[][] families, byte[][] splitKeys,
      int replicaCount) throws IOException {
    return createTable(tableName, families, splitKeys, replicaCount,
      new Configuration(getConfiguration()));
  }

  public Table createTable(TableName tableName, byte[][] families, int numVersions, byte[] startKey,
    byte[] endKey, int numRegions) throws IOException {
    TableDescriptor desc = createTableDescriptor(tableName, families, numVersions);

    getAdmin().createTable(desc, startKey, endKey, numRegions);
    // HBaseAdmin only waits for regions to appear in hbase:meta we
    // should wait until they are assigned
    waitUntilAllRegionsAssigned(tableName);
    return getConnection().getTable(tableName);
  }

  /**
   * Create a table.
   * @param c Configuration to use
   * @return A Table instance for the created table.
   */
  public Table createTable(TableDescriptor htd, byte[][] families, Configuration c)
    throws IOException {
    return createTable(htd, families, null, c);
  }

  /**
   * Create a table.
   * @param htd table descriptor
   * @param families array of column families
   * @param splitKeys array of split keys
   * @param c Configuration to use
   * @return A Table instance for the created table.
   * @throws IOException if getAdmin or createTable fails
   */
  public Table createTable(TableDescriptor htd, byte[][] families, byte[][] splitKeys,
      Configuration c) throws IOException {
    // Disable blooms (they are on by default as of 0.95) but we disable them here because
    // tests have hard coded counts of what to expect in block cache, etc., and blooms being
    // on is interfering.
    return createTable(htd, families, splitKeys, BloomType.NONE, HConstants.DEFAULT_BLOCKSIZE, c);
  }

  /**
   * Create a table.
   * @param htd table descriptor
   * @param families array of column families
   * @param splitKeys array of split keys
   * @param type Bloom type
   * @param blockSize block size
   * @param c Configuration to use
   * @return A Table instance for the created table.
   * @throws IOException if getAdmin or createTable fails
   */

  public Table createTable(TableDescriptor htd, byte[][] families, byte[][] splitKeys,
      BloomType type, int blockSize, Configuration c) throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(htd);
    for (byte[] family : families) {
      ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(family)
        .setBloomFilterType(type)
        .setBlocksize(blockSize);
      if (isNewVersionBehaviorEnabled()) {
          cfdb.setNewVersionBehavior(true);
      }
      builder.setColumnFamily(cfdb.build());
    }
    TableDescriptor td = builder.build();
    if (splitKeys != null) {
      getAdmin().createTable(td, splitKeys);
    } else {
      getAdmin().createTable(td);
    }
    // HBaseAdmin only waits for regions to appear in hbase:meta
    // we should wait until they are assigned
    waitUntilAllRegionsAssigned(td.getTableName());
    return getConnection().getTable(td.getTableName());
  }

  /**
   * Create a table.
   * @param htd table descriptor
   * @param splitRows array of split keys
   * @return A Table instance for the created table.
   * @throws IOException
   */
  public Table createTable(TableDescriptor htd, byte[][] splitRows)
      throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(htd);
    if (isNewVersionBehaviorEnabled()) {
      for (ColumnFamilyDescriptor family : htd.getColumnFamilies()) {
         builder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(family)
           .setNewVersionBehavior(true).build());
      }
    }
    if (splitRows != null) {
      getAdmin().createTable(builder.build(), splitRows);
    } else {
      getAdmin().createTable(builder.build());
    }
    // HBaseAdmin only waits for regions to appear in hbase:meta
    // we should wait until they are assigned
    waitUntilAllRegionsAssigned(htd.getTableName());
    return getConnection().getTable(htd.getTableName());
  }

  /**
   * Create a table.
   * @param tableName the table name
   * @param families the families
   * @param splitKeys the split keys
   * @param replicaCount the replica count
   * @param c Configuration to use
   * @return A Table instance for the created table.
   */
  public Table createTable(TableName tableName, byte[][] families, byte[][] splitKeys,
    int replicaCount, final Configuration c) throws IOException {
    TableDescriptor htd =
      TableDescriptorBuilder.newBuilder(tableName).setRegionReplication(replicaCount).build();
    return createTable(htd, families, splitKeys, c);
  }

  /**
   * Create a table.
   * @return A Table instance for the created table.
   */
  public Table createTable(TableName tableName, byte[] family, int numVersions) throws IOException {
    return createTable(tableName, new byte[][] { family }, numVersions);
  }

  /**
   * Create a table.
   * @return A Table instance for the created table.
   */
  public Table createTable(TableName tableName, byte[][] families, int numVersions)
      throws IOException {
    return createTable(tableName, families, numVersions, (byte[][]) null);
  }

  /**
   * Create a table.
   * @return A Table instance for the created table.
   */
  public Table createTable(TableName tableName, byte[][] families, int numVersions,
      byte[][] splitKeys) throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    for (byte[] family : families) {
      ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(family)
        .setMaxVersions(numVersions);
      if (isNewVersionBehaviorEnabled()) {
        cfBuilder.setNewVersionBehavior(true);
      }
      builder.setColumnFamily(cfBuilder.build());
    }
    if (splitKeys != null) {
      getAdmin().createTable(builder.build(), splitKeys);
    } else {
      getAdmin().createTable(builder.build());
    }
    // HBaseAdmin only waits for regions to appear in hbase:meta we should wait until they are
    // assigned
    waitUntilAllRegionsAssigned(tableName);
    return getConnection().getTable(tableName);
  }

  /**
   * Create a table with multiple regions.
   * @return A Table instance for the created table.
   */
  public Table createMultiRegionTable(TableName tableName, byte[][] families, int numVersions)
      throws IOException {
    return createTable(tableName, families, numVersions, KEYS_FOR_HBA_CREATE_TABLE);
  }

  /**
   * Create a table.
   * @return A Table instance for the created table.
   */
  public Table createTable(TableName tableName, byte[][] families,
    int numVersions, int blockSize) throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    for (byte[] family : families) {
      ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(family)
        .setMaxVersions(numVersions).setBlocksize(blockSize);
      if (isNewVersionBehaviorEnabled()) {
        cfBuilder.setNewVersionBehavior(true);
      }
      builder.setColumnFamily(cfBuilder.build());
    }
    getAdmin().createTable(builder.build());
    // HBaseAdmin only waits for regions to appear in hbase:meta we should wait until they are
    // assigned
    waitUntilAllRegionsAssigned(tableName);
    return getConnection().getTable(tableName);
  }

  public Table createTable(TableName tableName, byte[][] families,
      int numVersions, int blockSize, String cpName) throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    for (byte[] family : families) {
      ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(family)
        .setMaxVersions(numVersions).setBlocksize(blockSize);
      if (isNewVersionBehaviorEnabled()) {
        cfBuilder.setNewVersionBehavior(true);
      }
      builder.setColumnFamily(cfBuilder.build());
    }
    if (cpName != null) {
      builder.setCoprocessor(cpName);
    }
    getAdmin().createTable(builder.build());
    // HBaseAdmin only waits for regions to appear in hbase:meta we should wait until they are
    // assigned
    waitUntilAllRegionsAssigned(tableName);
    return getConnection().getTable(tableName);
  }

  /**
   * Create a table.
   * @return A Table instance for the created table.
   */
  public Table createTable(TableName tableName, byte[][] families, int[] numVersions)
    throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    int i = 0;
    for (byte[] family : families) {
      ColumnFamilyDescriptorBuilder cfBuilder =
        ColumnFamilyDescriptorBuilder.newBuilder(family).setMaxVersions(numVersions[i]);
      if (isNewVersionBehaviorEnabled()) {
        cfBuilder.setNewVersionBehavior(true);
      }
      builder.setColumnFamily(cfBuilder.build());
      i++;
    }
    getAdmin().createTable(builder.build());
    // HBaseAdmin only waits for regions to appear in hbase:meta we should wait until they are
    // assigned
    waitUntilAllRegionsAssigned(tableName);
    return getConnection().getTable(tableName);
  }

  /**
   * Create a table.
   * @return A Table instance for the created table.
   */
  public Table createTable(TableName tableName, byte[] family, byte[][] splitRows)
    throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(family);
    if (isNewVersionBehaviorEnabled()) {
      cfBuilder.setNewVersionBehavior(true);
    }
    builder.setColumnFamily(cfBuilder.build());
    getAdmin().createTable(builder.build(), splitRows);
    // HBaseAdmin only waits for regions to appear in hbase:meta we should wait until they are
    // assigned
    waitUntilAllRegionsAssigned(tableName);
    return getConnection().getTable(tableName);
  }

  /**
   * Create a table with multiple regions.
   * @return A Table instance for the created table.
   */
  public Table createMultiRegionTable(TableName tableName, byte[] family) throws IOException {
    return createTable(tableName, family, KEYS_FOR_HBA_CREATE_TABLE);
  }

  /**
   * Modify a table, synchronous.
   * @deprecated since 3.0.0 and will be removed in 4.0.0. Just use
   *   {@link Admin#modifyTable(TableDescriptor)} directly as it is synchronous now.
   * @see Admin#modifyTable(TableDescriptor)
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-22002">HBASE-22002</a>
   */
  @Deprecated
  public static void modifyTableSync(Admin admin, TableDescriptor desc)
      throws IOException, InterruptedException {
    admin.modifyTable(desc);
  }

  /**
   * Set the number of Region replicas.
   */
  public static void setReplicas(Admin admin, TableName table, int replicaCount)
    throws IOException, InterruptedException {
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(admin.getDescriptor(table))
      .setRegionReplication(replicaCount).build();
    admin.modifyTable(desc);
  }

  /**
   * Drop an existing table
   * @param tableName existing table
   */
  public void deleteTable(TableName tableName) throws IOException {
    try {
      getAdmin().disableTable(tableName);
    } catch (TableNotEnabledException e) {
      LOG.debug("Table: " + tableName + " already disabled, so just deleting it.");
    }
    getAdmin().deleteTable(tableName);
  }

  /**
   * Drop an existing table
   * @param tableName existing table
   */
  public void deleteTableIfAny(TableName tableName) throws IOException {
    try {
      deleteTable(tableName);
    } catch (TableNotFoundException e) {
      // ignore
    }
  }

  // ==========================================================================
  // Canned table and table descriptor creation

  public final static byte [] fam1 = Bytes.toBytes("colfamily11");
  public final static byte [] fam2 = Bytes.toBytes("colfamily21");
  public final static byte [] fam3 = Bytes.toBytes("colfamily31");
  public static final byte[][] COLUMNS = {fam1, fam2, fam3};
  private static final int MAXVERSIONS = 3;

  public static final char FIRST_CHAR = 'a';
  public static final char LAST_CHAR = 'z';
  public static final byte [] START_KEY_BYTES = {FIRST_CHAR, FIRST_CHAR, FIRST_CHAR};
  public static final String START_KEY = new String(START_KEY_BYTES, HConstants.UTF8_CHARSET);

  public TableDescriptorBuilder createModifyableTableDescriptor(final String name) {
    return createModifyableTableDescriptor(TableName.valueOf(name),
      ColumnFamilyDescriptorBuilder.DEFAULT_MIN_VERSIONS, MAXVERSIONS, HConstants.FOREVER,
      ColumnFamilyDescriptorBuilder.DEFAULT_KEEP_DELETED);
  }

  public TableDescriptor createTableDescriptor(final TableName name, final int minVersions,
    final int versions, final int ttl, KeepDeletedCells keepDeleted) {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(name);
    for (byte[] cfName : new byte[][] { fam1, fam2, fam3 }) {
      ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(cfName)
        .setMinVersions(minVersions).setMaxVersions(versions).setKeepDeletedCells(keepDeleted)
        .setBlockCacheEnabled(false).setTimeToLive(ttl);
      if (isNewVersionBehaviorEnabled()) {
        cfBuilder.setNewVersionBehavior(true);
      }
      builder.setColumnFamily(cfBuilder.build());
    }
    return builder.build();
  }

  public TableDescriptorBuilder createModifyableTableDescriptor(final TableName name,
    final int minVersions, final int versions, final int ttl, KeepDeletedCells keepDeleted) {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(name);
    for (byte[] cfName : new byte[][] { fam1, fam2, fam3 }) {
      ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(cfName)
        .setMinVersions(minVersions).setMaxVersions(versions).setKeepDeletedCells(keepDeleted)
        .setBlockCacheEnabled(false).setTimeToLive(ttl);
      if (isNewVersionBehaviorEnabled()) {
        cfBuilder.setNewVersionBehavior(true);
      }
      builder.setColumnFamily(cfBuilder.build());
    }
    return builder;
  }

  /**
   * Create a table of name <code>name</code>.
   * @param name Name to give table.
   * @return Column descriptor.
   */
  public TableDescriptor createTableDescriptor(final TableName name) {
    return createTableDescriptor(name, ColumnFamilyDescriptorBuilder.DEFAULT_MIN_VERSIONS,
      MAXVERSIONS, HConstants.FOREVER, ColumnFamilyDescriptorBuilder.DEFAULT_KEEP_DELETED);
  }

  public TableDescriptor createTableDescriptor(final TableName tableName, byte[] family) {
    return createTableDescriptor(tableName, new byte[][] { family }, 1);
  }

  public TableDescriptor createTableDescriptor(final TableName tableName, byte[][] families,
    int maxVersions) {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    for (byte[] family : families) {
      ColumnFamilyDescriptorBuilder cfBuilder =
        ColumnFamilyDescriptorBuilder.newBuilder(family).setMaxVersions(maxVersions);
      if (isNewVersionBehaviorEnabled()) {
        cfBuilder.setNewVersionBehavior(true);
      }
      builder.setColumnFamily(cfBuilder.build());
    }
    return builder.build();
  }

  /**
   * Create an HRegion that writes to the local tmp dirs
   * @param desc a table descriptor indicating which table the region belongs to
   * @param startKey the start boundary of the region
   * @param endKey the end boundary of the region
   * @return a region that writes to local dir for testing
   */
  public HRegion createLocalHRegion(TableDescriptor desc, byte[] startKey, byte[] endKey)
    throws IOException {
    RegionInfo hri = RegionInfoBuilder.newBuilder(desc.getTableName()).setStartKey(startKey)
      .setEndKey(endKey).build();
    return createLocalHRegion(hri, desc);
  }

  /**
   * Create an HRegion that writes to the local tmp dirs. Creates the WAL for you. Be sure to call
   * {@link HBaseTestingUtility#closeRegionAndWAL(HRegion)} when you're finished with it.
   */
  public HRegion createLocalHRegion(RegionInfo info, TableDescriptor desc) throws IOException {
    return createRegionAndWAL(info, getDataTestDir(), getConfiguration(), desc);
  }

  /**
   * Create an HRegion that writes to the local tmp dirs with specified wal
   * @param info regioninfo
   * @param conf configuration
   * @param desc table descriptor
   * @param wal wal for this region.
   * @return created hregion
   * @throws IOException
   */
  public HRegion createLocalHRegion(RegionInfo info, Configuration conf, TableDescriptor desc,
      WAL wal) throws IOException {
    return HRegion.createHRegion(info, getDataTestDir(), conf, desc, wal);
  }

  /**
   * @param tableName
   * @param startKey
   * @param stopKey
   * @param isReadOnly
   * @param families
   * @return A region on which you must call
   * {@link HBaseTestingUtility#closeRegionAndWAL(HRegion)} when done.
   * @throws IOException
   */
  public HRegion createLocalHRegion(TableName tableName, byte[] startKey, byte[] stopKey,
      Configuration conf, boolean isReadOnly, Durability durability, WAL wal, byte[]... families)
      throws IOException {
    return createLocalHRegionWithInMemoryFlags(tableName, startKey, stopKey, conf, isReadOnly,
        durability, wal, null, families);
  }

  public HRegion createLocalHRegionWithInMemoryFlags(TableName tableName, byte[] startKey,
    byte[] stopKey, Configuration conf, boolean isReadOnly, Durability durability, WAL wal,
    boolean[] compactedMemStore, byte[]... families) throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.setReadOnly(isReadOnly);
    int i = 0;
    for (byte[] family : families) {
      ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(family);
      if (compactedMemStore != null && i < compactedMemStore.length) {
        cfBuilder.setInMemoryCompaction(MemoryCompactionPolicy.BASIC);
      } else {
        cfBuilder.setInMemoryCompaction(MemoryCompactionPolicy.NONE);

      }
      i++;
      // Set default to be three versions.
      cfBuilder.setMaxVersions(Integer.MAX_VALUE);
      builder.setColumnFamily(cfBuilder.build());
    }
    builder.setDurability(durability);
    RegionInfo info =
      RegionInfoBuilder.newBuilder(tableName).setStartKey(startKey).setEndKey(stopKey).build();
    return createLocalHRegion(info, conf, builder.build(), wal);
  }

  //
  // ==========================================================================

  /**
   * Provide an existing table name to truncate.
   * Scans the table and issues a delete for each row read.
   * @param tableName existing table
   * @return HTable to that new table
   * @throws IOException
   */
  public Table deleteTableData(TableName tableName) throws IOException {
    Table table = getConnection().getTable(tableName);
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
   * Truncate a table using the admin command.
   * Effectively disables, deletes, and recreates the table.
   * @param tableName table which must exist.
   * @param preserveRegions keep the existing split points
   * @return HTable for the new table
   */
  public Table truncateTable(final TableName tableName, final boolean preserveRegions) throws
      IOException {
    Admin admin = getAdmin();
    if (!admin.isTableDisabled(tableName)) {
      admin.disableTable(tableName);
    }
    admin.truncateTable(tableName, preserveRegions);
    return getConnection().getTable(tableName);
  }

  /**
   * Truncate a table using the admin command.
   * Effectively disables, deletes, and recreates the table.
   * For previous behavior of issuing row deletes, see
   * deleteTableData.
   * Expressly does not preserve regions of existing table.
   * @param tableName table which must exist.
   * @return HTable for the new table
   */
  public Table truncateTable(final TableName tableName) throws IOException {
    return truncateTable(tableName, false);
  }

  /**
   * Load table with rows from 'aaa' to 'zzz'.
   * @param t Table
   * @param f Family
   * @return Count of rows loaded.
   * @throws IOException
   */
  public int loadTable(final Table t, final byte[] f) throws IOException {
    return loadTable(t, new byte[][] {f});
  }

  /**
   * Load table with rows from 'aaa' to 'zzz'.
   * @param t Table
   * @param f Family
   * @return Count of rows loaded.
   * @throws IOException
   */
  public int loadTable(final Table t, final byte[] f, boolean writeToWAL) throws IOException {
    return loadTable(t, new byte[][] {f}, null, writeToWAL);
  }

  /**
   * Load table of multiple column families with rows from 'aaa' to 'zzz'.
   * @param t Table
   * @param f Array of Families to load
   * @return Count of rows loaded.
   * @throws IOException
   */
  public int loadTable(final Table t, final byte[][] f) throws IOException {
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
  public int loadTable(final Table t, final byte[][] f, byte[] value) throws IOException {
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
  public int loadTable(final Table t, final byte[][] f, byte[] value,
      boolean writeToWAL) throws IOException {
    List<Put> puts = new ArrayList<>();
    for (byte[] row : HBaseTestingUtility.ROWS) {
      Put put = new Put(row);
      put.setDurability(writeToWAL ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
      for (int i = 0; i < f.length; i++) {
        byte[] value1 = value != null ? value : row;
        put.addColumn(f[i], f[i], value1);
      }
      puts.add(put);
    }
    t.put(puts);
    return puts.size();
  }

  /** A tracker for tracking and validating table rows
   * generated with {@link HBaseTestingUtility#loadTable(Table, byte[])}
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
              String row = new String(new byte[] {b1,b2,b3}, StandardCharsets.UTF_8);
              throw new RuntimeException("Row:" + row + " has a seen count of " + count + " " +
                  "instead of " + expectedCount);
            }
          }
        }
      }
    }
  }

  public int loadRegion(final HRegion r, final byte[] f) throws IOException {
    return loadRegion(r, f, false);
  }

  public int loadRegion(final Region r, final byte[] f) throws IOException {
    return loadRegion((HRegion)r, f);
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
          put.addColumn(f, null, k);
          if (r.getWAL() == null) {
            put.setDurability(Durability.SKIP_WAL);
          }
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
        r.flush(true);
      }
    }
    return rowCount;
  }

  public void loadNumericRows(final Table t, final byte[] f, int startRow, int endRow)
      throws IOException {
    for (int i = startRow; i < endRow; i++) {
      byte[] data = Bytes.toBytes(String.valueOf(i));
      Put put = new Put(data);
      put.addColumn(f, null, data);
      t.put(put);
    }
  }

  public void loadRandomRows(final Table t, final byte[] f, int rowSize, int totalRows)
      throws IOException {
    byte[] row = new byte[rowSize];
    for (int i = 0; i < totalRows; i++) {
      Bytes.random(row);
      Put put = new Put(row);
      put.addColumn(f, new byte[]{0}, new byte[]{0});
      t.put(put);
    }
  }

  public void verifyNumericRows(Table table, final byte[] f, int startRow, int endRow,
      int replicaId)
      throws IOException {
    for (int i = startRow; i < endRow; i++) {
      String failMsg = "Failed verification of row :" + i;
      byte[] data = Bytes.toBytes(String.valueOf(i));
      Get get = new Get(data);
      get.setReplicaId(replicaId);
      get.setConsistency(Consistency.TIMELINE);
      Result result = table.get(get);
      if (!result.containsColumn(f, null)) {
        throw new AssertionError(failMsg);
      }
      assertEquals(failMsg, 1, result.getColumnCells(f, null).size());
      Cell cell = result.getColumnLatestCell(f, null);
      if (!Bytes.equals(data, 0, data.length, cell.getValueArray(), cell.getValueOffset(),
        cell.getValueLength())) {
        throw new AssertionError(failMsg);
      }
    }
  }

  public void verifyNumericRows(Region region, final byte[] f, int startRow, int endRow)
      throws IOException {
    verifyNumericRows((HRegion)region, f, startRow, endRow);
  }

  public void verifyNumericRows(HRegion region, final byte[] f, int startRow, int endRow)
      throws IOException {
    verifyNumericRows(region, f, startRow, endRow, true);
  }

  public void verifyNumericRows(Region region, final byte[] f, int startRow, int endRow,
      final boolean present) throws IOException {
    verifyNumericRows((HRegion)region, f, startRow, endRow, present);
  }

  public void verifyNumericRows(HRegion region, final byte[] f, int startRow, int endRow,
      final boolean present) throws IOException {
    for (int i = startRow; i < endRow; i++) {
      String failMsg = "Failed verification of row :" + i;
      byte[] data = Bytes.toBytes(String.valueOf(i));
      Result result = region.get(new Get(data));

      boolean hasResult = result != null && !result.isEmpty();
      if (present != hasResult) {
        throw new AssertionError(
          failMsg + result + " expected:<" + present + "> but was:<" + hasResult + ">");
      }
      if (!present) continue;

      if (!result.containsColumn(f, null)) {
        throw new AssertionError(failMsg);
      }
      assertEquals(failMsg, 1, result.getColumnCells(f, null).size());
      Cell cell = result.getColumnLatestCell(f, null);
      if (!Bytes.equals(data, 0, data.length, cell.getValueArray(), cell.getValueOffset(),
        cell.getValueLength())) {
        throw new AssertionError(failMsg);
      }
    }
  }

  public void deleteNumericRows(final Table t, final byte[] f, int startRow, int endRow)
      throws IOException {
    for (int i = startRow; i < endRow; i++) {
      byte[] data = Bytes.toBytes(String.valueOf(i));
      Delete delete = new Delete(data);
      delete.addFamily(f);
      t.delete(delete);
    }
  }

  /**
   * Return the number of rows in the given table.
   * @param table to count rows
   * @return count of rows
   */
  public static int countRows(final Table table) throws IOException {
    return countRows(table, new Scan());
  }

  public static int countRows(final Table table, final Scan scan) throws IOException {
    try (ResultScanner results = table.getScanner(scan)) {
      int count = 0;
      while (results.next() != null) {
        count++;
      }
      return count;
    }
  }

  public int countRows(final Table table, final byte[]... families) throws IOException {
    Scan scan = new Scan();
    for (byte[] family: families) {
      scan.addFamily(family);
    }
    return countRows(table, scan);
  }

  /**
   * Return the number of rows in the given table.
   */
  public int countRows(final TableName tableName) throws IOException {
    Table table = getConnection().getTable(tableName);
    try {
      return countRows(table);
    } finally {
      table.close();
    }
  }

  public int countRows(final Region region) throws IOException {
    return countRows(region, new Scan());
  }

  public int countRows(final Region region, final Scan scan) throws IOException {
    InternalScanner scanner = region.getScanner(scan);
    try {
      return countRows(scanner);
    } finally {
      scanner.close();
    }
  }

  public int countRows(final InternalScanner scanner) throws IOException {
    int scannedCount = 0;
    List<Cell> results = new ArrayList<>();
    boolean hasMore = true;
    while (hasMore) {
      hasMore = scanner.next(results);
      scannedCount += results.size();
      results.clear();
    }
    return scannedCount;
  }

  /**
   * Return an md5 digest of the entire contents of a table.
   */
  public String checksumRows(final Table table) throws Exception {

    Scan scan = new Scan();
    ResultScanner results = table.getScanner(scan);
    MessageDigest digest = MessageDigest.getInstance("MD5");
    for (Result res : results) {
      digest.update(res.getRow());
    }
    results.close();
    return digest.toString();
  }

  /** All the row values for the data loaded by {@link #loadTable(Table, byte[])} */
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
   * Create rows in hbase:meta for regions of the specified table with the specified
   * start keys.  The first startKey should be a 0 length byte array if you
   * want to form a proper range of regions.
   * @param conf
   * @param htd
   * @param startKeys
   * @return list of region info for regions added to meta
   * @throws IOException
   */
  public List<RegionInfo> createMultiRegionsInMeta(final Configuration conf,
      final TableDescriptor htd, byte [][] startKeys)
  throws IOException {
    Table meta = getConnection().getTable(TableName.META_TABLE_NAME);
    Arrays.sort(startKeys, Bytes.BYTES_COMPARATOR);
    List<RegionInfo> newRegions = new ArrayList<>(startKeys.length);
    MetaTableAccessor
        .updateTableState(getConnection(), htd.getTableName(), TableState.State.ENABLED);
    // add custom ones
    for (int i = 0; i < startKeys.length; i++) {
      int j = (i + 1) % startKeys.length;
      RegionInfo hri = RegionInfoBuilder.newBuilder(htd.getTableName())
          .setStartKey(startKeys[i])
          .setEndKey(startKeys[j])
          .build();
      MetaTableAccessor.addRegionsToMeta(getConnection(), Collections.singletonList(hri), 1);
      newRegions.add(hri);
    }

    meta.close();
    return newRegions;
  }

  /**
   * Create an unmanaged WAL. Be sure to close it when you're through.
   */
  public static WAL createWal(final Configuration conf, final Path rootDir, final RegionInfo hri)
      throws IOException {
    // The WAL subsystem will use the default rootDir rather than the passed in rootDir
    // unless I pass along via the conf.
    Configuration confForWAL = new Configuration(conf);
    confForWAL.set(HConstants.HBASE_DIR, rootDir.toString());
    return new WALFactory(confForWAL, "hregion-" + RandomStringUtils.randomNumeric(8)).getWAL(hri);
  }


  /**
   * Create a region with it's own WAL. Be sure to call
   * {@link HBaseTestingUtility#closeRegionAndWAL(HRegion)} to clean up all resources.
   */
  public static HRegion createRegionAndWAL(final RegionInfo info, final Path rootDir,
      final Configuration conf, final TableDescriptor htd) throws IOException {
    return createRegionAndWAL(info, rootDir, conf, htd, true);
  }

  /**
   * Create a region with it's own WAL. Be sure to call
   * {@link HBaseTestingUtility#closeRegionAndWAL(HRegion)} to clean up all resources.
   */
  public static HRegion createRegionAndWAL(final RegionInfo info, final Path rootDir,
      final Configuration conf, final TableDescriptor htd, BlockCache blockCache)
      throws IOException {
    HRegion region = createRegionAndWAL(info, rootDir, conf, htd, false);
    region.setBlockCache(blockCache);
    region.initialize();
    return region;
  }
  /**
   * Create a region with it's own WAL. Be sure to call
   * {@link HBaseTestingUtility#closeRegionAndWAL(HRegion)} to clean up all resources.
   */
  public static HRegion createRegionAndWAL(final RegionInfo info, final Path rootDir,
      final Configuration conf, final TableDescriptor htd, MobFileCache mobFileCache)
      throws IOException {
    HRegion region = createRegionAndWAL(info, rootDir, conf, htd, false);
    region.setMobFileCache(mobFileCache);
    region.initialize();
    return region;
  }

  /**
   * Create a region with it's own WAL. Be sure to call
   * {@link HBaseTestingUtility#closeRegionAndWAL(HRegion)} to clean up all resources.
   */
  public static HRegion createRegionAndWAL(final RegionInfo info, final Path rootDir,
      final Configuration conf, final TableDescriptor htd, boolean initialize)
      throws IOException {
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0,
      0, null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    WAL wal = createWal(conf, rootDir, info);
    return HRegion.createHRegion(info, rootDir, conf, htd, wal, initialize);
  }

  /**
   * Returns all rows from the hbase:meta table.
   *
   * @throws IOException When reading the rows fails.
   */
  public List<byte[]> getMetaTableRows() throws IOException {
    // TODO: Redo using MetaTableAccessor class
    Table t = getConnection().getTable(TableName.META_TABLE_NAME);
    List<byte[]> rows = new ArrayList<>();
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
    // TODO: Redo using MetaTableAccessor.
    Table t = getConnection().getTable(TableName.META_TABLE_NAME);
    List<byte[]> rows = new ArrayList<>();
    ResultScanner s = t.getScanner(new Scan());
    for (Result result : s) {
      RegionInfo info = CatalogFamilyFormat.getRegionInfo(result);
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
   * Returns all regions of the specified table
   *
   * @param tableName the table name
   * @return all regions of the specified table
   * @throws IOException when getting the regions fails.
   */
  private List<RegionInfo> getRegions(TableName tableName) throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      return admin.getRegions(tableName);
    }
  }

  /*
   * Find any other region server which is different from the one identified by parameter
   * @param rs
   * @return another region server
   */
  public HRegionServer getOtherRegionServer(HRegionServer rs) {
    for (JVMClusterUtil.RegionServerThread rst :
      getMiniHBaseCluster().getRegionServerThreads()) {
      if (!(rst.getRegionServer() == rs)) {
        return rst.getRegionServer();
      }
    }
    return null;
  }

  /**
   * Tool to get the reference to the region server object that holds the
   * region of the specified user table.
   * @param tableName user table to lookup in hbase:meta
   * @return region server that holds it, null if the row doesn't exist
   * @throws IOException
   * @throws InterruptedException
   */
  public HRegionServer getRSForFirstRegionInTable(TableName tableName)
      throws IOException, InterruptedException {
    List<RegionInfo> regions = getRegions(tableName);
    if (regions == null || regions.isEmpty()) {
      return null;
    }
    LOG.debug("Found " + regions.size() + " regions for table " +
        tableName);

    byte[] firstRegionName = regions.stream()
        .filter(r -> !r.isOffline())
        .map(RegionInfo::getRegionName)
        .findFirst()
        .orElseThrow(() -> new IOException("online regions not found in table " + tableName));

    LOG.debug("firstRegionName=" + Bytes.toString(firstRegionName));
    long pause = getConfiguration().getLong(HConstants.HBASE_CLIENT_PAUSE,
      HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    int numRetries = getConfiguration().getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    RetryCounter retrier = new RetryCounter(numRetries+1, (int)pause, TimeUnit.MICROSECONDS);
    while(retrier.shouldRetry()) {
      int index = getMiniHBaseCluster().getServerWith(firstRegionName);
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
    // Set a very high max-disk-utilization percentage to avoid the NodeManagers from failing.
    conf.setIfUnset(
        "yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage",
        "99.0");
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

      Field modifiersField = ReflectionUtils.getModifiersField();
      modifiersField.setAccessible(true);
      modifiersField.setInt(logDirField, logDirField.getModifiers() & ~Modifier.FINAL);

      logDirField.set(null, new File(hadoopLogDir, "userlogs"));
    } catch (SecurityException e) {
      throw new RuntimeException(e);
    } catch (NoSuchFieldException e) {
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
    // Hadoop MiniMR overwrites this while it should not
    jobConf.set("mapreduce.cluster.local.dir", conf.get("mapreduce.cluster.local.dir"));
    LOG.info("Mini mapreduce cluster started");

    // In hadoop2, YARN/MR2 starts a mini cluster with its own conf instance and updates settings.
    // Our HBase MR jobs need several of these settings in order to properly run.  So we copy the
    // necessary config properties here.  YARN-129 required adding a few properties.
    conf.set("mapreduce.jobtracker.address", jobConf.get("mapreduce.jobtracker.address"));
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
    String mrJobHistoryWebappAddress =
      jobConf.get("mapreduce.jobhistory.webapp.address");
    if (mrJobHistoryWebappAddress != null) {
      conf.set("mapreduce.jobhistory.webapp.address", mrJobHistoryWebappAddress);
    }
    String yarnRMWebappAddress =
      jobConf.get("yarn.resourcemanager.webapp.address");
    if (yarnRMWebappAddress != null) {
      conf.set("yarn.resourcemanager.webapp.address", yarnRMWebappAddress);
    }
  }

  /**
   * Stops the previously started <code>MiniMRCluster</code>.
   */
  public void shutdownMiniMapReduceCluster() {
    if (mrCluster != null) {
      LOG.info("Stopping mini mapreduce cluster...");
      mrCluster.shutdown();
      mrCluster = null;
      LOG.info("Mini mapreduce cluster stopped");
    }
    // Restore configuration to point to local jobtracker
    conf.set("mapreduce.jobtracker.address", "local");
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
  public RegionServerServices createMockRegionServerService(RpcServerInterface rpc) throws
      IOException {
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
   * @param clazz The class for which to switch to debug logging.
   * @deprecated In 2.3.0, will be removed in 4.0.0. Only support changing log level on log4j now as
   *             HBase only uses log4j. You should do this by your own as it you know which log
   *             framework you are using then set the log level to debug is very easy.
   */
  @Deprecated
  public void enableDebug(Class<?> clazz) {
    Log4jUtils.enableDebug(clazz);
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

  public void expireSession(ZKWatcher nodeZK) throws Exception {
   expireSession(nodeZK, false);
  }

  /**
   * Expire a ZooKeeper session as recommended in ZooKeeper documentation
   * http://hbase.apache.org/book.html#trouble.zookeeper
   * There are issues when doing this:
   * [1] http://www.mail-archive.com/dev@zookeeper.apache.org/msg01942.html
   * [2] https://issues.apache.org/jira/browse/ZOOKEEPER-1105
   *
   * @param nodeZK - the ZK watcher to expire
   * @param checkStatus - true to check if we can create a Table with the
   *                    current configuration.
   */
  public void expireSession(ZKWatcher nodeZK, boolean checkStatus)
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
    long start = EnvironmentEdgeManager.currentTime();
    while (newZK.getState() != States.CONNECTED
         && EnvironmentEdgeManager.currentTime() - start < 1000) {
       Thread.sleep(1);
    }
    newZK.close();
    LOG.info("ZK Closed Session 0x" + Long.toHexString(sessionID));

    // Now closing & waiting to be sure that the clients get it.
    monitor.close();

    if (checkStatus) {
      getConnection().getTable(TableName.META_TABLE_NAME).close();
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
   * Resets the connections so that the next time getConnection() is called, a new connection is
   * created. This is needed in cases where the entire cluster / all the masters are shutdown and
   * the connection is not valid anymore.
   * TODO: There should be a more coherent way of doing this. Unfortunately the way tests are
   *   written, not all start() stop() calls go through this class. Most tests directly operate on
   *   the underlying mini/local hbase cluster. That makes it difficult for this wrapper class to
   *   maintain the connection state automatically. Cleaning this is a much bigger refactor.
   */
  public void invalidateConnection() throws IOException {
    closeConnection();
    // Update the master addresses if they changed.
    final String masterConfigBefore = conf.get(HConstants.MASTER_ADDRS_KEY);
    final String masterConfAfter = getMiniHBaseCluster().conf.get(HConstants.MASTER_ADDRS_KEY);
    LOG.info("Invalidated connection. Updating master addresses before: {} after: {}",
        masterConfigBefore, masterConfAfter);
    conf.set(HConstants.MASTER_ADDRS_KEY,
        getMiniHBaseCluster().conf.get(HConstants.MASTER_ADDRS_KEY));
  }

  /**
   * Get a shared Connection to the cluster.
   * this method is thread safe.
   * @return A Connection that can be shared. Don't close. Will be closed on shutdown of cluster.
   */
  public Connection getConnection() throws IOException {
    return getAsyncConnection().toConnection();
  }

  /**
   * Get a assigned Connection to the cluster.
   * this method is thread safe.
   * @param user assigned user
   * @return A Connection with assigned user.
   */
  public Connection getConnection(User user) throws IOException {
    return getAsyncConnection(user).toConnection();
  }

  /**
   * Get a shared AsyncClusterConnection to the cluster.
   * this method is thread safe.
   * @return An AsyncClusterConnection that can be shared. Don't close. Will be closed on shutdown of cluster.
   */
  public AsyncClusterConnection getAsyncConnection() throws IOException {
    try {
      return asyncConnection.updateAndGet(connection -> {
        if (connection == null) {
          try {
            User user = UserProvider.instantiate(conf).getCurrent();
            connection = getAsyncConnection(user);
          } catch(IOException ioe) {
            throw new UncheckedIOException("Failed to create connection", ioe);
          }
        }
        return connection;
      });
    } catch (UncheckedIOException exception) {
      throw exception.getCause();
    }
  }

  /**
   * Get a assigned AsyncClusterConnection to the cluster.
   * this method is thread safe.
   * @param user assigned user
   * @return An AsyncClusterConnection with assigned user.
   */
  public AsyncClusterConnection getAsyncConnection(User user) throws IOException {
    return ClusterConnectionFactory.createAsyncClusterConnection(conf, null, user);
  }

  public void closeConnection() throws IOException {
    if (hbaseAdmin != null) {
      Closeables.close(hbaseAdmin, true);
      hbaseAdmin = null;
    }
    AsyncClusterConnection asyncConnection = this.asyncConnection.getAndSet(null);
    if (asyncConnection != null) {
      Closeables.close(asyncConnection, true);
    }
  }

  /**
   * Returns an Admin instance which is shared between HBaseTestingUtility instance users.
   * Closing it has no effect, it will be closed automatically when the cluster shutdowns
   */
  public Admin getAdmin() throws IOException {
    if (hbaseAdmin == null){
      this.hbaseAdmin = getConnection().getAdmin();
    }
    return hbaseAdmin;
  }

  private Admin hbaseAdmin = null;

  /**
   * Returns an {@link Hbck} instance. Needs be closed when done.
   */
  public Hbck getHbck() throws IOException {
    return getConnection().getHbck();
  }

  /**
   * Unassign the named region.
   *
   * @param regionName  The region to unassign.
   */
  public void unassignRegion(String regionName) throws IOException {
    unassignRegion(Bytes.toBytes(regionName));
  }

  /**
   * Unassign the named region.
   *
   * @param regionName  The region to unassign.
   */
  public void unassignRegion(byte[] regionName) throws IOException {
    getAdmin().unassign(regionName, true);
  }

  /**
   * Closes the region containing the given row.
   *
   * @param row  The row to find the containing region.
   * @param table  The table to find the region.
   */
  public void unassignRegionByRow(String row, RegionLocator table) throws IOException {
    unassignRegionByRow(Bytes.toBytes(row), table);
  }

  /**
   * Closes the region containing the given row.
   *
   * @param row  The row to find the containing region.
   * @param table  The table to find the region.
   * @throws IOException
   */
  public void unassignRegionByRow(byte[] row, RegionLocator table) throws IOException {
    HRegionLocation hrl = table.getRegionLocation(row);
    unassignRegion(hrl.getRegion().getRegionName());
  }

  /**
   * Retrieves a splittable region randomly from tableName
   * @param tableName name of table
   * @param maxAttempts maximum number of attempts, unlimited for value of -1
   * @return the HRegion chosen, null if none was found within limit of maxAttempts
   */
  public HRegion getSplittableRegion(TableName tableName, int maxAttempts) {
    List<HRegion> regions = getHBaseCluster().getRegions(tableName);
    int regCount = regions.size();
    Set<Integer> attempted = new HashSet<>();
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
        idx = ThreadLocalRandom.current().nextInt(regCount);
        // if we have just tried this region, there is no need to try again
        if (attempted.contains(idx)) {
          continue;
        }
        HRegion region = regions.get(idx);
        if (region.checkSplit().isPresent()) {
          return region;
        }
        attempted.add(idx);
      }
      attempts++;
    } while (maxAttempts == -1 || attempts < maxAttempts);
    return null;
  }

  public MiniDFSCluster getDFSCluster() {
    return dfsCluster;
  }

  public void setDFSCluster(MiniDFSCluster cluster) throws IllegalStateException, IOException {
    setDFSCluster(cluster, true);
  }

  /**
   * Set the MiniDFSCluster
   * @param cluster cluster to use
   * @param requireDown require the that cluster not be "up" (MiniDFSCluster#isClusterUp) before
   * it is set.
   * @throws IllegalStateException if the passed cluster is up when it is required to be down
   * @throws IOException if the FileSystem could not be set from the passed dfs cluster
   */
  public void setDFSCluster(MiniDFSCluster cluster, boolean requireDown)
      throws IllegalStateException, IOException {
    if (dfsCluster != null && requireDown && dfsCluster.isClusterUp()) {
      throw new IllegalStateException("DFSCluster is already running! Shut it down first.");
    }
    this.dfsCluster = cluster;
    this.setFs();
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
  public void waitTableAvailable(TableName table)
      throws InterruptedException, IOException {
    waitTableAvailable(table.getName(), 30000);
  }

  public void waitTableAvailable(TableName table, long timeoutMillis)
      throws InterruptedException, IOException {
    waitFor(timeoutMillis, predicateTableAvailable(table));
  }

  /**
   * Wait until all regions in a table have been assigned
   * @param table Table to wait on.
   * @param timeoutMillis Timeout.
   */
  public void waitTableAvailable(byte[] table, long timeoutMillis)
      throws InterruptedException, IOException {
    waitFor(timeoutMillis, predicateTableAvailable(TableName.valueOf(table)));
  }

  public String explainTableAvailability(TableName tableName) throws IOException {
    StringBuilder msg =
      new StringBuilder(explainTableState(tableName, TableState.State.ENABLED)).append(", ");
    if (getHBaseCluster().getMaster().isAlive()) {
      Map<RegionInfo, ServerName> assignments = getHBaseCluster().getMaster().getAssignmentManager()
        .getRegionStates().getRegionAssignments();
      final List<Pair<RegionInfo, ServerName>> metaLocations =
        MetaTableAccessor.getTableRegionsAndLocations(getConnection(), tableName);
      for (Pair<RegionInfo, ServerName> metaLocation : metaLocations) {
        RegionInfo hri = metaLocation.getFirst();
        ServerName sn = metaLocation.getSecond();
        if (!assignments.containsKey(hri)) {
          msg.append(", region ").append(hri)
            .append(" not assigned, but found in meta, it expected to be on ").append(sn);
        } else if (sn == null) {
          msg.append(",  region ").append(hri).append(" assigned,  but has no server in meta");
        } else if (!sn.equals(assignments.get(hri))) {
          msg.append(",  region ").append(hri)
            .append(" assigned,  but has different servers in meta and AM ( ").append(sn)
            .append(" <> ").append(assignments.get(hri));
        }
      }
    }
    return msg.toString();
  }

  public String explainTableState(final TableName table, TableState.State state)
      throws IOException {
    TableState tableState = MetaTableAccessor.getTableState(getConnection(), table);
    if (tableState == null) {
      return "TableState in META: No table state in META for table " + table +
        " last state in meta (including deleted is " + findLastTableState(table) + ")";
    } else if (!tableState.inStates(state)) {
      return "TableState in META: Not " + state + " state, but " + tableState;
    } else {
      return "TableState in META: OK";
    }
  }

  public TableState findLastTableState(final TableName table) throws IOException {
    final AtomicReference<TableState> lastTableState = new AtomicReference<>(null);
    ClientMetaTableAccessor.Visitor visitor = new ClientMetaTableAccessor.Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (!Arrays.equals(r.getRow(), table.getName())) {
          return false;
        }
        TableState state = CatalogFamilyFormat.getTableState(r);
        if (state != null) {
          lastTableState.set(state);
        }
        return true;
      }
    };
    MetaTableAccessor.scanMeta(getConnection(), null, null,
      ClientMetaTableAccessor.QueryType.TABLE, Integer.MAX_VALUE, visitor);
    return lastTableState.get();
  }

  /**
   * Waits for a table to be 'enabled'.  Enabled means that table is set as 'enabled' and the
   * regions have been all assigned.  Will timeout after default period (30 seconds)
   * Tolerates nonexistent table.
   * @param table the table to wait on.
   * @throws InterruptedException if interrupted while waiting
   * @throws IOException if an IO problem is encountered
   */
  public void waitTableEnabled(TableName table)
      throws InterruptedException, IOException {
    waitTableEnabled(table, 30000);
  }

  /**
   * Waits for a table to be 'enabled'.  Enabled means that table is set as 'enabled' and the
   * regions have been all assigned.
   * @see #waitTableEnabled(TableName, long)
   * @param table Table to wait on.
   * @param timeoutMillis Time to wait on it being marked enabled.
   * @throws InterruptedException
   * @throws IOException
   */
  public void waitTableEnabled(byte[] table, long timeoutMillis)
  throws InterruptedException, IOException {
    waitTableEnabled(TableName.valueOf(table), timeoutMillis);
  }

  public void waitTableEnabled(TableName table, long timeoutMillis)
  throws IOException {
    waitFor(timeoutMillis, predicateTableEnabled(table));
  }

  /**
   * Waits for a table to be 'disabled'.  Disabled means that table is set as 'disabled'
   * Will timeout after default period (30 seconds)
   * @param table Table to wait on.
   * @throws InterruptedException
   * @throws IOException
   */
  public void waitTableDisabled(byte[] table)
          throws InterruptedException, IOException {
    waitTableDisabled(table, 30000);
  }

  public void waitTableDisabled(TableName table, long millisTimeout)
          throws InterruptedException, IOException {
    waitFor(millisTimeout, predicateTableDisabled(table));
  }

  /**
   * Waits for a table to be 'disabled'.  Disabled means that table is set as 'disabled'
   * @param table Table to wait on.
   * @param timeoutMillis Time to wait on it being marked disabled.
   * @throws InterruptedException
   * @throws IOException
   */
  public void waitTableDisabled(byte[] table, long timeoutMillis)
          throws InterruptedException, IOException {
    waitTableDisabled(TableName.valueOf(table), timeoutMillis);
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
   * works for DistributedFileSystem w/o Kerberos.
   * @param c Initial configuration
   * @param differentiatingSuffix Suffix to differentiate this user from others.
   * @return A new configuration instance with a different user set into it.
   * @throws IOException
   */
  public static User getDifferentUser(final Configuration c,
    final String differentiatingSuffix)
  throws IOException {
    FileSystem currentfs = FileSystem.get(c);
    if (!(currentfs instanceof DistributedFileSystem) || User.isHBaseSecurityEnabled(c)) {
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

  public static NavigableSet<String> getAllOnlineRegions(MiniHBaseCluster cluster)
      throws IOException {
    NavigableSet<String> online = new TreeSet<>();
    for (RegionServerThread rst : cluster.getLiveRegionServerThreads()) {
      try {
        for (RegionInfo region :
            ProtobufUtil.getOnlineRegions(rst.getRegionServer().getRSRpcServices())) {
          online.add(region.getRegionNameAsString());
        }
      } catch (RegionServerStoppedException e) {
        // That's fine.
      }
    }
    return online;
  }

  /**
   * Set maxRecoveryErrorCount in DFSClient.  In 0.20 pre-append its hard-coded to 5 and
   * makes tests linger.  Here is the exception you'll see:
   * <pre>
   * 2010-06-15 11:52:28,511 WARN  [DataStreamer for file /hbase/.logs/wal.1276627923013 block
   * blk_928005470262850423_1021] hdfs.DFSClient$DFSOutputStream(2657): Error Recovery for block
   * blk_928005470262850423_1021 failed  because recovery from primary datanode 127.0.0.1:53683
   * failed 4 times.  Pipeline was 127.0.0.1:53687, 127.0.0.1:53683. Will retry...
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
   * Uses directly the assignment manager to assign the region. and waits until the specified region
   * has completed assignment.
   * @return true if the region is assigned false otherwise.
   */
  public boolean assignRegion(final RegionInfo regionInfo)
      throws IOException, InterruptedException {
    final AssignmentManager am = getHBaseCluster().getMaster().getAssignmentManager();
    am.assign(regionInfo);
    return AssignmentTestingUtil.waitForAssignment(am, regionInfo);
  }

  /**
   * Move region to destination server and wait till region is completely moved and online
   *
   * @param destRegion region to move
   * @param destServer destination server of the region
   * @throws InterruptedException
   * @throws IOException
   */
  public void moveRegionAndWait(RegionInfo destRegion, ServerName destServer)
      throws InterruptedException, IOException {
    HMaster master = getMiniHBaseCluster().getMaster();
    // TODO: Here we start the move. The move can take a while.
    getAdmin().move(destRegion.getEncodedNameAsBytes(), destServer);
    while (true) {
      ServerName serverName = master.getAssignmentManager().getRegionStates()
          .getRegionServerOfRegion(destRegion);
      if (serverName != null && serverName.equals(destServer)) {
        assertRegionOnServer(destRegion, serverName, 2000);
        break;
      }
      Thread.sleep(10);
    }
  }

  /**
   * Wait until all regions for a table in hbase:meta have a non-empty
   * info:server, up to a configuable timeout value (default is 60 seconds)
   * This means all regions have been deployed,
   * master has been informed and updated hbase:meta with the regions deployed
   * server.
   * @param tableName the table name
   * @throws IOException
   */
  public void waitUntilAllRegionsAssigned(final TableName tableName) throws IOException {
    waitUntilAllRegionsAssigned(tableName,
      this.conf.getLong("hbase.client.sync.wait.timeout.msec", 60000));
  }

  /**
   * Waith until all system table's regions get assigned
   * @throws IOException
   */
  public void waitUntilAllSystemRegionsAssigned() throws IOException {
    waitUntilAllRegionsAssigned(TableName.META_TABLE_NAME);
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
    if (!TableName.isMetaTableName(tableName)) {
      try (final Table meta = getConnection().getTable(TableName.META_TABLE_NAME)) {
        LOG.debug("Waiting until all regions of table " + tableName + " get assigned. Timeout = " +
            timeout + "ms");
        waitFor(timeout, 200, true, new ExplainingPredicate<IOException>() {
          @Override
          public String explainFailure() throws IOException {
            return explainTableAvailability(tableName);
          }

          @Override
          public boolean evaluate() throws IOException {
            Scan scan = new Scan();
            scan.addFamily(HConstants.CATALOG_FAMILY);
            boolean tableFound = false;
            try (ResultScanner s = meta.getScanner(scan)) {
              for (Result r; (r = s.next()) != null;) {
                byte[] b = r.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
                RegionInfo info = RegionInfo.parseFromOrNull(b);
                if (info != null && info.getTable().equals(tableName)) {
                  // Get server hosting this region from catalog family. Return false if no server
                  // hosting this region, or if the server hosting this region was recently killed
                  // (for fault tolerance testing).
                  tableFound = true;
                  byte[] server =
                      r.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
                  if (server == null) {
                    return false;
                  } else {
                    byte[] startCode =
                        r.getValue(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER);
                    ServerName serverName =
                        ServerName.valueOf(Bytes.toString(server).replaceFirst(":", ",") + "," +
                            Bytes.toLong(startCode));
                    if (!getHBaseClusterInterface().isDistributedCluster() &&
                        getHBaseCluster().isKilledRS(serverName)) {
                      return false;
                    }
                  }
                  if (RegionStateStore.getRegionState(r, info) != RegionState.State.OPEN) {
                    return false;
                  }
                }
              }
            }
            if (!tableFound) {
              LOG.warn("Didn't find the entries for table " + tableName + " in meta, already deleted?");
            }
            return tableFound;
          }
        });
      }
    }
    LOG.info("All regions for table " + tableName + " assigned to meta. Checking AM states.");
    // check from the master state if we are using a mini cluster
    if (!getHBaseClusterInterface().isDistributedCluster()) {
      // So, all regions are in the meta table but make sure master knows of the assignments before
      // returning -- sometimes this can lag.
      HMaster master = getHBaseCluster().getMaster();
      final RegionStates states = master.getAssignmentManager().getRegionStates();
      waitFor(timeout, 200, new ExplainingPredicate<IOException>() {
        @Override
        public String explainFailure() throws IOException {
          return explainTableAvailability(tableName);
        }

        @Override
        public boolean evaluate() throws IOException {
          List<RegionInfo> hris = states.getRegionsOfTable(tableName);
          return hris != null && !hris.isEmpty();
        }
      });
    }
    LOG.info("All regions for table " + tableName + " assigned.");
  }

  /**
   * Do a small get/scan against one store. This is required because store
   * has no actual methods of querying itself, and relies on StoreScanner.
   */
  public static List<Cell> getFromStoreFile(HStore store,
                                                Get get) throws IOException {
    Scan scan = new Scan(get);
    InternalScanner scanner = (InternalScanner) store.getScanner(scan,
        scan.getFamilyMap().get(store.getColumnFamilyDescriptor().getName()),
        // originally MultiVersionConcurrencyControl.resetThreadReadPoint() was called to set
        // readpoint 0.
        0);

    List<Cell> result = new ArrayList<>();
    scanner.next(result);
    if (!result.isEmpty()) {
      // verify that we are on the row we want:
      Cell kv = result.get(0);
      if (!CellUtil.matchingRows(kv, get.getRow())) {
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
   * @return resulting split keys
   */
  public byte[][] getRegionSplitStartKeys(byte[] startKey, byte[] endKey, int numRegions){
    if (numRegions <= 3) {
      throw new AssertionError();
    }
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
    s.put(store.getColumnFamilyDescriptor().getName(), columns);

    return getFromStoreFile(store,get);
  }

  public static void assertKVListsEqual(String additionalMsg,
      final List<? extends Cell> expected,
      final List<? extends Cell> actual) {
    final int eLen = expected.size();
    final int aLen = actual.size();
    final int minLen = Math.min(eLen, aLen);

    int i;
    for (i = 0; i < minLen
        && CellComparator.getInstance().compare(expected.get(i), actual.get(i)) == 0;
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

  public static <T> String safeGetAsStr(List<T> lst, int i) {
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
  public Table createRandomTable(TableName tableName,
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
    final Table table = createTable(tableName, cfBytes,
        maxVersions,
        Bytes.toBytes(String.format(keyFormat, splitStartKey)),
        Bytes.toBytes(String.format(keyFormat, splitEndKey)),
        numRegions);

    if (hbaseCluster != null) {
      getMiniHBaseCluster().flushcache(TableName.META_TABLE_NAME);
    }

    BufferedMutator mutator = getConnection().getBufferedMutator(tableName);

    final Random rand = ThreadLocalRandom.current();
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
            put.addColumn(cf, qual, ts, value);
          } else if (rand.nextDouble() < 0.8) {
            del.addColumn(cf, qual, ts);
          } else {
            del.addColumns(cf, qual, ts);
          }
        }

        if (!put.isEmpty()) {
          mutator.mutate(put);
        }

        if (!del.isEmpty()) {
          mutator.mutate(del);
        }
      }
      LOG.info("Initiating flush #" + iFlush + " for table " + tableName);
      mutator.flush();
      if (hbaseCluster != null) {
        getMiniHBaseCluster().flushcache(table.getName());
      }
    }
    mutator.close();

    return table;
  }

  public static int randomFreePort() {
    return HBaseCommonTestingUtility.randomFreePort();
  }
  public static String randomMultiCastAddress() {
    return "226.1.1." + ThreadLocalRandom.current().nextInt(254);
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
    return createPreSplitLoadTestTable(conf, tableName,
      columnFamily, compression, dataBlockEncoding, DEFAULT_REGIONS_PER_SERVER, 1,
      Durability.USE_DEFAULT);
  }
  /**
   * Creates a pre-split table for load testing. If the table already exists,
   * logs a warning and continues.
   * @return the number of regions the table was split into
   */
  public static int createPreSplitLoadTestTable(Configuration conf, TableName tableName,
    byte[] columnFamily, Algorithm compression, DataBlockEncoding dataBlockEncoding,
    int numRegionsPerServer, int regionReplication, Durability durability) throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.setDurability(durability);
    builder.setRegionReplication(regionReplication);
    ColumnFamilyDescriptorBuilder cfBuilder =
      ColumnFamilyDescriptorBuilder.newBuilder(columnFamily);
    cfBuilder.setDataBlockEncoding(dataBlockEncoding);
    cfBuilder.setCompressionType(compression);
    return createPreSplitLoadTestTable(conf, builder.build(), cfBuilder.build(),
      numRegionsPerServer);
  }

  /**
   * Creates a pre-split table for load testing. If the table already exists,
   * logs a warning and continues.
   * @return the number of regions the table was split into
   */
  public static int createPreSplitLoadTestTable(Configuration conf, TableName tableName,
    byte[][] columnFamilies, Algorithm compression, DataBlockEncoding dataBlockEncoding,
    int numRegionsPerServer, int regionReplication, Durability durability) throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.setDurability(durability);
    builder.setRegionReplication(regionReplication);
    ColumnFamilyDescriptor[] hcds = new ColumnFamilyDescriptor[columnFamilies.length];
    for (int i = 0; i < columnFamilies.length; i++) {
      ColumnFamilyDescriptorBuilder cfBuilder =
        ColumnFamilyDescriptorBuilder.newBuilder(columnFamilies[i]);
      cfBuilder.setDataBlockEncoding(dataBlockEncoding);
      cfBuilder.setCompressionType(compression);
      hcds[i] = cfBuilder.build();
    }
    return createPreSplitLoadTestTable(conf, builder.build(), hcds, numRegionsPerServer);
  }

  /**
   * Creates a pre-split table for load testing. If the table already exists,
   * logs a warning and continues.
   * @return the number of regions the table was split into
   */
  public static int createPreSplitLoadTestTable(Configuration conf,
      TableDescriptor desc, ColumnFamilyDescriptor hcd) throws IOException {
    return createPreSplitLoadTestTable(conf, desc, hcd, DEFAULT_REGIONS_PER_SERVER);
  }

  /**
   * Creates a pre-split table for load testing. If the table already exists,
   * logs a warning and continues.
   * @return the number of regions the table was split into
   */
  public static int createPreSplitLoadTestTable(Configuration conf,
      TableDescriptor desc, ColumnFamilyDescriptor hcd, int numRegionsPerServer) throws IOException {
    return createPreSplitLoadTestTable(conf, desc, new ColumnFamilyDescriptor[] {hcd},
        numRegionsPerServer);
  }

  /**
   * Creates a pre-split table for load testing. If the table already exists,
   * logs a warning and continues.
   * @return the number of regions the table was split into
   */
  public static int createPreSplitLoadTestTable(Configuration conf,
      TableDescriptor desc, ColumnFamilyDescriptor[] hcds,
      int numRegionsPerServer) throws IOException {
    return createPreSplitLoadTestTable(conf, desc, hcds,
      new RegionSplitter.HexStringSplit(), numRegionsPerServer);
  }

  /**
   * Creates a pre-split table for load testing. If the table already exists,
   * logs a warning and continues.
   * @return the number of regions the table was split into
   */
  public static int createPreSplitLoadTestTable(Configuration conf,
      TableDescriptor td, ColumnFamilyDescriptor[] cds,
      SplitAlgorithm splitter, int numRegionsPerServer) throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(td);
    for (ColumnFamilyDescriptor cd : cds) {
      if (!td.hasColumnFamily(cd.getName())) {
        builder.setColumnFamily(cd);
      }
    }
    td = builder.build();
    int totalNumberOfRegions = 0;
    Connection unmanagedConnection = ConnectionFactory.createConnection(conf);
    Admin admin = unmanagedConnection.getAdmin();

    try {
      // create a table a pre-splits regions.
      // The number of splits is set as:
      //    region servers * regions per region server).
      int numberOfServers = admin.getRegionServers().size();
      if (numberOfServers == 0) {
        throw new IllegalStateException("No live regionservers");
      }

      totalNumberOfRegions = numberOfServers * numRegionsPerServer;
      LOG.info("Number of live regionservers: " + numberOfServers + ", " +
          "pre-splitting table into " + totalNumberOfRegions + " regions " +
          "(regions per server: " + numRegionsPerServer + ")");

      byte[][] splits = splitter.split(
          totalNumberOfRegions);

      admin.createTable(td, splits);
    } catch (MasterNotRunningException e) {
      LOG.error("Master not running", e);
      throw new IOException(e);
    } catch (TableExistsException e) {
      LOG.warn("Table " + td.getTableName() +
          " already exists, continuing");
    } finally {
      admin.close();
      unmanagedConnection.close();
    }
    return totalNumberOfRegions;
  }

  public static int getMetaRSPort(Connection connection) throws IOException {
    try (RegionLocator locator = connection.getRegionLocator(TableName.META_TABLE_NAME)) {
      return locator.getRegionLocation(Bytes.toBytes("")).getPort();
    }
  }

  /**
   * Due to async racing issue, a region may not be in the online region list of a region server
   * yet, after the assignment znode is deleted and the new assignment is recorded in master.
   */
  public void assertRegionOnServer(final RegionInfo hri, final ServerName server,
    final long timeout) throws IOException, InterruptedException {
    long timeoutTime = EnvironmentEdgeManager.currentTime() + timeout;
    while (true) {
      List<RegionInfo> regions = getAdmin().getRegions(server);
      if (regions.stream().anyMatch(r -> RegionInfo.COMPARATOR.compare(r, hri) == 0)) return;
      long now = EnvironmentEdgeManager.currentTime();
      if (now > timeoutTime) break;
      Thread.sleep(10);
    }
    throw new AssertionError(
      "Could not find region " + hri.getRegionNameAsString() + " on server " + server);
  }

  /**
   * Check to make sure the region is open on the specified
   * region server, but not on any other one.
   */
  public void assertRegionOnlyOnServer(
      final RegionInfo hri, final ServerName server,
      final long timeout) throws IOException, InterruptedException {
    long timeoutTime = EnvironmentEdgeManager.currentTime() + timeout;
    while (true) {
      List<RegionInfo> regions = getAdmin().getRegions(server);
      if (regions.stream().anyMatch(r -> RegionInfo.COMPARATOR.compare(r, hri) == 0)) {
        List<JVMClusterUtil.RegionServerThread> rsThreads =
          getHBaseCluster().getLiveRegionServerThreads();
        for (JVMClusterUtil.RegionServerThread rsThread: rsThreads) {
          HRegionServer rs = rsThread.getRegionServer();
          if (server.equals(rs.getServerName())) {
            continue;
          }
          Collection<HRegion> hrs = rs.getOnlineRegionsLocalContext();
          for (HRegion r: hrs) {
            if (r.getRegionInfo().getRegionId() == hri.getRegionId()) {
              throw new AssertionError("Region should not be double assigned");
            }
          }
        }
        return; // good, we are happy
      }
      long now = EnvironmentEdgeManager.currentTime();
      if (now > timeoutTime) break;
      Thread.sleep(10);
    }
    throw new AssertionError(
      "Could not find region " + hri.getRegionNameAsString() + " on server " + server);
  }

  public HRegion createTestRegion(String tableName, ColumnFamilyDescriptor cd) throws IOException {
    TableDescriptor td =
        TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName)).setColumnFamily(cd).build();
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(tableName)).build();
    return createRegionAndWAL(info, getDataTestDir(), getConfiguration(), td);
  }

  public HRegion createTestRegion(String tableName, ColumnFamilyDescriptor cd,
      BlockCache blockCache) throws IOException {
    TableDescriptor td =
        TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName)).setColumnFamily(cd).build();
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(tableName)).build();
    return createRegionAndWAL(info, getDataTestDir(), getConfiguration(), td, blockCache);
  }

  public static void setFileSystemURI(String fsURI) {
    FS_URI = fsURI;
  }

  /**
   * Returns a {@link Predicate} for checking that there are no regions in transition in master
   */
  public ExplainingPredicate<IOException> predicateNoRegionsInTransition() {
    return new ExplainingPredicate<IOException>() {
      @Override
      public String explainFailure() throws IOException {
        final RegionStates regionStates = getMiniHBaseCluster().getMaster()
            .getAssignmentManager().getRegionStates();
        return "found in transition: " + regionStates.getRegionsInTransition().toString();
      }

      @Override
      public boolean evaluate() throws IOException {
        HMaster master = getMiniHBaseCluster().getMaster();
        if (master == null) return false;
        AssignmentManager am = master.getAssignmentManager();
        if (am == null) return false;
        return !am.hasRegionsInTransition();
      }
    };
  }

  /**
   * Returns a {@link Predicate} for checking that table is enabled
   */
  public Waiter.Predicate<IOException> predicateTableEnabled(final TableName tableName) {
    return new ExplainingPredicate<IOException>() {
      @Override
      public String explainFailure() throws IOException {
        return explainTableState(tableName, TableState.State.ENABLED);
      }

      @Override
      public boolean evaluate() throws IOException {
        return getAdmin().tableExists(tableName) && getAdmin().isTableEnabled(tableName);
      }
    };
  }

  /**
   * Returns a {@link Predicate} for checking that table is enabled
   */
  public Waiter.Predicate<IOException> predicateTableDisabled(final TableName tableName) {
    return new ExplainingPredicate<IOException>() {
      @Override
      public String explainFailure() throws IOException {
        return explainTableState(tableName, TableState.State.DISABLED);
      }

      @Override
      public boolean evaluate() throws IOException {
        return getAdmin().isTableDisabled(tableName);
      }
    };
  }

  /**
   * Returns a {@link Predicate} for checking that table is enabled
   */
  public Waiter.Predicate<IOException> predicateTableAvailable(final TableName tableName) {
    return new ExplainingPredicate<IOException>() {
      @Override
      public String explainFailure() throws IOException {
        return explainTableAvailability(tableName);
      }

      @Override
      public boolean evaluate() throws IOException {
        boolean tableAvailable = getAdmin().isTableAvailable(tableName);
        if (tableAvailable) {
          try (Table table = getConnection().getTable(tableName)) {
            TableDescriptor htd = table.getDescriptor();
            for (HRegionLocation loc : getConnection().getRegionLocator(tableName)
                .getAllRegionLocations()) {
              Scan scan = new Scan().withStartRow(loc.getRegion().getStartKey())
                  .withStopRow(loc.getRegion().getEndKey()).setOneRowLimit()
                  .setMaxResultsPerColumnFamily(1).setCacheBlocks(false);
              for (byte[] family : htd.getColumnFamilyNames()) {
                scan.addFamily(family);
              }
              try (ResultScanner scanner = table.getScanner(scan)) {
                scanner.next();
              }
            }
          }
        }
        return tableAvailable;
      }
    };
  }

  /**
   * Wait until no regions in transition.
   * @param timeout How long to wait.
   * @throws IOException
   */
  public void waitUntilNoRegionsInTransition(final long timeout) throws IOException {
    waitFor(timeout, predicateNoRegionsInTransition());
  }

  /**
   * Wait until no regions in transition. (time limit 15min)
   * @throws IOException
   */
  public void waitUntilNoRegionsInTransition() throws IOException {
    waitUntilNoRegionsInTransition(15 * 60000);
  }

  /**
   * Wait until labels is ready in VisibilityLabelsCache.
   * @param timeoutMillis
   * @param labels
   */
  public void waitLabelAvailable(long timeoutMillis, final String... labels) {
    final VisibilityLabelsCache labelsCache = VisibilityLabelsCache.get();
    waitFor(timeoutMillis, new Waiter.ExplainingPredicate<RuntimeException>() {

      @Override
      public boolean evaluate() {
        for (String label : labels) {
          if (labelsCache.getLabelOrdinal(label) == 0) {
            return false;
          }
        }
        return true;
      }

      @Override
      public String explainFailure() {
        for (String label : labels) {
          if (labelsCache.getLabelOrdinal(label) == 0) {
            return label + " is not available yet";
          }
        }
        return "";
      }
    });
  }

  /**
   * Create a set of column descriptors with the combination of compression,
   * encoding, bloom codecs available.
   * @return the list of column descriptors
   */
  public static List<ColumnFamilyDescriptor> generateColumnDescriptors() {
    return generateColumnDescriptors("");
  }

  /**
   * Create a set of column descriptors with the combination of compression,
   * encoding, bloom codecs available.
   * @param prefix family names prefix
   * @return the list of column descriptors
   */
  public static List<ColumnFamilyDescriptor> generateColumnDescriptors(final String prefix) {
    List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
    long familyId = 0;
    for (Compression.Algorithm compressionType: getSupportedCompressionAlgorithms()) {
      for (DataBlockEncoding encodingType: DataBlockEncoding.values()) {
        for (BloomType bloomType: BloomType.values()) {
          String name = String.format("%s-cf-!@#&-%d!@#", prefix, familyId);
          ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
            ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(name));
          columnFamilyDescriptorBuilder.setCompressionType(compressionType);
          columnFamilyDescriptorBuilder.setDataBlockEncoding(encodingType);
          columnFamilyDescriptorBuilder.setBloomFilterType(bloomType);
          columnFamilyDescriptors.add(columnFamilyDescriptorBuilder.build());
          familyId++;
        }
      }
    }
    return columnFamilyDescriptors;
  }

  /**
   * Get supported compression algorithms.
   * @return supported compression algorithms.
   */
  public static Compression.Algorithm[] getSupportedCompressionAlgorithms() {
    String[] allAlgos = HFile.getSupportedCompressionAlgorithms();
    List<Compression.Algorithm> supportedAlgos = new ArrayList<>();
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

  public Result getClosestRowBefore(Region r, byte[] row, byte[] family) throws IOException {
    Scan scan = new Scan().withStartRow(row);
    scan.setReadType(ReadType.PREAD);
    scan.setCaching(1);
    scan.setReversed(true);
    scan.addFamily(family);
    try (RegionScanner scanner = r.getScanner(scan)) {
      List<Cell> cells = new ArrayList<>(1);
      scanner.next(cells);
      if (r.getRegionInfo().isMetaRegion() && !isTargetTable(row, cells.get(0))) {
        return null;
      }
      return Result.create(cells);
    }
  }

  private boolean isTargetTable(final byte[] inRow, Cell c) {
    String inputRowString = Bytes.toString(inRow);
    int i = inputRowString.indexOf(HConstants.DELIMITER);
    String outputRowString = Bytes.toString(c.getRowArray(), c.getRowOffset(), c.getRowLength());
    int o = outputRowString.indexOf(HConstants.DELIMITER);
    return inputRowString.substring(0, i).equals(outputRowString.substring(0, o));
  }

  /**
   * Sets up {@link MiniKdc} for testing security.
   * Uses {@link HBaseKerberosUtils} to set the given keytab file as
   * {@link HBaseKerberosUtils#KRB_KEYTAB_FILE}.
   * FYI, there is also the easier-to-use kerby KDC server and utility for using it,
   * {@link org.apache.hadoop.hbase.util.SimpleKdcServerUtil}. The kerby KDC server is preferred;
   * less baggage. It came in in HBASE-5291.
   */
  public MiniKdc setupMiniKdc(File keytabFile) throws Exception {
    Properties conf = MiniKdc.createConf();
    conf.put(MiniKdc.DEBUG, true);
    MiniKdc kdc = null;
    File dir = null;
    // There is time lag between selecting a port and trying to bind with it. It's possible that
    // another service captures the port in between which'll result in BindException.
    boolean bindException;
    int numTries = 0;
    do {
      try {
        bindException = false;
        dir = new File(getDataTestDir("kdc").toUri().getPath());
        kdc = new MiniKdc(conf, dir);
        kdc.start();
      } catch (BindException e) {
        FileUtils.deleteDirectory(dir);  // clean directory
        numTries++;
        if (numTries == 3) {
          LOG.error("Failed setting up MiniKDC. Tried " + numTries + " times.");
          throw e;
        }
        LOG.error("BindException encountered when setting up MiniKdc. Trying again.");
        bindException = true;
      }
    } while (bindException);
    HBaseKerberosUtils.setKeytabFileForTesting(keytabFile.getAbsolutePath());
    return kdc;
  }

  public int getNumHFiles(final TableName tableName, final byte[] family) {
    int numHFiles = 0;
    for (RegionServerThread regionServerThread : getMiniHBaseCluster().getRegionServerThreads()) {
      numHFiles+= getNumHFilesForRS(regionServerThread.getRegionServer(), tableName,
                                    family);
    }
    return numHFiles;
  }

  public int getNumHFilesForRS(final HRegionServer rs, final TableName tableName,
                               final byte[] family) {
    int numHFiles = 0;
    for (Region region : rs.getRegions(tableName)) {
      numHFiles += region.getStore(family).getStorefilesCount();
    }
    return numHFiles;
  }

  private void assertEquals(String message, int expected, int actual) {
    if (expected == actual) {
      return;
    }
    String formatted = "";
    if (message != null && !"".equals(message)) {
      formatted = message + " ";
    }
    throw new AssertionError(formatted + "expected:<" + expected + "> but was:<" + actual + ">");
  }

  public void verifyTableDescriptorIgnoreTableName(TableDescriptor ltd, TableDescriptor rtd) {
    if (ltd.getValues().hashCode() != rtd.getValues().hashCode()) {
      throw new AssertionError();
    }
    assertEquals("", ltd.getValues().hashCode(), rtd.getValues().hashCode());
    Collection<ColumnFamilyDescriptor> ltdFamilies = Arrays.asList(ltd.getColumnFamilies());
    Collection<ColumnFamilyDescriptor> rtdFamilies = Arrays.asList(rtd.getColumnFamilies());
    assertEquals("", ltdFamilies.size(), rtdFamilies.size());
    for (Iterator<ColumnFamilyDescriptor> it = ltdFamilies.iterator(),
      it2 = rtdFamilies.iterator(); it.hasNext();) {
      assertEquals("", 0, ColumnFamilyDescriptor.COMPARATOR.compare(it.next(), it2.next()));
    }
  }

  /**
   * Await the successful return of {@code condition}, sleeping {@code sleepMillis} between
   * invocations.
   */
  public static void await(final long sleepMillis, final BooleanSupplier condition)
    throws InterruptedException {
    try {
      while (!condition.getAsBoolean()) {
        Thread.sleep(sleepMillis);
      }
    } catch (RuntimeException e) {
      if (e.getCause() instanceof AssertionError) {
        throw (AssertionError) e.getCause();
      }
      throw e;
    }
  }
}
