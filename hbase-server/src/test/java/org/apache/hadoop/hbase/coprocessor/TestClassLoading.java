/*
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
package org.apache.hadoop.hbase.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.TestServerCustomProtocol;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.ClassLoaderTestHelper;
import org.apache.hadoop.hbase.util.CoprocessorClassLoader;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.RegionLoad;

import java.io.*;
import java.util.*;

import org.junit.*;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * Test coprocessors class loading.
 */
@Category({CoprocessorTests.class, MediumTests.class})
public class TestClassLoading {
  private static final Log LOG = LogFactory.getLog(TestClassLoading.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static MiniDFSCluster cluster;

  static final TableName tableName = TableName.valueOf("TestClassLoading");
  static final String cpName1 = "TestCP1";
  static final String cpName2 = "TestCP2";
  static final String cpName3 = "TestCP3";
  static final String cpName4 = "TestCP4";
  static final String cpName5 = "TestCP5";
  static final String cpName6 = "TestCP6";

  private static Class<?> regionCoprocessor1 = ColumnAggregationEndpoint.class;
  // TOOD: Fix the import of this handler.  It is coming in from a package that is far away.
  private static Class<?> regionCoprocessor2 = TestServerCustomProtocol.PingHandler.class;
  private static Class<?> regionServerCoprocessor = SampleRegionWALObserver.class;
  private static Class<?> masterCoprocessor = BaseMasterObserver.class;

  private static final String[] regionServerSystemCoprocessors =
      new String[]{
      regionServerCoprocessor.getSimpleName()
  };

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();

    // regionCoprocessor1 will be loaded on all regionservers, since it is
    // loaded for any tables (user or meta).
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        regionCoprocessor1.getName());

    // regionCoprocessor2 will be loaded only on regionservers that serve a
    // user table region. Therefore, if there are no user tables loaded,
    // this coprocessor will not be loaded on any regionserver.
    conf.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        regionCoprocessor2.getName());

    conf.setStrings(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY,
        regionServerCoprocessor.getName());
    conf.setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        masterCoprocessor.getName());
    TEST_UTIL.startMiniCluster(1);
    cluster = TEST_UTIL.getDFSCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  static File buildCoprocessorJar(String className) throws Exception {
    String code = "import org.apache.hadoop.hbase.coprocessor.*;" +
      "public class " + className + " extends BaseRegionObserver {}";
    return ClassLoaderTestHelper.buildJar(
      TEST_UTIL.getDataTestDir().toString(), className, code);
  }

  @Test
  // HBASE-3516: Test CP Class loading from HDFS
  public void testClassLoadingFromHDFS() throws Exception {
    FileSystem fs = cluster.getFileSystem();

    File jarFile1 = buildCoprocessorJar(cpName1);
    File jarFile2 = buildCoprocessorJar(cpName2);

    // copy the jars into dfs
    fs.copyFromLocalFile(new Path(jarFile1.getPath()),
      new Path(fs.getUri().toString() + Path.SEPARATOR));
    String jarFileOnHDFS1 = fs.getUri().toString() + Path.SEPARATOR +
      jarFile1.getName();
    Path pathOnHDFS1 = new Path(jarFileOnHDFS1);
    assertTrue("Copy jar file to HDFS failed.",
      fs.exists(pathOnHDFS1));
    LOG.info("Copied jar file to HDFS: " + jarFileOnHDFS1);

    fs.copyFromLocalFile(new Path(jarFile2.getPath()),
        new Path(fs.getUri().toString() + Path.SEPARATOR));
    String jarFileOnHDFS2 = fs.getUri().toString() + Path.SEPARATOR +
      jarFile2.getName();
    Path pathOnHDFS2 = new Path(jarFileOnHDFS2);
    assertTrue("Copy jar file to HDFS failed.",
      fs.exists(pathOnHDFS2));
    LOG.info("Copied jar file to HDFS: " + jarFileOnHDFS2);

    // create a table that references the coprocessors
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor("test"));
      // without configuration values
    htd.setValue("COPROCESSOR$1", jarFileOnHDFS1.toString() + "|" + cpName1 +
      "|" + Coprocessor.PRIORITY_USER);
      // with configuration values
    htd.setValue("COPROCESSOR$2", jarFileOnHDFS2.toString() + "|" + cpName2 +
      "|" + Coprocessor.PRIORITY_USER + "|k1=v1,k2=v2,k3=v3");
    Admin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(tableName)) {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
      }
      admin.deleteTable(tableName);
    }
    CoprocessorClassLoader.clearCache();
    byte[] startKey = {10, 63};
    byte[] endKey = {12, 43};
    admin.createTable(htd, startKey, endKey, 4);
    waitForTable(htd.getTableName());

    // verify that the coprocessors were loaded
    boolean foundTableRegion=false;
    boolean found1 = true, found2 = true, found2_k1 = true, found2_k2 = true, found2_k3 = true;
    Map<HRegion, Set<ClassLoader>> regionsActiveClassLoaders =
        new HashMap<HRegion, Set<ClassLoader>>();
    MiniHBaseCluster hbase = TEST_UTIL.getHBaseCluster();
    for (HRegion region:
        hbase.getRegionServer(0).getOnlineRegionsLocalContext()) {
      if (region.getRegionNameAsString().startsWith(tableName.getNameAsString())) {
        foundTableRegion = true;
        CoprocessorEnvironment env;
        env = region.getCoprocessorHost().findCoprocessorEnvironment(cpName1);
        found1 = found1 && (env != null);
        env = region.getCoprocessorHost().findCoprocessorEnvironment(cpName2);
        found2 = found2 && (env != null);
        if (env != null) {
          Configuration conf = env.getConfiguration();
          found2_k1 = found2_k1 && (conf.get("k1") != null);
          found2_k2 = found2_k2 && (conf.get("k2") != null);
          found2_k3 = found2_k3 && (conf.get("k3") != null);
        } else {
          found2_k1 = found2_k2 = found2_k3 = false;
        }
        regionsActiveClassLoaders
            .put(region, ((CoprocessorHost) region.getCoprocessorHost()).getExternalClassLoaders());
      }
    }

    assertTrue("No region was found for table " + tableName, foundTableRegion);
    assertTrue("Class " + cpName1 + " was missing on a region", found1);
    assertTrue("Class " + cpName2 + " was missing on a region", found2);
    assertTrue("Configuration key 'k1' was missing on a region", found2_k1);
    assertTrue("Configuration key 'k2' was missing on a region", found2_k2);
    assertTrue("Configuration key 'k3' was missing on a region", found2_k3);
    // check if CP classloaders are cached
    assertNotNull(jarFileOnHDFS1 + " was not cached",
      CoprocessorClassLoader.getIfCached(pathOnHDFS1));
    assertNotNull(jarFileOnHDFS2 + " was not cached",
      CoprocessorClassLoader.getIfCached(pathOnHDFS2));
    //two external jar used, should be one classloader per jar
    assertEquals("The number of cached classloaders should be equal to the number" +
      " of external jar files",
      2, CoprocessorClassLoader.getAllCached().size());
    //check if region active classloaders are shared across all RS regions
    Set<ClassLoader> externalClassLoaders = new HashSet<ClassLoader>(
      CoprocessorClassLoader.getAllCached());
    for (Map.Entry<HRegion, Set<ClassLoader>> regionCP : regionsActiveClassLoaders.entrySet()) {
      assertTrue("Some CP classloaders for region " + regionCP.getKey() + " are not cached."
        + " ClassLoader Cache:" + externalClassLoaders
        + " Region ClassLoaders:" + regionCP.getValue(),
        externalClassLoaders.containsAll(regionCP.getValue()));
    }
  }

  private String getLocalPath(File file) {
    return new Path(file.toURI()).toString();
  }

  @Test
  // HBASE-3516: Test CP Class loading from local file system
  public void testClassLoadingFromLocalFS() throws Exception {
    File jarFile = buildCoprocessorJar(cpName3);

    // create a table that references the jar
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(cpName3));
    htd.addFamily(new HColumnDescriptor("test"));
    htd.setValue("COPROCESSOR$1", getLocalPath(jarFile) + "|" + cpName3 + "|" +
      Coprocessor.PRIORITY_USER);
    Admin admin = TEST_UTIL.getHBaseAdmin();
    admin.createTable(htd);
    waitForTable(htd.getTableName());

    // verify that the coprocessor was loaded
    boolean found = false;
    MiniHBaseCluster hbase = TEST_UTIL.getHBaseCluster();
    for (HRegion region:
        hbase.getRegionServer(0).getOnlineRegionsLocalContext()) {
      if (region.getRegionNameAsString().startsWith(cpName3)) {
        found = (region.getCoprocessorHost().findCoprocessor(cpName3) != null);
      }
    }
    assertTrue("Class " + cpName3 + " was missing on a region", found);
  }

  @Test
  // HBASE-6308: Test CP classloader is the CoprocessorClassLoader
  public void testPrivateClassLoader() throws Exception {
    File jarFile = buildCoprocessorJar(cpName4);

    // create a table that references the jar
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(cpName4));
    htd.addFamily(new HColumnDescriptor("test"));
    htd.setValue("COPROCESSOR$1", getLocalPath(jarFile) + "|" + cpName4 + "|" +
      Coprocessor.PRIORITY_USER);
    Admin admin = TEST_UTIL.getHBaseAdmin();
    admin.createTable(htd);
    waitForTable(htd.getTableName());

    // verify that the coprocessor was loaded correctly
    boolean found = false;
    MiniHBaseCluster hbase = TEST_UTIL.getHBaseCluster();
    for (HRegion region:
        hbase.getRegionServer(0).getOnlineRegionsLocalContext()) {
      if (region.getRegionNameAsString().startsWith(cpName4)) {
        Coprocessor cp = region.getCoprocessorHost().findCoprocessor(cpName4);
        if (cp != null) {
          found = true;
          assertEquals("Class " + cpName4 + " was not loaded by CoprocessorClassLoader",
            cp.getClass().getClassLoader().getClass(), CoprocessorClassLoader.class);
        }
      }
    }
    assertTrue("Class " + cpName4 + " was missing on a region", found);
  }

  @Test
  // HBase-3810: Registering a Coprocessor at HTableDescriptor should be
  // less strict
  public void testHBase3810() throws Exception {
    // allowed value pattern: [path] | class name | [priority] | [key values]

    File jarFile1 = buildCoprocessorJar(cpName1);
    File jarFile2 = buildCoprocessorJar(cpName2);
    File jarFile5 = buildCoprocessorJar(cpName5);
    File jarFile6 = buildCoprocessorJar(cpName6);

    String cpKey1 = "COPROCESSOR$1";
    String cpKey2 = " Coprocessor$2 ";
    String cpKey3 = " coprocessor$03 ";

    String cpValue1 = getLocalPath(jarFile1) + "|" + cpName1 + "|" +
        Coprocessor.PRIORITY_USER;
    String cpValue2 = getLocalPath(jarFile2) + " | " + cpName2 + " | ";
    // load from default class loader
    String cpValue3 =
        " | org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver | | k=v ";

    // create a table that references the jar
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor("test"));

    // add 3 coprocessors by setting htd attributes directly.
    htd.setValue(cpKey1, cpValue1);
    htd.setValue(cpKey2, cpValue2);
    htd.setValue(cpKey3, cpValue3);

    // add 2 coprocessor by using new htd.addCoprocessor() api
    htd.addCoprocessor(cpName5, new Path(getLocalPath(jarFile5)),
        Coprocessor.PRIORITY_USER, null);
    Map<String, String> kvs = new HashMap<String, String>();
    kvs.put("k1", "v1");
    kvs.put("k2", "v2");
    kvs.put("k3", "v3");
    htd.addCoprocessor(cpName6, new Path(getLocalPath(jarFile6)),
        Coprocessor.PRIORITY_USER, kvs);

    Admin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(tableName)) {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
      }
      admin.deleteTable(tableName);
    }
    admin.createTable(htd);
    waitForTable(htd.getTableName());

    // verify that the coprocessor was loaded
    boolean found_2 = false, found_1 = false, found_3 = false,
        found_5 = false, found_6 = false;
    boolean found6_k1 = false, found6_k2 = false, found6_k3 = false,
        found6_k4 = false;

    MiniHBaseCluster hbase = TEST_UTIL.getHBaseCluster();
    for (HRegion region:
        hbase.getRegionServer(0).getOnlineRegionsLocalContext()) {
      if (region.getRegionNameAsString().startsWith(tableName.getNameAsString())) {
        found_1 = found_1 ||
            (region.getCoprocessorHost().findCoprocessor(cpName1) != null);
        found_2 = found_2 ||
            (region.getCoprocessorHost().findCoprocessor(cpName2) != null);
        found_3 = found_3 ||
            (region.getCoprocessorHost().findCoprocessor("SimpleRegionObserver")
                != null);
        found_5 = found_5 ||
            (region.getCoprocessorHost().findCoprocessor(cpName5) != null);

        CoprocessorEnvironment env =
            region.getCoprocessorHost().findCoprocessorEnvironment(cpName6);
        if (env != null) {
          found_6 = true;
          Configuration conf = env.getConfiguration();
          found6_k1 = conf.get("k1") != null;
          found6_k2 = conf.get("k2") != null;
          found6_k3 = conf.get("k3") != null;
        }
      }
    }

    assertTrue("Class " + cpName1 + " was missing on a region", found_1);
    assertTrue("Class " + cpName2 + " was missing on a region", found_2);
    assertTrue("Class SimpleRegionObserver was missing on a region", found_3);
    assertTrue("Class " + cpName5 + " was missing on a region", found_5);
    assertTrue("Class " + cpName6 + " was missing on a region", found_6);

    assertTrue("Configuration key 'k1' was missing on a region", found6_k1);
    assertTrue("Configuration key 'k2' was missing on a region", found6_k2);
    assertTrue("Configuration key 'k3' was missing on a region", found6_k3);
    assertFalse("Configuration key 'k4' wasn't configured", found6_k4);
  }

  @Test
  public void testClassLoadingFromLibDirInJar() throws Exception {
    loadingClassFromLibDirInJar("/lib/");
  }

  @Test
  public void testClassLoadingFromRelativeLibDirInJar() throws Exception {
    loadingClassFromLibDirInJar("lib/");
  }

  void loadingClassFromLibDirInJar(String libPrefix) throws Exception {
    FileSystem fs = cluster.getFileSystem();

    File innerJarFile1 = buildCoprocessorJar(cpName1);
    File innerJarFile2 = buildCoprocessorJar(cpName2);
    File outerJarFile = new File(TEST_UTIL.getDataTestDir().toString(), "outer.jar");

    ClassLoaderTestHelper.addJarFilesToJar(
      outerJarFile, libPrefix, innerJarFile1, innerJarFile2);

    // copy the jars into dfs
    fs.copyFromLocalFile(new Path(outerJarFile.getPath()),
      new Path(fs.getUri().toString() + Path.SEPARATOR));
    String jarFileOnHDFS = fs.getUri().toString() + Path.SEPARATOR +
      outerJarFile.getName();
    assertTrue("Copy jar file to HDFS failed.",
      fs.exists(new Path(jarFileOnHDFS)));
    LOG.info("Copied jar file to HDFS: " + jarFileOnHDFS);

    // create a table that references the coprocessors
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor("test"));
      // without configuration values
    htd.setValue("COPROCESSOR$1", jarFileOnHDFS.toString() + "|" + cpName1 +
      "|" + Coprocessor.PRIORITY_USER);
      // with configuration values
    htd.setValue("COPROCESSOR$2", jarFileOnHDFS.toString() + "|" + cpName2 +
      "|" + Coprocessor.PRIORITY_USER + "|k1=v1,k2=v2,k3=v3");
    Admin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(tableName)) {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
      }
      admin.deleteTable(tableName);
    }
    admin.createTable(htd);
    waitForTable(htd.getTableName());

    // verify that the coprocessors were loaded
    boolean found1 = false, found2 = false, found2_k1 = false,
        found2_k2 = false, found2_k3 = false;
    MiniHBaseCluster hbase = TEST_UTIL.getHBaseCluster();
    for (HRegion region:
        hbase.getRegionServer(0).getOnlineRegionsLocalContext()) {
      if (region.getRegionNameAsString().startsWith(tableName.getNameAsString())) {
        CoprocessorEnvironment env;
        env = region.getCoprocessorHost().findCoprocessorEnvironment(cpName1);
        if (env != null) {
          found1 = true;
        }
        env = region.getCoprocessorHost().findCoprocessorEnvironment(cpName2);
        if (env != null) {
          found2 = true;
          Configuration conf = env.getConfiguration();
          found2_k1 = conf.get("k1") != null;
          found2_k2 = conf.get("k2") != null;
          found2_k3 = conf.get("k3") != null;
        }
      }
    }
    assertTrue("Class " + cpName1 + " was missing on a region", found1);
    assertTrue("Class " + cpName2 + " was missing on a region", found2);
    assertTrue("Configuration key 'k1' was missing on a region", found2_k1);
    assertTrue("Configuration key 'k2' was missing on a region", found2_k2);
    assertTrue("Configuration key 'k3' was missing on a region", found2_k3);
  }

  @Test
  public void testRegionServerCoprocessorsReported() throws Exception {
    // This was a test for HBASE-4070.
    // We are removing coprocessors from region load in HBASE-5258.
    // Therefore, this test now only checks system coprocessors.
    assertAllRegionServers(regionServerSystemCoprocessors,null);
  }

  /**
   * return the subset of all regionservers
   * (actually returns set of ServerLoads)
   * which host some region in a given table.
   * used by assertAllRegionServers() below to
   * test reporting of loaded coprocessors.
   * @param tableName : given table.
   * @return subset of all servers.
   */
  Map<ServerName, ServerLoad> serversForTable(String tableName) {
    Map<ServerName, ServerLoad> serverLoadHashMap =
        new HashMap<ServerName, ServerLoad>();
    for(Map.Entry<ServerName,ServerLoad> server:
        TEST_UTIL.getMiniHBaseCluster().getMaster().getServerManager().
            getOnlineServers().entrySet()) {
      for( Map.Entry<byte[], RegionLoad> region:
          server.getValue().getRegionsLoad().entrySet()) {
        if (region.getValue().getNameAsString().equals(tableName)) {
          // this server hosts a region of tableName: add this server..
          serverLoadHashMap.put(server.getKey(),server.getValue());
          // .. and skip the rest of the regions that it hosts.
          break;
        }
      }
    }
    return serverLoadHashMap;
  }

  void assertAllRegionServers(String[] expectedCoprocessors, String tableName)
      throws InterruptedException {
    Map<ServerName, ServerLoad> servers;
    String[] actualCoprocessors = null;
    boolean success = false;
    for(int i = 0; i < 5; i++) {
      if (tableName == null) {
        //if no tableName specified, use all servers.
        servers =
            TEST_UTIL.getMiniHBaseCluster().getMaster().getServerManager().
                getOnlineServers();
      } else {
        servers = serversForTable(tableName);
      }
      boolean any_failed = false;
      for(Map.Entry<ServerName,ServerLoad> server: servers.entrySet()) {
        actualCoprocessors = server.getValue().getRsCoprocessors();
        if (!Arrays.equals(actualCoprocessors, expectedCoprocessors)) {
          LOG.debug("failed comparison: actual: " +
              Arrays.toString(actualCoprocessors) +
              " ; expected: " + Arrays.toString(expectedCoprocessors));
          any_failed = true;
          break;
        }
      }
      if (any_failed == false) {
        success = true;
        break;
      }
      LOG.debug("retrying after failed comparison: " + i);
      Thread.sleep(1000);
    }
    assertTrue(success);
  }

  @Test
  public void testMasterCoprocessorsReported() {
    // HBASE 4070: Improve region server metrics to report loaded coprocessors
    // to master: verify that the master is reporting the correct set of
    // loaded coprocessors.
    final String loadedMasterCoprocessorsVerify =
        "[" + masterCoprocessor.getSimpleName() + "]";
    String loadedMasterCoprocessors =
        java.util.Arrays.toString(
            TEST_UTIL.getHBaseCluster().getMaster().getMasterCoprocessors());
    assertEquals(loadedMasterCoprocessorsVerify, loadedMasterCoprocessors);
  }

  private void waitForTable(TableName name) throws InterruptedException, IOException {
    // First wait until all regions are online
    TEST_UTIL.waitTableEnabled(name);
    // Now wait a bit longer for the coprocessor hosts to load the CPs
    Thread.sleep(1000);
  }

}

