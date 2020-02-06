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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.CoprocessorDescriptor;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.TestServerCustomProtocol;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassLoaderTestHelper;
import org.apache.hadoop.hbase.util.CoprocessorClassLoader;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test coprocessors class loading.
 */
@Category({CoprocessorTests.class, MediumTests.class})
public class TestClassLoading {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestClassLoading.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestClassLoading.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  public static class TestMasterCoprocessor implements MasterCoprocessor, MasterObserver {
    @Override
    public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }
  }

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
  private static Class<?> regionServerCoprocessor = SampleRegionWALCoprocessor.class;
  private static Class<?> masterCoprocessor = TestMasterCoprocessor.class;

  private static final String[] regionServerSystemCoprocessors =
      new String[]{ regionServerCoprocessor.getSimpleName() };

  private static final String[] masterRegionServerSystemCoprocessors = new String[] {
      regionCoprocessor1.getSimpleName(), MultiRowMutationEndpoint.class.getSimpleName(),
      regionServerCoprocessor.getSimpleName() };

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
    String code =
        "import org.apache.hadoop.hbase.coprocessor.*;" +
            "public class " + className + " implements RegionCoprocessor {}";
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
    TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
    tdb.setColumnFamily(ColumnFamilyDescriptorBuilder
      .newBuilder(Bytes.toBytes("test")).build());
    // without configuration values
    tdb.setValue("COPROCESSOR$1", jarFileOnHDFS1 + "|" + cpName1
      + "|" + Coprocessor.PRIORITY_USER);
    // with configuration values
    tdb.setValue("COPROCESSOR$2", jarFileOnHDFS2 + "|" + cpName2
      + "|" + Coprocessor.PRIORITY_USER + "|k1=v1,k2=v2,k3=v3");
    Admin admin = TEST_UTIL.getAdmin();
    if (admin.tableExists(tableName)) {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
      }
      admin.deleteTable(tableName);
    }
    CoprocessorClassLoader.clearCache();
    byte[] startKey = {10, 63};
    byte[] endKey = {12, 43};
    TableDescriptor tableDescriptor = tdb.build();
    admin.createTable(tableDescriptor, startKey, endKey, 4);
    waitForTable(tableDescriptor.getTableName());

    // verify that the coprocessors were loaded
    boolean foundTableRegion=false;
    boolean found1 = true, found2 = true, found2_k1 = true, found2_k2 = true, found2_k3 = true;
    Map<Region, Set<ClassLoader>> regionsActiveClassLoaders = new HashMap<>();
    MiniHBaseCluster hbase = TEST_UTIL.getHBaseCluster();
    for (HRegion region:
        hbase.getRegionServer(0).getOnlineRegionsLocalContext()) {
      if (region.getRegionInfo().getRegionNameAsString().startsWith(tableName.getNameAsString())) {
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
          found2_k1 = false;
          found2_k2 = false;
          found2_k3 = false;
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
    Set<ClassLoader> externalClassLoaders = new HashSet<>(
      CoprocessorClassLoader.getAllCached());
    for (Map.Entry<Region, Set<ClassLoader>> regionCP : regionsActiveClassLoaders.entrySet()) {
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
    TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(TableName.valueOf(cpName3));
    tdb.setColumnFamily(ColumnFamilyDescriptorBuilder
      .newBuilder(Bytes.toBytes("test")).build());
    tdb.setValue("COPROCESSOR$1", getLocalPath(jarFile) + "|" + cpName3 + "|" +
      Coprocessor.PRIORITY_USER);
    TableDescriptor tableDescriptor = tdb.build();
    Admin admin = TEST_UTIL.getAdmin();
    admin.createTable(tableDescriptor);
    waitForTable(tableDescriptor.getTableName());

    // verify that the coprocessor was loaded
    boolean found = false;
    MiniHBaseCluster hbase = TEST_UTIL.getHBaseCluster();
    for (HRegion region: hbase.getRegionServer(0).getOnlineRegionsLocalContext()) {
      if (region.getRegionInfo().getRegionNameAsString().startsWith(cpName3)) {
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
    TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(TableName.valueOf(cpName4));
    tdb.setColumnFamily(ColumnFamilyDescriptorBuilder
      .newBuilder(Bytes.toBytes("test")).build());
    tdb.setValue("COPROCESSOR$1", getLocalPath(jarFile) + "|" + cpName4 + "|" +
      Coprocessor.PRIORITY_USER);
    TableDescriptor tableDescriptor = tdb.build();
    Admin admin = TEST_UTIL.getAdmin();
    admin.createTable(tableDescriptor);
    waitForTable(tableDescriptor.getTableName());

    // verify that the coprocessor was loaded correctly
    boolean found = false;
    MiniHBaseCluster hbase = TEST_UTIL.getHBaseCluster();
    for (HRegion region: hbase.getRegionServer(0).getOnlineRegionsLocalContext()) {
      if (region.getRegionInfo().getRegionNameAsString().startsWith(cpName4)) {
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
    TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
    tdb.setColumnFamily(ColumnFamilyDescriptorBuilder
      .newBuilder(Bytes.toBytes("test")).build());

    // add 3 coprocessors by setting htd attributes directly.
    tdb.setValue(cpKey1, cpValue1);
    tdb.setValue(cpKey2, cpValue2);
    tdb.setValue(cpKey3, cpValue3);

    // add 2 coprocessor by using new htd.setCoprocessor() api
    CoprocessorDescriptor coprocessorDescriptor = CoprocessorDescriptorBuilder
      .newBuilder(cpName5)
      .setJarPath(new Path(getLocalPath(jarFile5)).toString())
      .setPriority(Coprocessor.PRIORITY_USER)
      .setProperties(Collections.emptyMap())
      .build();
    tdb.setCoprocessor(coprocessorDescriptor);
    Map<String, String> kvs = new HashMap<>();
    kvs.put("k1", "v1");
    kvs.put("k2", "v2");
    kvs.put("k3", "v3");

    coprocessorDescriptor = CoprocessorDescriptorBuilder
      .newBuilder(cpName6)
      .setJarPath(new Path(getLocalPath(jarFile6)).toString())
      .setPriority(Coprocessor.PRIORITY_USER)
      .setProperties(kvs)
      .build();
    tdb.setCoprocessor(coprocessorDescriptor);

    Admin admin = TEST_UTIL.getAdmin();
    if (admin.tableExists(tableName)) {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
      }
      admin.deleteTable(tableName);
    }

    TableDescriptor tableDescriptor = tdb.build();
    admin.createTable(tableDescriptor);
    waitForTable(tableDescriptor.getTableName());

    // verify that the coprocessor was loaded
    boolean found_2 = false, found_1 = false, found_3 = false,
        found_5 = false, found_6 = false;
    boolean found6_k1 = false, found6_k2 = false, found6_k3 = false,
        found6_k4 = false;

    MiniHBaseCluster hbase = TEST_UTIL.getHBaseCluster();
    for (HRegion region: hbase.getRegionServer(0).getOnlineRegionsLocalContext()) {
      if (region.getRegionInfo().getRegionNameAsString().startsWith(tableName.getNameAsString())) {
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
    TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
    tdb.setColumnFamily(ColumnFamilyDescriptorBuilder
      .newBuilder(Bytes.toBytes("test")).build());
      // without configuration values
    tdb.setValue("COPROCESSOR$1", jarFileOnHDFS + "|" + cpName1
      + "|" + Coprocessor.PRIORITY_USER);
      // with configuration values
    tdb.setValue("COPROCESSOR$2", jarFileOnHDFS + "|" + cpName2
      + "|" + Coprocessor.PRIORITY_USER + "|k1=v1,k2=v2,k3=v3");
    Admin admin = TEST_UTIL.getAdmin();
    if (admin.tableExists(tableName)) {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
      }
      admin.deleteTable(tableName);
    }

    TableDescriptor tableDescriptor = tdb.build();
    admin.createTable(tableDescriptor);
    waitForTable(tableDescriptor.getTableName());

    // verify that the coprocessors were loaded
    boolean found1 = false, found2 = false, found2_k1 = false,
        found2_k2 = false, found2_k3 = false;
    MiniHBaseCluster hbase = TEST_UTIL.getHBaseCluster();
    for (HRegion region: hbase.getRegionServer(0).getOnlineRegionsLocalContext()) {
      if (region.getRegionInfo().getRegionNameAsString().startsWith(tableName.getNameAsString())) {
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
    assertAllRegionServers(null);
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
  Map<ServerName, ServerMetrics> serversForTable(String tableName) {
    Map<ServerName, ServerMetrics> serverLoadHashMap = new HashMap<>();
    for(Map.Entry<ServerName, ServerMetrics> server:
        TEST_UTIL.getMiniHBaseCluster().getMaster().getServerManager().
            getOnlineServers().entrySet()) {
      for(Map.Entry<byte[], RegionMetrics> region:
          server.getValue().getRegionMetrics().entrySet()) {
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

  void assertAllRegionServers(String tableName) throws InterruptedException {
    Map<ServerName, ServerMetrics> servers;
    boolean success = false;
    String[] expectedCoprocessors = regionServerSystemCoprocessors;
    if (tableName == null) {
      // if no tableName specified, use all servers.
      servers = TEST_UTIL.getMiniHBaseCluster().getMaster().getServerManager().getOnlineServers();
    } else {
      servers = serversForTable(tableName);
    }
    for (int i = 0; i < 5; i++) {
      boolean any_failed = false;
      for(Map.Entry<ServerName, ServerMetrics> server: servers.entrySet()) {
        String[] actualCoprocessors =
          server.getValue().getCoprocessorNames().stream().toArray(size -> new String[size]);
        if (!Arrays.equals(actualCoprocessors, expectedCoprocessors)) {
          LOG.debug("failed comparison: actual: " +
              Arrays.toString(actualCoprocessors) +
              " ; expected: " + Arrays.toString(expectedCoprocessors));
          any_failed = true;
          expectedCoprocessors = switchExpectedCoprocessors(expectedCoprocessors);
          break;
        }
        expectedCoprocessors = switchExpectedCoprocessors(expectedCoprocessors);
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

  private String[] switchExpectedCoprocessors(String[] expectedCoprocessors) {
    if (Arrays.equals(regionServerSystemCoprocessors, expectedCoprocessors)) {
      expectedCoprocessors = masterRegionServerSystemCoprocessors;
    } else {
      expectedCoprocessors = regionServerSystemCoprocessors;
    }
    return expectedCoprocessors;
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
