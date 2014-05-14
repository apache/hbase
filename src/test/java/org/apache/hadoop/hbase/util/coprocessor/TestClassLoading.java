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
package org.apache.hadoop.hbase.util.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.observers.TestHRegionObserverBypassCoprocessor.TestCoprocessor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
* Test coprocessors class loading.
*/
public class TestClassLoading {
 private static final Log LOG = LogFactory.getLog(TestClassLoading.class);
 private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
 private static final byte[] DUMMY = Bytes.toBytes("dummy");
 private static final byte[] TEST = Bytes.toBytes("test");

 private static MiniDFSCluster cluster;

 static final String tableName = "TestClassLoading";
 static final String cpName1 = "TestCP1";
 static final String cpName2 = "TestCP2";

 private static Class<?> testCoprocessor = TestCoprocessor.class;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // load TestCoprocessor in the beginning
    conf.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        testCoprocessor.getName());
    TEST_UTIL.startMiniCluster(1);
    cluster = TEST_UTIL.getDFSCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(tableName)) {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
      }
      admin.deleteTable(tableName);
    }
    TEST_UTIL.createTable(Bytes.toBytes(tableName),
        new byte[][] { DUMMY, TEST });
  }

  static File buildCoprocessorJar(String className) throws Exception {
    String code = "import org.apache.hadoop.hbase.coprocessor.observers.BaseRegionObserver;\n"
        + "public class " + className + " extends BaseRegionObserver {}";
    return ClassLoaderTestHelper.buildJar(TEST_UTIL.getDFSCluster()
        .getDataDirectory().toString(), className, code);
  }

  public static String buildCorrectPathForCoprocessorJar(String dataDir) {
    System.out.println("dataDir: " + dataDir);
    String newStr = dataDir + File.separator+"coprocessors" + File.separator+"test" + File.separator + "1";
    System.out.println(newStr);
    return newStr;
  }

  @Test
  public void testClassLoadingFromHDFS() throws Exception {
    FileSystem fs = cluster.getFileSystem();
    File jarFile1 = buildCoprocessorJar(cpName1);
    System.out.println(jarFile1.getName());
    File jarFile2 = buildCoprocessorJar(cpName2);
    // have to create directories because we are not placing the files on hdfs
    // root
    assertTrue(fs.mkdirs(new Path("/coprocessors/test/1")));
    // copy the jars into dfs
    fs.moveFromLocalFile(new Path(jarFile1.getPath()), new Path(fs.getUri()
        .toString(), buildCorrectPathForCoprocessorJar("") + Path.SEPARATOR));

    String jarFileOnHDFS1 = buildCorrectPathForCoprocessorJar(fs.getUri()
        .toString()) + Path.SEPARATOR + jarFile1.getName();
    Path pathOnHDFS1 = new Path(jarFileOnHDFS1);
    System.out.println(jarFileOnHDFS1);
    assertTrue("Copy jar file to HDFS failed.", fs.exists(pathOnHDFS1));
    LOG.info("Copied jar file to HDFS: " + jarFileOnHDFS1);

    fs.moveFromLocalFile(new Path(jarFile2.getPath()), new Path(fs.getUri()
        .toString(), buildCorrectPathForCoprocessorJar("") + Path.SEPARATOR));

    String jarFileOnHDFS2 = buildCorrectPathForCoprocessorJar(fs.getUri()
        .toString()) + Path.SEPARATOR + jarFile2.getName();
    Path pathOnHDFS2 = new Path(jarFileOnHDFS2);
    assertTrue("Copy jar file to HDFS failed.", fs.exists(pathOnHDFS2));
    LOG.info("Copied jar file to HDFS: " + jarFileOnHDFS2);
    Configuration conf = TEST_UTIL.getConfiguration();

    // check if only TestCoprocessor is currently loaded
    List<HRegion> regions = TEST_UTIL.getMiniHBaseCluster().getRegions(Bytes.toBytes(tableName));
    Set<String> expectedCoprocessorSimpleName = new HashSet<>();
    Set<String> allCoprocessors = RegionCoprocessorHost
        .getEverLoadedCoprocessors();
    assertEquals("Number of coprocessors ever loaded", 1,
        allCoprocessors.size());
    assertEquals("Expected loaded coprocessor",
        TestCoprocessor.class.getName(), allCoprocessors.toArray()[0]);
    // do online config change and confirm the new coprocessor is loaded
    CoprocessorClassLoader.clearCache();
    // remove the firstly added coprocessor
    conf.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY, "");
    conf.set(CoprocessorHost.USER_REGION_COPROCESSOR_FROM_HDFS_KEY,
        pathOnHDFS1 + "," + cpName1);

    // invoke online configuration change
    HRegionServer.configurationManager.notifyAllObservers(conf);
    // check everloaded coprocessors
    allCoprocessors = RegionCoprocessorHost.getEverLoadedCoprocessors();
    assertEquals("Number of coprocessors ever loaded", 2,
        allCoprocessors.size());
    expectedCoprocessorSimpleName.add(cpName1);

    for (HRegion r : regions) {
      Set<String> currentCoprocessors = r.getCoprocessorHost()
          .getCoprocessors();
      assertEquals("Number of current coprocessors", 1,
          currentCoprocessors.size());
      assertEquals("Expected loaded coprocessors",
          expectedCoprocessorSimpleName, currentCoprocessors);
    }
    //now load the second coprocessor too
    String current = conf.get(CoprocessorHost.USER_REGION_COPROCESSOR_FROM_HDFS_KEY);
    current +="," + pathOnHDFS2+"," + cpName2;
    conf.set(CoprocessorHost.USER_REGION_COPROCESSOR_FROM_HDFS_KEY, current);
    // invoke online config change
    HRegionServer.configurationManager.notifyAllObservers(conf);
    allCoprocessors = RegionCoprocessorHost.getEverLoadedCoprocessors();
    assertEquals("Number of ever loaded coprocessors", 3,
        allCoprocessors.size());
    expectedCoprocessorSimpleName.add(cpName2);
    for (HRegion r : regions) {
      Set<String> currentCoprocessors = r.getCoprocessorHost().getCoprocessors();
      assertTrue("Number of currently loaded coprocessors",
          currentCoprocessors.size() == 2);
      assertEquals("Expected loaded coprocessors",
          expectedCoprocessorSimpleName, currentCoprocessors);
    }
  }

}
