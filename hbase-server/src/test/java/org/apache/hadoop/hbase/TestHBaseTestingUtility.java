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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.http.ssl.KeyStoreTestUtil;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test our testing utility class
 */
@Category({MiscTests.class, LargeTests.class})
public class TestHBaseTestingUtility {
  private static final int NUMTABLES = 1;
  private static final int NUMROWS = 100;
  private static final int NUMREGIONS = 10;

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseTestingUtility.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseTestingUtility.class);

  @Rule
  public TestName name = new TestName();

  /**
   * Basic sanity test that spins up multiple HDFS and HBase clusters that share
   * the same ZK ensemble. We then create the same table in both and make sure
   * that what we insert in one place doesn't end up in the other.
   * @throws Exception
   */
  @Test
  public void testMultiClusters() throws Exception {
    // Create three clusters

    // Cluster 1.
    HBaseTestingUtility htu1 = new HBaseTestingUtility();
    // Set a different zk path for each cluster
    htu1.getConfiguration().set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    htu1.startMiniZKCluster();

    // Cluster 2
    HBaseTestingUtility htu2 = new HBaseTestingUtility();
    htu2.getConfiguration().set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    htu2.getConfiguration().set(HConstants.ZOOKEEPER_CLIENT_PORT,
      htu1.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT, "-1"));
    htu2.setZkCluster(htu1.getZkCluster());

    // Cluster 3; seed it with the conf from htu1 so we pickup the 'right'
    // zk cluster config; it is set back into the config. as part of the
    // start of minizkcluster.
    HBaseTestingUtility htu3 = new HBaseTestingUtility();
    htu3.getConfiguration().set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/3");
    htu3.getConfiguration().set(HConstants.ZOOKEEPER_CLIENT_PORT,
      htu1.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT, "-1"));
    htu3.setZkCluster(htu1.getZkCluster());

    try {
      htu1.startMiniCluster();
      htu2.startMiniCluster();
      htu3.startMiniCluster();

      final TableName tableName = TableName.valueOf(name.getMethodName());
      final byte[] FAM_NAME = Bytes.toBytes("fam");
      final byte[] ROW = Bytes.toBytes("row");
      final byte[] QUAL_NAME = Bytes.toBytes("qual");
      final byte[] VALUE = Bytes.toBytes("value");

      Table table1 = htu1.createTable(tableName, FAM_NAME);
      Table table2 = htu2.createTable(tableName, FAM_NAME);

      Put put = new Put(ROW);
      put.addColumn(FAM_NAME, QUAL_NAME, VALUE);
      table1.put(put);

      Get get = new Get(ROW);
      get.addColumn(FAM_NAME, QUAL_NAME);
      Result res = table1.get(get);
      assertEquals(1, res.size());

      res = table2.get(get);
      assertEquals(0, res.size());

      table1.close();
      table2.close();

    } finally {
      htu3.shutdownMiniCluster();
      htu2.shutdownMiniCluster();
      htu1.shutdownMiniCluster();
    }
  }

  @Test public void testMiniCluster() throws Exception {
    HBaseTestingUtility hbt = new HBaseTestingUtility();

    MiniHBaseCluster cluster = hbt.startMiniCluster();
    try {
      assertEquals(1, cluster.getLiveRegionServerThreads().size());
    } finally {
      hbt.shutdownMiniCluster();
    }
  }

  @Test
  public void testMiniClusterBindToWildcard() throws Exception {
    HBaseTestingUtility hbt = new HBaseTestingUtility();
    hbt.getConfiguration().set("hbase.regionserver.ipc.address", "0.0.0.0");
    MiniHBaseCluster cluster = hbt.startMiniCluster();
    try {
      assertEquals(1, cluster.getLiveRegionServerThreads().size());
    } finally {
      hbt.shutdownMiniCluster();
    }
  }

  @Test
  public void testMiniClusterWithSSLOn() throws Exception {
    final String BASEDIR = System.getProperty("test.build.dir",
        "target/test-dir") + "/" + TestHBaseTestingUtility.class.getSimpleName();
    String sslConfDir = KeyStoreTestUtil.getClasspathDir(TestHBaseTestingUtility.class);
    String keystoresDir = new File(BASEDIR).getAbsolutePath();

    HBaseTestingUtility hbt = new HBaseTestingUtility();
    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();

    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, hbt.getConfiguration(), false);

    hbt.getConfiguration().set("hbase.ssl.enabled", "true");
    hbt.getConfiguration().addResource("ssl-server.xml");
    hbt.getConfiguration().addResource("ssl-client.xml");

    MiniHBaseCluster cluster = hbt.startMiniCluster();
    try {
      assertEquals(1, cluster.getLiveRegionServerThreads().size());
    } finally {
      hbt.shutdownMiniCluster();
    }
  }

  /**
   *  Test that we can start and stop multiple time a cluster
   *   with the same HBaseTestingUtility.
   */
  @Test public void testMultipleStartStop() throws Exception{
    HBaseTestingUtility htu1 = new HBaseTestingUtility();
    Path foo = new Path("foo");

    htu1.startMiniCluster();
    htu1.getDFSCluster().getFileSystem().create(foo);
    assertTrue( htu1.getDFSCluster().getFileSystem().exists(foo));
    htu1.shutdownMiniCluster();

    htu1.startMiniCluster();
    assertFalse( htu1.getDFSCluster().getFileSystem().exists(foo));
    htu1.getDFSCluster().getFileSystem().create(foo);
    assertTrue( htu1.getDFSCluster().getFileSystem().exists(foo));
    htu1.shutdownMiniCluster();
  }

  @Test
  public void testMiniZooKeeperWithOneServer() throws Exception {
    HBaseTestingUtility hbt = new HBaseTestingUtility();
    MiniZooKeeperCluster cluster1 = hbt.startMiniZKCluster();
    try {
      assertEquals(0, cluster1.getBackupZooKeeperServerNum());
      assertTrue((cluster1.killCurrentActiveZooKeeperServer() == -1));
    } finally {
      hbt.shutdownMiniZKCluster();
    }
  }

  @Test
  public void testMiniZooKeeperWithMultipleServers() throws Exception {
    HBaseTestingUtility hbt = new HBaseTestingUtility();
    // set up zookeeper cluster with 5 zk servers
    MiniZooKeeperCluster cluster2 = hbt.startMiniZKCluster(5);
    int defaultClientPort = 21818;
    cluster2.setDefaultClientPort(defaultClientPort);
    try {
      assertEquals(4, cluster2.getBackupZooKeeperServerNum());

      // killing the current active zk server
      int currentActivePort = cluster2.killCurrentActiveZooKeeperServer();
      assertTrue(currentActivePort >= defaultClientPort);
      // Check if the client port is returning a proper value
      assertTrue(cluster2.getClientPort() == currentActivePort);

      // kill another active zk server
      currentActivePort = cluster2.killCurrentActiveZooKeeperServer();
      assertTrue(currentActivePort >= defaultClientPort);
      assertTrue(cluster2.getClientPort() == currentActivePort);
      assertEquals(2, cluster2.getBackupZooKeeperServerNum());
      assertEquals(3, cluster2.getZooKeeperServerNum());

      // killing the backup zk servers
      cluster2.killOneBackupZooKeeperServer();
      cluster2.killOneBackupZooKeeperServer();
      assertEquals(0, cluster2.getBackupZooKeeperServerNum());
      assertEquals(1, cluster2.getZooKeeperServerNum());

      // killing the last zk server
      currentActivePort = cluster2.killCurrentActiveZooKeeperServer();
      assertTrue(currentActivePort == -1);
      assertTrue(cluster2.getClientPort() == currentActivePort);
      // this should do nothing.
      cluster2.killOneBackupZooKeeperServer();
      assertEquals(-1, cluster2.getBackupZooKeeperServerNum());
      assertEquals(0, cluster2.getZooKeeperServerNum());
    } finally {
      hbt.shutdownMiniZKCluster();
    }
  }

  @Test
  public void testMiniZooKeeperWithMultipleClientPorts() throws Exception {
    int defaultClientPort = 8888;
    int i, j;
    HBaseTestingUtility hbt = new HBaseTestingUtility();

    // Test 1 - set up zookeeper cluster with same number of ZK servers and specified client ports
    int [] clientPortList1 = {1111, 1112, 1113};
    MiniZooKeeperCluster cluster1 = hbt.startMiniZKCluster(clientPortList1.length, clientPortList1);
    try {
      List<Integer> clientPortListInCluster = cluster1.getClientPortList();

      for (i = 0; i < clientPortListInCluster.size(); i++) {
        // cannot assert the specific port due to the port conflict in which situation
        // it always chooses a bigger port by +1. The below is the same.
        assertTrue(clientPortListInCluster.get(i).intValue() >= clientPortList1[i]);
      }
    } finally {
      hbt.shutdownMiniZKCluster();
    }

    // Test 2 - set up zookeeper cluster with more ZK servers than specified client ports
    hbt.getConfiguration().setInt("test.hbase.zookeeper.property.clientPort", defaultClientPort);
    int [] clientPortList2 = {2222, 2223};
    MiniZooKeeperCluster cluster2 =
        hbt.startMiniZKCluster(clientPortList2.length + 2, clientPortList2);

    try {
      List<Integer> clientPortListInCluster = cluster2.getClientPortList();

      for (i = 0, j = 0; i < clientPortListInCluster.size(); i++) {
        if (i < clientPortList2.length) {
          assertTrue(clientPortListInCluster.get(i).intValue() >= clientPortList2[i]);
        } else {
          // servers with no specified client port will use defaultClientPort or some other ports
          // based on defaultClientPort
          assertTrue(clientPortListInCluster.get(i).intValue() >= defaultClientPort + j);
          j++;
        }
      }
    } finally {
      hbt.shutdownMiniZKCluster();
    }

    // Test 3 - set up zookeeper cluster with invalid client ports
    hbt.getConfiguration().setInt("test.hbase.zookeeper.property.clientPort", defaultClientPort);
    int [] clientPortList3 = {3333, -3334, 3335, 0};
    MiniZooKeeperCluster cluster3 =
        hbt.startMiniZKCluster(clientPortList3.length + 1, clientPortList3);

    try {
      List<Integer> clientPortListInCluster = cluster3.getClientPortList();

      for (i = 0, j = 0; i < clientPortListInCluster.size(); i++) {
        // Servers will only use valid client ports; if ports are not specified or invalid,
        // the default port or a port based on default port will be used.
        if (i < clientPortList3.length && clientPortList3[i] > 0) {
          assertTrue(clientPortListInCluster.get(i).intValue() >= clientPortList3[i]);
        } else {
          assertTrue(clientPortListInCluster.get(i).intValue() >= defaultClientPort + j);
          j++;
        }
      }
    } finally {
      hbt.shutdownMiniZKCluster();
    }

    // Test 4 - set up zookeeper cluster with default port and some other ports used
    // This test tests that the defaultClientPort and defaultClientPort+2 are used, so
    // the algorithm should choice defaultClientPort+1 and defaultClientPort+3 to fill
    // out the ports for servers without ports specified.
    hbt.getConfiguration().setInt("test.hbase.zookeeper.property.clientPort", defaultClientPort);
    int [] clientPortList4 = {-4444, defaultClientPort+2, 4446, defaultClientPort};
    MiniZooKeeperCluster cluster4 =
        hbt.startMiniZKCluster(clientPortList4.length + 1, clientPortList4);

    try {
      List<Integer> clientPortListInCluster = cluster4.getClientPortList();

      for (i = 0, j = 1; i < clientPortListInCluster.size(); i++) {
        // Servers will only use valid client ports; if ports are not specified or invalid,
        // the default port or a port based on default port will be used.
        if (i < clientPortList4.length && clientPortList4[i] > 0) {
          assertTrue(clientPortListInCluster.get(i).intValue() >= clientPortList4[i]);
        } else {
          assertTrue(clientPortListInCluster.get(i).intValue() >= defaultClientPort + j);
          j +=2;
        }
      }
    } finally {
      hbt.shutdownMiniZKCluster();
    }

    // Test 5 - set up zookeeper cluster with same ports specified - fail is expected.
    int [] clientPortList5 = {5555, 5556, 5556};

    try {
      MiniZooKeeperCluster cluster5 =
          hbt.startMiniZKCluster(clientPortList5.length, clientPortList5);
      assertTrue(cluster5.getClientPort() == -1); // expected failure
    } catch (Exception e) {
      // exception is acceptable
    } finally {
      hbt.shutdownMiniZKCluster();
    }
  }

  @Test public void testMiniDFSCluster() throws Exception {
    HBaseTestingUtility hbt = new HBaseTestingUtility();
    MiniDFSCluster cluster = hbt.startMiniDFSCluster(null);
    FileSystem dfs = cluster.getFileSystem();
    Path dir = new Path("dir");
    Path qualifiedDir = dfs.makeQualified(dir);
    LOG.info("dir=" + dir + ", qualifiedDir=" + qualifiedDir);
    assertFalse(dfs.exists(qualifiedDir));
    assertTrue(dfs.mkdirs(qualifiedDir));
    assertTrue(dfs.delete(qualifiedDir, true));
    hbt.shutdownMiniCluster();
  }

  @Test public void testSetupClusterTestBuildDir() throws Exception {
    HBaseTestingUtility hbt = new HBaseTestingUtility();
    Path testdir = hbt.getClusterTestDir();
    LOG.info("uuid-subdir=" + testdir);
    FileSystem fs = hbt.getTestFileSystem();

    assertFalse(fs.exists(testdir));

    hbt.startMiniDFSCluster(null);
    assertTrue(fs.exists(testdir));

    hbt.shutdownMiniCluster();
    assertFalse(fs.exists(testdir));
  }

  @Test public void testTestDir() throws Exception {
    HBaseTestingUtility hbt = new HBaseTestingUtility();
    Path testdir = hbt.getDataTestDir();
    LOG.info("testdir=" + testdir);
    FileSystem fs = hbt.getTestFileSystem();
    assertTrue(!fs.exists(testdir));
    assertTrue(fs.mkdirs(testdir));
    assertTrue(hbt.cleanupTestDir());
  }

  @Test public void testResolvePortConflict() throws Exception {
    // raises port conflict between 1st call and 2nd call of randomPort() by mocking Random object
    Random random = mock(Random.class);
    when(random.nextInt(anyInt()))
      .thenAnswer(new Answer<Integer>() {
        int[] numbers = { 1, 1, 2 };
        int count = 0;

        @Override
        public Integer answer(InvocationOnMock invocation) {
          int ret = numbers[count];
          count++;
          return ret;
        }
      });

    HBaseTestingUtility.PortAllocator.AvailablePortChecker portChecker =
      mock(HBaseTestingUtility.PortAllocator.AvailablePortChecker.class);
    when(portChecker.available(anyInt())).thenReturn(true);

    HBaseTestingUtility.PortAllocator portAllocator =
      new HBaseTestingUtility.PortAllocator(random, portChecker);

    int port1 = portAllocator.randomFreePort();
    int port2 = portAllocator.randomFreePort();
    assertNotEquals(port1, port2);
    Mockito.verify(random, Mockito.times(3)).nextInt(anyInt());
  }

  @Test
  public void testOverridingOfDefaultPorts() throws Exception {

    // confirm that default port properties being overridden to random
    Configuration defaultConfig = HBaseConfiguration.create();
    defaultConfig.setInt(HConstants.MASTER_INFO_PORT, HConstants.DEFAULT_MASTER_INFOPORT);
    defaultConfig.setInt(HConstants.REGIONSERVER_INFO_PORT,
        HConstants.DEFAULT_REGIONSERVER_INFOPORT);
    HBaseTestingUtility htu = new HBaseTestingUtility(defaultConfig);
    try {
      MiniHBaseCluster defaultCluster = htu.startMiniCluster();
      assertNotEquals(HConstants.DEFAULT_MASTER_INFOPORT,
          defaultCluster.getConfiguration().getInt(HConstants.MASTER_INFO_PORT, 0));
      assertNotEquals(HConstants.DEFAULT_REGIONSERVER_INFOPORT,
          defaultCluster.getConfiguration().getInt(HConstants.REGIONSERVER_INFO_PORT, 0));
    } finally {
      htu.shutdownMiniCluster();
    }

    // confirm that nonDefault (custom) port settings are NOT overridden
    Configuration altConfig = HBaseConfiguration.create();
    final int nonDefaultMasterInfoPort = 3333;
    final int nonDefaultRegionServerPort = 4444;
    altConfig.setInt(HConstants.MASTER_INFO_PORT, nonDefaultMasterInfoPort);
    altConfig.setInt(HConstants.REGIONSERVER_INFO_PORT, nonDefaultRegionServerPort);
    htu = new HBaseTestingUtility(altConfig);
    try {
      MiniHBaseCluster customCluster = htu.startMiniCluster();
      assertEquals(nonDefaultMasterInfoPort,
              customCluster.getConfiguration().getInt(HConstants.MASTER_INFO_PORT, 0));
      assertEquals(nonDefaultRegionServerPort,
          customCluster.getConfiguration().getInt(HConstants.REGIONSERVER_INFO_PORT, 0));
    } finally {
      htu.shutdownMiniCluster();
    }
  }

  // This test demonstrates how long killHBTU takes vs. shutdownHBTU takes
  // for realistic results, adjust NUMROWS, NUMTABLES to much larger number.
  @Test
  public void testKillMiniHBaseCluster() throws Exception {

    HBaseTestingUtility htu = new HBaseTestingUtility();
    htu.startMiniZKCluster();

    try {
      htu.startMiniHBaseCluster();

      TableName tableName;
      byte[] FAM_NAME;

      for(int i = 0; i < NUMTABLES; i++) {
        tableName = TableName.valueOf(name.getMethodName() + i);
        FAM_NAME = Bytes.toBytes("fam" + i);

        try (Table table = htu.createMultiRegionTable(tableName, FAM_NAME, NUMREGIONS)) {
          htu.loadRandomRows(table, FAM_NAME, 100, NUMROWS);
        }
      }
    } finally {
      htu.killMiniHBaseCluster();
      htu.shutdownMiniZKCluster();
    }
  }
}
