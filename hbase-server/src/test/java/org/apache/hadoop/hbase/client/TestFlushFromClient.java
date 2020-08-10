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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MediumTests.class, ClientTests.class})
public class TestFlushFromClient {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFlushFromClient.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestFlushFromClient.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static AsyncConnection asyncConn;
  private static final byte[][] SPLITS = new byte[][]{Bytes.toBytes("3"), Bytes.toBytes("7")};
  private static final List<byte[]> ROWS = Arrays.asList(
    Bytes.toBytes("1"),
    Bytes.toBytes("4"),
    Bytes.toBytes("8"));
  private static final byte[] FAMILY_1 = Bytes.toBytes("f1");
  private static final byte[] FAMILY_2 = Bytes.toBytes("f2");
  public static final byte[][] FAMILIES = {FAMILY_1, FAMILY_2};
  @Rule
  public TestName name = new TestName();

  public TableName tableName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(ROWS.size());
    asyncConn = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    IOUtils.cleanup(null, asyncConn);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    tableName = TableName.valueOf(name.getMethodName());
    try (Table t = TEST_UTIL.createTable(tableName, FAMILIES, SPLITS)) {
      List<Put> puts = ROWS.stream().map(r -> new Put(r)).collect(Collectors.toList());
      for (int i = 0; i != 20; ++i) {
        byte[] value = Bytes.toBytes(i);
        puts.forEach(p -> {
          p.addColumn(FAMILY_1, value, value);
          p.addColumn(FAMILY_2, value, value);
        });
      }
      t.put(puts);
    }
    assertFalse(getRegionInfo().isEmpty());
    assertTrue(getRegionInfo().stream().allMatch(r -> r.getMemStoreDataSize() != 0));
  }

  @After
  public void tearDown() throws Exception {
    for (TableDescriptor htd : TEST_UTIL.getAdmin().listTableDescriptors()) {
      LOG.info("Tear down, remove table=" + htd.getTableName());
      TEST_UTIL.deleteTable(htd.getTableName());
    }
  }

  @Test
  public void testFlushTable() throws Exception {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      admin.flush(tableName);
      assertFalse(getRegionInfo().stream().anyMatch(r -> r.getMemStoreDataSize() != 0));
    }
  }

  @Test
  public void testFlushTableFamily() throws Exception {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      long sizeBeforeFlush = getRegionInfo().get(0).getMemStoreDataSize();
      admin.flush(tableName, FAMILY_1);
      assertFalse(getRegionInfo().stream().
        anyMatch(r -> r.getMemStoreDataSize() != sizeBeforeFlush / 2));
    }
  }

  @Test
  public void testAsyncFlushTable() throws Exception {
    AsyncAdmin admin = asyncConn.getAdmin();
    admin.flush(tableName).get();
    assertFalse(getRegionInfo().stream().anyMatch(r -> r.getMemStoreDataSize() != 0));
  }

  @Test
  public void testAsyncFlushTableFamily() throws Exception {
    AsyncAdmin admin = asyncConn.getAdmin();
    long sizeBeforeFlush = getRegionInfo().get(0).getMemStoreDataSize();
    admin.flush(tableName, FAMILY_1).get();
    assertFalse(getRegionInfo().stream().
      anyMatch(r -> r.getMemStoreDataSize() != sizeBeforeFlush / 2));
  }

  @Test
  public void testFlushRegion() throws Exception {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      for (HRegion r : getRegionInfo()) {
        admin.flushRegion(r.getRegionInfo().getRegionName());
        TimeUnit.SECONDS.sleep(1);
        assertEquals(0, r.getMemStoreDataSize());
      }
    }
  }

  @Test
  public void testFlushRegionFamily() throws Exception {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      for (HRegion r : getRegionInfo()) {
        long sizeBeforeFlush = r.getMemStoreDataSize();
        admin.flushRegion(r.getRegionInfo().getRegionName(), FAMILY_1);
        TimeUnit.SECONDS.sleep(1);
        assertEquals(sizeBeforeFlush / 2, r.getMemStoreDataSize());
      }
    }
  }

  @Test
  public void testAsyncFlushRegion() throws Exception {
    AsyncAdmin admin = asyncConn.getAdmin();
    for (HRegion r : getRegionInfo()) {
      admin.flushRegion(r.getRegionInfo().getRegionName()).get();
      TimeUnit.SECONDS.sleep(1);
      assertEquals(0, r.getMemStoreDataSize());
    }
  }

  @Test
  public void testAsyncFlushRegionFamily() throws Exception {
    AsyncAdmin admin = asyncConn.getAdmin();
    for (HRegion r : getRegionInfo()) {
      long sizeBeforeFlush = r.getMemStoreDataSize();
      admin.flushRegion(r.getRegionInfo().getRegionName(), FAMILY_1).get();
      TimeUnit.SECONDS.sleep(1);
      assertEquals(sizeBeforeFlush / 2, r.getMemStoreDataSize());
    }
  }

  @Test
  public void testFlushRegionServer() throws Exception {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      for (HRegionServer rs : TEST_UTIL.getHBaseCluster()
            .getLiveRegionServerThreads()
            .stream().map(JVMClusterUtil.RegionServerThread::getRegionServer)
            .collect(Collectors.toList())) {
        admin.flushRegionServer(rs.getServerName());
        assertFalse(getRegionInfo(rs).stream().anyMatch(r -> r.getMemStoreDataSize() != 0));
      }
    }
  }

  @Test
  public void testAsyncFlushRegionServer() throws Exception {
    AsyncAdmin admin = asyncConn.getAdmin();
    for (HRegionServer rs : TEST_UTIL.getHBaseCluster()
      .getLiveRegionServerThreads()
      .stream().map(JVMClusterUtil.RegionServerThread::getRegionServer)
      .collect(Collectors.toList())) {
      admin.flushRegionServer(rs.getServerName()).get();
      assertFalse(getRegionInfo(rs).stream().anyMatch(r -> r.getMemStoreDataSize() != 0));
    }
  }

  private List<HRegion> getRegionInfo() {
    return TEST_UTIL.getHBaseCluster().getRegions(tableName);
  }

  private List<HRegion> getRegionInfo(HRegionServer rs) {
    return rs.getRegions().stream()
      .filter(v -> v.getTableDescriptor().getTableName().equals(tableName))
      .collect(Collectors.toList());
  }
}
