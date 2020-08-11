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

import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
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
public class TestArchiveWalFromClient {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestArchiveWalFromClient.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestArchiveWalFromClient.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] ROW = Bytes.toBytes("testRow");
  private static final byte[] FAMILY = Bytes.toBytes("testFamily");
  private static final byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static final byte[] VALUE = Bytes.toBytes("testValue");
  @Rule
  public TestName name = new TestName();

  public TableName tableName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    tableName = TableName.valueOf(name.getMethodName());
  }

  @After
  public void tearDown() throws Exception {
    for (TableDescriptor htd : TEST_UTIL.getAdmin().listTableDescriptors()) {
      LOG.info("Tear down, remove table=" + htd.getTableName());
      TEST_UTIL.deleteTable(htd.getTableName());
    }
  }

  @Test
  public void testArchiveEarliestWAL() throws Exception {
    try (Table t = TEST_UTIL.createTable(tableName, FAMILY)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      t.put(put);
    }
    try (Admin admin = TEST_UTIL.getAdmin()) {
      HRegionServer rs = TEST_UTIL.getHBaseCluster().getRegionServer(0);
      assertTrue(getRegionInfo().get(0).getMemStoreDataSize() > 0);
      AbstractFSWAL wal = (AbstractFSWAL) rs.getWALs().get(0);
      assertEquals(0, wal.getNumRolledLogFiles());
      // do not use admin.rollWALWriter here, since it is async call
      wal.rollWriter();
      assertEquals(1, wal.getNumRolledLogFiles());
      admin.archiveWAL(rs.getServerName());
      assertEquals(0, wal.getNumRolledLogFiles());
      // the region should be flushed after archive
      assertFalse(getRegionInfo().get(0).getMemStoreDataSize() > 0);
    }
  }

  private List<HRegion> getRegionInfo() {
    return TEST_UTIL.getHBaseCluster().getRegions(tableName);
  }

}
