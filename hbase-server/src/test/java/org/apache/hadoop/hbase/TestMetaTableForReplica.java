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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test {@link org.apache.hadoop.hbase.TestMetaTableForReplica}.
 */
@Category({ MiscTests.class, MediumTests.class })
@SuppressWarnings("deprecation")
public class TestMetaTableForReplica {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMetaTableForReplica.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMetaTableForReplica.class);
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static Connection connection;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration c = UTIL.getConfiguration();
    // quicker heartbeat interval for faster DN death notification
    c.setInt("hbase.ipc.client.connect.max.retries", 1);
    c.setInt(HConstants.ZK_SESSION_TIMEOUT, 1000);
    // Start cluster having non-default hbase meta table name
    c.setStrings(HConstants.HBASE_META_TABLE_SUFFIX, "test");
    UTIL.startMiniCluster(3);
    connection = ConnectionFactory.createConnection(c);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    connection.close();
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testStateOfMetaForReplica() {
    HMaster m = UTIL.getMiniHBaseCluster().getMaster();
    assertTrue(m.waitForMetaOnline());
  }

  @Test
  public void testNameOfMetaForReplica() {
    // Check the correctness of the meta table for replica
    String metaTableName = TableName.META_TABLE_NAME.getNameWithNamespaceInclAsString();
    assertNotNull(metaTableName);

    // Check if name of the meta table for replica is not same as default table
    assertEquals(0,
      TableName.META_TABLE_NAME.compareTo(TableName.getDefaultNameOfMetaForReplica()));
  }

  @Test
  public void testGetNonExistentRegionFromMetaFromReplica() throws IOException {
    final String name = this.name.getMethodName();
    LOG.info("Started " + name);
    Pair<RegionInfo, ServerName> pair =
      MetaTableAccessor.getRegion(connection, Bytes.toBytes("nonexistent-region"));
    assertNull(pair);
    LOG.info("Finished " + name);
  }

  @Test
  public void testGetExistentRegionFromMetaFromReplica() throws IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    LOG.info("Started " + tableName);
    UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);
    assertEquals(1, MetaTableAccessor.getTableRegions(connection, tableName).size());
  }
}
