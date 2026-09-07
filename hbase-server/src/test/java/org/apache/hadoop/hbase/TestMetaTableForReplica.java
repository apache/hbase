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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test {@link org.apache.hadoop.hbase.TestMetaTableForReplica}.
 */
@Tag(MiscTests.TAG)
@Tag(MediumTests.TAG)
@SuppressWarnings("deprecation")
public class TestMetaTableForReplica {

  private static final Logger LOG = LoggerFactory.getLogger(TestMetaTableForReplica.class);
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static Connection connection;

  @BeforeAll
  public static void beforeClass() throws Exception {
    Configuration c = UTIL.getConfiguration();
    // quicker heartbeat interval for faster DN death notification
    c.setInt("hbase.ipc.client.connect.max.retries", 1);
    c.setInt(HConstants.ZK_SESSION_TIMEOUT, 1000);
    // Start cluster having non-default hbase meta table name
    UTIL.startMiniCluster(3);
    connection = ConnectionFactory.createConnection(c);
  }

  @AfterAll
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
  public void testMetaTableNameForReplicaWithoutSuffix() throws IOException {
    testNameOfMetaForReplica();
    testGetNonExistentRegionFromMetaFromReplica();
    testGetExistentRegionFromMetaFromReplica();
  }

  private void testNameOfMetaForReplica() {
    // Check the correctness of the meta table for replica
    String metaTableName = TableName.META_TABLE_NAME.getNameWithNamespaceInclAsString();
    assertNotNull(metaTableName);

    // Check if name of the meta table for replica is same as the default meta table
    assertEquals(0,
      TableName.META_TABLE_NAME.compareTo(TableName.getDefaultNameOfMetaForReplica()));
  }

  private void testGetNonExistentRegionFromMetaFromReplica() throws IOException {
    LOG.info("Started testGetNonExistentRegionFromMetaFromReplica");
    Pair<RegionInfo, ServerName> pair =
      MetaTableAccessor.getRegion(connection, Bytes.toBytes("nonexistent-region"));
    assertNull(pair);
    LOG.info("Finished testGetNonExistentRegionFromMetaFromReplica");
  }

  private void testGetExistentRegionFromMetaFromReplica() throws IOException {
    final TableName tableName = TableName.valueOf("testMetaTableNameForReplicaWithoutSuffix");
    LOG.info("Started " + tableName);
    UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);
    assertEquals(1, MetaTableAccessor.getTableRegions(connection, tableName).size());
  }

  @Test
  public void testMetaTableNameForReplicaWithSuffix() {
    // TableName.META_TABLE_NAME is assigned in the class initializer, before a test can set a
    // configuration, so assert on the method the initializer itself calls.
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.HBASE_META_TABLE_SUFFIX, "replica1");
    TableName withSuffix = TableName.initializeHbaseMetaTableName(conf);

    assertNotEquals(TableName.getDefaultNameOfMetaForReplica(), withSuffix,
      "a configured suffix should not produce the default meta table name");
    assertEquals(TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "meta_replica1"),
      withSuffix, "meta table name should carry the configured suffix");
  }

}
