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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

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
  private static Field metaTableName;
  private static Object originalMetaTableName;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration c = UTIL.getConfiguration();
    // quicker heartbeat interval for faster DN death notification
    c.setInt("hbase.ipc.client.connect.max.retries", 1);
    c.setInt(HConstants.ZK_SESSION_TIMEOUT, 1000);
    // Start cluster having non-default hbase meta table name
    UTIL.startMiniCluster(3);
    connection = ConnectionFactory.createConnection(c);
    // Save the original value of META_TABLE_NAME before any test runs.x
    metaTableName = TableName.class.getDeclaredField("META_TABLE_NAME");
    originalMetaTableName = metaTableName.get(null);
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
    final String name = this.name.getMethodName();
    LOG.info("Started " + name);
    Pair<RegionInfo, ServerName> pair =
      MetaTableAccessor.getRegion(connection, Bytes.toBytes("nonexistent-region"));
    assertNull(pair);
    LOG.info("Finished " + name);
  }

  private void testGetExistentRegionFromMetaFromReplica() throws IOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    LOG.info("Started " + tableName);
    UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);
    assertEquals(1, MetaTableAccessor.getTableRegions(connection, tableName).size());
  }

  @Test
  public void testMetaTableNameForReplicaWithSuffix() throws Exception {
    // This test actively changes the META_TABLE_NAME to a non-default value and verifies it.
    Configuration conf = HBaseConfiguration.create();
    String suffix = "replica1";
    conf.set(HConstants.HBASE_META_TABLE_SUFFIX, suffix);

    // Re-initialize the static final META_TABLE_NAME for the testing to a non-default value.
    TableName expectedMetaTableName = TableName.initializeHbaseMetaTableName(conf);
    setStaticFinalField(metaTableName, expectedMetaTableName);

    TableName currentMetaName = TableName.META_TABLE_NAME;
    TableName defaultMetaName = TableName.getDefaultNameOfMetaForReplica();

    // The current meta table name is not the default one.
    assertNotEquals("META_TABLE_NAME should not be the default. ", defaultMetaName,
      currentMetaName);

    // The current meta table name has the configured suffix.
    assertEquals("META_TABLE_NAME should have the configured suffix",
      expectedMetaTableName, currentMetaName);

    //restore default value of META_TABLE_NAME
    setDefaultMetaTableName();
  }

  private static void setDefaultMetaTableName() throws Exception {
    if (originalMetaTableName != null) {
      setStaticFinalField(metaTableName, originalMetaTableName);
    }
  }

  /**
   * A helper method to modify a static final field using reflection. This is necessary for testing
   * code that reads a configuration only once during class loading.
   * @param field The field to modify.
   * @param newValue The new value to set.
   * @throws Exception if reflection fails.
   */
  private static void setStaticFinalField(Field field, Object newValue) throws Exception {
    field.setAccessible(true);
    // Using MethodHandles to get a trusted lookup with the necessary permissions to modify it.
    // NOTE: For this to work, the JVM running the test must be started with arguments like:
    // --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
    var lookup = MethodHandles.privateLookupIn(Field.class, MethodHandles.lookup());
    var handle = lookup.findVarHandle(Field.class, "modifiers", int.class);
    handle.set(field, field.getModifiers() & ~Modifier.FINAL);
    field.set(null, newValue);
  }
}
