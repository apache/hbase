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
package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot.SpaceQuotaStatus;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hbase.thirdparty.com.google.common.collect.HashMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Throttle;

/**
 * Test the quota table helpers (e.g. CRUD operations)
 */
@Category({MasterTests.class, MediumTests.class})
public class TestQuotaTableUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestQuotaTableUtil.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private Connection connection;
  private int tableNameCounter;

  @Rule
  public TestName testName = new TestName();

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REFRESH_CONF_KEY, 2000);
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compactionThreshold", 10);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", true);
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws IOException {
    this.connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
    this.tableNameCounter = 0;
  }

  @After
  public void after() throws IOException {
    this.connection.close();
  }

  @Test
  public void testDeleteSnapshots() throws Exception {
    TableName tn = TableName.valueOf(name.getMethodName());
    try (Table t = connection.getTable(QuotaTableUtil.QUOTA_TABLE_NAME)) {
      Quotas quota = Quotas.newBuilder().setSpace(
          QuotaProtos.SpaceQuota.newBuilder().setSoftLimit(7L)
              .setViolationPolicy(QuotaProtos.SpaceViolationPolicy.NO_WRITES).build()).build();
      QuotaUtil.addTableQuota(connection, tn, quota);

      String snapshotName = name.getMethodName() + "_snapshot";
      t.put(QuotaTableUtil.createPutForSnapshotSize(tn, snapshotName, 3L));
      t.put(QuotaTableUtil.createPutForSnapshotSize(tn, snapshotName, 5L));
      assertEquals(1, QuotaTableUtil.getObservedSnapshotSizes(connection).size());

      List<Delete> deletes = QuotaTableUtil.createDeletesForExistingTableSnapshotSizes(connection);
      assertEquals(1, deletes.size());

      t.delete(deletes);
      assertEquals(0, QuotaTableUtil.getObservedSnapshotSizes(connection).size());

      String ns = name.getMethodName();
      t.put(QuotaTableUtil.createPutForNamespaceSnapshotSize(ns, 5L));
      t.put(QuotaTableUtil.createPutForNamespaceSnapshotSize(ns, 3L));
      assertEquals(3L, QuotaTableUtil.getNamespaceSnapshotSize(connection, ns));

      deletes = QuotaTableUtil.createDeletesForExistingNamespaceSnapshotSizes(connection);
      assertEquals(1, deletes.size());

      t.delete(deletes);
      assertEquals(0L, QuotaTableUtil.getNamespaceSnapshotSize(connection, ns));

      t.put(QuotaTableUtil.createPutForSnapshotSize(TableName.valueOf("t1"), "s1", 3L));
      t.put(QuotaTableUtil.createPutForSnapshotSize(TableName.valueOf("t2"), "s2", 3L));
      t.put(QuotaTableUtil.createPutForSnapshotSize(TableName.valueOf("t3"), "s3", 3L));
      t.put(QuotaTableUtil.createPutForSnapshotSize(TableName.valueOf("t4"), "s4", 3L));
      t.put(QuotaTableUtil.createPutForSnapshotSize(TableName.valueOf("t1"), "s5", 3L));

      t.put(QuotaTableUtil.createPutForNamespaceSnapshotSize("ns1", 3L));
      t.put(QuotaTableUtil.createPutForNamespaceSnapshotSize("ns2", 3L));
      t.put(QuotaTableUtil.createPutForNamespaceSnapshotSize("ns3", 3L));

      assertEquals(5,QuotaTableUtil.getTableSnapshots(connection).size());
      assertEquals(3,QuotaTableUtil.getNamespaceSnapshots(connection).size());

      Multimap<TableName, String> tableSnapshotEntriesToRemove = HashMultimap.create();
      tableSnapshotEntriesToRemove.put(TableName.valueOf("t1"), "s1");
      tableSnapshotEntriesToRemove.put(TableName.valueOf("t3"), "s3");
      tableSnapshotEntriesToRemove.put(TableName.valueOf("t4"), "s4");

      Set<String> namespaceSnapshotEntriesToRemove = new HashSet<>();
      namespaceSnapshotEntriesToRemove.add("ns2");
      namespaceSnapshotEntriesToRemove.add("ns1");

      deletes =
          QuotaTableUtil.createDeletesForExistingTableSnapshotSizes(tableSnapshotEntriesToRemove);
      assertEquals(3, deletes.size());
      deletes = QuotaTableUtil
          .createDeletesForExistingNamespaceSnapshotSizes(namespaceSnapshotEntriesToRemove);
      assertEquals(2, deletes.size());
    }
  }

  @Test
  public void testTableQuotaUtil() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    Quotas quota = Quotas.newBuilder()
              .setThrottle(Throttle.newBuilder()
                .setReqNum(ProtobufUtil.toTimedQuota(1000, TimeUnit.SECONDS, QuotaScope.MACHINE))
                .setWriteNum(ProtobufUtil.toTimedQuota(600, TimeUnit.SECONDS, QuotaScope.MACHINE))
                .setReadSize(ProtobufUtil.toTimedQuota(8192, TimeUnit.SECONDS, QuotaScope.MACHINE))
                .build())
              .build();

    // Add user quota and verify it
    QuotaUtil.addTableQuota(this.connection, tableName, quota);
    Quotas resQuota = QuotaUtil.getTableQuota(this.connection, tableName);
    assertEquals(quota, resQuota);

    // Remove user quota and verify it
    QuotaUtil.deleteTableQuota(this.connection, tableName);
    resQuota = QuotaUtil.getTableQuota(this.connection, tableName);
    assertEquals(null, resQuota);
  }

  @Test
  public void testNamespaceQuotaUtil() throws Exception {
    final String namespace = "testNamespaceQuotaUtilNS";

    Quotas quota = Quotas.newBuilder()
              .setThrottle(Throttle.newBuilder()
                .setReqNum(ProtobufUtil.toTimedQuota(1000, TimeUnit.SECONDS, QuotaScope.MACHINE))
                .setWriteNum(ProtobufUtil.toTimedQuota(600, TimeUnit.SECONDS, QuotaScope.MACHINE))
                .setReadSize(ProtobufUtil.toTimedQuota(8192, TimeUnit.SECONDS, QuotaScope.MACHINE))
                .build())
              .build();

    // Add user quota and verify it
    QuotaUtil.addNamespaceQuota(this.connection, namespace, quota);
    Quotas resQuota = QuotaUtil.getNamespaceQuota(this.connection, namespace);
    assertEquals(quota, resQuota);

    // Remove user quota and verify it
    QuotaUtil.deleteNamespaceQuota(this.connection, namespace);
    resQuota = QuotaUtil.getNamespaceQuota(this.connection, namespace);
    assertEquals(null, resQuota);
  }

  @Test
  public void testUserQuotaUtil() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final String namespace = "testNS";
    final String user = "testUser";

    Quotas quotaNamespace = Quotas.newBuilder()
              .setThrottle(Throttle.newBuilder()
                .setReqNum(ProtobufUtil.toTimedQuota(50000, TimeUnit.SECONDS, QuotaScope.MACHINE))
                .build())
              .build();
    Quotas quotaTable = Quotas.newBuilder()
              .setThrottle(Throttle.newBuilder()
                .setReqNum(ProtobufUtil.toTimedQuota(1000, TimeUnit.SECONDS, QuotaScope.MACHINE))
                .setWriteNum(ProtobufUtil.toTimedQuota(600, TimeUnit.SECONDS, QuotaScope.MACHINE))
                .setReadSize(ProtobufUtil.toTimedQuota(10000, TimeUnit.SECONDS, QuotaScope.MACHINE))
                .build())
              .build();
    Quotas quota = Quotas.newBuilder()
              .setThrottle(Throttle.newBuilder()
                .setReqSize(ProtobufUtil.toTimedQuota(8192, TimeUnit.SECONDS, QuotaScope.MACHINE))
                .setWriteSize(ProtobufUtil.toTimedQuota(4096, TimeUnit.SECONDS, QuotaScope.MACHINE))
                .setReadNum(ProtobufUtil.toTimedQuota(1000, TimeUnit.SECONDS, QuotaScope.MACHINE))
                .build())
              .build();

    // Add user global quota
    QuotaUtil.addUserQuota(this.connection, user, quota);
    Quotas resQuota = QuotaUtil.getUserQuota(this.connection, user);
    assertEquals(quota, resQuota);

    // Add user quota for table
    QuotaUtil.addUserQuota(this.connection, user, tableName, quotaTable);
    Quotas resQuotaTable = QuotaUtil.getUserQuota(this.connection, user, tableName);
    assertEquals(quotaTable, resQuotaTable);

    // Add user quota for namespace
    QuotaUtil.addUserQuota(this.connection, user, namespace, quotaNamespace);
    Quotas resQuotaNS = QuotaUtil.getUserQuota(this.connection, user, namespace);
    assertEquals(quotaNamespace, resQuotaNS);

    // Delete user global quota
    QuotaUtil.deleteUserQuota(this.connection, user);
    resQuota = QuotaUtil.getUserQuota(this.connection, user);
    assertEquals(null, resQuota);

    // Delete user quota for table
    QuotaUtil.deleteUserQuota(this.connection, user, tableName);
    resQuotaTable = QuotaUtil.getUserQuota(this.connection, user, tableName);
    assertEquals(null, resQuotaTable);

    // Delete user quota for namespace
    QuotaUtil.deleteUserQuota(this.connection, user, namespace);
    resQuotaNS = QuotaUtil.getUserQuota(this.connection, user, namespace);
    assertEquals(null, resQuotaNS);
  }

  @Test
  public void testSerDeViolationPolicies() throws Exception {
    final TableName tn1 = getUniqueTableName();
    final SpaceQuotaSnapshot snapshot1 = new SpaceQuotaSnapshot(
        new SpaceQuotaStatus(SpaceViolationPolicy.DISABLE), 512L, 1024L);
    final TableName tn2 = getUniqueTableName();
    final SpaceQuotaSnapshot snapshot2 = new SpaceQuotaSnapshot(
        new SpaceQuotaStatus(SpaceViolationPolicy.NO_INSERTS), 512L, 1024L);
    final TableName tn3 = getUniqueTableName();
    final SpaceQuotaSnapshot snapshot3 = new SpaceQuotaSnapshot(
        new SpaceQuotaStatus(SpaceViolationPolicy.NO_WRITES), 512L, 1024L);
    List<Put> puts = new ArrayList<>();
    puts.add(QuotaTableUtil.createPutForSpaceSnapshot(tn1, snapshot1));
    puts.add(QuotaTableUtil.createPutForSpaceSnapshot(tn2, snapshot2));
    puts.add(QuotaTableUtil.createPutForSpaceSnapshot(tn3, snapshot3));
    final Map<TableName,SpaceQuotaSnapshot> expectedPolicies = new HashMap<>();
    expectedPolicies.put(tn1, snapshot1);
    expectedPolicies.put(tn2, snapshot2);
    expectedPolicies.put(tn3, snapshot3);

    final Map<TableName,SpaceQuotaSnapshot> actualPolicies = new HashMap<>();
    try (Table quotaTable = connection.getTable(QuotaUtil.QUOTA_TABLE_NAME)) {
      quotaTable.put(puts);
      ResultScanner scanner = quotaTable.getScanner(QuotaTableUtil.makeQuotaSnapshotScan());
      for (Result r : scanner) {
        QuotaTableUtil.extractQuotaSnapshot(r, actualPolicies);
      }
      scanner.close();
    }

    assertEquals(expectedPolicies, actualPolicies);
  }

  @Test
  public void testSerdeTableSnapshotSizes() throws Exception {
    TableName tn1 = TableName.valueOf("tn1");
    TableName tn2 = TableName.valueOf("tn2");
    try (Table quotaTable = connection.getTable(QuotaTableUtil.QUOTA_TABLE_NAME)) {
      for (int i = 0; i < 3; i++) {
        Put p = QuotaTableUtil.createPutForSnapshotSize(tn1, "tn1snap" + i, 1024L * (1+i));
        quotaTable.put(p);
      }
      for (int i = 0; i < 3; i++) {
        Put p = QuotaTableUtil.createPutForSnapshotSize(tn2, "tn2snap" + i, 2048L * (1+i));
        quotaTable.put(p);
      }

      verifyTableSnapshotSize(quotaTable, tn1, "tn1snap0", 1024L);
      verifyTableSnapshotSize(quotaTable, tn1, "tn1snap1", 2048L);
      verifyTableSnapshotSize(quotaTable, tn1, "tn1snap2", 3072L);

      verifyTableSnapshotSize(quotaTable, tn2, "tn2snap0", 2048L);
      verifyTableSnapshotSize(quotaTable, tn2, "tn2snap1", 4096L);
      verifyTableSnapshotSize(quotaTable, tn2, "tn2snap2", 6144L);

      cleanUpSnapshotSizes();
    }
  }

  @Test
  public void testReadNamespaceSnapshotSizes() throws Exception {
    String ns1 = "ns1";
    String ns2 = "ns2";
    String defaultNs = NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR;
    try (Table quotaTable = connection.getTable(QuotaTableUtil.QUOTA_TABLE_NAME)) {
      quotaTable.put(QuotaTableUtil.createPutForNamespaceSnapshotSize(ns1, 1024L));
      quotaTable.put(QuotaTableUtil.createPutForNamespaceSnapshotSize(ns2, 2048L));
      quotaTable.put(QuotaTableUtil.createPutForNamespaceSnapshotSize(defaultNs, 8192L));

      assertEquals(1024L, QuotaTableUtil.getNamespaceSnapshotSize(connection, ns1));
      assertEquals(2048L, QuotaTableUtil.getNamespaceSnapshotSize(connection, ns2));
      assertEquals(8192L, QuotaTableUtil.getNamespaceSnapshotSize(connection, defaultNs));

      cleanUpSnapshotSizes();
    }
  }

  private TableName getUniqueTableName() {
    return TableName.valueOf(testName.getMethodName() + "_" + tableNameCounter++);
  }

  private void verifyTableSnapshotSize(
      Table quotaTable, TableName tn, String snapshotName, long expectedSize) throws IOException {
    Result r = quotaTable.get(QuotaTableUtil.makeGetForSnapshotSize(tn, snapshotName));
    CellScanner cs = r.cellScanner();
    assertTrue(cs.advance());
    Cell c = cs.current();
    assertEquals(expectedSize, QuotaProtos.SpaceQuotaSnapshot.parseFrom(
        UnsafeByteOperations.unsafeWrap(
            c.getValueArray(), c.getValueOffset(), c.getValueLength())).getQuotaUsage());
    assertFalse(cs.advance());
  }

  private void cleanUpSnapshotSizes() throws IOException {
    try (Table t = connection.getTable(QuotaTableUtil.QUOTA_TABLE_NAME)) {
      QuotaTableUtil.createDeletesForExistingTableSnapshotSizes(connection);
      List<Delete> deletes =
          QuotaTableUtil.createDeletesForExistingNamespaceSnapshotSizes(connection);
      deletes.addAll(QuotaTableUtil.createDeletesForExistingTableSnapshotSizes(connection));
      t.delete(deletes);
    }
  }
}
