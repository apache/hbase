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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot.SpaceQuotaStatus;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Throttle;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test the quota table helpers (e.g. CRUD operations)
 */
@Category({MasterTests.class, MediumTests.class})
public class TestQuotaTableUtil {

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
    puts.add(QuotaTableUtil.createPutSpaceSnapshot(tn1, snapshot1));
    puts.add(QuotaTableUtil.createPutSpaceSnapshot(tn2, snapshot2));
    puts.add(QuotaTableUtil.createPutSpaceSnapshot(tn3, snapshot3));
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

  private TableName getUniqueTableName() {
    return TableName.valueOf(testName.getMethodName() + "_" + tableNameCounter++);
  }
}
