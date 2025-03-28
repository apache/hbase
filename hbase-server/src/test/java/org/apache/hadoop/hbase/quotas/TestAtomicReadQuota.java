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
package org.apache.hadoop.hbase.quotas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestAtomicReadQuota {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAtomicReadQuota.class);
  private static final Logger LOG = LoggerFactory.getLogger(TestAtomicReadQuota.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final TableName TABLE_NAME = TableName.valueOf(UUID.randomUUID().toString());
  private static final byte[] FAMILY = Bytes.toBytes("cf");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");

  @AfterClass
  public static void tearDown() throws Exception {
    ThrottleQuotaTestUtil.clearQuotaCache(TEST_UTIL);
    EnvironmentEdgeManager.reset();
    TEST_UTIL.deleteTable(TABLE_NAME);
    TEST_UTIL.shutdownMiniCluster();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REFRESH_CONF_KEY, 1000);
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    QuotaCache.TEST_FORCE_REFRESH = true;
  }

  @Test
  public void testIncrementCountedAgainstReadCapacity() throws Exception {
    setupGenericQuota();

    Increment inc = new Increment(Bytes.toBytes(UUID.randomUUID().toString()));
    inc.addColumn(FAMILY, QUALIFIER, 1);
    testThrottle(table -> table.increment(inc));
  }

  @Test
  public void testConditionalRowMutationsCountedAgainstReadCapacity() throws Exception {
    setupGenericQuota();

    byte[] row = Bytes.toBytes(UUID.randomUUID().toString());
    Increment inc = new Increment(row);
    inc.addColumn(FAMILY, Bytes.toBytes("doot"), 1);
    Put put = new Put(row);
    put.addColumn(FAMILY, Bytes.toBytes("doot"), Bytes.toBytes("v"));

    RowMutations rowMutations = new RowMutations(row);
    rowMutations.add(inc);
    rowMutations.add(put);
    testThrottle(table -> table.mutateRow(rowMutations));
  }

  @Test
  public void testNonConditionalRowMutationsOmittedFromReadCapacity() throws Exception {
    setupGenericQuota();

    byte[] row = Bytes.toBytes(UUID.randomUUID().toString());
    Put put = new Put(row);
    put.addColumn(FAMILY, Bytes.toBytes("doot"), Bytes.toBytes("v"));

    RowMutations rowMutations = new RowMutations(row);
    rowMutations.add(put);
    try (Table table = getTable()) {
      for (int i = 0; i < 100; i++) {
        table.mutateRow(rowMutations);
      }
    }
  }

  @Test
  public void testNonAtomicPutOmittedFromReadCapacity() throws Exception {
    setupGenericQuota();
    runNonAtomicPuts();
  }

  @Test
  public void testNonAtomicMultiPutOmittedFromReadCapacity() throws Exception {
    setupGenericQuota();
    runNonAtomicPuts();
  }

  @Test
  public void testCheckAndMutateCountedAgainstReadCapacity() throws Exception {
    setupGenericQuota();

    byte[] row = Bytes.toBytes(UUID.randomUUID().toString());
    byte[] value = Bytes.toBytes("v");
    Put put = new Put(row);
    put.addColumn(FAMILY, Bytes.toBytes("doot"), value);
    CheckAndMutate checkAndMutate =
      CheckAndMutate.newBuilder(row).ifEquals(FAMILY, QUALIFIER, value).build(put);

    testThrottle(table -> table.checkAndMutate(checkAndMutate));
  }

  @Test
  public void testAtomicBatchCountedAgainstReadCapacity() throws Exception {
    setupGenericQuota();

    byte[] row = Bytes.toBytes(UUID.randomUUID().toString());
    Increment inc = new Increment(row);
    inc.addColumn(FAMILY, Bytes.toBytes("doot"), 1);

    List<Increment> incs = new ArrayList<>(2);
    incs.add(inc);
    incs.add(inc);

    testThrottle(table -> {
      Object[] results = new Object[] {};
      table.batch(incs, results);
      return results;
    });
  }

  @Test
  public void testAtomicBatchCountedAgainstAtomicOnlyReqNum() throws Exception {
    setupAtomicOnlyReqNumQuota();

    byte[] row = Bytes.toBytes(UUID.randomUUID().toString());
    Increment inc = new Increment(row);
    inc.addColumn(FAMILY, Bytes.toBytes("doot"), 1);

    List<Increment> incs = new ArrayList<>(2);
    incs.add(inc);
    incs.add(inc);

    testThrottle(table -> {
      Object[] results = new Object[] {};
      table.batch(incs, results);
      return results;
    });
  }

  @Test
  public void testAtomicBatchCountedAgainstAtomicOnlyReadSize() throws Exception {
    setupAtomicOnlyReadSizeQuota();

    byte[] row = Bytes.toBytes(UUID.randomUUID().toString());
    Increment inc = new Increment(row);
    inc.addColumn(FAMILY, Bytes.toBytes("doot"), 1);

    List<Increment> incs = new ArrayList<>(2);
    incs.add(inc);
    incs.add(inc);

    testThrottle(table -> {
      Object[] results = new Object[] {};
      table.batch(incs, results);
      return results;
    });
  }

  @Test
  public void testNonAtomicWritesIgnoredByAtomicOnlyReqNum() throws Exception {
    setupAtomicOnlyReqNumQuota();
    runNonAtomicPuts();
  }

  @Test
  public void testNonAtomicWritesIgnoredByAtomicOnlyReadSize() throws Exception {
    setupAtomicOnlyReadSizeQuota();
    runNonAtomicPuts();
  }

  @Test
  public void testNonAtomicReadsIgnoredByAtomicOnlyReqNum() throws Exception {
    setupAtomicOnlyReqNumQuota();
    runNonAtomicReads();
  }

  @Test
  public void testNonAtomicReadsIgnoredByAtomicOnlyReadSize() throws Exception {
    setupAtomicOnlyReadSizeQuota();
    runNonAtomicReads();
  }

  private void runNonAtomicPuts() throws Exception {
    Put put1 = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
    put1.addColumn(FAMILY, Bytes.toBytes("doot"), Bytes.toBytes("v"));
    Put put2 = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
    put2.addColumn(FAMILY, Bytes.toBytes("doot"), Bytes.toBytes("v"));

    Increment inc = new Increment(Bytes.toBytes(UUID.randomUUID().toString()));
    inc.addColumn(FAMILY, Bytes.toBytes("doot"), 1);

    List<Put> puts = new ArrayList<>(2);
    puts.add(put1);
    puts.add(put2);

    try (Table table = getTable()) {
      for (int i = 0; i < 100; i++) {
        table.put(puts);
      }
    }
  }

  private void runNonAtomicReads() throws Exception {
    try (Table table = getTable()) {
      byte[] row = Bytes.toBytes(UUID.randomUUID().toString());
      Get get = new Get(row);
      table.get(get);
    }
  }

  private void setupGenericQuota() throws Exception {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      admin.setQuota(QuotaSettingsFactory.throttleUser(User.getCurrent().getShortName(),
        ThrottleType.READ_NUMBER, 1, TimeUnit.MINUTES));
    }
    ThrottleQuotaTestUtil.triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAME);
  }

  private void setupAtomicOnlyReqNumQuota() throws Exception {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      admin.setQuota(QuotaSettingsFactory.throttleUser(User.getCurrent().getShortName(),
        ThrottleType.ATOMIC_REQUEST_NUMBER, 1, TimeUnit.MINUTES));
    }
    ThrottleQuotaTestUtil.triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAME);
  }

  private void setupAtomicOnlyReadSizeQuota() throws Exception {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      admin.setQuota(QuotaSettingsFactory.throttleUser(User.getCurrent().getShortName(),
        ThrottleType.ATOMIC_READ_SIZE, 1, TimeUnit.MINUTES));
    }
    ThrottleQuotaTestUtil.triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAME);
  }

  private void cleanupQuota() throws Exception {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      admin.setQuota(QuotaSettingsFactory.unthrottleUser(User.getCurrent().getShortName()));
    }
    ThrottleQuotaTestUtil.triggerUserCacheRefresh(TEST_UTIL, true, TABLE_NAME);
  }

  private void testThrottle(ThrowingFunction<Table, ?> request) throws Exception {
    try (Table table = getTable()) {
      // we have a read quota configured, so this should fail
      TEST_UTIL.waitFor(60_000, () -> {
        try {
          request.run(table);
          return false;
        } catch (Exception e) {
          boolean success = e.getCause() instanceof RpcThrottlingException;
          if (!success) {
            LOG.error("Unexpected exception", e);
          }
          return success;
        }
      });
    } finally {
      cleanupQuota();
    }
  }

  private Table getTable() throws IOException {
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 100);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    return TEST_UTIL.getConnection().getTableBuilder(TABLE_NAME, null).setOperationTimeout(250)
      .build();
  }

  @FunctionalInterface
  private interface ThrowingFunction<I, O> {
    O run(I input) throws Exception;
  }

}
