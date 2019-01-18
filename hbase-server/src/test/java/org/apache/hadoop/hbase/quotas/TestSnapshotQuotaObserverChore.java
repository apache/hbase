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
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.quotas.SnapshotQuotaObserverChore.SnapshotWithSize;
import org.apache.hadoop.hbase.quotas.SpaceQuotaHelperForTests.NoFilesToDischarge;
import org.apache.hadoop.hbase.quotas.SpaceQuotaHelperForTests.SpaceQuotaSnapshotPredicate;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.testclassification.MediumTests;
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

import org.apache.hbase.thirdparty.com.google.common.collect.HashMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;

/**
 * Test class for the {@link SnapshotQuotaObserverChore}.
 */
@Category(MediumTests.class)
public class TestSnapshotQuotaObserverChore {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSnapshotQuotaObserverChore.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotQuotaObserverChore.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final AtomicLong COUNTER = new AtomicLong();

  @Rule
  public TestName testName = new TestName();

  private Connection conn;
  private Admin admin;
  private SpaceQuotaHelperForTests helper;
  private HMaster master;
  private SnapshotQuotaObserverChore testChore;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    SpaceQuotaHelperForTests.updateConfigForQuotas(conf);
    // Clean up the compacted files faster than normal (15s instead of 2mins)
    conf.setInt("hbase.hfile.compaction.discharger.interval", 15 * 1000);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws Exception {
    conn = TEST_UTIL.getConnection();
    admin = TEST_UTIL.getAdmin();
    helper = new SpaceQuotaHelperForTests(TEST_UTIL, testName, COUNTER);
    master = TEST_UTIL.getHBaseCluster().getMaster();
    helper.removeAllQuotas(conn);
    testChore = new SnapshotQuotaObserverChore(
        TEST_UTIL.getConnection(), TEST_UTIL.getConfiguration(), master.getFileSystem(), master,
        null);
  }

  @Test
  public void testSnapshotSizePersistence() throws IOException {
    final Admin admin = TEST_UTIL.getAdmin();
    final TableName tn = TableName.valueOf("quota_snapshotSizePersistence");
    if (admin.tableExists(tn)) {
      admin.disableTable(tn);
      admin.deleteTable(tn);
    }
    HTableDescriptor desc = new HTableDescriptor(tn);
    desc.addFamily(new HColumnDescriptor(QuotaTableUtil.QUOTA_FAMILY_USAGE));
    admin.createTable(desc);

    Multimap<TableName,SnapshotWithSize> snapshotsWithSizes = HashMultimap.create();
    try (Table table = conn.getTable(tn)) {
      // Writing no values will result in no records written.
      verify(table, () -> {
        testChore.persistSnapshotSizes(table, snapshotsWithSizes);
        assertEquals(0, count(table));
      });

      verify(table, () -> {
        TableName originatingTable = TableName.valueOf("t1");
        snapshotsWithSizes.put(originatingTable, new SnapshotWithSize("ss1", 1024L));
        snapshotsWithSizes.put(originatingTable, new SnapshotWithSize("ss2", 4096L));
        testChore.persistSnapshotSizes(table, snapshotsWithSizes);
        assertEquals(2, count(table));
        assertEquals(1024L, extractSnapshotSize(table, originatingTable, "ss1"));
        assertEquals(4096L, extractSnapshotSize(table, originatingTable, "ss2"));
      });

      snapshotsWithSizes.clear();
      verify(table, () -> {
        snapshotsWithSizes.put(TableName.valueOf("t1"), new SnapshotWithSize("ss1", 1024L));
        snapshotsWithSizes.put(TableName.valueOf("t2"), new SnapshotWithSize("ss2", 4096L));
        snapshotsWithSizes.put(TableName.valueOf("t3"), new SnapshotWithSize("ss3", 8192L));
        testChore.persistSnapshotSizes(table, snapshotsWithSizes);
        assertEquals(3, count(table));
        assertEquals(1024L, extractSnapshotSize(table, TableName.valueOf("t1"), "ss1"));
        assertEquals(4096L, extractSnapshotSize(table, TableName.valueOf("t2"), "ss2"));
        assertEquals(8192L, extractSnapshotSize(table, TableName.valueOf("t3"), "ss3"));
      });
    }
  }

  @Test
  public void testSnapshotsFromTables() throws Exception {
    TableName tn1 = helper.createTableWithRegions(1);
    TableName tn2 = helper.createTableWithRegions(1);
    TableName tn3 = helper.createTableWithRegions(1);

    // Set a space quota on table 1 and 2 (but not 3)
    admin.setQuota(QuotaSettingsFactory.limitTableSpace(
        tn1, SpaceQuotaHelperForTests.ONE_GIGABYTE, SpaceViolationPolicy.NO_INSERTS));
    admin.setQuota(QuotaSettingsFactory.limitTableSpace(
        tn2, SpaceQuotaHelperForTests.ONE_GIGABYTE, SpaceViolationPolicy.NO_INSERTS));

    // Create snapshots on each table (we didn't write any data, so just skipflush)
    admin.snapshot(new SnapshotDescription(tn1 + "snapshot", tn1, SnapshotType.SKIPFLUSH));
    admin.snapshot(new SnapshotDescription(tn2 + "snapshot", tn2, SnapshotType.SKIPFLUSH));
    admin.snapshot(new SnapshotDescription(tn3 + "snapshot", tn3, SnapshotType.SKIPFLUSH));

    Multimap<TableName,String> mapping = testChore.getSnapshotsToComputeSize();
    assertEquals(2, mapping.size());
    assertEquals(1, mapping.get(tn1).size());
    assertEquals(tn1 + "snapshot", mapping.get(tn1).iterator().next());
    assertEquals(1, mapping.get(tn2).size());
    assertEquals(tn2 + "snapshot", mapping.get(tn2).iterator().next());

    admin.snapshot(new SnapshotDescription(tn2 + "snapshot1", tn2, SnapshotType.SKIPFLUSH));
    admin.snapshot(new SnapshotDescription(tn3 + "snapshot1", tn3, SnapshotType.SKIPFLUSH));

    mapping = testChore.getSnapshotsToComputeSize();
    assertEquals(3, mapping.size());
    assertEquals(1, mapping.get(tn1).size());
    assertEquals(tn1 + "snapshot", mapping.get(tn1).iterator().next());
    assertEquals(2, mapping.get(tn2).size());
    assertEquals(
        new HashSet<String>(Arrays.asList(tn2 + "snapshot", tn2 + "snapshot1")), mapping.get(tn2));
  }

  @Test
  public void testSnapshotsFromNamespaces() throws Exception {
    NamespaceDescriptor ns = NamespaceDescriptor.create("snapshots_from_namespaces").build();
    admin.createNamespace(ns);

    TableName tn1 = helper.createTableWithRegions(ns.getName(), 1);
    TableName tn2 = helper.createTableWithRegions(ns.getName(), 1);
    TableName tn3 = helper.createTableWithRegions(1);

    // Set a throttle quota on 'default' namespace
    admin.setQuota(QuotaSettingsFactory.throttleNamespace(tn3.getNamespaceAsString(),
      ThrottleType.WRITE_NUMBER, 100, TimeUnit.SECONDS));
    // Set a user throttle quota
    admin.setQuota(
      QuotaSettingsFactory.throttleUser("user", ThrottleType.WRITE_NUMBER, 100, TimeUnit.MINUTES));

    // Set a space quota on the namespace
    admin.setQuota(QuotaSettingsFactory.limitNamespaceSpace(
        ns.getName(), SpaceQuotaHelperForTests.ONE_GIGABYTE, SpaceViolationPolicy.NO_INSERTS));

    // Create snapshots on each table (we didn't write any data, so just skipflush)
    admin.snapshot(new SnapshotDescription(
        tn1.getQualifierAsString() + "snapshot", tn1, SnapshotType.SKIPFLUSH));
    admin.snapshot(new SnapshotDescription(
        tn2.getQualifierAsString() + "snapshot", tn2, SnapshotType.SKIPFLUSH));
    admin.snapshot(new SnapshotDescription(
        tn3.getQualifierAsString() + "snapshot", tn3, SnapshotType.SKIPFLUSH));

    Multimap<TableName,String> mapping = testChore.getSnapshotsToComputeSize();
    assertEquals(2, mapping.size());
    assertEquals(1, mapping.get(tn1).size());
    assertEquals(tn1.getQualifierAsString() + "snapshot", mapping.get(tn1).iterator().next());
    assertEquals(1, mapping.get(tn2).size());
    assertEquals(tn2.getQualifierAsString() + "snapshot", mapping.get(tn2).iterator().next());

    admin.snapshot(new SnapshotDescription(
        tn2.getQualifierAsString() + "snapshot1", tn2, SnapshotType.SKIPFLUSH));
    admin.snapshot(new SnapshotDescription(
        tn3.getQualifierAsString() + "snapshot2", tn3, SnapshotType.SKIPFLUSH));

    mapping = testChore.getSnapshotsToComputeSize();
    assertEquals(3, mapping.size());
    assertEquals(1, mapping.get(tn1).size());
    assertEquals(tn1.getQualifierAsString() + "snapshot", mapping.get(tn1).iterator().next());
    assertEquals(2, mapping.get(tn2).size());
    assertEquals(
        new HashSet<String>(Arrays.asList(tn2.getQualifierAsString() + "snapshot",
            tn2.getQualifierAsString() + "snapshot1")), mapping.get(tn2));
  }

  @Test
  public void testSnapshotSize() throws Exception {
    // Create a table and set a quota
    TableName tn1 = helper.createTableWithRegions(5);
    admin.setQuota(QuotaSettingsFactory.limitTableSpace(
        tn1, SpaceQuotaHelperForTests.ONE_GIGABYTE, SpaceViolationPolicy.NO_INSERTS));

    // Write some data and flush it
    helper.writeData(tn1, 256L * SpaceQuotaHelperForTests.ONE_KILOBYTE);
    admin.flush(tn1);

    final long snapshotSize = TEST_UTIL.getMiniHBaseCluster().getRegions(tn1).stream()
        .flatMap(r -> r.getStores().stream()).mapToLong(HStore::getHFilesSize).sum();

    // Wait for the Master chore to run to see the usage (with a fudge factor)
    TEST_UTIL.waitFor(30_000, new SpaceQuotaSnapshotPredicate(conn, tn1) {
      @Override
      boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return snapshot.getUsage() == snapshotSize;
      }
    });

    // Create a snapshot on the table
    final String snapshotName = tn1 + "snapshot";
    admin.snapshot(new SnapshotDescription(snapshotName, tn1, SnapshotType.SKIPFLUSH));

    // Get the snapshots
    Multimap<TableName,String> snapshotsToCompute = testChore.getSnapshotsToComputeSize();
    assertEquals(
        "Expected to see the single snapshot: " + snapshotsToCompute, 1, snapshotsToCompute.size());

    // Get the size of our snapshot
    Multimap<TableName,SnapshotWithSize> snapshotsWithSize = testChore.computeSnapshotSizes(
        snapshotsToCompute);
    assertEquals(1, snapshotsWithSize.size());
    SnapshotWithSize sws = Iterables.getOnlyElement(snapshotsWithSize.get(tn1));
    assertEquals(snapshotName, sws.getName());
    // The snapshot should take up no space since the table refers to it completely
    assertEquals(0, sws.getSize());

    // Write some more data, flush it, and then major_compact the table
    helper.writeData(tn1, 256L * SpaceQuotaHelperForTests.ONE_KILOBYTE);
    admin.flush(tn1);
    TEST_UTIL.compact(tn1, true);

    // Test table should reflect it's original size since ingest was deterministic
    TEST_UTIL.waitFor(30_000, new SpaceQuotaSnapshotPredicate(conn, tn1) {
      private final long regionSize = TEST_UTIL.getMiniHBaseCluster().getRegions(tn1).stream()
          .flatMap(r -> r.getStores().stream()).mapToLong(HStore::getHFilesSize).sum();

      @Override
      boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        LOG.debug("Current usage=" + snapshot.getUsage() + " snapshotSize=" + snapshotSize);
        // The usage of table space consists of region size and snapshot size
        return closeInSize(snapshot.getUsage(), snapshotSize + regionSize,
            SpaceQuotaHelperForTests.ONE_KILOBYTE);
      }
    });

    // Wait for no compacted files on the regions of our table
    TEST_UTIL.waitFor(30_000, new NoFilesToDischarge(TEST_UTIL.getMiniHBaseCluster(), tn1));

    // Still should see only one snapshot
    snapshotsToCompute = testChore.getSnapshotsToComputeSize();
    assertEquals(
        "Expected to see the single snapshot: " + snapshotsToCompute, 1, snapshotsToCompute.size());
    snapshotsWithSize = testChore.computeSnapshotSizes(
            snapshotsToCompute);
    assertEquals(1, snapshotsWithSize.size());
    sws = Iterables.getOnlyElement(snapshotsWithSize.get(tn1));
    assertEquals(snapshotName, sws.getName());
    // The snapshot should take up the size the table originally took up
    assertEquals(snapshotSize, sws.getSize());
  }

  @Test
  public void testPersistingSnapshotsForNamespaces() throws Exception {
    Multimap<TableName,SnapshotWithSize> snapshotsWithSizes = HashMultimap.create();
    TableName tn1 = TableName.valueOf("ns1:tn1");
    TableName tn2 = TableName.valueOf("ns1:tn2");
    TableName tn3 = TableName.valueOf("ns2:tn1");
    TableName tn4 = TableName.valueOf("ns2:tn2");
    TableName tn5 = TableName.valueOf("tn1");

    snapshotsWithSizes.put(tn1, new SnapshotWithSize("", 1024L));
    snapshotsWithSizes.put(tn2, new SnapshotWithSize("", 1024L));
    snapshotsWithSizes.put(tn3, new SnapshotWithSize("", 512L));
    snapshotsWithSizes.put(tn4, new SnapshotWithSize("", 1024L));
    snapshotsWithSizes.put(tn5, new SnapshotWithSize("", 3072L));

    Map<String,Long> nsSizes = testChore.groupSnapshotSizesByNamespace(snapshotsWithSizes);
    assertEquals(3, nsSizes.size());
    assertEquals(2048L, (long) nsSizes.get("ns1"));
    assertEquals(1536L, (long) nsSizes.get("ns2"));
    assertEquals(3072L, (long) nsSizes.get(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR));
  }

  private long count(Table t) throws IOException {
    try (ResultScanner rs = t.getScanner(new Scan())) {
      long sum = 0;
      for (Result r : rs) {
        while (r.advance()) {
          sum++;
        }
      }
      return sum;
    }
  }

  private long extractSnapshotSize(
      Table quotaTable, TableName tn, String snapshot) throws IOException {
    Get g = QuotaTableUtil.makeGetForSnapshotSize(tn, snapshot);
    Result r = quotaTable.get(g);
    assertNotNull(r);
    CellScanner cs = r.cellScanner();
    cs.advance();
    Cell c = cs.current();
    assertNotNull(c);
    return QuotaTableUtil.extractSnapshotSize(
        c.getValueArray(), c.getValueOffset(), c.getValueLength());
  }

  private void verify(Table t, IOThrowingRunnable test) throws IOException {
    admin.disableTable(t.getName());
    admin.truncateTable(t.getName(), false);
    test.run();
  }

  @FunctionalInterface
  private interface IOThrowingRunnable {
    void run() throws IOException;
  }

  /**
   * Computes if {@code size2} is within {@code delta} of {@code size1}, inclusive.
   */
  boolean closeInSize(long size1, long size2, long delta) {
    long lower = size1 - delta;
    long upper = size1 + delta;
    return lower <= size2 && size2 <= upper;
  }
}
