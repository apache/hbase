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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.quotas.SpaceQuotaHelperForTests.SpaceQuotaSnapshotPredicate;
import org.apache.hadoop.hbase.testclassification.LargeTests;
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

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;

/**
 * Test class to exercise the inclusion of snapshots in space quotas
 */
@Category({LargeTests.class})
public class TestSpaceQuotasWithSnapshots {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSpaceQuotasWithSnapshots.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSpaceQuotasWithSnapshots.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  // Global for all tests in the class
  private static final AtomicLong COUNTER = new AtomicLong(0);
  private static final long FUDGE_FOR_TABLE_SIZE = 500L * SpaceQuotaHelperForTests.ONE_KILOBYTE;

  @Rule
  public TestName testName = new TestName();
  private SpaceQuotaHelperForTests helper;
  private Connection conn;
  private Admin admin;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    SpaceQuotaHelperForTests.updateConfigForQuotas(conf);
    TEST_UTIL.startMiniCluster(1);
    // Wait till quota table onlined.
    TEST_UTIL.waitFor(10000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return TEST_UTIL.getAdmin().tableExists(QuotaTableUtil.QUOTA_TABLE_NAME);
      }
    });
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void removeAllQuotas() throws Exception {
    helper = new SpaceQuotaHelperForTests(TEST_UTIL, testName, COUNTER);
    conn = TEST_UTIL.getConnection();
    admin = TEST_UTIL.getAdmin();
  }

  @Test
  public void testTablesInheritSnapshotSize() throws Exception {
    TableName tn = helper.createTableWithRegions(1);
    LOG.info("Writing data");
    // Set a quota
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(
        tn, SpaceQuotaHelperForTests.ONE_GIGABYTE, SpaceViolationPolicy.NO_INSERTS);
    admin.setQuota(settings);
    // Write some data
    final long initialSize = 2L * SpaceQuotaHelperForTests.ONE_MEGABYTE;
    helper.writeData(tn, initialSize);

    LOG.info("Waiting until table size reflects written data");
    // Wait until that data is seen by the master
    TEST_UTIL.waitFor(30 * 1000, 500, new SpaceQuotaSnapshotPredicate(conn, tn) {
      @Override boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return snapshot.getUsage() >= initialSize;
      }
    });

    // Make sure we see the final quota usage size
    waitForStableQuotaSize(conn, tn, null);

    // The actual size on disk after we wrote our data the first time
    final long actualInitialSize = conn.getAdmin().getCurrentSpaceQuotaSnapshot(tn).getUsage();
    LOG.info("Initial table size was " + actualInitialSize);

    LOG.info("Snapshot the table");
    final String snapshot1 = tn.toString() + "_snapshot1";
    admin.snapshot(snapshot1, tn);

    // Write the same data again, then flush+compact. This should make sure that
    // the snapshot is referencing files that the table no longer references.
    LOG.info("Write more data");
    helper.writeData(tn, initialSize);
    LOG.info("Flush the table");
    admin.flush(tn);
    LOG.info("Synchronously compacting the table");
    TEST_UTIL.compact(tn, true);

    final long upperBound = initialSize + FUDGE_FOR_TABLE_SIZE;
    final long lowerBound = initialSize - FUDGE_FOR_TABLE_SIZE;

    // Store the actual size after writing more data and then compacting it down to one file
    LOG.info("Waiting for the region reports to reflect the correct size, between ("
        + lowerBound + ", " + upperBound + ")");
    TEST_UTIL.waitFor(30 * 1000, 500, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        long size = getRegionSizeReportForTable(conn, tn);
        return size < upperBound && size > lowerBound;
      }
    });

    // Make sure we see the "final" new size for the table, not some intermediate
    waitForStableRegionSizeReport(conn, tn);
    final long finalSize = getRegionSizeReportForTable(conn, tn);
    assertNotNull("Did not expect to see a null size", finalSize);
    LOG.info("Last seen size: " + finalSize);

    // Make sure the QuotaObserverChore has time to reflect the new region size reports
    // (we saw above). The usage of the table should *not* decrease when we check it below,
    // though, because the snapshot on our table will cause the table to "retain" the size.
    TEST_UTIL.waitFor(20 * 1000, 500, new SpaceQuotaSnapshotPredicate(conn, tn) {
      @Override
      public boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return snapshot.getUsage() >= finalSize;
      }
    });

    // The final usage should be the sum of the initial size (referenced by the snapshot) and the
    // new size we just wrote above.
    long expectedFinalSize = actualInitialSize + finalSize;
    LOG.info(
        "Expecting table usage to be " + actualInitialSize + " + " + finalSize
        + " = " + expectedFinalSize);
    // The size of the table (WRT quotas) should now be approximately double what it was previously
    TEST_UTIL.waitFor(30 * 1000, 1000, new SpaceQuotaSnapshotPredicate(conn, tn) {
      @Override boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        LOG.debug("Checking for " + expectedFinalSize + " == " + snapshot.getUsage());
        return expectedFinalSize == snapshot.getUsage();
      }
    });

    Map<String,Long> snapshotSizes = QuotaTableUtil.getObservedSnapshotSizes(conn);
    Long size = snapshotSizes.get(snapshot1);
    assertNotNull("Did not observe the size of the snapshot", size);
    assertEquals(
        "The recorded size of the HBase snapshot was not the size we expected", actualInitialSize,
        size.longValue());
  }

  @Test
  public void testNamespacesInheritSnapshotSize() throws Exception {
    String ns = helper.createNamespace().getName();
    TableName tn = helper.createTableWithRegions(ns, 1);
    LOG.info("Writing data");
    // Set a quota
    QuotaSettings settings = QuotaSettingsFactory.limitNamespaceSpace(
        ns, SpaceQuotaHelperForTests.ONE_GIGABYTE, SpaceViolationPolicy.NO_INSERTS);
    admin.setQuota(settings);

    // Write some data and flush it to disk
    final long initialSize = 2L * SpaceQuotaHelperForTests.ONE_MEGABYTE;
    helper.writeData(tn, initialSize);
    admin.flush(tn);

    LOG.info("Waiting until namespace size reflects written data");
    // Wait until that data is seen by the master
    TEST_UTIL.waitFor(30 * 1000, 500, new SpaceQuotaSnapshotPredicate(conn, ns) {
      @Override boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return snapshot.getUsage() >= initialSize;
      }
    });

    // Make sure we see the "final" new size for the table, not some intermediate
    waitForStableQuotaSize(conn, null, ns);

    // The actual size on disk after we wrote our data the first time
    final long actualInitialSize = conn.getAdmin().getCurrentSpaceQuotaSnapshot(ns).getUsage();
    LOG.info("Initial table size was " + actualInitialSize);

    LOG.info("Snapshot the table");
    final String snapshot1 = tn.getQualifierAsString() + "_snapshot1";
    admin.snapshot(snapshot1, tn);

    // Write the same data again, then flush+compact. This should make sure that
    // the snapshot is referencing files that the table no longer references.
    LOG.info("Write more data");
    helper.writeData(tn, initialSize);
    LOG.info("Flush the table");
    admin.flush(tn);
    LOG.info("Synchronously compacting the table");
    TEST_UTIL.compact(tn, true);

    final long upperBound = initialSize + FUDGE_FOR_TABLE_SIZE;
    final long lowerBound = initialSize - FUDGE_FOR_TABLE_SIZE;

    LOG.info("Waiting for the region reports to reflect the correct size, between ("
        + lowerBound + ", " + upperBound + ")");
    TEST_UTIL.waitFor(30 * 1000, 500, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        Map<TableName, Long> sizes = conn.getAdmin().getSpaceQuotaTableSizes();
        LOG.debug("Master observed table sizes from region size reports: " + sizes);
        Long size = sizes.get(tn);
        if (null == size) {
          return false;
        }
        return size < upperBound && size > lowerBound;
      }
    });

    // Make sure we see the "final" new size for the table, not some intermediate
    waitForStableRegionSizeReport(conn, tn);
    final long finalSize = getRegionSizeReportForTable(conn, tn);
    assertNotNull("Did not expect to see a null size", finalSize);
    LOG.info("Final observed size of table: " + finalSize);

    // Make sure the QuotaObserverChore has time to reflect the new region size reports
    // (we saw above). The usage of the table should *not* decrease when we check it below,
    // though, because the snapshot on our table will cause the table to "retain" the size.
    TEST_UTIL.waitFor(20 * 1000, 500, new SpaceQuotaSnapshotPredicate(conn, ns) {
      @Override
      public boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return snapshot.getUsage() >= finalSize;
      }
    });

    // The final usage should be the sum of the initial size (referenced by the snapshot) and the
    // new size we just wrote above.
    long expectedFinalSize = actualInitialSize + finalSize;
    LOG.info(
        "Expecting namespace usage to be " + actualInitialSize + " + " + finalSize
        + " = " + expectedFinalSize);
    // The size of the table (WRT quotas) should now be approximately double what it was previously
    TEST_UTIL.waitFor(30 * 1000, 1000, new SpaceQuotaSnapshotPredicate(conn, ns) {
      @Override boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        LOG.debug("Checking for " + expectedFinalSize + " == " + snapshot.getUsage());
        return expectedFinalSize == snapshot.getUsage();
      }
    });

    Map<String,Long> snapshotSizes = QuotaTableUtil.getObservedSnapshotSizes(conn);
    Long size = snapshotSizes.get(snapshot1);
    assertNotNull("Did not observe the size of the snapshot", size);
    assertEquals(
        "The recorded size of the HBase snapshot was not the size we expected", actualInitialSize,
        size.longValue());
  }

  @Test
  public void testTablesWithSnapshots() throws Exception {
    final Connection conn = TEST_UTIL.getConnection();
    final SpaceViolationPolicy policy = SpaceViolationPolicy.NO_INSERTS;
    final TableName tn = helper.createTableWithRegions(10);

    // 3MB limit on the table
    final long tableLimit = 3L * SpaceQuotaHelperForTests.ONE_MEGABYTE;
    TEST_UTIL.getAdmin().setQuota(QuotaSettingsFactory.limitTableSpace(tn, tableLimit, policy));

    LOG.info("Writing first data set");
    // Write more data than should be allowed and flush it to disk
    helper.writeData(tn, 1L * SpaceQuotaHelperForTests.ONE_MEGABYTE, "q1");

    LOG.info("Creating snapshot");
    TEST_UTIL.getAdmin().snapshot(tn.toString() + "snap1", tn, SnapshotType.FLUSH);

    LOG.info("Writing second data set");
    // Write some more data
    helper.writeData(tn, 1L * SpaceQuotaHelperForTests.ONE_MEGABYTE, "q2");

    LOG.info("Flushing and major compacting table");
    // Compact the table to force the snapshot to own all of its files
    TEST_UTIL.getAdmin().flush(tn);
    TEST_UTIL.compact(tn, true);

    LOG.info("Checking for quota violation");
    // Wait to observe the quota moving into violation
    TEST_UTIL.waitFor(60_000, 1_000, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        Scan s = QuotaTableUtil.makeQuotaSnapshotScanForTable(tn);
        try (Table t = conn.getTable(QuotaTableUtil.QUOTA_TABLE_NAME)) {
          ResultScanner rs = t.getScanner(s);
          try {
            Result r = Iterables.getOnlyElement(rs);
            CellScanner cs = r.cellScanner();
            assertTrue(cs.advance());
            Cell c = cs.current();
            SpaceQuotaSnapshot snapshot = SpaceQuotaSnapshot.toSpaceQuotaSnapshot(
                QuotaProtos.SpaceQuotaSnapshot.parseFrom(
                  UnsafeByteOperations.unsafeWrap(
                      c.getValueArray(), c.getValueOffset(), c.getValueLength())));
            LOG.info(
                snapshot.getUsage() + "/" + snapshot.getLimit() + " " + snapshot.getQuotaStatus());
            // We expect to see the table move to violation
            return snapshot.getQuotaStatus().isInViolation();
          } finally {
            if (null != rs) {
              rs.close();
            }
          }
        }
      }
    });
  }

  @Test
  public void testRematerializedTablesDoNoInheritSpace() throws Exception {
    TableName tn = helper.createTableWithRegions(1);
    TableName tn2 = helper.getNextTableName();
    LOG.info("Writing data");
    // Set a quota on both tables
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(
        tn, SpaceQuotaHelperForTests.ONE_GIGABYTE, SpaceViolationPolicy.NO_INSERTS);
    admin.setQuota(settings);
    QuotaSettings settings2 = QuotaSettingsFactory.limitTableSpace(
        tn2, SpaceQuotaHelperForTests.ONE_GIGABYTE, SpaceViolationPolicy.NO_INSERTS);
    admin.setQuota(settings2);
    // Write some data
    final long initialSize = 2L * SpaceQuotaHelperForTests.ONE_MEGABYTE;
    helper.writeData(tn, initialSize);

    LOG.info("Waiting until table size reflects written data");
    // Wait until that data is seen by the master
    TEST_UTIL.waitFor(30 * 1000, 500, new SpaceQuotaSnapshotPredicate(conn, tn) {
      @Override boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return snapshot.getUsage() >= initialSize;
      }
    });

    // Make sure we see the final quota usage size
    waitForStableQuotaSize(conn, tn, null);

    // The actual size on disk after we wrote our data the first time
    final long actualInitialSize = conn.getAdmin().getCurrentSpaceQuotaSnapshot(tn).getUsage();
    LOG.info("Initial table size was " + actualInitialSize);

    LOG.info("Snapshot the table");
    final String snapshot1 = tn.toString() + "_snapshot1";
    admin.snapshot(snapshot1, tn);

    admin.cloneSnapshot(snapshot1, tn2);

    // Write some more data to the first table
    helper.writeData(tn, initialSize, "q2");
    admin.flush(tn);

    // Watch the usage of the first table with some more data to know when the new
    // region size reports were sent to the master
    TEST_UTIL.waitFor(30_000, 1_000, new SpaceQuotaSnapshotPredicate(conn, tn) {
      @Override
      boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return snapshot.getUsage() >= actualInitialSize * 2;
      }
    });

    // We know that reports were sent by our RS, verify that they take up zero size.
    SpaceQuotaSnapshot snapshot =
      (SpaceQuotaSnapshot) conn.getAdmin().getCurrentSpaceQuotaSnapshot(tn2);
    assertNotNull(snapshot);
    assertEquals(0, snapshot.getUsage());

    // Compact the cloned table to force it to own its own files.
    TEST_UTIL.compact(tn2, true);
    // After the table is compacted, it should have its own files and be the same size as originally
    // But The compaction result file has an additional compaction event tracker
    TEST_UTIL.waitFor(30_000, 1_000, new SpaceQuotaSnapshotPredicate(conn, tn2) {
      @Override
      boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return snapshot.getUsage() >= actualInitialSize;
      }
    });
  }

  void waitForStableQuotaSize(Connection conn, TableName tn, String ns) throws Exception {
    // For some stability in the value before proceeding
    // Helps make sure that we got the actual last value, not some inbetween
    AtomicLong lastValue = new AtomicLong(-1);
    AtomicInteger counter = new AtomicInteger(0);
    TEST_UTIL.waitFor(15_000, 500, new SpaceQuotaSnapshotPredicate(conn, tn, ns) {
      @Override boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        LOG.debug("Last observed size=" + lastValue.get());
        if (snapshot.getUsage() == lastValue.get()) {
          int numMatches = counter.incrementAndGet();
          if (numMatches >= 5) {
            return true;
          }
          // Not yet..
          return false;
        }
        counter.set(0);
        lastValue.set(snapshot.getUsage());
        return false;
      }
    });
  }

  long getRegionSizeReportForTable(Connection conn, TableName tn) throws IOException {
    Map<TableName, Long> sizes = conn.getAdmin().getSpaceQuotaTableSizes();
    Long value = sizes.get(tn);
    if (null == value) {
      return 0L;
    }
    return value.longValue();
  }

  void waitForStableRegionSizeReport(Connection conn, TableName tn) throws Exception {
    // For some stability in the value before proceeding
    // Helps make sure that we got the actual last value, not some inbetween
    AtomicLong lastValue = new AtomicLong(-1);
    AtomicInteger counter = new AtomicInteger(0);
    TEST_UTIL.waitFor(15_000, 500, new Predicate<Exception>() {
      @Override public boolean evaluate() throws Exception {
        LOG.debug("Last observed size=" + lastValue.get());
        long actual = getRegionSizeReportForTable(conn, tn);
        if (actual == lastValue.get()) {
          int numMatches = counter.incrementAndGet();
          if (numMatches >= 5) {
            return true;
          }
          // Not yet..
          return false;
        }
        counter.set(0);
        lastValue.set(actual);
        return false;
      }
    });
  }
}
