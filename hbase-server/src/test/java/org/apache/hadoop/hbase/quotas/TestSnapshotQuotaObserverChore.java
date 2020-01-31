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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.quotas.SpaceQuotaHelperForTests.NoFilesToDischarge;
import org.apache.hadoop.hbase.quotas.SpaceQuotaHelperForTests.SpaceQuotaSnapshotPredicate;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil.SnapshotVisitor;
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

import org.apache.hbase.thirdparty.com.google.common.collect.HashMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;

import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest.StoreFile;

/**
 * Test class for the {@link SnapshotQuotaObserverChore}.
 */
@Category(LargeTests.class)
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
    Map<String,Long> namespaceSnapshotSizes = testChore.computeSnapshotSizes(
        snapshotsToCompute);
    assertEquals(1, namespaceSnapshotSizes.size());
    Long size = namespaceSnapshotSizes.get(tn1.getNamespaceAsString());
    assertNotNull(size);
    // The snapshot should take up no space since the table refers to it completely
    assertEquals(0, size.longValue());

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
    namespaceSnapshotSizes = testChore.computeSnapshotSizes(
            snapshotsToCompute);
    assertEquals(1, namespaceSnapshotSizes.size());
    size = namespaceSnapshotSizes.get(tn1.getNamespaceAsString());
    assertNotNull(size);
    // The snapshot should take up the size the table originally took up
    assertEquals(snapshotSize, size.longValue());
  }

  @Test
  public void testPersistingSnapshotsForNamespaces() throws Exception {
    TableName tn1 = TableName.valueOf("ns1:tn1");
    TableName tn2 = TableName.valueOf("ns1:tn2");
    TableName tn3 = TableName.valueOf("ns2:tn1");
    TableName tn4 = TableName.valueOf("ns2:tn2");
    TableName tn5 = TableName.valueOf("tn1");
    // Shim in a custom factory to avoid computing snapshot sizes.
    FileArchiverNotifierFactory test = new FileArchiverNotifierFactory() {
      Map<TableName,Long> tableToSize = ImmutableMap.of(
          tn1, 1024L, tn2, 1024L, tn3, 512L, tn4, 1024L, tn5, 3072L);
      @Override
      public FileArchiverNotifier get(
          Connection conn, Configuration conf, FileSystem fs, TableName tn) {
        return new FileArchiverNotifier() {
          @Override public void addArchivedFiles(Set<Entry<String,Long>> fileSizes)
              throws IOException {}

          @Override
          public long computeAndStoreSnapshotSizes(Collection<String> currentSnapshots)
              throws IOException {
            return tableToSize.get(tn);
          }
        };
      }
    };
    try {
      FileArchiverNotifierFactoryImpl.setInstance(test);

      Multimap<TableName,String> snapshotsToCompute = HashMultimap.create();
      snapshotsToCompute.put(tn1, "");
      snapshotsToCompute.put(tn2, "");
      snapshotsToCompute.put(tn3, "");
      snapshotsToCompute.put(tn4, "");
      snapshotsToCompute.put(tn5, "");
      Map<String,Long> nsSizes = testChore.computeSnapshotSizes(snapshotsToCompute);
      assertEquals(3, nsSizes.size());
      assertEquals(2048L, (long) nsSizes.get("ns1"));
      assertEquals(1536L, (long) nsSizes.get("ns2"));
      assertEquals(3072L, (long) nsSizes.get(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR));
    } finally {
      FileArchiverNotifierFactoryImpl.reset();
    }
  }

  @Test
  public void testRemovedSnapshots() throws Exception {
    // Create a table and set a quota
    TableName tn1 = helper.createTableWithRegions(1);
    admin.setQuota(QuotaSettingsFactory.limitTableSpace(tn1, SpaceQuotaHelperForTests.ONE_GIGABYTE,
        SpaceViolationPolicy.NO_INSERTS));

    // Write some data and flush it
    helper.writeData(tn1, 256L * SpaceQuotaHelperForTests.ONE_KILOBYTE); // 256 KB

    final AtomicReference<Long> lastSeenSize = new AtomicReference<>();
    // Wait for the Master chore to run to see the usage (with a fudge factor)
    TEST_UTIL.waitFor(30_000, new SpaceQuotaSnapshotPredicate(conn, tn1) {
      @Override
      boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        lastSeenSize.set(snapshot.getUsage());
        return snapshot.getUsage() > 230L * SpaceQuotaHelperForTests.ONE_KILOBYTE;
      }
    });

    // Create a snapshot on the table
    final String snapshotName1 = tn1 + "snapshot1";
    admin.snapshot(new SnapshotDescription(snapshotName1, tn1, SnapshotType.SKIPFLUSH));

    // Snapshot size has to be 0 as the snapshot shares the data with the table
    final Table quotaTable = conn.getTable(QuotaUtil.QUOTA_TABLE_NAME);
    TEST_UTIL.waitFor(30_000, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        Get g = QuotaTableUtil.makeGetForSnapshotSize(tn1, snapshotName1);
        Result r = quotaTable.get(g);
        if (r == null || r.isEmpty()) {
          return false;
        }
        r.advance();
        Cell c = r.current();
        return QuotaTableUtil.parseSnapshotSize(c) == 0;
      }
    });
    // Total usage has to remain same as what we saw before taking a snapshot
    TEST_UTIL.waitFor(30_000, new SpaceQuotaSnapshotPredicate(conn, tn1) {
      @Override
      boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return snapshot.getUsage() == lastSeenSize.get();
      }
    });

    // Major compact the table to force a rewrite
    TEST_UTIL.compact(tn1, true);
    // Now the snapshot size has to prev total size
    TEST_UTIL.waitFor(30_000, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        Get g = QuotaTableUtil.makeGetForSnapshotSize(tn1, snapshotName1);
        Result r = quotaTable.get(g);
        if (r == null || r.isEmpty()) {
          return false;
        }
        r.advance();
        Cell c = r.current();
        // The compaction result file has an additional compaction event tracker
        return lastSeenSize.get() == QuotaTableUtil.parseSnapshotSize(c);
      }
    });
    // The total size now has to be equal/more than double of prev total size
    // as double the number of store files exist now.
    final AtomicReference<Long> sizeAfterCompaction = new AtomicReference<>();
    TEST_UTIL.waitFor(30_000, new SpaceQuotaSnapshotPredicate(conn, tn1) {
      @Override
      boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        sizeAfterCompaction.set(snapshot.getUsage());
        return snapshot.getUsage() >= 2 * lastSeenSize.get();
      }
    });

    // Delete the snapshot
    admin.deleteSnapshot(snapshotName1);
    // Total size has to come down to prev totalsize - snapshot size(which was removed)
    TEST_UTIL.waitFor(30_000, new SpaceQuotaSnapshotPredicate(conn, tn1) {
      @Override
      boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return snapshot.getUsage() == (sizeAfterCompaction.get() - lastSeenSize.get());
      }
    });
  }

  @Test
  public void testBucketingFilesToSnapshots() throws Exception {
    // Create a table and set a quota
    TableName tn1 = helper.createTableWithRegions(1);
    admin.setQuota(QuotaSettingsFactory.limitTableSpace(
        tn1, SpaceQuotaHelperForTests.ONE_GIGABYTE, SpaceViolationPolicy.NO_INSERTS));

    // Write some data and flush it
    helper.writeData(tn1, 256L * SpaceQuotaHelperForTests.ONE_KILOBYTE);
    admin.flush(tn1);

    final AtomicReference<Long> lastSeenSize = new AtomicReference<>();
    // Wait for the Master chore to run to see the usage (with a fudge factor)
    TEST_UTIL.waitFor(30_000, new SpaceQuotaSnapshotPredicate(conn, tn1) {
      @Override
      boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        lastSeenSize.set(snapshot.getUsage());
        return snapshot.getUsage() > 230L * SpaceQuotaHelperForTests.ONE_KILOBYTE;
      }
    });

    // Create a snapshot on the table
    final String snapshotName1 = tn1 + "snapshot1";
    admin.snapshot(new SnapshotDescription(snapshotName1, tn1, SnapshotType.SKIPFLUSH));
    // Major compact the table to force a rewrite
    TEST_UTIL.compact(tn1, true);

    // Make sure that the snapshot owns the size
    final Table quotaTable = conn.getTable(QuotaUtil.QUOTA_TABLE_NAME);
    TEST_UTIL.waitFor(30_000, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        LOG.info("Waiting to see quota snapshot1 size");
        debugFilesForSnapshot(tn1, snapshotName1);
        Get g = QuotaTableUtil.makeGetForSnapshotSize(tn1, snapshotName1);
        Result r = quotaTable.get(g);
        if (r == null || r.isEmpty()) {
          return false;
        }
        r.advance();
        Cell c = r.current();
        // The compaction result file has an additional compaction event tracker
        return lastSeenSize.get() == QuotaTableUtil.parseSnapshotSize(c);
      }
    });

    LOG.info("Snapshotting table again");
    // Create another snapshot on the table
    final String snapshotName2 = tn1 + "snapshot2";
    admin.snapshot(new SnapshotDescription(snapshotName2, tn1, SnapshotType.SKIPFLUSH));
    LOG.info("Compacting table");
    // Major compact the table to force a rewrite
    TEST_UTIL.compact(tn1, true);

    // Make sure that the snapshot owns the size
    TEST_UTIL.waitFor(30_000, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        LOG.info("Waiting to see quota snapshot2 size");
        debugFilesForSnapshot(tn1, snapshotName2);
        Get g = QuotaTableUtil.makeGetForSnapshotSize(tn1, snapshotName2);
        Result r = quotaTable.get(g);
        if (r == null || r.isEmpty()) {
          return false;
        }
        r.advance();
        Cell c = r.current();
        return closeInSize(lastSeenSize.get(),
            QuotaTableUtil.parseSnapshotSize(c), SpaceQuotaHelperForTests.ONE_KILOBYTE);
      }
    });

    Get g = QuotaTableUtil.createGetNamespaceSnapshotSize(tn1.getNamespaceAsString());
    Result r = quotaTable.get(g);
    assertNotNull(r);
    assertFalse(r.isEmpty());
    r.advance();
    long size = QuotaTableUtil.parseSnapshotSize(r.current());
    // Two snapshots of equal size.
    assertTrue(closeInSize(lastSeenSize.get() * 2, size, SpaceQuotaHelperForTests.ONE_KILOBYTE));
  }

  /**
   * Prints details about every file referenced by the snapshot with the given name.
   */
  void debugFilesForSnapshot(TableName table, String snapshot) throws IOException {
    final Configuration conf = TEST_UTIL.getConfiguration();
    final FileSystem fs = TEST_UTIL.getTestFileSystem();
    final Path snapshotDir = new Path(conf.get("hbase.rootdir"), HConstants.SNAPSHOT_DIR_NAME);
    SnapshotReferenceUtil.visitReferencedFiles(conf, fs, new Path(snapshotDir, snapshot),
        new SnapshotVisitor() {
          @Override
          public void storeFile(
              RegionInfo regionInfo, String familyName, StoreFile storeFile) throws IOException {
            LOG.info("Snapshot={} references file={}, size={}", snapshot, storeFile.getName(),
                storeFile.getFileSize());
          }
        }
    );
  }

  /**
   * Computes if {@code size2} is within {@code delta} of {@code size1}, inclusive.
   *
   * The size of our store files will change after the first major compaction as the last
   * compaction gets serialized into the store file (see the fields referenced by
   * COMPACTION_EVENT_KEY in HFilePrettyPrinter).
   */
  boolean closeInSize(long size1, long size2, long delta) {
    long lower = size1 - delta;
    long upper = size1 + delta;
    return lower <= size2 && size2 <= upper;
  }
}
