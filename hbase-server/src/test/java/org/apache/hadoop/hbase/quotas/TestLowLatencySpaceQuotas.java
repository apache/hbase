/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClientServiceCallable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RpcRetryingCaller;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.quotas.SpaceQuotaHelperForTests.SpaceQuotaSnapshotPredicate;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

@Category({MediumTests.class})
public class TestLowLatencySpaceQuotas {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLowLatencySpaceQuotas.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  // Global for all tests in the class
  private static final AtomicLong COUNTER = new AtomicLong(0);

  @Rule
  public TestName testName = new TestName();
  private SpaceQuotaHelperForTests helper;
  private Connection conn;
  private Admin admin;

  @BeforeClass
  public static void setup() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // The default 1s period for QuotaObserverChore is good.
    SpaceQuotaHelperForTests.updateConfigForQuotas(conf);
    // Set the period/delay to read region size from HDFS to be very long
    conf.setInt(FileSystemUtilizationChore.FS_UTILIZATION_CHORE_PERIOD_KEY, 1000 * 120);
    conf.setInt(FileSystemUtilizationChore.FS_UTILIZATION_CHORE_DELAY_KEY, 1000 * 120);
    // Set the same long period/delay to compute snapshot sizes
    conf.setInt(SnapshotQuotaObserverChore.SNAPSHOT_QUOTA_CHORE_PERIOD_KEY, 1000 * 120);
    conf.setInt(SnapshotQuotaObserverChore.SNAPSHOT_QUOTA_CHORE_DELAY_KEY, 1000 * 120);
    // Clean up the compacted files faster than normal (5s instead of 2mins)
    conf.setInt("hbase.hfile.compaction.discharger.interval", 5 * 1000);

    TEST_UTIL.startMiniCluster(1);
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
    helper.waitForQuotaTable(conn);
  }

  @Test
  public void testFlushes() throws Exception {
    TableName tn = helper.createTableWithRegions(1);
    // Set a quota
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(
        tn, SpaceQuotaHelperForTests.ONE_GIGABYTE, SpaceViolationPolicy.NO_INSERTS);
    admin.setQuota(settings);

    // Write some data
    final long initialSize = 2L * SpaceQuotaHelperForTests.ONE_MEGABYTE;
    helper.writeData(tn, initialSize);

    // Make sure a flush happened
    admin.flush(tn);

    // We should be able to observe the system recording an increase in size (even
    // though we know the filesystem scanning did not happen).
    TEST_UTIL.waitFor(30 * 1000, 500, new SpaceQuotaSnapshotPredicate(conn, tn) {
      @Override boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return snapshot.getUsage() >= initialSize;
      }
    });
  }

  @Test
  public void testMajorCompaction() throws Exception {
    TableName tn = helper.createTableWithRegions(1);
    // Set a quota
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(
        tn, SpaceQuotaHelperForTests.ONE_GIGABYTE, SpaceViolationPolicy.NO_INSERTS);
    admin.setQuota(settings);

    // Write some data and flush it to disk.
    final long sizePerBatch = 2L * SpaceQuotaHelperForTests.ONE_MEGABYTE;
    helper.writeData(tn, sizePerBatch);
    admin.flush(tn);

    // Write the same data again, flushing it to a second file
    helper.writeData(tn, sizePerBatch);
    admin.flush(tn);

    // After two flushes, both hfiles would contain similar data. We should see 2x the data.
    TEST_UTIL.waitFor(30 * 1000, 500, new SpaceQuotaSnapshotPredicate(conn, tn) {
      @Override boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return snapshot.getUsage() >= 2L * sizePerBatch;
      }
    });

    // Rewrite the two files into one.
    admin.majorCompact(tn);

    // After we major compact the table, we should notice quickly that the amount of data in the
    // table is much closer to reality (the duplicate entries across the two files are removed).
    TEST_UTIL.waitFor(30 * 1000, 500, new SpaceQuotaSnapshotPredicate(conn, tn) {
      @Override boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return snapshot.getUsage() >= sizePerBatch && snapshot.getUsage() <= 2L * sizePerBatch;
      }
    });
  }

  @Test
  public void testMinorCompaction() throws Exception {
    TableName tn = helper.createTableWithRegions(1);
    // Set a quota
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(
        tn, SpaceQuotaHelperForTests.ONE_GIGABYTE, SpaceViolationPolicy.NO_INSERTS);
    admin.setQuota(settings);

    // Write some data and flush it to disk.
    final long sizePerBatch = 2L * SpaceQuotaHelperForTests.ONE_MEGABYTE;
    final long numBatches = 6;
    for (long i = 0; i < numBatches; i++) {
      helper.writeData(tn, sizePerBatch);
      admin.flush(tn);
    }

    HRegion region = Iterables.getOnlyElement(TEST_UTIL.getHBaseCluster().getRegions(tn));
    long numFiles = getNumHFilesForRegion(region);
    assertEquals(numBatches, numFiles);

    // After two flushes, both hfiles would contain similar data. We should see 2x the data.
    TEST_UTIL.waitFor(30 * 1000, 500, new SpaceQuotaSnapshotPredicate(conn, tn) {
      @Override boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return snapshot.getUsage() >= numFiles * sizePerBatch;
      }
    });

    // Rewrite some files into fewer
    TEST_UTIL.compact(tn, false);
    long numFilesAfterMinorCompaction = getNumHFilesForRegion(region);

    // After we major compact the table, we should notice quickly that the amount of data in the
    // table is much closer to reality (the duplicate entries across the two files are removed).
    TEST_UTIL.waitFor(30 * 1000, 500, new SpaceQuotaSnapshotPredicate(conn, tn) {
      @Override boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return snapshot.getUsage() >= numFilesAfterMinorCompaction * sizePerBatch &&
            snapshot.getUsage() <= (numFilesAfterMinorCompaction + 1) * sizePerBatch;
      }
    });
  }

  private long getNumHFilesForRegion(HRegion region) {
    return region.getStores().stream().mapToLong((s) -> s.getNumHFiles()).sum();
  }

  @Test
  public void testBulkLoading() throws Exception {
    TableName tn = helper.createTableWithRegions(1);
    // Set a quota
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(
        tn, SpaceQuotaHelperForTests.ONE_GIGABYTE, SpaceViolationPolicy.NO_INSERTS);
    admin.setQuota(settings);
    admin.compactionSwitch(false,
      admin.getRegionServers().stream().map(ServerName::toString).collect(Collectors.toList()));

    ClientServiceCallable<Void> callable = helper.generateFileToLoad(tn, 3, 550);
    // Make sure the files are about as long as we expect
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    FileStatus[] files = fs.listStatus(
        new Path(fs.getHomeDirectory(), testName.getMethodName() + "_files"));
    long totalSize = 0;
    for (FileStatus file : files) {
      assertTrue(
          "Expected the file, " + file.getPath() + ",  length to be larger than 25KB, but was "
              + file.getLen(),
          file.getLen() > 25 * SpaceQuotaHelperForTests.ONE_KILOBYTE);
      totalSize += file.getLen();
    }

    RpcRetryingCallerFactory factory = new RpcRetryingCallerFactory(TEST_UTIL.getConfiguration());
    RpcRetryingCaller<Void> caller = factory.<Void> newCaller();
    caller.callWithRetries(callable, Integer.MAX_VALUE);

    final long finalTotalSize = totalSize;
    try {
      TEST_UTIL.waitFor(30 * 1000, 500, new SpaceQuotaSnapshotPredicate(conn, tn) {
        @Override
        boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
          return snapshot.getUsage() >= finalTotalSize;
        }
      });
    } finally {
      admin.compactionSwitch(true,
        admin.getRegionServers().stream().map(ServerName::toString).collect(Collectors.toList()));
    }
  }

  @Test
  public void testSnapshotSizes() throws Exception {
    TableName tn = helper.createTableWithRegions(1);
    // Set a quota
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(
        tn, SpaceQuotaHelperForTests.ONE_GIGABYTE, SpaceViolationPolicy.NO_INSERTS);
    admin.setQuota(settings);

    // Write some data and flush it to disk.
    final long sizePerBatch = 2L * SpaceQuotaHelperForTests.ONE_MEGABYTE;
    helper.writeData(tn, sizePerBatch);
    admin.flush(tn);

    final String snapshot1 = "snapshot1";
    admin.snapshot(snapshot1, tn, SnapshotType.SKIPFLUSH);

    // Compute the size of the file for the Region we'll send to archive
    Region region = Iterables.getOnlyElement(TEST_UTIL.getHBaseCluster().getRegions(tn));
    List<? extends Store> stores = region.getStores();
    long summer = 0;
    for (Store store : stores) {
      summer += store.getStorefilesSize();
    }
    final long storeFileSize = summer;

    // Wait for the table to show the usage
    TEST_UTIL.waitFor(30 * 1000, 500, new SpaceQuotaSnapshotPredicate(conn, tn) {
      @Override boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return snapshot.getUsage() == storeFileSize;
      }
    });

    // Spoof a "full" computation of snapshot size. Normally the chore handles this, but we want
    // to test in the absence of this chore.
    FileArchiverNotifier notifier = TEST_UTIL.getHBaseCluster().getMaster()
        .getSnapshotQuotaObserverChore().getNotifierForTable(tn);
    notifier.computeAndStoreSnapshotSizes(Collections.singletonList(snapshot1));

    // Force a major compaction to create a new file and push the old file to the archive
    TEST_UTIL.compact(tn, true);

    // After moving the old file to archive/, the space of this table should double
    // We have a new file created by the majc referenced by the table and the snapshot still
    // referencing the old file.
    TEST_UTIL.waitFor(30 * 1000, 500, new SpaceQuotaSnapshotPredicate(conn, tn) {
      @Override boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return snapshot.getUsage() >= 2 * storeFileSize;
      }
    });

    try (Table quotaTable = conn.getTable(QuotaUtil.QUOTA_TABLE_NAME)) {
      Result r = quotaTable.get(QuotaTableUtil.makeGetForSnapshotSize(tn, snapshot1));
      assertTrue("Expected a non-null, non-empty Result", r != null && !r.isEmpty());
      assertTrue(r.advance());
      assertEquals("The snapshot's size should be the same as the origin store file",
          storeFileSize, QuotaTableUtil.parseSnapshotSize(r.current()));

      r = quotaTable.get(QuotaTableUtil.createGetNamespaceSnapshotSize(tn.getNamespaceAsString()));
      assertTrue("Expected a non-null, non-empty Result", r != null && !r.isEmpty());
      assertTrue(r.advance());
      assertEquals("The snapshot's size should be the same as the origin store file",
          storeFileSize, QuotaTableUtil.parseSnapshotSize(r.current()));
    }
  }
}
