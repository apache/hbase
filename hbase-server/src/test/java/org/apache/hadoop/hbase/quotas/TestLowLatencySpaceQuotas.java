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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClientServiceCallable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RpcRetryingCaller;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.quotas.SpaceQuotaHelperForTests.SpaceQuotaSnapshotPredicate;
import org.apache.hadoop.hbase.regionserver.HRegion;
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
    // Set the period to read region size from HDFS to be very long
    conf.setInt(FileSystemUtilizationChore.FS_UTILIZATION_CHORE_PERIOD_KEY, 1000 * 120);
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

    ClientServiceCallable<Boolean> callable = helper.generateFileToLoad(tn, 3, 550);
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
    RpcRetryingCaller<Boolean> caller = factory.<Boolean> newCaller();
    assertTrue("The bulk load failed", caller.callWithRetries(callable, Integer.MAX_VALUE));

    final long finalTotalSize = totalSize;
    TEST_UTIL.waitFor(30 * 1000, 500, new SpaceQuotaSnapshotPredicate(conn, tn) {
      @Override boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception {
        return snapshot.getUsage() >= finalTotalSize;
      }
    });
  }
}
