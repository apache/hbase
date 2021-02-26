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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Optional;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.regionserver.FlushLifeCycleTracker;
import org.apache.hadoop.hbase.snapshot.MobSnapshotTestingUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ LargeTests.class, ClientTests.class })
public class TestMobCloneSnapshotFromClientCloneLinksAfterDelete
    extends CloneSnapshotFromClientCloneLinksAfterDeleteTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMobCloneSnapshotFromClientCloneLinksAfterDelete.class);

  private static boolean delayFlush = false;

  /**
   * This coprocessor is used to delay the flush.
   */
  public static class DelayFlushCoprocessor implements RegionCoprocessor, RegionObserver {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preFlush(ObserverContext<RegionCoprocessorEnvironment> e,
        FlushLifeCycleTracker tracker) throws IOException {
      if (delayFlush) {
        try {
          if (Bytes.compareTo(e.getEnvironment().getRegionInfo().getStartKey(),
            HConstants.EMPTY_START_ROW) != 0) {
            Thread.sleep(100);
          }
        } catch (InterruptedException e1) {
          throw new InterruptedIOException(e1.getMessage());
        }
      }
    }
  }

  protected static void setupConfiguration() {
    CloneSnapshotFromClientTestBase.setupConfiguration();
    TEST_UTIL.getConfiguration().setLong(TimeToLiveHFileCleaner.TTL_CONF_KEY, 0);
    TEST_UTIL.getConfiguration().setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 0);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setupConfiguration();
    TEST_UTIL.startMiniCluster(3);
  }

  @Override
  protected void createTable() throws IOException, InterruptedException {
    MobSnapshotTestingUtils.createMobTable(TEST_UTIL, tableName,
      SnapshotTestingUtils.getSplitKeys(), getNumReplicas(), DelayFlushCoprocessor.class.getName(),
      FAMILY);
  }

  @Override
  protected int numRowsToLoad() {
    return 20;
  }

  @Override
  protected int countRows(Table table) throws IOException {
    return MobSnapshotTestingUtils.countMobRows(table);
  }

  @Test
  @Override
  public void testCloneLinksAfterDelete() throws IOException, InterruptedException {
    // delay the flush to make sure
    delayFlush = true;
    SnapshotTestingUtils.loadData(TEST_UTIL, tableName, 20, FAMILY);
    long tid = System.currentTimeMillis();
    byte[] snapshotName3 = Bytes.toBytes("snaptb3-" + tid);
    TableName clonedTableName3 =
      TableName.valueOf(name.getMethodName() + System.currentTimeMillis());
    admin.snapshot(snapshotName3, tableName);
    delayFlush = false;
    int snapshot3Rows = -1;
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      snapshot3Rows = TEST_UTIL.countRows(table);
    }
    admin.cloneSnapshot(snapshotName3, clonedTableName3);
    admin.deleteSnapshot(snapshotName3);
    super.testCloneLinksAfterDelete();
    verifyRowCount(TEST_UTIL, clonedTableName3, snapshot3Rows);
    admin.disableTable(clonedTableName3);
    admin.deleteTable(clonedTableName3);
  }
}
