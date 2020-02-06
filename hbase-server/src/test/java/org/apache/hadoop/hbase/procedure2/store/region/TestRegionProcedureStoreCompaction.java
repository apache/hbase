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
package org.apache.hadoop.hbase.procedure2.store.region;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

@Category({ MasterTests.class, MediumTests.class })
public class TestRegionProcedureStoreCompaction extends RegionProcedureStoreTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionProcedureStoreCompaction.class);

  private int compactMin = 4;

  @Override
  protected void configure(Configuration conf) {
    conf.setInt(RegionFlusherAndCompactor.COMPACT_MIN_KEY, compactMin);
    conf.setInt(HMaster.HBASE_MASTER_CLEANER_INTERVAL, 500);
    conf.setLong(TimeToLiveHFileCleaner.TTL_CONF_KEY, 5000);
  }

  private int getStorefilesCount() {
    return Iterables.getOnlyElement(store.region.getStores()).getStorefilesCount();
  }

  @Test
  public void test() throws IOException, InterruptedException {
    for (int i = 0; i < compactMin - 1; i++) {
      store.insert(new RegionProcedureStoreTestProcedure(), null);
      store.region.flush(true);
    }
    assertEquals(compactMin - 1, getStorefilesCount());
    store.insert(new RegionProcedureStoreTestProcedure(), null);
    store.flusherAndCompactor.requestFlush();
    htu.waitFor(15000, () -> getStorefilesCount() == 1);
    Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePathForRootDir(
      new Path(htu.getDataTestDir(), RegionProcedureStore.MASTER_PROCEDURE_DIR),
      store.region.getRegionInfo(), RegionProcedureStore.FAMILY);
    FileSystem fs = storeArchiveDir.getFileSystem(htu.getConfiguration());
    // after compaction, the old hfiles should have been compacted
    htu.waitFor(15000, () -> {
      try {
        FileStatus[] fses = fs.listStatus(storeArchiveDir);
        return fses != null && fses.length == compactMin;
      } catch (FileNotFoundException e) {
        return false;
      }
    });
    // ttl has not expired, so should not delete any files
    Thread.sleep(1000);
    FileStatus[] compactedHFiles = fs.listStatus(storeArchiveDir);
    assertEquals(4, compactedHFiles.length);
    Thread.sleep(2000);
    // touch one file
    long currentTime = System.currentTimeMillis();
    fs.setTimes(compactedHFiles[0].getPath(), currentTime, currentTime);
    Thread.sleep(3000);
    // only the touched file is still there after clean up
    FileStatus[] remainingHFiles = fs.listStatus(storeArchiveDir);
    assertEquals(1, remainingHFiles.length);
    assertEquals(compactedHFiles[0].getPath(), remainingHFiles[0].getPath());
    Thread.sleep(6000);
    // the touched file should also be cleaned up and then the cleaner will delete the parent
    // directory since it is empty.
    assertFalse(fs.exists(storeArchiveDir));
  }
}
