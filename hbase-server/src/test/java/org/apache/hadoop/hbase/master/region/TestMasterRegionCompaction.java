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
package org.apache.hadoop.hbase.master.region;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveMasterLocalStoreHFileCleaner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestMasterRegionCompaction extends MasterRegionTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterRegionCompaction.class);

  private int compactMin = 4;

  private HFileCleaner hfileCleaner;

  @Override
  protected void configure(MasterRegionParams params) {
    params.compactMin(compactMin);
  }

  @Override
  protected void postSetUp() throws IOException {
    Configuration conf = htu.getConfiguration();
    conf.setLong(TimeToLiveMasterLocalStoreHFileCleaner.TTL_CONF_KEY, 5000);
    Path testDir = htu.getDataTestDir();
    FileSystem fs = testDir.getFileSystem(conf);
    Path globalArchivePath = HFileArchiveUtil.getArchivePath(conf);
    hfileCleaner = new HFileCleaner(500, new Stoppable() {

      private volatile boolean stopped = false;

      @Override
      public void stop(String why) {
        stopped = true;
      }

      @Override
      public boolean isStopped() {
        return stopped;
      }
    }, conf, fs, globalArchivePath, cleanerPool);
    choreService.scheduleChore(hfileCleaner);
  }

  private int getStorefilesCount() {
    return region.region.getStores().stream().mapToInt(Store::getStorefilesCount).sum();
  }

  private void assertFileCount(FileSystem fs, Path storeArchiveDir, int expected)
    throws IOException {
    FileStatus[] compactedHFiles = fs.listStatus(storeArchiveDir);
    assertEquals(expected, compactedHFiles.length);
  }

  @Test
  public void test() throws IOException, InterruptedException {
    for (int i = 0; i < compactMin - 1; i++) {
      final int index = i;
      region.update(
        r -> r.put(new Put(Bytes.toBytes(index)).addColumn(CF1, QUALIFIER, Bytes.toBytes(index))
          .addColumn(CF2, QUALIFIER, Bytes.toBytes(index))));
      region.flush(true);
    }
    assertEquals(2 * (compactMin - 1), getStorefilesCount());
    region.update(r -> r.put(new Put(Bytes.toBytes(compactMin - 1)).addColumn(CF1, QUALIFIER,
      Bytes.toBytes(compactMin - 1))));
    region.flusherAndCompactor.requestFlush();
    htu.waitFor(15000, () -> getStorefilesCount() == 2);
    Path store1ArchiveDir = HFileArchiveUtil.getStoreArchivePathForRootDir(htu.getDataTestDir(),
      region.region.getRegionInfo(), CF1);
    Path store2ArchiveDir = HFileArchiveUtil.getStoreArchivePathForRootDir(htu.getDataTestDir(),
      region.region.getRegionInfo(), CF2);
    FileSystem fs = store1ArchiveDir.getFileSystem(htu.getConfiguration());
    // after compaction, the old hfiles should have been compacted
    htu.waitFor(15000, () -> {
      try {
        FileStatus[] fses1 = fs.listStatus(store1ArchiveDir);
        FileStatus[] fses2 = fs.listStatus(store2ArchiveDir);
        return fses1 != null && fses1.length == compactMin && fses2 != null &&
          fses2.length == compactMin - 1;
      } catch (FileNotFoundException e) {
        return false;
      }
    });
    // ttl has not expired, so should not delete any files
    Thread.sleep(1000);
    FileStatus[] compactedHFiles = fs.listStatus(store1ArchiveDir);
    assertEquals(compactMin, compactedHFiles.length);
    assertFileCount(fs, store2ArchiveDir, compactMin - 1);
    Thread.sleep(2000);
    // touch one file

    long currentTime = System.currentTimeMillis();
    fs.setTimes(compactedHFiles[0].getPath(), currentTime, currentTime);
    Thread.sleep(3000);
    // only the touched file is still there after clean up
    FileStatus[] remainingHFiles = fs.listStatus(store1ArchiveDir);
    assertEquals(1, remainingHFiles.length);
    assertEquals(compactedHFiles[0].getPath(), remainingHFiles[0].getPath());
    assertFalse(fs.exists(store2ArchiveDir));
    Thread.sleep(6000);
    // the touched file should also be cleaned up and then the cleaner will delete the parent
    // directory since it is empty.
    assertFalse(fs.exists(store1ArchiveDir));
  }
}
