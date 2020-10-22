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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.master.cleaner.LogCleaner;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveMasterLocalStoreWALCleaner;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestMasterRegionWALCleaner extends MasterRegionTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterRegionWALCleaner.class);

  private static long TTL_MS = 5000;

  private LogCleaner logCleaner;

  private Path globalWALArchiveDir;

  @Override
  protected void postSetUp() throws IOException {
    Configuration conf = htu.getConfiguration();
    conf.setLong(TimeToLiveMasterLocalStoreWALCleaner.TTL_CONF_KEY, TTL_MS);
    Path testDir = htu.getDataTestDir();
    FileSystem fs = testDir.getFileSystem(conf);
    globalWALArchiveDir = new Path(testDir, HConstants.HREGION_OLDLOGDIR_NAME);
    logCleaner = new LogCleaner(1000, new Stoppable() {

      private volatile boolean stopped = false;

      @Override
      public void stop(String why) {
        stopped = true;
      }

      @Override
      public boolean isStopped() {
        return stopped;
      }
    }, conf, fs, globalWALArchiveDir, cleanerPool);
    choreService.scheduleChore(logCleaner);
  }

  @Test
  public void test() throws IOException, InterruptedException {
    region
      .update(r -> r.put(new Put(Bytes.toBytes(1)).addColumn(CF1, QUALIFIER, Bytes.toBytes(1))));
    region.flush(true);
    Path testDir = htu.getDataTestDir();
    FileSystem fs = testDir.getFileSystem(htu.getConfiguration());
    // no archived wal files yet
    assertFalse(fs.exists(globalWALArchiveDir));
    region.requestRollAll();
    region.waitUntilWalRollFinished();
    // should have one
    FileStatus[] files = fs.listStatus(globalWALArchiveDir);  
    assertEquals(1, files.length);
    Thread.sleep(2000); 
    // should still be there  
    assertTrue(fs.exists(files[0].getPath()));  
    Thread.sleep(6000);
    // should have been cleaned
    assertEquals(0, fs.listStatus(globalWALArchiveDir).length);
  }
}
