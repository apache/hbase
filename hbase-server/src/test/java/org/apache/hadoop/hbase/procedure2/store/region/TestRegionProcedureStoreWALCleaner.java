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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.cleaner.DirScanPool;
import org.apache.hadoop.hbase.master.cleaner.LogCleaner;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveProcedureWALCleaner;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.LoadCounter;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestRegionProcedureStoreWALCleaner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionProcedureStoreWALCleaner.class);

  private HBaseCommonTestingUtility htu;

  private FileSystem fs;

  private RegionProcedureStore store;

  private ChoreService choreService;

  private DirScanPool dirScanPool;

  private LogCleaner logCleaner;

  private Path globalWALArchiveDir;

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtility();
    Configuration conf = htu.getConfiguration();
    conf.setBoolean(MemStoreLAB.USEMSLAB_KEY, false);
    Path testDir = htu.getDataTestDir();
    fs = testDir.getFileSystem(conf);
    CommonFSUtils.setWALRootDir(conf, testDir);
    globalWALArchiveDir = new Path(testDir, HConstants.HREGION_OLDLOGDIR_NAME);
    choreService = new ChoreService("Region-Procedure-Store");
    dirScanPool = new DirScanPool(conf);
    conf.setLong(TimeToLiveProcedureWALCleaner.TTL_CONF_KEY, 5000);
    conf.setInt(HMaster.HBASE_MASTER_CLEANER_INTERVAL, 1000);
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
    }, conf, fs, globalWALArchiveDir, dirScanPool);
    choreService.scheduleChore(logCleaner);
    store = RegionProcedureStoreTestHelper.createStore(conf, new LoadCounter());
  }

  @After
  public void tearDown() throws IOException {
    store.stop(true);
    logCleaner.cancel();
    dirScanPool.shutdownNow();
    choreService.shutdown();
    htu.cleanupTestDir();
  }

  @Test
  public void test() throws IOException, InterruptedException {
    RegionProcedureStoreTestProcedure proc = new RegionProcedureStoreTestProcedure();
    store.insert(proc, null);
    store.region.flush(true);
    // no archived wal files yet
    assertFalse(fs.exists(globalWALArchiveDir));
    store.walRoller.requestRollAll();
    store.walRoller.waitUntilWalRollFinished();
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
