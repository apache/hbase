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

package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Category({RegionServerTests.class, MediumTests.class})
public class TestLogRoller {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestLogRoller.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final int LOG_ROLL_PERIOD = 20 * 1000;
  private static final String LOG_DIR = "WALs";
  private static final String ARCHIVE_DIR = "archiveWALs";
  private static final String WAL_PREFIX = "test-log-roller";
  private static Configuration CONF;
  private static LogRoller ROLLER;
  private static Path ROOT_DIR;
  private static FileSystem FS;

  @Before
  public void setup() throws Exception {
    CONF = TEST_UTIL.getConfiguration();
    CONF.setInt("hbase.regionserver.logroll.period", LOG_ROLL_PERIOD);
    CONF.setInt(HConstants.THREAD_WAKE_FREQUENCY, 300);
    ROOT_DIR = TEST_UTIL.getRandomDir();
    FS = FileSystem.get(CONF);
    RegionServerServices services = Mockito.mock(RegionServerServices.class);
    Mockito.when(services.getConfiguration()).thenReturn(CONF);
    ROLLER = new LogRoller(services);
    ROLLER.start();
  }

  @After
  public void tearDown() throws Exception {
    ROLLER.close();
    FS.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * verify that each wal roll separately
   */
  @Test
  public void testRequestRollWithMultiWal() throws Exception {
    // add multiple wal
    Map<FSHLog, Path> wals = new HashMap<>();
    for (int i = 1; i <= 3; i++) {
      FSHLog wal = new FSHLog(FS, ROOT_DIR, LOG_DIR, ARCHIVE_DIR, CONF, null,
        true, WAL_PREFIX, "." + i);
      wal.init();
      wals.put(wal, wal.getCurrentFileName());
      ROLLER.addWAL(wal);
      Thread.sleep(1000);
    }

    // request roll
    Iterator<Map.Entry<FSHLog, Path>> it = wals.entrySet().iterator();
    Map.Entry<FSHLog, Path> walEntry = it.next();
    walEntry.getKey().requestLogRoll();
    Thread.sleep(5000);

    assertNotEquals(walEntry.getValue(), walEntry.getKey().getCurrentFileName());
    walEntry.setValue(walEntry.getKey().getCurrentFileName());
    while (it.hasNext()) {
      walEntry = it.next();
      assertEquals(walEntry.getValue(), walEntry.getKey().getCurrentFileName());
    }

    // period roll
    Thread.sleep(LOG_ROLL_PERIOD + 5000);
    for (Map.Entry<FSHLog, Path> entry : wals.entrySet()) {
      assertNotEquals(entry.getValue(), entry.getKey().getCurrentFileName());
      entry.getKey().close();
    }
  }
}
