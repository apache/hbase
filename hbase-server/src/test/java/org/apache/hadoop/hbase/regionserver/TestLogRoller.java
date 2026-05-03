/*
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Tag(RegionServerTests.TAG)
@Tag(MediumTests.TAG)
public class TestLogRoller {

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final int LOG_ROLL_PERIOD = 20 * 1000;
  private static final String LOG_DIR = "WALs";
  private static final String ARCHIVE_DIR = "archiveWALs";
  private static final String WAL_PREFIX = "test-log-roller";
  private static Configuration CONF;
  private static LogRoller ROLLER;
  private static Path ROOT_DIR;
  private static FileSystem FS;

  @BeforeEach
  public void setUp() throws Exception {
    CONF = TEST_UTIL.getConfiguration();
    CONF.setInt("hbase.regionserver.logroll.period", LOG_ROLL_PERIOD);
    CONF.setInt(HConstants.THREAD_WAKE_FREQUENCY, 300);
    ROOT_DIR = TEST_UTIL.getRandomDir();
    FS = FileSystem.get(CONF);
    FS.mkdirs(new Path(ROOT_DIR, LOG_DIR));
    RegionServerServices services = Mockito.mock(RegionServerServices.class);
    Mockito.when(services.getConfiguration()).thenReturn(CONF);
    ROLLER = new LogRoller(services);
    ROLLER.start();
  }

  @AfterEach
  public void tearDown() throws Exception {
    ROLLER.close();
    FS.close();
    TEST_UTIL.cleanupTestDir();
  }

  @Test
  public void testRemoveClosedWAL() throws Exception {
    assertEquals(0, ROLLER.getWalNeedsRoll().size());
    for (int i = 1; i <= 3; i++) {
      FSHLog wal = new FSHLog(FS, ROOT_DIR, LOG_DIR, ARCHIVE_DIR, CONF, null, true, WAL_PREFIX,
        getWALSuffix(i));
      ROLLER.addWAL(wal);
    }

    assertEquals(3, ROLLER.getWalNeedsRoll().size());
    Iterator<WAL> it = ROLLER.getWalNeedsRoll().keySet().iterator();
    WAL wal = it.next();
    assertTrue(ROLLER.getWalNeedsRoll().containsKey(wal));

    wal.close();
    Thread.sleep(LOG_ROLL_PERIOD + 5000);

    assertEquals(2, ROLLER.getWalNeedsRoll().size());
    assertFalse(ROLLER.getWalNeedsRoll().containsKey(wal));

    wal = it.next();
    wal.close();
    wal = it.next();
    wal.close();
    Thread.sleep(LOG_ROLL_PERIOD + 5000);

    assertEquals(0, ROLLER.getWalNeedsRoll().size());
  }

  /**
   * verify that each wal roll separately
   */
  @Test
  public void testRequestRollWithMultiWal() throws Exception {
    // add multiple wal
    Map<FSHLog, Path> wals = new HashMap<>();
    for (int i = 1; i <= 3; i++) {
      FSHLog wal = new FSHLog(FS, ROOT_DIR, LOG_DIR, ARCHIVE_DIR, CONF, null, true, WAL_PREFIX,
        getWALSuffix(i));
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

  private static String getWALSuffix(int id) {
    return "." + id;
  }
}
