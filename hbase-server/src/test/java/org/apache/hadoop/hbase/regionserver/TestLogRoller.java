/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@Category({RegionServerTests.class, MediumTests.class})
public class TestLogRoller {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestLogRoller.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration CONF;
  private static Path ROOT_DIR;
  private static FileSystem FS;

  @Before
  public void setup() throws IOException {
    CONF = TEST_UTIL.getConfiguration();
    ROOT_DIR = TEST_UTIL.getRandomDir();
    FS = FileSystem.get(CONF);
  }

  @After
  public void tearDown() throws Exception {
    FS.close();
  }

  /**
   * when use multiwal and a wal request roll, whether other wals roll together.
   */
  @Test
  public void testWalRequestRollWithMultiWal() throws Exception {
    // create LogRoller and add multiple wal
    RegionServerServices services = Mockito.mock(RegionServerServices.class);
    Mockito.when(services.getConfiguration()).thenReturn(CONF);
    LogRoller roller = new LogRoller(services);
    roller.start();
    Map<MyFSHLog, Path>  wals = new HashMap<>();
    for (int i = 1; i <= 3; i++) {
      MyFSHLog wal = new MyFSHLog(FS, ROOT_DIR, "WALs",
          "archiveWALs", CONF, null, true, "wal-test", "." + i);
      wal.init();
      roller.addWAL(wal);
      wals.put(wal, wal.getCurrentFileName());
    }
    // first wal request roll
    Iterator<Map.Entry<MyFSHLog, Path>> iterator = wals.entrySet().iterator();
    Map.Entry<MyFSHLog, Path> entry = iterator.next();
    entry.getKey().requestLogRoll();
    while (!roller.walRollFinished() || entry.getKey().isRolling()) {
      TimeUnit.MILLISECONDS.sleep(100);
    }
    // current file name of first wal is changed
    assertNotEquals(entry.getKey().getCurrentFileName(), entry.getValue());
    // current file name of other wals is not changed
    while (iterator.hasNext()) {
      entry = iterator.next();
      assertEquals(entry.getKey().getCurrentFileName(), entry.getValue());
    }
  }

  private static class MyFSHLog extends FSHLog {
    MyFSHLog(final FileSystem fs, final Path rootDir, final String logDir,
        final String archiveDir, final Configuration conf, final List<WALActionsListener> listeners,
        final boolean failIfWALExists, final String prefix, final String suffix) throws IOException {
      super(fs, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix, suffix);
    }

    boolean isRolling() {
      return rollRequested.get();
    }
  }
}
