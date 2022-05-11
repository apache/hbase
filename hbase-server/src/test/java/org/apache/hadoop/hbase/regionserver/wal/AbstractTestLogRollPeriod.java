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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that verifies that the log is forced to be rolled every "hbase.regionserver.logroll.period"
 */
public abstract class AbstractTestLogRollPeriod {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractTestLogRollPeriod.class);

  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final static long LOG_ROLL_PERIOD = 4000;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // disable the ui
    TEST_UTIL.getConfiguration().setInt("hbase.regionsever.info.port", -1);

    TEST_UTIL.getConfiguration().setLong("hbase.regionserver.logroll.period", LOG_ROLL_PERIOD);

    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Tests that the LogRoller perform the roll even if there are no edits
   */
  @Test
  public void testNoEdits() throws Exception {
    TableName tableName = TableName.valueOf("TestLogRollPeriodNoEdits");
    TEST_UTIL.createTable(tableName, "cf");
    try {
      Table table = TEST_UTIL.getConnection().getTable(tableName);
      try {
        HRegionServer server = TEST_UTIL.getRSForFirstRegionInTable(tableName);
        WAL log = server.getWAL(null);
        checkMinLogRolls(log, 5);
      } finally {
        table.close();
      }
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  /**
   * Tests that the LogRoller perform the roll with some data in the log
   */
  @Test
  public void testWithEdits() throws Exception {
    final TableName tableName = TableName.valueOf("TestLogRollPeriodWithEdits");
    final String family = "cf";

    TEST_UTIL.createTable(tableName, family);
    try {
      HRegionServer server = TEST_UTIL.getRSForFirstRegionInTable(tableName);
      WAL log = server.getWAL(null);
      final Table table = TEST_UTIL.getConnection().getTable(tableName);

      Thread writerThread = new Thread("writer") {
        @Override
        public void run() {
          try {
            long row = 0;
            while (!interrupted()) {
              Put p = new Put(Bytes.toBytes(String.format("row%d", row)));
              p.addColumn(Bytes.toBytes(family), Bytes.toBytes("col"), Bytes.toBytes(row));
              table.put(p);
              row++;

              Thread.sleep(LOG_ROLL_PERIOD / 16);
            }
          } catch (Exception e) {
            LOG.warn(e.toString(), e);
          }
        }
      };

      try {
        writerThread.start();
        checkMinLogRolls(log, 5);
      } finally {
        writerThread.interrupt();
        writerThread.join();
        table.close();
      }
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  private void checkMinLogRolls(final WAL log, final int minRolls) throws Exception {
    final List<Path> paths = new ArrayList<>();
    log.registerWALActionsListener(new WALActionsListener() {
      @Override
      public void postLogRoll(Path oldFile, Path newFile) {
        LOG.debug("postLogRoll: oldFile=" + oldFile + " newFile=" + newFile);
        paths.add(newFile);
      }
    });

    // Sleep until we should get at least min-LogRoll events
    long wtime = System.currentTimeMillis();
    Thread.sleep((minRolls + 1) * LOG_ROLL_PERIOD);
    // Do some extra sleep in case the machine is slow,
    // and the log-roll is not triggered exactly on LOG_ROLL_PERIOD.
    final int NUM_RETRIES = 1 + 8 * (minRolls - paths.size());
    for (int retry = 0; paths.size() < minRolls && retry < NUM_RETRIES; ++retry) {
      Thread.sleep(LOG_ROLL_PERIOD / 4);
    }
    wtime = System.currentTimeMillis() - wtime;
    LOG.info(String.format("got %d rolls after %dms (%dms each) - expected at least %d rolls",
      paths.size(), wtime, wtime / paths.size(), minRolls));
    assertFalse(paths.size() < minRolls);
  }
}
