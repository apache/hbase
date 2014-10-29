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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test many concurrent appenders to an {@link #HLog} while rolling the log.
 */
@Category(SmallTests.class)
public class TestLogRollingNoCluster {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static byte [] EMPTY_1K_ARRAY = new byte[1024];
  private static final int THREAD_COUNT = 100; // Spin up this many threads

  /**
   * Spin up a bunch of threads and have them all append to a WAL.  Roll the
   * WAL frequently to try and trigger NPE.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testContendedLogRolling() throws IOException, InterruptedException {
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    Path dir = TEST_UTIL.getDataTestDir();
    HLog wal = HLogFactory.createHLog(fs, dir, "logs",
      TEST_UTIL.getConfiguration());
    
    Appender [] appenders = null;

    final int count = THREAD_COUNT;
    appenders = new Appender[count];
    try {
      for (int i = 0; i < count; i++) {
        // Have each appending thread write 'count' entries
        appenders[i] = new Appender(wal, i, count);
      }
      for (int i = 0; i < count; i++) {
        appenders[i].start();
      }
      for (int i = 0; i < count; i++) {
        //ensure that all threads are joined before closing the wal
        appenders[i].join();
      }
    } finally {
      wal.close();
    }
    for (int i = 0; i < count; i++) {
      assertFalse(appenders[i].isException());
    }
  }

  /**
   * Appender thread.  Appends to passed wal file.
   */
  static class Appender extends Thread {
    private final Log log;
    private final HLog wal;
    private final int count;
    private Exception e = null;

    Appender(final HLog wal, final int index, final int count) {
      super("" + index);
      this.wal = wal;
      this.count = count;
      this.log = LogFactory.getLog("Appender:" + getName());
    }

    /**
     * @return Call when the thread is done.
     */
    boolean isException() {
      return !isAlive() && this.e != null;
    }

    Exception getException() {
      return this.e;
    }

    @Override
    public void run() {
      this.log.info(getName() +" started");
      final AtomicLong sequenceId = new AtomicLong(1);
      try {
        for (int i = 0; i < this.count; i++) {
          long now = System.currentTimeMillis();
          // Roll every ten edits if the log has anything in it.
          if (i % 10 == 0 && ((FSHLog) this.wal).getNumEntries() > 0) {
            this.wal.rollWriter();
          }
          WALEdit edit = new WALEdit();
          byte[] bytes = Bytes.toBytes(i);
          edit.add(new KeyValue(bytes, bytes, bytes, now, EMPTY_1K_ARRAY));

          this.wal.append(HRegionInfo.FIRST_META_REGIONINFO,
              TableName.META_TABLE_NAME,
              edit, now, TEST_UTIL.getMetaTableDescriptor(), sequenceId);
        }
        String msg = getName() + " finished";
        if (isException())
          this.log.info(msg, getException());
        else
          this.log.info(msg);
      } catch (Exception e) {
        this.e = e;
        log.info("Caught exception from Appender:" + getName(), e);
      }
    }
  }

  //@org.junit.Rule
  //public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
  //  new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}
