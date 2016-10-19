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
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test many concurrent appenders to an {@link #WAL} while rolling the log.
 */
@Category(SmallTests.class)
public class TestLogRollingNoCluster {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static byte [] EMPTY_1K_ARRAY = new byte[1024];
  private static final int NUM_THREADS = 100; // Spin up this many threads
  private static final int NUM_ENTRIES = 100; // How many entries to write

  /** ProtobufLogWriter that simulates higher latencies in sync() call */
  public static class HighLatencySyncWriter extends  ProtobufLogWriter {
    @Override
    public void sync() throws IOException {
      Threads.sleep(ThreadLocalRandom.current().nextInt(10));
      super.sync();
      Threads.sleep(ThreadLocalRandom.current().nextInt(10));
    }
  }

  /**
   * Spin up a bunch of threads and have them all append to a WAL.  Roll the
   * WAL frequently to try and trigger NPE.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testContendedLogRolling() throws Exception {
    TEST_UTIL.startMiniDFSCluster(3);
    Path dir = TEST_UTIL.getDataTestDirOnTestFS();

    // The implementation needs to know the 'handler' count.
    TEST_UTIL.getConfiguration().setInt(HConstants.REGION_SERVER_HANDLER_COUNT, NUM_THREADS);
    final Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    FSUtils.setRootDir(conf, dir);
    conf.set("hbase.regionserver.hlog.writer.impl", HighLatencySyncWriter.class.getName());
    final WALFactory wals = new WALFactory(conf, null, TestLogRollingNoCluster.class.getName());
    final WAL wal = wals.getWAL(new byte[]{});
    
    Appender [] appenders = null;

    final int numThreads = NUM_THREADS;
    appenders = new Appender[numThreads];
    try {
      for (int i = 0; i < numThreads; i++) {
        // Have each appending thread write 'count' entries
        appenders[i] = new Appender(wal, i, NUM_ENTRIES);
      }
      for (int i = 0; i < numThreads; i++) {
        appenders[i].start();
      }
      for (int i = 0; i < numThreads; i++) {
        //ensure that all threads are joined before closing the wal
        appenders[i].join();
      }
    } finally {
      wals.close();
    }
    for (int i = 0; i < numThreads; i++) {
      assertFalse(appenders[i].isException());
    }
    TEST_UTIL.shutdownMiniDFSCluster();
  }

  /**
   * Appender thread.  Appends to passed wal file.
   */
  static class Appender extends Thread {
    private final Log log;
    private final WAL wal;
    private final int count;
    private Exception e = null;

    Appender(final WAL wal, final int index, final int count) {
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
          // Roll every ten edits
          if (i % 10 == 0) {
            this.wal.rollWriter();
          }
          WALEdit edit = new WALEdit();
          byte[] bytes = Bytes.toBytes(i);
          edit.add(new KeyValue(bytes, bytes, bytes, now, EMPTY_1K_ARRAY));
          final HRegionInfo hri = HRegionInfo.FIRST_META_REGIONINFO;
          final FSTableDescriptors fts = new FSTableDescriptors(TEST_UTIL.getConfiguration());
          final HTableDescriptor htd = fts.get(TableName.META_TABLE_NAME);
          final long txid = wal.append(htd, hri, new WALKey(hri.getEncodedNameAsBytes(),
              TableName.META_TABLE_NAME, now), edit, sequenceId, true, null);
          Threads.sleep(ThreadLocalRandom.current().nextInt(5));
          wal.sync(txid);
        }
        String msg = getName() + " finished";
        if (isException())
          this.log.info(msg, getException());
        else
          this.log.info(msg);
      } catch (Exception e) {
        this.e = e;
        log.info("Caught exception from Appender:" + getName(), e);
      } finally {
        // Call sync on our log.else threads just hang out.
        try {
          this.wal.sync();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  //@org.junit.Rule
  //public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
  //  new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}
