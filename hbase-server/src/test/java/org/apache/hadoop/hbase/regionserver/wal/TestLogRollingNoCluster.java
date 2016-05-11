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
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;

/**
 * Test many concurrent appenders to an WAL while rolling the log.
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestLogRollingNoCluster {
  @Rule public final TestRule timeout = CategoryBasedTimeout.builder().withTimeout(this.getClass()).
      withLookingForStuckThread(true).build();
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
    Path dir = TEST_UTIL.getDataTestDir();
    // The implementation needs to know the 'handler' count.
    TEST_UTIL.getConfiguration().setInt(HConstants.REGION_SERVER_HANDLER_COUNT, THREAD_COUNT);
    final Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.set(WALFactory.WAL_PROVIDER, "filesystem");
    FSUtils.setRootDir(conf, dir);
    final WALFactory wals = new WALFactory(conf, null, TestLogRollingNoCluster.class.getName());
    final WAL wal = wals.getWAL(new byte[]{}, null);
    
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
      wals.close();
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
      final MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
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
          final HTableDescriptor htd = TEST_UTIL.getMetaTableDescriptor();
          NavigableMap<byte[], Integer> scopes = new TreeMap<byte[], Integer>(
              Bytes.BYTES_COMPARATOR);
          for(byte[] fam : htd.getFamiliesKeys()) {
            scopes.put(fam, 0);
          }
          final long txid = wal.append(hri, new WALKey(hri.getEncodedNameAsBytes(),
              TableName.META_TABLE_NAME, now, mvcc, scopes), edit, true);
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
