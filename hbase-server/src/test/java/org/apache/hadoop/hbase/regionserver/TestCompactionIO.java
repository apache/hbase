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

import static org.apache.hadoop.hbase.HBaseTestingUtility.START_KEY;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test compaction IO cancellation.
 */
@Category(MediumTests.class)
public class TestCompactionIO {
  private static final HBaseTestingUtility UTIL = HBaseTestingUtility.createLocalHTU();
  private static final CountDownLatch latch = new CountDownLatch(1);
  /**
   * verify that a compaction stuck in IO is aborted when we attempt to close a region
   * @throws Exception
   */
  @Test
  public void testInterruptCompactionIO() throws Exception {
    byte [] STARTROW = Bytes.toBytes(START_KEY);
    byte [] COLUMN_FAMILY = fam1;
    Configuration conf = UTIL.getConfiguration();
    conf.setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024*1024);
    conf.setInt("hbase.hregion.memstore.block.multiplier", 100);
    conf.set(DefaultStoreEngine.DEFAULT_COMPACTOR_CLASS_KEY, BlockedCompactor.class.getName());
    int compactionThreshold = conf.getInt("hbase.hstore.compactionThreshold", 3);

    final HRegion r = UTIL.createLocalHRegion(UTIL.createTableDescriptor("TestCompactionIO"), null, null);

    //Create a couple store files w/ 15KB (over 10KB interval)
    int jmax = (int) Math.ceil(15.0/compactionThreshold);
    byte [] pad = new byte[1000]; // 1 KB chunk
    for (int i = 0; i < compactionThreshold; i++) {
      Put p = new Put(Bytes.add(STARTROW, Bytes.toBytes(i)));
      p.setDurability(Durability.SKIP_WAL);
      for (int j = 0; j < jmax; j++) {
        p.add(COLUMN_FAMILY, Bytes.toBytes(j), pad);
      }
      UTIL.loadRegion(r, COLUMN_FAMILY);
      r.put(p);
      r.flushcache();
    }
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          latch.await();
          Thread.sleep(1000);
          r.close();
        } catch (Exception x) {
          throw new RuntimeException(x);
        }
      }
    }).start();
    // hangs
    r.compactStores();

    // ensure that the compaction stopped, all old files are intact,
    Store s = r.stores.get(COLUMN_FAMILY);
    assertEquals(compactionThreshold, s.getStorefilesCount());
    assertTrue(s.getStorefilesSize() > 15*1000);
    // and no new store files persisted past compactStores()
    FileStatus[] ls = r.getFilesystem().listStatus(r.getRegionFileSystem().getTempDir());

    // this is happening after the compaction start, the DefaultCompactor does not
    // clean tmp files when it encounters an IOException. Should it?
    assertEquals(1, ls.length);
  }

  public static class BlockedCompactor extends DefaultCompactor {
    public BlockedCompactor(final Configuration conf, final Store store) {
      super(conf, store);
    }
    @Override
    protected boolean performCompaction(InternalScanner scanner,
        CellSink writer, long smallestReadPoint) throws IOException {
      CellSink myWriter = new CellSink() {
        @Override
        public void append(KeyValue kv) throws IOException {
          try {
            Thread.sleep(100000);
          } catch (InterruptedException ie) {
            throw new InterruptedIOException(ie.getMessage());
          }
        }
      };
      latch.countDown();
      return super.performCompaction(scanner, myWriter, smallestReadPoint);
    }
  }
}
