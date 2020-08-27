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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TestIncrementsFromClientSide;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Increments with some concurrency against a region to ensure we get the right answer.
 * Test is parameterized to run the fast and slow path increments; if fast,
 * HRegion.INCREMENT_FAST_BUT_NARROW_CONSISTENCY_KEY is true.
 *
 * <p>There is similar test up in TestAtomicOperation. It does a test where it has 100 threads
 * doing increments across two column families all on one row and the increments are connected to
 * prove atomicity on row.
 */
@Category(MediumTests.class)
public class TestRegionIncrement {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionIncrement.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionIncrement.class);
  @Rule public TestName name = new TestName();
  private static HBaseTestingUtility TEST_UTIL;
  private final static byte [] INCREMENT_BYTES = Bytes.toBytes("increment");
  private static final int THREAD_COUNT = 10;
  private static final int INCREMENT_COUNT = 10000;

  @Before
  public void setUp() throws Exception {
    TEST_UTIL = HBaseTestingUtility.createLocalHTU();
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.cleanupTestDir();
  }

  private HRegion getRegion(final Configuration conf, final String tableName) throws IOException {
    FSHLog wal = new FSHLog(FileSystem.get(conf), TEST_UTIL.getDataTestDir(),
      TEST_UTIL.getDataTestDir().toString(), conf);
    wal.init();
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0,
      0, null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    return (HRegion)TEST_UTIL.createLocalHRegion(Bytes.toBytes(tableName),
      HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, tableName, conf,
      false, Durability.SKIP_WAL, wal, INCREMENT_BYTES);
  }

  private void closeRegion(final HRegion region) throws IOException {
    region.close();
    region.getWAL().close();
  }

  @Test
  public void testMVCCCausingMisRead() throws IOException {
    final HRegion region = getRegion(TEST_UTIL.getConfiguration(), this.name.getMethodName());
    try {
      // ADD TEST HERE!!
    } finally {
      closeRegion(region);
    }
  }

  /**
   * Increments a single cell a bunch of times.
   */
  private static class SingleCellIncrementer extends Thread {
    private final int count;
    private final HRegion region;
    private final Increment increment;

    SingleCellIncrementer(final int i, final int count, final HRegion region,
        final Increment increment) {
      super("" + i);
      setDaemon(true);
      this.count = count;
      this.region = region;
      this.increment = increment;
    }

    @Override
    public void run() {
      for (int i = 0; i < this.count; i++) {
        try {
          this.region.increment(this.increment);
          // LOG.info(getName() + " " + i);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * Increments a random row's Cell <code>count</code> times.
   */
  private static class CrossRowCellIncrementer extends Thread {
    private final int count;
    private final HRegion region;
    private final Increment [] increments;

    CrossRowCellIncrementer(final int i, final int count, final HRegion region, final int range) {
      super("" + i);
      setDaemon(true);
      this.count = count;
      this.region = region;
      this.increments = new Increment[range];
      for (int ii = 0; ii < range; ii++) {
        this.increments[ii] = new Increment(Bytes.toBytes(i));
        this.increments[ii].addColumn(INCREMENT_BYTES, INCREMENT_BYTES, 1);
      }
    }

    @Override
    public void run() {
      for (int i = 0; i < this.count; i++) {
        try {
          int index = ThreadLocalRandom.current().nextInt(0, this.increments.length);
          this.region.increment(this.increments[index]);
          // LOG.info(getName() + " " + index);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * Have each thread update its own Cell. Avoid contention with another thread.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testUnContendedSingleCellIncrement()
  throws IOException, InterruptedException {
    final HRegion region = getRegion(TEST_UTIL.getConfiguration(),
        TestIncrementsFromClientSide.filterStringSoTableNameSafe(this.name.getMethodName()));
    long startTime = System.currentTimeMillis();
    try {
      SingleCellIncrementer [] threads = new SingleCellIncrementer[THREAD_COUNT];
      for (int i = 0; i < threads.length; i++) {
        byte [] rowBytes = Bytes.toBytes(i);
        Increment increment = new Increment(rowBytes);
        increment.addColumn(INCREMENT_BYTES, INCREMENT_BYTES, 1);
        threads[i] = new SingleCellIncrementer(i, INCREMENT_COUNT, region, increment);
      }
      for (int i = 0; i < threads.length; i++) {
        threads[i].start();
      }
      for (int i = 0; i < threads.length; i++) {
        threads[i].join();
      }
      RegionScanner regionScanner = region.getScanner(new Scan());
      List<Cell> cells = new ArrayList<>(THREAD_COUNT);
      while(regionScanner.next(cells)) continue;
      assertEquals(THREAD_COUNT, cells.size());
      long total = 0;
      for (Cell cell: cells) total +=
        Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
      assertEquals(INCREMENT_COUNT * THREAD_COUNT, total);
    } finally {
      closeRegion(region);
      LOG.info(this.name.getMethodName() + " " + (System.currentTimeMillis() - startTime) + "ms");
    }
  }

  /**
   * Have each thread update its own Cell. Avoid contention with another thread.
   * This is
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testContendedAcrossCellsIncrement()
  throws IOException, InterruptedException {
    final HRegion region = getRegion(TEST_UTIL.getConfiguration(),
        TestIncrementsFromClientSide.filterStringSoTableNameSafe(this.name.getMethodName()));
    long startTime = System.currentTimeMillis();
    try {
      CrossRowCellIncrementer [] threads = new CrossRowCellIncrementer[THREAD_COUNT];
      for (int i = 0; i < threads.length; i++) {
        threads[i] = new CrossRowCellIncrementer(i, INCREMENT_COUNT, region, THREAD_COUNT);
      }
      for (int i = 0; i < threads.length; i++) {
        threads[i].start();
      }
      for (int i = 0; i < threads.length; i++) {
        threads[i].join();
      }
      RegionScanner regionScanner = region.getScanner(new Scan());
      List<Cell> cells = new ArrayList<>(100);
      while(regionScanner.next(cells)) continue;
      assertEquals(THREAD_COUNT, cells.size());
      long total = 0;
      for (Cell cell: cells) total +=
        Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
      assertEquals(INCREMENT_COUNT * THREAD_COUNT, total);
    } finally {
      closeRegion(region);
      LOG.info(this.name.getMethodName() + " " + (System.currentTimeMillis() - startTime) + "ms");
    }
  }
}
