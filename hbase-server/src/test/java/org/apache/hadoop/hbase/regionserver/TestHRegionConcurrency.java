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

import static org.apache.hadoop.hbase.HBaseTestingUtil.fam1;
import static org.apache.hadoop.hbase.HBaseTestingUtil.fam2;
import static org.apache.hadoop.hbase.HBaseTestingUtil.fam3;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MultithreadedTestUtil;
import org.apache.hadoop.hbase.MultithreadedTestUtil.RepeatingTestThread;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ClientInternalHelper;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowRegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.wal.NettyAsyncFSWALConfigHelper;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Concurrent region operations with regular and in-memory-compacting MemStores.
 */
@Tag(VerySlowRegionServerTests.TAG)
@Tag(LargeTests.TAG)
public class TestHRegionConcurrency {

  private static final Logger LOG = LoggerFactory.getLogger(TestHRegionConcurrency.class);
  private static final EventLoopGroup GROUP = new NioEventLoopGroup();
  private static final long THREAD_JOIN_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(30);

  private HBaseTestingUtil testUtil;
  private Configuration conf;
  private HRegion region;
  private String method;
  private TableName tableName;
  private final byte[] qual1 = Bytes.toBytes("qual1");
  private final byte[] qual2 = Bytes.toBytes("qual2");
  private final byte[] qual3 = Bytes.toBytes("qual3");

  protected boolean isInMemory() {
    return false;
  }

  @BeforeEach
  public void setup(TestInfo testInfo) {
    testUtil = new HBaseTestingUtil();
    conf = testUtil.getConfiguration();
    NettyAsyncFSWALConfigHelper.setEventLoopConfig(conf, GROUP, NioSocketChannel.class);
    method = testInfo.getTestMethod().get().getName();
    tableName = TableName.valueOf(method);
    conf.set(CompactingMemStore.IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY, String.valueOf(0.09));
    conf.setLong(AbstractFSWAL.WAL_SYNC_TIMEOUT_MS, 10000);
  }

  @AfterEach
  public void tearDown() throws IOException {
    try {
      HBaseTestingUtil.closeRegionAndWAL(region);
    } finally {
      EnvironmentEdgeManagerTestHelper.reset();
      testUtil.cleanupTestDir();
    }
  }

  private HRegion initHRegion(Configuration configuration, byte[]... families) throws IOException {
    RegionInfo hri = RegionInfoBuilder.newBuilder(tableName).build();
    WAL wal = HBaseTestingUtil.createWal(configuration,
      testUtil.getDataTestDirOnTestFS(method + ".log"), hri);
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0, 0, null,
      MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    if (isInMemory()) {
      boolean[] flags = new boolean[families.length];
      Arrays.fill(flags, true);
      return testUtil.createLocalHRegionWithInMemoryFlags(tableName, null, null, configuration,
        false, Durability.SYNC_WAL, wal, flags, families);
    }
    return testUtil.createLocalHRegion(tableName, null, null, configuration, false,
      Durability.SYNC_WAL, wal, families);
  }

  private Configuration initSplit() {
    conf.setInt("hbase.hstore.compactionThreshold", 2);
    conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 10 * 1000);
    conf.setLong("hbase.client.pause", 15 * 1000);
    conf.setLong(HConstants.HREGION_MAX_FILESIZE, 1024 * 128);
    return conf;
  }

  private static void interruptAndJoinThreads(Thread... threads) {
    for (Thread thread : threads) {
      if (thread != null) {
        thread.interrupt();
      }
    }
    joinThreads(threads);
  }

  private static void joinThreads(Thread... threads) {
    long deadline = System.nanoTime() + THREAD_JOIN_TIMEOUT_NS;
    boolean interrupted = false;
    List<String> aliveThreads = new ArrayList<>();
    for (Thread thread : threads) {
      if (thread == null) {
        continue;
      }
      while (thread.isAlive()) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          break;
        }
        try {
          TimeUnit.NANOSECONDS.timedJoin(thread, remaining);
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
      if (thread.isAlive()) {
        aliveThreads.add(thread.getName());
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    assertTrue(aliveThreads.isEmpty(), "Threads did not stop: " + aliveThreads);
  }

  @Test
  public void testGetWhileRegionClose() throws Exception {
    Configuration hc = initSplit();
    byte[][] families = { fam1, fam2, fam3 };
    region = initHRegion(hc, families);
    final int startRow = 100;
    for (byte[] qualifier : new byte[][] { qual1, qual2, qual3 }) {
      for (int i = startRow; i < startRow + 100; i++) {
        Put put = new Put(Bytes.toBytes("" + i));
        put.setDurability(Durability.SKIP_WAL);
        for (byte[] family : families) {
          put.addColumn(family, qualifier, null);
        }
        region.put(put);
      }
    }

    AtomicBoolean done = new AtomicBoolean(false);
    CountDownLatch initialGets = new CountDownLatch(5);
    GetTillDoneOrException[] threads = new GetTillDoneOrException[10];
    try {
      for (int i = 0; i < threads.length / 2; i++) {
        threads[i] =
          new GetTillDoneOrException(region, i, Bytes.toBytes("" + startRow), done, initialGets);
        threads[i].start();
      }
      assertTrue(initialGets.await(30, TimeUnit.SECONDS), "Initial getters did not read");
      // Force the closing state while readers are still active.
      region.closing.set(true);
      for (int i = threads.length / 2; i < threads.length; i++) {
        threads[i] =
          new GetTillDoneOrException(region, i, Bytes.toBytes("" + startRow), done, initialGets);
        threads[i].start();
      }
      joinThreads(threads);
      for (GetTillDoneOrException thread : threads) {
        assertInstanceOf(NotServingRegionException.class, thread.error, thread.getName());
      }
    } finally {
      done.set(true);
      interruptAndJoinThreads(threads);
    }
  }

  private class GetTillDoneOrException extends Thread {
    private final HRegion testRegion;
    private final Get get;
    private final AtomicBoolean done;
    private final CountDownLatch initialGets;
    private Throwable error;

    GetTillDoneOrException(HRegion testRegion, int i, byte[] row, AtomicBoolean done,
      CountDownLatch initialGets) {
      super("Getter-" + i + "-" + method);
      this.testRegion = testRegion;
      this.get = new Get(row);
      this.done = done;
      this.initialGets = initialGets;
    }

    @Override
    public void run() {
      boolean firstGet = true;
      while (!done.get() && !isInterrupted()) {
        try {
          assertTrue(testRegion.get(get).size() > 0);
          if (firstGet) {
            initialGets.countDown();
            firstGet = false;
          }
        } catch (Throwable t) {
          error = t;
          return;
        }
      }
    }
  }

  /**
   * Flushes the cache in a thread while scanning. The tests verify that the scan is coherent - e.g.
   * the returned results are always of the same or later update as the previous results. scan /
   * compact thread join
   */
  @Test
  public void testFlushCacheWhileScanning() throws IOException, InterruptedException {
    byte[] family = Bytes.toBytes("family");
    int numRows = 1000;
    int flushAndScanInterval = 10;
    int compactInterval = 10 * flushAndScanInterval;

    this.region = initHRegion(conf, family);
    FlushThread flushThread = new FlushThread();
    try {
      flushThread.start();

      Scan scan = new Scan();
      scan.addFamily(family);
      scan.setFilter(new SingleColumnValueFilter(family, qual1, CompareOperator.EQUAL,
        new BinaryComparator(Bytes.toBytes(5L))));

      int expectedCount = 0;
      List<Cell> res = new ArrayList<>();

      boolean toggle = true;
      for (long i = 0; i < numRows; i++) {
        Put put = new Put(Bytes.toBytes(i));
        put.setDurability(Durability.SKIP_WAL);
        put.addColumn(family, qual1, Bytes.toBytes(i % 10));
        region.put(put);

        if (i != 0 && i % compactInterval == 0) {
          LOG.debug("iteration = " + i + " ts=" + EnvironmentEdgeManager.currentTime());
          region.compact(true);
        }

        if (i % 10 == 5L) {
          expectedCount++;
        }

        if (i != 0 && i % flushAndScanInterval == 0) {
          res.clear();
          try (InternalScanner scanner = region.getScanner(scan)) {
            if (toggle) {
              flushThread.flush();
            }
            while (scanner.next(res)) {
              // ignore
            }
          }
          if (!toggle) {
            flushThread.flush();
          }
          assertEquals(expectedCount, res.size(),
            "toggle=" + toggle + "i=" + i + " ts=" + EnvironmentEdgeManager.currentTime());
          toggle = !toggle;
        }
      }

    } finally {
      flushThread.done();
      try {
        joinThreads(flushThread);
      } finally {
        interruptAndJoinThreads(flushThread);
      }
      flushThread.checkNoError();
      HBaseTestingUtil.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  private class FlushThread extends Thread {
    private volatile boolean done;
    private Throwable error = null;

    FlushThread() {
      super("FlushThread-" + method);
    }

    public void done() {
      synchronized (this) {
        done = true;
        notifyAll();
      }
    }

    public void checkNoError() {
      if (error != null) {
        assertNull(error);
      }
    }

    @Override
    public void run() {
      while (!done && !Thread.currentThread().isInterrupted()) {
        synchronized (this) {
          if (done) {
            break;
          }
          try {
            wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
          if (done) {
            break;
          }
        }
        try {
          region.flush(true);
        } catch (IOException e) {
          if (!done) {
            LOG.error("Error while flushing cache", e);
            error = e;
          }
          break;
        } catch (Throwable t) {
          LOG.error("Uncaught exception", t);
          throw t;
        }
      }
    }

    public void flush() {
      synchronized (this) {
        notify();
      }
    }
  }

  /**
   * So can be overridden in subclasses.
   */
  protected int getNumQualifiersForTestWritesWhileScanning() {
    return 100;
  }

  /**
   * So can be overridden in subclasses.
   */
  protected int getTestCountForTestWritesWhileScanning() {
    return 100;
  }

  /**
   * Writes very wide records and scans for the latest every time.. Flushes and compacts the region
   * every now and then to keep things realistic. by flush / scan / compaction when joining threads
   */
  @Test
  public void testWritesWhileScanning() throws IOException, InterruptedException {
    int testCount = getTestCountForTestWritesWhileScanning();
    int numRows = 1;
    int numFamilies = 10;
    int numQualifiers = getNumQualifiersForTestWritesWhileScanning();
    int flushInterval = 7;
    int compactInterval = 5 * flushInterval;
    byte[][] families = new byte[numFamilies][];
    for (int i = 0; i < numFamilies; i++) {
      families[i] = Bytes.toBytes("family" + i);
    }
    byte[][] qualifiers = new byte[numQualifiers][];
    for (int i = 0; i < numQualifiers; i++) {
      qualifiers[i] = Bytes.toBytes("qual" + i);
    }

    this.region = initHRegion(conf, families);
    FlushThread flushThread = new FlushThread();
    PutThread putThread = new PutThread(numRows, families, qualifiers);
    try {
      putThread.start();
      putThread.waitForFirstPut();

      flushThread.start();

      Scan scan = new Scan().withStartRow(Bytes.toBytes("row0")).withStopRow(Bytes.toBytes("row1"));

      int expectedCount = numFamilies * numQualifiers;
      List<Cell> res = new ArrayList<>();

      long prevTimestamp = 0L;
      for (int i = 0; i < testCount; i++) {

        if (i != 0 && i % compactInterval == 0) {
          region.compact(true);
          for (HStore store : region.getStores()) {
            store.closeAndArchiveCompactedFiles();
          }
        }

        if (i != 0 && i % flushInterval == 0) {
          flushThread.flush();
        }

        boolean previousEmpty = res.isEmpty();
        res.clear();
        try (InternalScanner scanner = region.getScanner(scan)) {
          boolean moreRows;
          do {
            moreRows = scanner.next(res);
          } while (moreRows);
        }
        if (!res.isEmpty() || !previousEmpty || i > compactInterval) {
          assertEquals(expectedCount, res.size(), "i=" + i);
          long timestamp = res.get(0).getTimestamp();
          assertTrue(timestamp >= prevTimestamp,
            "Timestamps were broke: " + timestamp + " prev: " + prevTimestamp);
          prevTimestamp = timestamp;
        }
      }

      putThread.done();

      region.flush(true);

    } finally {
      putThread.done();
      flushThread.done();
      try {
        joinThreads(flushThread, putThread);
      } finally {
        interruptAndJoinThreads(flushThread, putThread);
      }
      flushThread.checkNoError();
      putThread.checkNoError();

      try {
        HBaseTestingUtil.closeRegionAndWAL(this.region);
      } catch (DroppedSnapshotException dse) {
        // We could get this on way out because we interrupt the background flusher and it could
        // fail anywhere causing a DSE over in the background flusher... only it is not properly
        // dealt with so could still be memory hanging out when we get to here -- memory we can't
        // flush because the accounting is 'off' since original DSE.
      }
      this.region = null;
    }
  }

  private final class PutThread extends Thread {
    private volatile boolean done;
    private volatile int numPutsFinished = 0;

    private Throwable error = null;
    private int numRows;
    private byte[][] families;
    private byte[][] qualifiers;

    private PutThread(int numRows, byte[][] families, byte[][] qualifiers) {
      super("PutThread-" + method);
      this.numRows = numRows;
      this.families = families;
      this.qualifiers = qualifiers;
    }

    /**
     * Block calling thread until this instance of PutThread has put at least one row.
     */
    public void waitForFirstPut() throws InterruptedException {
      // wait until put thread actually puts some data
      while (isAlive() && numPutsFinished == 0) {
        checkNoError();
        Thread.sleep(50);
      }
    }

    public void done() {
      done = true;
      interrupt();
    }

    public void checkNoError() {
      if (error != null) {
        assertNull(error);
      }
    }

    @Override
    public void run() {
      while (!done && !Thread.currentThread().isInterrupted()) {
        try {
          for (int r = 0; r < numRows; r++) {
            byte[] row = Bytes.toBytes("row" + r);
            Put put = new Put(row);
            put.setDurability(Durability.SKIP_WAL);
            byte[] value = Bytes.toBytes(String.valueOf(numPutsFinished));
            for (byte[] family : families) {
              for (byte[] qualifier : qualifiers) {
                put.addColumn(family, qualifier, numPutsFinished, value);
              }
            }
            region.put(put);
            numPutsFinished++;
            if (numPutsFinished > 0 && numPutsFinished % 47 == 0) {
              LOG.debug("put iteration = {}", numPutsFinished);
              Delete delete = new Delete(row, (long) numPutsFinished - 30);
              region.delete(delete);
            }
            numPutsFinished++;
          }
        } catch (InterruptedIOException e) {
          // This is fine. It means we are done, or didn't get the lock on time
          LOG.info("Interrupted", e);
        } catch (IOException e) {
          LOG.error("Error while putting records", e);
          error = e;
          break;
        }
      }

    }

  }

  /**
   * Writes very wide records and gets the latest row every time.. Flushes and compacts the region
   * aggressivly to catch issues. by flush / scan / compaction when joining threads
   */
  @Test
  public void testWritesWhileGetting() throws Exception {
    int testCount = 50;
    int numRows = 1;
    int numFamilies = 10;
    int numQualifiers = 100;
    int compactInterval = 100;
    byte[][] families = new byte[numFamilies][];
    for (int i = 0; i < numFamilies; i++) {
      families[i] = Bytes.toBytes("family" + i);
    }
    byte[][] qualifiers = new byte[numQualifiers][];
    for (int i = 0; i < numQualifiers; i++) {
      qualifiers[i] = Bytes.toBytes("qual" + i);
    }

    // This test flushes constantly and can cause many files to be created,
    // possibly
    // extending over the ulimit. Make sure compactions are aggressive in
    // reducing
    // the number of HFiles created.
    Configuration configuration = HBaseConfiguration.create(conf);
    configuration.setInt("hbase.hstore.compaction.min", 1);
    configuration.setInt("hbase.hstore.compaction.max", 1000);
    this.region = initHRegion(configuration, families);
    PutThread putThread = new PutThread(numRows, families, qualifiers);
    MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext(configuration);
    RepeatingTestThread flushThread = createCompactingFlushThread(ctx);
    try {
      putThread.start();
      putThread.waitForFirstPut();
      ctx.addThread(flushThread);
      ctx.startThreads();

      Get get = new Get(Bytes.toBytes("row0"));
      Result result = null;

      int expectedCount = numFamilies * numQualifiers;

      long prevTimestamp = 0L;
      for (int i = 0; i < testCount; i++) {
        LOG.info("testWritesWhileGetting verify turn " + i);
        boolean previousEmpty = result == null || result.isEmpty();
        result = region.get(get);
        if (!result.isEmpty() || !previousEmpty || i > compactInterval) {
          assertEquals(expectedCount, result.size(), "i=" + i);
          // TODO this was removed, now what dangit?!
          // search looking for the qualifier in question?
          long timestamp = 0;
          for (Cell kv : result.rawCells()) {
            if (
              CellUtil.matchingFamily(kv, families[0])
                && CellUtil.matchingQualifier(kv, qualifiers[0])
            ) {
              timestamp = kv.getTimestamp();
            }
          }
          assertTrue(timestamp >= prevTimestamp);
          prevTimestamp = timestamp;
          ExtendedCell previousKV = null;

          for (ExtendedCell kv : ClientInternalHelper.getExtendedRawCells(result)) {
            byte[] thisValue = CellUtil.cloneValue(kv);
            if (previousKV != null) {
              if (Bytes.compareTo(CellUtil.cloneValue(previousKV), thisValue) != 0) {
                LOG.warn("These two KV should have the same value." + " Previous KV:" + previousKV
                  + "(memStoreTS:" + previousKV.getSequenceId() + ")" + ", New KV: " + kv
                  + "(memStoreTS:" + kv.getSequenceId() + ")");
                assertEquals(0, Bytes.compareTo(CellUtil.cloneValue(previousKV), thisValue));
              }
            }
            previousKV = kv;
          }
        }
      }
    } finally {
      stopWriteThreads(putThread, ctx, flushThread);
    }
    region.flush(true);
  }

  /**
   * TestCase for increment
   */
  private static class Incrementer implements Runnable {
    private HRegion region;
    private final static byte[] incRow = Bytes.toBytes("incRow");
    private final static byte[] family = Bytes.toBytes("family");
    private final static byte[] qualifier = Bytes.toBytes("qualifier");
    private final static long ONE = 1L;
    private int incCounter;

    public Incrementer(HRegion region, int incCounter) {
      this.region = region;
      this.incCounter = incCounter;
    }

    @Override
    public void run() {
      int count = 0;
      while (count < incCounter && !Thread.currentThread().isInterrupted()) {
        Increment inc = new Increment(incRow);
        inc.addColumn(family, qualifier, ONE);
        count++;
        try {
          region.increment(inc);
        } catch (IOException e) {
          LOG.info("Count=" + count + ", " + e);
          break;
        }
      }
    }
  }

  /**
   * Test case to check increment function with memstore flushing
   */
  @Test
  public void testParallelIncrementWithMemStoreFlush() throws Exception {
    byte[] family = Incrementer.family;
    this.region = initHRegion(conf, family);
    final HRegion region = this.region;
    final AtomicBoolean incrementDone = new AtomicBoolean(false);
    Runnable flusher = new Runnable() {
      @Override
      public void run() {
        while (!incrementDone.get() && !Thread.currentThread().isInterrupted()) {
          try {
            region.flush(true);
          } catch (Exception e) {
            if (!incrementDone.get()) {
              LOG.warn("Failed to flush region", e);
            }
          }
        }
      }
    };

    // after all increment finished, the row will increment to 20*100 = 2000
    int threadNum = 20;
    int incCounter = 100;
    long expected = (long) threadNum * incCounter;
    List<Thread> incrementers = new ArrayList<>(threadNum);
    Thread flushThread = new Thread(flusher, "FlushThread-" + method);
    try {
      for (int i = 0; i < threadNum; i++) {
        Thread incrementer =
          new Thread(new Incrementer(this.region, incCounter), "Incrementer-" + i + "-" + method);
        incrementers.add(incrementer);
        incrementer.start();
      }
      flushThread.start();
      for (Thread incrementer : incrementers) {
        incrementer.join();
      }

      incrementDone.set(true);
      joinThreads(flushThread);

      Get get = new Get(Incrementer.incRow);
      get.addColumn(Incrementer.family, Incrementer.qualifier);
      get.readVersions(1);
      Result res = this.region.get(get);
      List<Cell> kvs = res.getColumnCells(Incrementer.family, Incrementer.qualifier);

      // we just got the latest version
      assertEquals(1, kvs.size());
      Cell kv = kvs.get(0);
      assertEquals(expected, Bytes.toLong(kv.getValueArray(), kv.getValueOffset()));
    } finally {
      incrementDone.set(true);
      incrementers.add(flushThread);
      interruptAndJoinThreads(incrementers.toArray(new Thread[0]));
    }
  }

  /**
   * TestCase for append
   */
  private static class Appender implements Runnable {
    private HRegion region;
    private final static byte[] appendRow = Bytes.toBytes("appendRow");
    private final static byte[] family = Bytes.toBytes("family");
    private final static byte[] qualifier = Bytes.toBytes("qualifier");
    private final static byte[] CHAR = Bytes.toBytes("a");
    private int appendCounter;

    public Appender(HRegion region, int appendCounter) {
      this.region = region;
      this.appendCounter = appendCounter;
    }

    @Override
    public void run() {
      int count = 0;
      while (count < appendCounter && !Thread.currentThread().isInterrupted()) {
        Append app = new Append(appendRow);
        app.addColumn(family, qualifier, CHAR);
        count++;
        try {
          region.append(app);
        } catch (IOException e) {
          LOG.info("Count=" + count + ", max=" + appendCounter + ", " + e);
          break;
        }
      }
    }
  }

  /**
   * Test case to check append function with memstore flushing
   */
  @Test
  public void testParallelAppendWithMemStoreFlush() throws Exception {
    byte[] family = Appender.family;
    this.region = initHRegion(conf, family);
    final HRegion region = this.region;
    final AtomicBoolean appendDone = new AtomicBoolean(false);
    Runnable flusher = new Runnable() {
      @Override
      public void run() {
        while (!appendDone.get() && !Thread.currentThread().isInterrupted()) {
          try {
            region.flush(true);
          } catch (Exception e) {
            if (!appendDone.get()) {
              LOG.warn("Failed to flush region", e);
            }
          }
        }
      }
    };

    // After all append finished, the value will append to threadNum *
    // appendCounter Appender.CHAR
    int threadNum = 20;
    int appendCounter = 100;
    byte[] expected = new byte[threadNum * appendCounter];
    for (int i = 0; i < threadNum * appendCounter; i++) {
      System.arraycopy(Appender.CHAR, 0, expected, i, 1);
    }
    List<Thread> appenders = new ArrayList<>(threadNum);
    Thread flushThread = new Thread(flusher, "FlushThread-" + method);
    try {
      for (int i = 0; i < threadNum; i++) {
        Thread appender =
          new Thread(new Appender(this.region, appendCounter), "Appender-" + i + "-" + method);
        appenders.add(appender);
        appender.start();
      }
      flushThread.start();
      for (Thread appender : appenders) {
        appender.join();
      }

      appendDone.set(true);
      joinThreads(flushThread);

      Get get = new Get(Appender.appendRow);
      get.addColumn(Appender.family, Appender.qualifier);
      get.readVersions(1);
      Result res = this.region.get(get);
      List<Cell> kvs = res.getColumnCells(Appender.family, Appender.qualifier);

      // we just got the latest version
      assertEquals(1, kvs.size());
      Cell kv = kvs.get(0);
      byte[] appendResult = new byte[kv.getValueLength()];
      System.arraycopy(kv.getValueArray(), kv.getValueOffset(), appendResult, 0,
        kv.getValueLength());
      assertArrayEquals(expected, appendResult);
    } finally {
      appendDone.set(true);
      appenders.add(flushThread);
      interruptAndJoinThreads(appenders.toArray(new Thread[0]));
    }
  }

  @Test
  public void testMutateRowInParallel() throws Exception {
    final int numReaderThreads = 100;
    final CountDownLatch latch = new CountDownLatch(numReaderThreads);

    final byte[] row = Bytes.toBytes("row");
    final byte[] q1 = Bytes.toBytes("q1");
    final byte[] q2 = Bytes.toBytes("q2");
    final byte[] q3 = Bytes.toBytes("q3");
    final byte[] q4 = Bytes.toBytes("q4");
    final String v1 = "v1";
    final String v2 = "v2";

    // We need to ensure the timestamp of the delete operation is more than the previous one
    final AtomicLong deleteTimestamp = new AtomicLong();

    region = initHRegion(conf, fam1);

    // Initial values
    region.batchMutate(new Mutation[] { new Put(row).addColumn(fam1, q1, Bytes.toBytes(v1))
      .addColumn(fam1, q2, deleteTimestamp.getAndIncrement(), Bytes.toBytes(v2))
      .addColumn(fam1, q3, Bytes.toBytes(1L)).addColumn(fam1, q4, Bytes.toBytes("a")) });

    final AtomicReference<AssertionError> assertionError = new AtomicReference<>();
    List<Thread> threads = new ArrayList<>(numReaderThreads + 1);

    // Writer thread
    Thread writerThread = new Thread(() -> {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          // If all the reader threads finish, then stop the writer thread
          if (latch.await(0, TimeUnit.MILLISECONDS)) {
            return;
          }

          // Execute the mutations. This should be done atomically
          region.mutateRow(new RowMutations(row)
            .add(Arrays.asList(new Put(row).addColumn(fam1, q1, Bytes.toBytes(v2)),
              new Delete(row).addColumns(fam1, q2, deleteTimestamp.getAndIncrement()),
              new Increment(row).addColumn(fam1, q3, 1L),
              new Append(row).addColumn(fam1, q4, Bytes.toBytes("b")))));

          // We need to ensure the timestamps of the Increment/Append operations are more than the
          // previous ones
          Result result = region.get(new Get(row).addColumn(fam1, q3).addColumn(fam1, q4));
          long tsIncrement = result.getColumnLatestCell(fam1, q3).getTimestamp();
          long tsAppend = result.getColumnLatestCell(fam1, q4).getTimestamp();

          // Put the initial values
          region.batchMutate(new Mutation[] { new Put(row).addColumn(fam1, q1, Bytes.toBytes(v1))
            .addColumn(fam1, q2, deleteTimestamp.getAndIncrement(), Bytes.toBytes(v2))
            .addColumn(fam1, q3, tsIncrement + 1, Bytes.toBytes(1L))
            .addColumn(fam1, q4, tsAppend + 1, Bytes.toBytes("a")) });
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        assertionError.compareAndSet(null, new AssertionError(e));
      }
    }, "WriterThread-" + method);
    threads.add(writerThread);

    try {
      writerThread.start();

      // Reader threads
      for (int i = 0; i < numReaderThreads; i++) {
        Thread readerThread = new Thread(() -> {
          try {
            for (int j = 0; j < 10000 && assertionError.get() == null
              && !Thread.currentThread().isInterrupted(); j++) {
              // Verify the values
              Result result = region.get(new Get(row));

              // The values should be equals to either the initial values or the values after
              // executing the mutations
              String q1Value = Bytes.toString(result.getValue(fam1, q1));
              if (v1.equals(q1Value)) {
                assertEquals(v2, Bytes.toString(result.getValue(fam1, q2)));
                assertEquals(1L, Bytes.toLong(result.getValue(fam1, q3)));
                assertEquals("a", Bytes.toString(result.getValue(fam1, q4)));
              } else if (v2.equals(q1Value)) {
                assertNull(Bytes.toString(result.getValue(fam1, q2)));
                assertEquals(2L, Bytes.toLong(result.getValue(fam1, q3)));
                assertEquals("ab", Bytes.toString(result.getValue(fam1, q4)));
              } else {
                fail("the qualifier " + Bytes.toString(q1) + " should be " + v1 + " or " + v2
                  + ", but " + q1Value);
              }
            }
          } catch (Exception e) {
            assertionError.compareAndSet(null, new AssertionError(e));
          } catch (AssertionError e) {
            assertionError.compareAndSet(null, e);
          } finally {
            latch.countDown();
          }
        }, "ReaderThread-" + i + "-" + method);
        threads.add(readerThread);
        readerThread.start();
      }

      writerThread.join();

      if (assertionError.get() != null) {
        throw assertionError.get();
      }
    } finally {
      interruptAndJoinThreads(threads.toArray(new Thread[0]));
    }
  }

  private RepeatingTestThread createCompactingFlushThread(MultithreadedTestUtil.TestContext ctx) {
    RepeatingTestThread thread = new RepeatingTestThread(ctx) {
      @Override
      public void doAnAction() throws Exception {
        region.flush(true);
        region.compact(false);
        for (HStore store : region.getStores()) {
          store.closeAndArchiveCompactedFiles();
        }
      }
    };
    thread.setName("CompactingFlushThread-" + method);
    return thread;
  }

  private void stopWriteThreads(PutThread putThread, MultithreadedTestUtil.TestContext ctx,
    Thread flushThread) throws Exception {
    putThread.done();
    ctx.setStopFlag(true);
    try {
      joinThreads(putThread, flushThread);
    } finally {
      interruptAndJoinThreads(putThread, flushThread);
    }
    ctx.stop();
    putThread.checkNoError();
  }

  @Test
  public void testWriteThreadsCleanupAfterFlushFailure() throws Exception {
    region = initHRegion(conf, fam1);
    PutThread putThread = new PutThread(1, new byte[][] { fam1 }, new byte[][] { qual1 });
    MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext(conf);
    RepeatingTestThread flushThread = createCompactingFlushThread(ctx);
    HRegion failingRegion = mock(HRegion.class);
    doThrow(new IOException("Injected flush failure")).when(failingRegion).flush(true);
    try {
      putThread.start();
      putThread.waitForFirstPut();
      ctx.addThread(flushThread);
      ctx.startThreads();
      IOException error = assertThrows(IOException.class, () -> {
        try {
          failingRegion.flush(true);
        } finally {
          stopWriteThreads(putThread, ctx, flushThread);
        }
      });
      assertEquals("Injected flush failure", error.getMessage());
      assertFalse(putThread.isAlive());
      assertFalse(flushThread.isAlive());
    } finally {
      stopWriteThreads(putThread, ctx, flushThread);
    }
  }

  @Test
  public void testStopBeforeThreadStart() throws Exception {
    region = initHRegion(conf, fam1);
    PutThread putThread = new PutThread(1, new byte[][] { fam1 }, new byte[][] { qual1 });
    FlushThread flushThread = new FlushThread();
    putThread.done();
    flushThread.done();
    try {
      putThread.start();
      flushThread.start();
      joinThreads(putThread, flushThread);
      putThread.checkNoError();
      flushThread.checkNoError();
      assertEquals(0, putThread.numPutsFinished);
    } finally {
      interruptAndJoinThreads(putThread, flushThread);
    }
  }

  @Test
  public void testThreadCleanupPreservesInterrupt() {
    CountDownLatch release = new CountDownLatch(1);
    Thread worker = new Thread(() -> {
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }, "CleanupWorker-" + method);
    try {
      worker.start();
      Thread.currentThread().interrupt();
      interruptAndJoinThreads(worker);
      assertTrue(Thread.currentThread().isInterrupted());
      assertFalse(worker.isAlive());
    } finally {
      Thread.interrupted();
      release.countDown();
      interruptAndJoinThreads(worker);
    }
  }
}
