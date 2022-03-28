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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MultithreadedTestUtil;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestContext;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestThread;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowRegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
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
 * Testing of HRegion.incrementColumnValue, HRegion.increment,
 * and HRegion.append
 */
@Category({VerySlowRegionServerTests.class, LargeTests.class}) // Starts 100 threads
public class TestAtomicOperation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAtomicOperation.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAtomicOperation.class);
  @Rule public TestName name = new TestName();

  HRegion region = null;
  private HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  // Test names
  static  byte[] tableName;
  static final byte[] qual1 = Bytes.toBytes("qual1");
  static final byte[] qual2 = Bytes.toBytes("qual2");
  static final byte[] qual3 = Bytes.toBytes("qual3");
  static final byte[] value1 = Bytes.toBytes("value1");
  static final byte[] value2 = Bytes.toBytes("value2");
  static final byte [] row = Bytes.toBytes("rowA");
  static final byte [] row2 = Bytes.toBytes("rowB");

  @Before
  public void setup() {
    tableName = Bytes.toBytes(name.getMethodName());
  }

  @After
  public void teardown() throws IOException {
    if (region != null) {
      CacheConfig cacheConfig = region.getStores().get(0).getCacheConfig();
      region.close();
      WAL wal = region.getWAL();
      if (wal != null) {
        wal.close();
      }
      cacheConfig.getBlockCache().ifPresent(BlockCache::shutdown);
      region = null;
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // New tests that doesn't spin up a mini cluster but rather just test the
  // individual code pieces in the HRegion.
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Test basic append operation.
   * More tests in
   * @see org.apache.hadoop.hbase.client.TestFromClientSide#testAppend()
   */
  @Test
  public void testAppend() throws IOException {
    initHRegion(tableName, name.getMethodName(), fam1);
    String v1 = "Ultimate Answer to the Ultimate Question of Life,"+
    " The Universe, and Everything";
    String v2 = " is... 42.";
    Append a = new Append(row);
    a.setReturnResults(false);
    a.addColumn(fam1, qual1, Bytes.toBytes(v1));
    a.addColumn(fam1, qual2, Bytes.toBytes(v2));
    assertTrue(region.append(a, HConstants.NO_NONCE, HConstants.NO_NONCE).isEmpty());
    a = new Append(row);
    a.addColumn(fam1, qual1, Bytes.toBytes(v2));
    a.addColumn(fam1, qual2, Bytes.toBytes(v1));
    Result result = region.append(a, HConstants.NO_NONCE, HConstants.NO_NONCE);
    assertEquals(0, Bytes.compareTo(Bytes.toBytes(v1+v2), result.getValue(fam1, qual1)));
    assertEquals(0, Bytes.compareTo(Bytes.toBytes(v2+v1), result.getValue(fam1, qual2)));
  }

  @Test
  public void testAppendWithMultipleFamilies() throws IOException {
    final byte[] fam3 = Bytes.toBytes("colfamily31");
    initHRegion(tableName, name.getMethodName(), fam1, fam2, fam3);
    String v1 = "Appended";
    String v2 = "Value";

    Append a = new Append(row);
    a.setReturnResults(false);
    a.addColumn(fam1, qual1, Bytes.toBytes(v1));
    a.addColumn(fam2, qual2, Bytes.toBytes(v2));
    Result result = region.append(a, HConstants.NO_NONCE, HConstants.NO_NONCE);
    assertTrue("Expected an empty result but result contains " + result.size() + " keys",
      result.isEmpty());

    a = new Append(row);
    a.addColumn(fam2, qual2, Bytes.toBytes(v1));
    a.addColumn(fam1, qual1, Bytes.toBytes(v2));
    a.addColumn(fam3, qual3, Bytes.toBytes(v2));
    a.addColumn(fam1, qual2, Bytes.toBytes(v1));

    result = region.append(a, HConstants.NO_NONCE, HConstants.NO_NONCE);

    byte[] actualValue1 = result.getValue(fam1, qual1);
    byte[] actualValue2 = result.getValue(fam2, qual2);
    byte[] actualValue3 = result.getValue(fam3, qual3);
    byte[] actualValue4 = result.getValue(fam1, qual2);

    assertNotNull("Value1 should bot be null", actualValue1);
    assertNotNull("Value2 should bot be null", actualValue2);
    assertNotNull("Value3 should bot be null", actualValue3);
    assertNotNull("Value4 should bot be null", actualValue4);
    assertEquals(0, Bytes.compareTo(Bytes.toBytes(v1 + v2), actualValue1));
    assertEquals(0, Bytes.compareTo(Bytes.toBytes(v2 + v1), actualValue2));
    assertEquals(0, Bytes.compareTo(Bytes.toBytes(v2), actualValue3));
    assertEquals(0, Bytes.compareTo(Bytes.toBytes(v1), actualValue4));
  }

  @Test
  public void testAppendWithNonExistingFamily() throws IOException {
    initHRegion(tableName, name.getMethodName(), fam1);
    final String v1 = "Value";
    final Append a = new Append(row);
    a.addColumn(fam1, qual1, Bytes.toBytes(v1));
    a.addColumn(fam2, qual2, Bytes.toBytes(v1));
    Result result = null;
    try {
      result = region.append(a, HConstants.NO_NONCE, HConstants.NO_NONCE);
      fail("Append operation should fail with NoSuchColumnFamilyException.");
    } catch (NoSuchColumnFamilyException e) {
      assertEquals(null, result);
    } catch (Exception e) {
      fail("Append operation should fail with NoSuchColumnFamilyException.");
    }
  }

  @Test
  public void testIncrementWithNonExistingFamily() throws IOException {
    initHRegion(tableName, name.getMethodName(), fam1);
    final Increment inc = new Increment(row);
    inc.addColumn(fam1, qual1, 1);
    inc.addColumn(fam2, qual2, 1);
    inc.setDurability(Durability.ASYNC_WAL);
    try {
      region.increment(inc, HConstants.NO_NONCE, HConstants.NO_NONCE);
    } catch (NoSuchColumnFamilyException e) {
      final Get g = new Get(row);
      final Result result = region.get(g);
      assertEquals(null, result.getValue(fam1, qual1));
      assertEquals(null, result.getValue(fam2, qual2));
    } catch (Exception e) {
      fail("Increment operation should fail with NoSuchColumnFamilyException.");
    }
  }

  /**
   * Test multi-threaded increments.
   */
  @Test
  public void testIncrementMultiThreads() throws IOException {
    boolean fast = true;
    LOG.info("Starting test testIncrementMultiThreads");
    // run a with mixed column families (1 and 3 versions)
    initHRegion(tableName, name.getMethodName(), new int[] {1,3}, fam1, fam2);

    // Create 100 threads, each will increment by its own quantity. All 100 threads update the
    // same row over two column families.
    int numThreads = 100;
    int incrementsPerThread = 1000;
    Incrementer[] all = new Incrementer[numThreads];
    int expectedTotal = 0;
    // create all threads
    for (int i = 0; i < numThreads; i++) {
      all[i] = new Incrementer(region, i, i, incrementsPerThread);
      expectedTotal += (i * incrementsPerThread);
    }

    // run all threads
    for (int i = 0; i < numThreads; i++) {
      all[i].start();
    }

    // wait for all threads to finish
    for (int i = 0; i < numThreads; i++) {
      try {
        all[i].join();
      } catch (InterruptedException e) {
        LOG.info("Ignored", e);
      }
    }
    assertICV(row, fam1, qual1, expectedTotal, fast);
    assertICV(row, fam1, qual2, expectedTotal*2, fast);
    assertICV(row, fam2, qual3, expectedTotal*3, fast);
    LOG.info("testIncrementMultiThreads successfully verified that total is " + expectedTotal);
  }


  private void assertICV(byte [] row,
                         byte [] familiy,
                         byte[] qualifier,
                         long amount,
                         boolean fast) throws IOException {
    // run a get and see?
    Get get = new Get(row);
    if (fast) get.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
    get.addColumn(familiy, qualifier);
    Result result = region.get(get);
    assertEquals(1, result.size());

    Cell kv = result.rawCells()[0];
    long r = Bytes.toLong(CellUtil.cloneValue(kv));
    assertEquals(amount, r);
  }

  private void initHRegion (byte [] tableName, String callingMethod,
      byte[] ... families)
    throws IOException {
    initHRegion(tableName, callingMethod, null, families);
  }

  private void initHRegion(byte[] tableName, String callingMethod, int[] maxVersions,
    byte[]... families) throws IOException {
    TableDescriptorBuilder builder =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));

    int i = 0;
    for (byte[] family : families) {
      ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(family)
        .setMaxVersions(maxVersions != null ? maxVersions[i++] : 1).build();
      builder.setColumnFamily(familyDescriptor);
    }
    TableDescriptor tableDescriptor = builder.build();
    RegionInfo info = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName()).build();
    region = TEST_UTIL.createLocalHRegion(info, tableDescriptor);
  }

  /**
   * A thread that makes increment calls always on the same row, this.row against two column
   * families on this row.
   */
  public static class Incrementer extends Thread {

    private final Region region;
    private final int numIncrements;
    private final int amount;


    public Incrementer(Region region, int threadNumber, int amount, int numIncrements) {
      super("Incrementer." + threadNumber);
      this.region = region;
      this.numIncrements = numIncrements;
      this.amount = amount;
      setDaemon(true);
    }

    @Override
    public void run() {
      for (int i = 0; i < numIncrements; i++) {
        try {
          Increment inc = new Increment(row);
          inc.addColumn(fam1, qual1, amount);
          inc.addColumn(fam1, qual2, amount*2);
          inc.addColumn(fam2, qual3, amount*3);
          inc.setDurability(Durability.ASYNC_WAL);
          Result result = region.increment(inc);
          if (result != null) {
            assertEquals(Bytes.toLong(result.getValue(fam1, qual1))*2,
              Bytes.toLong(result.getValue(fam1, qual2)));
            assertTrue(result.getValue(fam2, qual3) != null);
            assertEquals(Bytes.toLong(result.getValue(fam1, qual1))*3,
              Bytes.toLong(result.getValue(fam2, qual3)));
            assertEquals(Bytes.toLong(result.getValue(fam1, qual1))*2,
               Bytes.toLong(result.getValue(fam1, qual2)));
            long fam1Increment = Bytes.toLong(result.getValue(fam1, qual1))*3;
            long fam2Increment = Bytes.toLong(result.getValue(fam2, qual3));
            assertEquals("fam1=" + fam1Increment + ", fam2=" + fam2Increment,
              fam1Increment, fam2Increment);
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Test
  public void testAppendMultiThreads() throws IOException {
    LOG.info("Starting test testAppendMultiThreads");
    // run a with mixed column families (1 and 3 versions)
    initHRegion(tableName, name.getMethodName(), new int[] {1,3}, fam1, fam2);

    int numThreads = 100;
    int opsPerThread = 100;
    AtomicOperation[] all = new AtomicOperation[numThreads];
    final byte[] val = new byte[]{1};

    AtomicInteger failures = new AtomicInteger(0);
    // create all threads
    for (int i = 0; i < numThreads; i++) {
      all[i] = new AtomicOperation(region, opsPerThread, null, failures) {
        @Override
        public void run() {
          for (int i=0; i<numOps; i++) {
            try {
              Append a = new Append(row);
              a.addColumn(fam1, qual1, val);
              a.addColumn(fam1, qual2, val);
              a.addColumn(fam2, qual3, val);
              a.setDurability(Durability.ASYNC_WAL);
              region.append(a, HConstants.NO_NONCE, HConstants.NO_NONCE);

              Get g = new Get(row);
              Result result = region.get(g);
              assertEquals(result.getValue(fam1, qual1).length, result.getValue(fam1, qual2).length);
              assertEquals(result.getValue(fam1, qual1).length, result.getValue(fam2, qual3).length);
            } catch (IOException e) {
              e.printStackTrace();
              failures.incrementAndGet();
              fail();
            }
          }
        }
      };
    }

    // run all threads
    for (int i = 0; i < numThreads; i++) {
      all[i].start();
    }

    // wait for all threads to finish
    for (int i = 0; i < numThreads; i++) {
      try {
        all[i].join();
      } catch (InterruptedException e) {
      }
    }
    assertEquals(0, failures.get());
    Get g = new Get(row);
    Result result = region.get(g);
    assertEquals(10000, result.getValue(fam1, qual1).length);
    assertEquals(10000, result.getValue(fam1, qual2).length);
    assertEquals(10000, result.getValue(fam2, qual3).length);
  }
  /**
   * Test multi-threaded row mutations.
   */
  @Test
  public void testRowMutationMultiThreads() throws IOException {
    LOG.info("Starting test testRowMutationMultiThreads");
    initHRegion(tableName, name.getMethodName(), fam1);

    // create 10 threads, each will alternate between adding and
    // removing a column
    int numThreads = 10;
    int opsPerThread = 250;
    AtomicOperation[] all = new AtomicOperation[numThreads];

    AtomicLong timeStamps = new AtomicLong(0);
    AtomicInteger failures = new AtomicInteger(0);
    // create all threads
    for (int i = 0; i < numThreads; i++) {
      all[i] = new AtomicOperation(region, opsPerThread, timeStamps, failures) {
        @Override
        public void run() {
          boolean op = true;
          for (int i=0; i<numOps; i++) {
            try {
              // throw in some flushes
              if (i%10==0) {
                synchronized(region) {
                  LOG.debug("flushing");
                  region.flush(true);
                  if (i%100==0) {
                    region.compact(false);
                  }
                }
              }
              long ts = timeStamps.incrementAndGet();
              RowMutations rm = new RowMutations(row);
              if (op) {
                Put p = new Put(row, ts);
                p.addColumn(fam1, qual1, value1);
                p.setDurability(Durability.ASYNC_WAL);
                rm.add(p);
                Delete d = new Delete(row);
                d.addColumns(fam1, qual2, ts);
                d.setDurability(Durability.ASYNC_WAL);
                rm.add(d);
              } else {
                Delete d = new Delete(row);
                d.addColumns(fam1, qual1, ts);
                d.setDurability(Durability.ASYNC_WAL);
                rm.add(d);
                Put p = new Put(row, ts);
                p.addColumn(fam1, qual2, value2);
                p.setDurability(Durability.ASYNC_WAL);
                rm.add(p);
              }
              region.mutateRow(rm);
              op ^= true;
              // check: should always see exactly one column
              Get g = new Get(row);
              Result r = region.get(g);
              if (r.size() != 1) {
                LOG.debug(Objects.toString(r));
                failures.incrementAndGet();
                fail();
              }
            } catch (IOException e) {
              e.printStackTrace();
              failures.incrementAndGet();
              fail();
            }
          }
        }
      };
    }

    // run all threads
    for (int i = 0; i < numThreads; i++) {
      all[i].start();
    }

    // wait for all threads to finish
    for (int i = 0; i < numThreads; i++) {
      try {
        all[i].join();
      } catch (InterruptedException e) {
      }
    }
    assertEquals(0, failures.get());
  }


  /**
   * Test multi-threaded region mutations.
   */
  @Test
  public void testMultiRowMutationMultiThreads() throws IOException {

    LOG.info("Starting test testMultiRowMutationMultiThreads");
    initHRegion(tableName, name.getMethodName(), fam1);

    // create 10 threads, each will alternate between adding and
    // removing a column
    int numThreads = 10;
    int opsPerThread = 250;
    AtomicOperation[] all = new AtomicOperation[numThreads];

    AtomicLong timeStamps = new AtomicLong(0);
    AtomicInteger failures = new AtomicInteger(0);
    final List<byte[]> rowsToLock = Arrays.asList(row, row2);
    // create all threads
    for (int i = 0; i < numThreads; i++) {
      all[i] = new AtomicOperation(region, opsPerThread, timeStamps, failures) {
        @Override
        public void run() {
          boolean op = true;
          for (int i=0; i<numOps; i++) {
            try {
              // throw in some flushes
              if (i%10==0) {
                synchronized(region) {
                  LOG.debug("flushing");
                  region.flush(true);
                  if (i%100==0) {
                    region.compact(false);
                  }
                }
              }
              long ts = timeStamps.incrementAndGet();
              List<Mutation> mrm = new ArrayList<>();
              if (op) {
                Put p = new Put(row2, ts);
                p.addColumn(fam1, qual1, value1);
                p.setDurability(Durability.ASYNC_WAL);
                mrm.add(p);
                Delete d = new Delete(row);
                d.addColumns(fam1, qual1, ts);
                d.setDurability(Durability.ASYNC_WAL);
                mrm.add(d);
              } else {
                Delete d = new Delete(row2);
                d.addColumns(fam1, qual1, ts);
                d.setDurability(Durability.ASYNC_WAL);
                mrm.add(d);
                Put p = new Put(row, ts);
                p.setDurability(Durability.ASYNC_WAL);
                p.addColumn(fam1, qual1, value2);
                mrm.add(p);
              }
              region.mutateRowsWithLocks(mrm, rowsToLock, HConstants.NO_NONCE, HConstants.NO_NONCE);
              op ^= true;
              // check: should always see exactly one column
              Scan s = new Scan().withStartRow(row);
              RegionScanner rs = region.getScanner(s);
              List<Cell> r = new ArrayList<>();
              while (rs.next(r))
                ;
              rs.close();
              if (r.size() != 1) {
                LOG.debug(Objects.toString(r));
                failures.incrementAndGet();
                fail();
              }
            } catch (IOException e) {
              e.printStackTrace();
              failures.incrementAndGet();
              fail();
            }
          }
        }
      };
    }

    // run all threads
    for (int i = 0; i < numThreads; i++) {
      all[i].start();
    }

    // wait for all threads to finish
    for (int i = 0; i < numThreads; i++) {
      try {
        all[i].join();
      } catch (InterruptedException e) {
      }
    }
    assertEquals(0, failures.get());
  }

  public static class AtomicOperation extends Thread {
    protected final HRegion region;
    protected final int numOps;
    protected final AtomicLong timeStamps;
    protected final AtomicInteger failures;

    public AtomicOperation(HRegion region, int numOps, AtomicLong timeStamps,
        AtomicInteger failures) {
      this.region = region;
      this.numOps = numOps;
      this.timeStamps = timeStamps;
      this.failures = failures;
    }
  }

  private static CountDownLatch latch = new CountDownLatch(1);
  private enum TestStep {
    INIT,                  // initial put of 10 to set value of the cell
    PUT_STARTED,           // began doing a put of 50 to cell
    PUT_COMPLETED,         // put complete (released RowLock, but may not have advanced MVCC).
    CHECKANDPUT_STARTED,   // began checkAndPut: if 10 -> 11
    CHECKANDPUT_COMPLETED  // completed checkAndPut
    // NOTE: at the end of these steps, the value of the cell should be 50, not 11!
  }
  private static volatile TestStep testStep = TestStep.INIT;
  private final String family = "f1";

  /**
   * Test written as a verifier for HBASE-7051, CheckAndPut should properly read
   * MVCC.
   *
   * Moved into TestAtomicOperation from its original location, TestHBase7051
   */
  @Test
  public void testPutAndCheckAndPutInParallel() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setClass(HConstants.REGION_IMPL, MockHRegion.class, HeapSize.class);
    TableDescriptorBuilder tableDescriptorBuilder =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName()));
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
    this.region = TEST_UTIL.createLocalHRegion(tableDescriptorBuilder.build(), null, null);
    Put[] puts = new Put[1];
    Put put = new Put(Bytes.toBytes("r1"));
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("q1"), Bytes.toBytes("10"));
    puts[0] = put;

    region.batchMutate(puts);
    MultithreadedTestUtil.TestContext ctx =
      new MultithreadedTestUtil.TestContext(conf);
    ctx.addThread(new PutThread(ctx, region));
    ctx.addThread(new CheckAndPutThread(ctx, region));
    ctx.startThreads();
    while (testStep != TestStep.CHECKANDPUT_COMPLETED) {
      Thread.sleep(100);
    }
    ctx.stop();
    Scan s = new Scan();
    RegionScanner scanner = region.getScanner(s);
    List<Cell> results = new ArrayList<>();
    ScannerContext scannerContext = ScannerContext.newBuilder().setBatchLimit(2).build();
    scanner.next(results, scannerContext);
    for (Cell keyValue : results) {
      assertEquals("50",Bytes.toString(CellUtil.cloneValue(keyValue)));
    }
  }

  private class PutThread extends TestThread {
    private Region region;
    PutThread(TestContext ctx, Region region) {
      super(ctx);
      this.region = region;
    }

    @Override
    public void doWork() throws Exception {
      Put[] puts = new Put[1];
      Put put = new Put(Bytes.toBytes("r1"));
      put.addColumn(Bytes.toBytes(family), Bytes.toBytes("q1"), Bytes.toBytes("50"));
      puts[0] = put;
      testStep = TestStep.PUT_STARTED;
      region.batchMutate(puts);
    }
  }

  private class CheckAndPutThread extends TestThread {
    private Region region;
    CheckAndPutThread(TestContext ctx, Region region) {
      super(ctx);
      this.region = region;
   }

    @Override
    public void doWork() throws Exception {
      Put[] puts = new Put[1];
      Put put = new Put(Bytes.toBytes("r1"));
      put.addColumn(Bytes.toBytes(family), Bytes.toBytes("q1"), Bytes.toBytes("11"));
      puts[0] = put;
      while (testStep != TestStep.PUT_COMPLETED) {
        Thread.sleep(100);
      }
      testStep = TestStep.CHECKANDPUT_STARTED;
      region.checkAndMutate(Bytes.toBytes("r1"), Bytes.toBytes(family), Bytes.toBytes("q1"),
        CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("10")), put);
      testStep = TestStep.CHECKANDPUT_COMPLETED;
    }
  }

  public static class MockHRegion extends HRegion {

    public MockHRegion(Path tableDir, WAL log, FileSystem fs, Configuration conf,
        final RegionInfo regionInfo, final TableDescriptor htd, RegionServerServices rsServices) {
      super(tableDir, log, fs, conf, regionInfo, htd, rsServices);
    }

    @Override
    protected RowLock getRowLockInternal(final byte[] row, boolean readLock,
        final RowLock prevRowlock) throws IOException {
      if (testStep == TestStep.CHECKANDPUT_STARTED) {
        latch.countDown();
      }
      return new WrappedRowLock(super.getRowLockInternal(row, readLock, null));
    }

    public class WrappedRowLock implements RowLock {

      private final RowLock rowLock;

      private WrappedRowLock(RowLock rowLock) {
        this.rowLock = rowLock;
      }


      @Override
      public void release() {
        if (testStep == TestStep.INIT) {
          this.rowLock.release();
          return;
        }

        if (testStep == TestStep.PUT_STARTED) {
          try {
            testStep = TestStep.PUT_COMPLETED;
            this.rowLock.release();
            // put has been written to the memstore and the row lock has been released, but the
            // MVCC has not been advanced.  Prior to fixing HBASE-7051, the following order of
            // operations would cause the non-atomicity to show up:
            // 1) Put releases row lock (where we are now)
            // 2) CheckAndPut grabs row lock and reads the value prior to the put (10)
            //    because the MVCC has not advanced
            // 3) Put advances MVCC
            // So, in order to recreate this order, we wait for the checkAndPut to grab the rowLock
            // (see below), and then wait some more to give the checkAndPut time to read the old
            // value.
            latch.await();
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        else if (testStep == TestStep.CHECKANDPUT_STARTED) {
          this.rowLock.release();
        }
      }
    }
  }
}
