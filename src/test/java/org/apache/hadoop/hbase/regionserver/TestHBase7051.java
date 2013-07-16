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
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.MultithreadedTestUtil;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestContext;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestThread;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HashedBytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

/**
 * Test of HBASE-7051; that checkAndPuts and puts behave atomically with respect to each other.
 * Rather than perform a bunch of trials to verify atomicity, this test recreates a race condition
 * that causes the test to fail if checkAndPut doesn't wait for outstanding put transactions
 * to complete.  It does this by invasively overriding HRegion function to affect the timing of
 * the operations.
 */
@Category(SmallTests.class)
public class TestHBase7051 {

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
  	 
  @Test
  public void testPutAndCheckAndPutInParallel() throws Exception {

    final String tableName = "testPutAndCheckAndPut";
    Configuration conf = HBaseConfiguration.create();
    conf.setClass(HConstants.REGION_IMPL, MockHRegion.class, HeapSize.class);
    final MockHRegion region = (MockHRegion) TestHRegion.initHRegion(Bytes.toBytes(tableName),
        tableName, conf, Bytes.toBytes(family));

    List<Pair<Mutation, Integer>> putsAndLocks = Lists.newArrayList();
    Put[] puts = new Put[1];
    Put put = new Put(Bytes.toBytes("r1"));
    put.add(Bytes.toBytes(family), Bytes.toBytes("q1"), Bytes.toBytes("10"));
    puts[0] = put;
    Pair<Mutation, Integer> pair = new Pair<Mutation, Integer>(puts[0], null);

    putsAndLocks.add(pair);

    region.batchMutate(putsAndLocks.toArray(new Pair[0]));
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
    List<KeyValue> results = new ArrayList<KeyValue>();
    scanner.next(results, 2);
    for (KeyValue keyValue : results) {
      assertEquals("50",Bytes.toString(keyValue.getValue()));
    }

  }

  private class PutThread extends TestThread {
    private MockHRegion region;
    PutThread(TestContext ctx, MockHRegion region) {
      super(ctx);
      this.region = region;
    }

    public void doWork() throws Exception {
      List<Pair<Mutation, Integer>> putsAndLocks = Lists.newArrayList();
      Put[] puts = new Put[1];
      Put put = new Put(Bytes.toBytes("r1"));
      put.add(Bytes.toBytes(family), Bytes.toBytes("q1"), Bytes.toBytes("50"));
      puts[0] = put;
      Pair<Mutation, Integer> pair = new Pair<Mutation, Integer>(puts[0], null);
      putsAndLocks.add(pair);
      testStep = TestStep.PUT_STARTED;
      region.batchMutate(putsAndLocks.toArray(new Pair[0]));
    }
  }

  private class CheckAndPutThread extends TestThread {
    private MockHRegion region;
    CheckAndPutThread(TestContext ctx, MockHRegion region) {
      super(ctx);
      this.region = region;
   }

    public void doWork() throws Exception {
      Put[] puts = new Put[1];
      Put put = new Put(Bytes.toBytes("r1"));
      put.add(Bytes.toBytes(family), Bytes.toBytes("q1"), Bytes.toBytes("11"));
      puts[0] = put;
      while (testStep != TestStep.PUT_COMPLETED) {
        Thread.sleep(100);
      }
      testStep = TestStep.CHECKANDPUT_STARTED;
      region.checkAndMutate(Bytes.toBytes("r1"), Bytes.toBytes(family), Bytes.toBytes("q1"),
        CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("10")), put, null, true);
      testStep = TestStep.CHECKANDPUT_COMPLETED;
    }
  }

  public static class MockHRegion extends HRegion {

    public MockHRegion(Path tableDir, HLog log, FileSystem fs, Configuration conf,
        final HRegionInfo regionInfo, final HTableDescriptor htd, RegionServerServices rsServices) {
      super(tableDir, log, fs, conf, regionInfo, htd, rsServices);
    }

    @Override
    public void releaseRowLock(Integer lockId) {
      if (testStep == TestStep.INIT) {
        super.releaseRowLock(lockId);
        return;
      }

      if (testStep == TestStep.PUT_STARTED) {
        try {
          testStep = TestStep.PUT_COMPLETED;
          super.releaseRowLock(lockId);
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
        super.releaseRowLock(lockId);
      }
    }

    @Override
    public Integer getLock(Integer lockid, HashedBytes row, boolean waitForLock) throws IOException {
      if (testStep == TestStep.CHECKANDPUT_STARTED) {
        latch.countDown();
      }
      return super.getLock(lockid, row, waitForLock);
    }

  }

}
