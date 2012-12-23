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
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(SmallTests.class)
public class TestHBase7051 {

  private static volatile boolean putCompleted = false;
  private static CountDownLatch latch = new CountDownLatch(1);
  private boolean checkAndPutCompleted = false;
  private static int count = 0;

  @Test
  public void testPutAndCheckAndPutInParallel() throws Exception {

    final String tableName = "testPutAndCheckAndPut";
    final String family = "f1";
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

    count++;
    region.batchMutate(putsAndLocks.toArray(new Pair[0]));
    makeCheckAndPut(family, region);

    makePut(family, region);
    while (!checkAndPutCompleted) {
      Thread.sleep(100);
    }
    Scan s = new Scan();
    RegionScanner scanner = region.getScanner(s);
    List<KeyValue> results = new ArrayList<KeyValue>();
    scanner.next(results, 2);
    for (KeyValue keyValue : results) {
      assertEquals("50",Bytes.toString(keyValue.getValue()));
    }

  }

  private void makePut(final String family, final MockHRegion region) {
    new Thread() {
      public void run() {
        List<Pair<Mutation, Integer>> putsAndLocks = Lists.newArrayList();
        Put[] puts = new Put[1];
        Put put = new Put(Bytes.toBytes("r1"));
        put.add(Bytes.toBytes(family), Bytes.toBytes("q1"), Bytes.toBytes("50"));
        puts[0] = put;
        try {
          Pair<Mutation, Integer> pair = new Pair<Mutation, Integer>(puts[0], null);
          putsAndLocks.add(pair);
          count++;
          region.batchMutate(putsAndLocks.toArray(new Pair[0]));
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }.start();
  }

  private void makeCheckAndPut(final String family, final MockHRegion region) {
    new Thread() {

      public void run() {
        Put[] puts = new Put[1];
        Put put = new Put(Bytes.toBytes("r1"));
        put.add(Bytes.toBytes(family), Bytes.toBytes("q1"), Bytes.toBytes("11"));
        puts[0] = put;
        try {
          while (putCompleted == false) {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }
          count++;
          region.checkAndMutate(Bytes.toBytes("r1"), Bytes.toBytes(family), Bytes.toBytes("q1"),
              CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("10")), put, null, true);
          checkAndPutCompleted = true;
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }.start();
  }

  public static class MockHRegion extends HRegion {

    public MockHRegion(Path tableDir, HLog log, FileSystem fs, Configuration conf,
        final HRegionInfo regionInfo, final HTableDescriptor htd, RegionServerServices rsServices) {
      super(tableDir, log, fs, conf, regionInfo, htd, rsServices);
    }

    @Override
    public void releaseRowLock(Integer lockId) {
      if (count == 1) {
        super.releaseRowLock(lockId);
        return;
      }

      if (count == 2) {
        try {
          putCompleted = true;
          super.releaseRowLock(lockId);
          latch.await();
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      if (count == 3) {
        super.releaseRowLock(lockId);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        latch.countDown();
      }
    }

    @Override
    public Integer getLock(Integer lockid, byte[] row, boolean waitForLock) throws IOException {
      if (count == 3) {
        latch.countDown();
      }
      return super.getLock(lockid, row, waitForLock);
    }

  }

}
