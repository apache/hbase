package org.apache.hadoop.hbase.io.hfile.histogram;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram.Bucket;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHFileHistogramE2E {
  private static final byte[] TABLE =
      Bytes.toBytes("TestHFileHistogramE2ESingleStore");
  private static final byte[] FAMILY = Bytes.toBytes("family");
  private static final Log LOG = LogFactory.getLog(TestHFileHistogramE2E.class);
  private HBaseTestingUtility util = new HBaseTestingUtility();
  private final int numBuckets = 100;

  @Before
  public void setUp() throws Exception {
    util.getConfiguration().setInt(HFileHistogram.HFILEHISTOGRAM_BINCOUNT,
        numBuckets);
    util.startMiniCluster(3);
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testSingleStore() throws IOException {
    HTable table = util.createTable(TABLE, FAMILY);
    util.loadTable(table, FAMILY);
    util.flush(TABLE);
    assertTrue(util.getHBaseCluster().getRegions(TABLE).size() == 1);
    HRegion region = util.getHBaseCluster().getRegions(TABLE).get(0);
    HFileHistogram hist = region.getHistogram();
    assertTrue(hist != null);
    boolean first = true;
    List<Bucket> buckets = hist.getUniformBuckets();
    assertTrue(buckets != null);
    assertTrue(buckets.size() > 0);
    Bucket prevBucket = buckets.get(0);
    for (Bucket b : buckets) {
      if (first) {
        first = false;
        prevBucket = b;
        continue;
      }
      assertTrue(Bytes.compareTo(b.getStartRow(), prevBucket.getEndRow()) >= 0);
      assertTrue(Bytes.compareTo(b.getEndRow(), prevBucket.getStartRow()) > 0);
    }
  }

  @Test
  public void testHistogramSerDeE2E() throws IOException {
    byte[] TABLE2 = Bytes.toBytes("TestHistogramSerDeE2E");
    HTable table = util.createTable(TABLE2, FAMILY);
    util.loadTable(table, FAMILY);
    util.flush(TABLE2);
    assertTrue(util.getHBaseCluster().getRegions(TABLE2).size() == 1);
    HRegion region = util.getHBaseCluster().getRegions(TABLE2).get(0);
    List<Bucket> buckets = region.getHistogram().getUniformBuckets();
    assertTrue(buckets != null);
    assertTrue(buckets.size() > 0);
    List<Bucket> serBuckets = table.getHistogramForColumnFamily(
        region.getStartKey(), FAMILY);
    assertTrue(serBuckets != null);
    assertTrue(serBuckets.size() > 0);
    assertTrue(compareBuckets(buckets, serBuckets));
  }

  public boolean compareBuckets(List<Bucket> buckets1, List<Bucket> buckets2) {
    int len1 = buckets1.size();
    int len2 = buckets2.size();
    assertTrue(len1 == len2);
    for (int i=0; i<len1; i++) {
      Bucket b1 = buckets1.get(i);
      Bucket b2 = buckets2.get(i);
      if (!b1.equals(b2)) return false;
    }
    return true;
  }

  private List<byte[]> putRandomKVs(HTable table, int numEntries, int rowSize)
      throws IOException {
    List<byte[]> inputList = new ArrayList<byte[]>();
    // The error estimation holds for more than 10000 entries.
    // We wouldn't be using this feature if it weren't bigger than that.
    Random r = new Random();
    for (int i = 0; i < numEntries; i++) {
      byte[] arr = new byte[rowSize];
      r.nextBytes(arr);
      KeyValue kv = new KeyValue(arr, (long)0);
      inputList.add(kv.getRow());
      table.put(new Put(kv.getRow()).add(FAMILY, null, kv.getRow()));
      if (i%10000 == 0) {
        table.flushCommits();
        util.flush();
      }
    }
    return inputList;
  }

  @Test
  public void testHistogramError() throws IOException {
    byte[] TABLE3 = Bytes.toBytes("testHistogramError");
    HTable table = util.createTable(TABLE3, FAMILY);
    util.flush(TABLE3);
    Random r = new Random();
    int numEntries = 100000 + r.nextInt(100000);
    int expectedBucketCnt = numEntries/numBuckets;
    List<byte[]> inputList = putRandomKVs(table, numEntries, 15);
    Collections.sort(inputList, Bytes.BYTES_COMPARATOR);
    List<HRegion> regions = util.getHBaseCluster().getRegions(TABLE3);
    assertTrue(regions.size() == 1);
    HRegion region = regions.get(0);
    List<Bucket> lst = table.getHistogram(region.getStartKey());
    assertTrue(lst.size() > 0);

    TestUniformSplitHistogram.checkError(inputList, lst,
        0.2, expectedBucketCnt);
  }
}
