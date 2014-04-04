package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram.Bucket;
import org.apache.hadoop.hbase.io.hfile.histogram.TestUniformSplitHistogram;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtilities;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestHFileHistogramE2E {
  private static final byte[] TABLE =
      Bytes.toBytes("TestHFileHistogramE2ESingleStore");
  private static final byte[] FAMILY = Bytes.toBytes("family");
  @SuppressWarnings("unused")
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
    Assert.assertTrue(util.getHBaseCluster().getRegions(TABLE).size() == 1);
    HRegion region = util.getHBaseCluster().getRegions(TABLE).get(0);
    HFileHistogram hist = region.getHistogram();
    Assert.assertTrue(hist != null);
    boolean first = true;
    List<Bucket> buckets = hist.getUniformBuckets();
    int idx = 0;
    Assert.assertTrue(buckets != null);
    Assert.assertTrue(buckets.size() > 0);
    Bucket prevBucket = buckets.get(0);
    for (Bucket b : buckets) {
      if (first) {
        first = false;
        prevBucket = b;
        idx++;
        continue;
      }
      Assert.assertTrue(Bytes.compareTo(b.getStartRow(), prevBucket.getEndRow()) >= 0);
      Assert.assertTrue(Bytes.toStringBinary(b.getEndRow()) + " : " +
          Bytes.toStringBinary(prevBucket.getStartRow()),
          ++idx >= buckets.size() || // The last bucket
          Bytes.compareTo(b.getEndRow(), prevBucket.getStartRow()) > 0);
      prevBucket = b;
    }
  }

  @Test
  public void testHistogramSerDeE2E() throws IOException {
    byte[] TABLE2 = Bytes.toBytes("TestHistogramSerDeE2E");
    HTable table = util.createTable(TABLE2, FAMILY);
    util.loadTable(table, FAMILY);
    util.flush(TABLE2);
    Assert.assertTrue(util.getHBaseCluster().getRegions(TABLE2).size() == 1);
    HRegion region = util.getHBaseCluster().getRegions(TABLE2).get(0);
    List<Bucket> buckets = region.getHistogram().getUniformBuckets();
    Assert.assertTrue(buckets != null);
    Assert.assertTrue(buckets.size() > 0);
    List<Bucket> serBuckets = table.getHistogramForColumnFamily(
        region.getStartKey(), FAMILY);
    Assert.assertTrue(serBuckets != null);
    Assert.assertTrue(serBuckets.size() > 1);
    Assert.assertTrue(Bytes.equals(serBuckets.get(0).getStartRow(),
        region.getStartKey()));
    Assert.assertTrue(Bytes.equals(serBuckets.get(serBuckets.size() - 1)
        .getEndRow(),
        region.getEndKey()));
    buckets = HRegionUtilities
        .adjustHistogramBoundariesToRegionBoundaries(buckets, region.getStartKey(), region.getEndKey());
    Assert.assertTrue(compareBuckets(buckets, serBuckets));
  }

  public boolean compareBuckets(List<Bucket> buckets1, List<Bucket> buckets2) {
    int len1 = buckets1.size();
    int len2 = buckets2.size();
    Assert.assertTrue(len1 == len2);
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
    Assert.assertTrue(regions.size() == 1);
    HRegion region = regions.get(0);
    List<Bucket> lst = table.getHistogram(region.getStartKey());
    Assert.assertTrue(lst.size() > 0);

    TestUniformSplitHistogram.checkError(inputList, lst,
        0.2, expectedBucketCnt);
  }

  @Test
  public void testHistogramForAllRegions() throws IOException {
    byte[] tableName = Bytes.toBytes("TestHistogramForAllRegions");
    byte[] cf = Bytes.toBytes("cf");
    HTable table = util.createTable(tableName, new byte[][] { cf }, 3,
        Bytes.toBytes("bbb"), Bytes.toBytes("yyy"), 25);

    util.loadTable(table, cf);
    util.flush(tableName);

    Assert.assertTrue(util.getHBaseCluster().getRegions(tableName).size() > 1);
    Map<byte[], byte[]> map = table.getStartEndKeysMap();
    List<List<Bucket>> buckets = table.getHistogramsForAllRegions();
    Assert.assertTrue(map.size() == buckets.size());
    int regionIndex = 0;
    int regionCnt = map.size();
    for (List<Bucket> bucketsForRegion : buckets) {
      Assert.assertTrue(bucketsForRegion.size() > 1);
      Assert.assertTrue(map.containsKey(bucketsForRegion.get(0).getStartRow()));
      Assert.assertTrue(Bytes.equals(
          map.get(bucketsForRegion.get(0).getStartRow()),
          bucketsForRegion.get(bucketsForRegion.size() - 1).getEndRow()));
      Bucket prevBucket = null;
      for (Bucket b : bucketsForRegion) {
        // Checking that the buckets returned have the following 3 properties
        // * curBucket.startRow >= prevBucket.startRow
        // * curBucket.startRow >= prevBucket.endRow
        // * curBucket.endRow >= prevBucket.endRow
        if (prevBucket != null) {
          Assert.assertTrue(
              Bytes.toStringBinary(b.getStartRow())
              + " not greater than "
              + Bytes.toStringBinary(prevBucket.getStartRow()),
              Bytes.compareTo(b.getStartRow(), prevBucket.getStartRow()) > 0);
          if (regionIndex < (regionCnt - 1)) {
            // last region's end row is going to be an empty row,
            // so we need to special case it here.
            Assert.assertTrue(
                Bytes.toStringBinary(b.getEndRow())
                + " not greater than "
                + Bytes.toStringBinary(prevBucket.getEndRow()),
                Bytes.compareTo(b.getEndRow(), prevBucket.getEndRow()) >= 0);
            Assert.assertTrue(
                Bytes.toStringBinary(b.getEndRow())
                + " not greater than "
                + Bytes.toStringBinary(prevBucket.getStartRow()),
                Bytes.compareTo(b.getEndRow(), prevBucket.getStartRow()) >= 0);
          }
        }
        prevBucket = b;
      }
      prevBucket = null;
      regionIndex++;
    }
    int tries = 5;
    while (tries-- >= 0) {
      List<List<Bucket>> batchBuckets = null;
      try {
        batchBuckets =
            table.batchgetHistogramsForAllRegions();
      } catch (IOException e) {
        continue;
      }
      compareHistograms(buckets, batchBuckets);
    }
  }

  private void compareHistograms(List<List<Bucket>> buckets,
      List<List<Bucket>> batchBuckets) {
    assertTrue(buckets.size() == batchBuckets.size());
    for (int i = 0; i < buckets.size(); i++) {
      assertTrue(compareBuckets(buckets.get(i), batchBuckets.get(i)));
    }
  }
}
