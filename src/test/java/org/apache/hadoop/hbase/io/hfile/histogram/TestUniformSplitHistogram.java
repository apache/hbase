package org.apache.hadoop.hbase.io.hfile.histogram;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram.Bucket;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestUniformSplitHistogram {

  private final static Log LOG =
      LogFactory.getLog(TestUniformSplitHistogram.class);
  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testUniformHistogram() {
    UniformSplitHFileHistogram hist = new UniformSplitHFileHistogram(100);
    Random r = new Random();
    int size = 10;
    for (int i = 0; i < 100; i++) {
      byte[] arr = new byte[size];
      r.nextBytes(arr);
      KeyValue kv = new KeyValue(arr, (long)0);
      hist.add(kv);
    }
    List<Bucket> lst = hist.getUniformBuckets();
    assertTrue(lst.size() > 0);
    Bucket prevBucket = null;
    int bucketIndex = 0;
    for (Bucket b : lst) {
      bucketIndex++;
      if (prevBucket != null) {
        System.out.println(bucketIndex);
        assertTrue(Bytes.toStringBinary(b.getStartRow())
            + " not greater than "
            + Bytes.toStringBinary(prevBucket.getStartRow()),
            Bytes.compareTo(b.getStartRow(), prevBucket.getStartRow()) > 0);
        assertTrue(Bytes.toStringBinary(b.getEndRow())
            + " not greater than "
            + Bytes.toStringBinary(prevBucket.getEndRow()),
            Bytes.compareTo(b.getEndRow(), prevBucket.getEndRow()) >= 0);
        assertTrue(Bytes.toStringBinary(b.getEndRow())
            + " not greater than "
            + Bytes.toStringBinary(prevBucket.getStartRow()),
            Bytes.compareTo(b.getEndRow(), prevBucket.getStartRow()) >= 0);
      }
      prevBucket = b;
    }
  }

  @Test
  public void testUniformHistogramError() {
    for (int numRuns = 0; numRuns < 100; numRuns++) {
      int numBuckets = 100;
      UniformSplitHFileHistogram hist = new UniformSplitHFileHistogram(numBuckets);
      Random r = new Random();
      int size = 10;
      List<byte[]> inputList = new ArrayList<byte[]>();
      // The error estimation holds for more than 10000 entries.
      // We wouldn't be using this feature if it weren't bigger than that.
      int numEntries = 10000 + r.nextInt(10000);
      int expectedBucketCnt = numEntries/numBuckets;
      for (int i = 0; i < numEntries; i++) {
        byte[] arr = new byte[size];
        r.nextBytes(arr);
        KeyValue kv = new KeyValue(arr, (long)0);
        inputList.add(kv.getRow());
        hist.add(kv);
      }
      List<Bucket> lst = hist.getUniformBuckets();

      // 20 error is an observation, this test gives an estimate of how much
      // error you can expect.
      checkError(inputList, lst, 0.2, expectedBucketCnt);
    }
  }

  public static void checkError(List<byte[]> inputList, List<Bucket> lst,
      double errorPct, int expectedBucketCnt) {
    Collections.sort(inputList, Bytes.BYTES_COMPARATOR);
    assertTrue(lst.size() > 0);
    int numEntries = inputList.size();
    int i = 0;
    int j = i;
    int bucketIndex = 0;
    int error = 0;
    for (Bucket b : lst) {
      while (i<numEntries && isWithinBucket(b, inputList.get(i))) {
        i++;
      }
      LOG.debug("Bucket #" + bucketIndex++ + ", Actual : "
          + (i-j) + ", From Bucket :" + b.getCount());
      error += Math.abs(((i-j) - expectedBucketCnt));
      j = i;
    }
    LOG.debug("numEntries : " + numEntries + ", error :" + error
        + "expectedNBucketCnt : " + expectedBucketCnt);
    assertTrue(error/(double)numEntries < 0.2);
  }

  public static boolean isWithinBucket(Bucket b, byte[] row) {
    return (Bytes.compareTo(b.getStartRow(), row) <= 0
        && Bytes.compareTo(b.getEndRow(), row) >= 0);
  }
}
