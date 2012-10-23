package org.apache.hadoop.hbase.regionserver.kvaggregator;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class TestKeyValueAggregator {
  private static byte[] TABLE = Bytes.toBytes("TestKeyValueAggregator");
  private static byte[] FAMILY = Bytes.toBytes("family");
  private static byte[] START_KEY = Bytes.toBytes("aaa");
  private static byte[] END_KEY = Bytes.toBytes("zzz");
  private static int BLOCK_SIZE = 70;

  private static HBaseTestingUtility TEST_UTIL = null;
  private static HTableDescriptor TESTTABLEDESC = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    SchemaMetrics.setUseTableNameInTest(true);
    TEST_UTIL = new HBaseTestingUtility();
    TESTTABLEDESC = new HTableDescriptor(TABLE);

    TESTTABLEDESC.addFamily(new HColumnDescriptor(FAMILY).setMaxVersions(10)
        .setBlockCacheEnabled(true).setBlocksize(BLOCK_SIZE)
        .setCompressionType(Compression.Algorithm.NONE));
    TEST_UTIL
        .getConfiguration()
        .set(HConstants.KV_AGGREGATOR,
            "org.apache.hadoop.hbase.regionserver.kvaggregator.LowerToUpperAggregator");
  }

  @Test
  public void testDummyKvAggregator() throws Exception {
    HRegion r = HBaseTestCase.createNewHRegion(TESTTABLEDESC, START_KEY,
        END_KEY, TEST_UTIL.getConfiguration());
    Put[] puts = new Put[25];
    // put some lowercase strings
    for (int i = 0; i < 25; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put put = new Put(row);
      byte[] qualifier = Bytes.toBytes("qual" + i);
      byte[] value = Bytes.toBytes("ab" + (char) (i + 97));
      put.add(FAMILY, qualifier, value);
      puts[i] = put;
    }
    r.put(puts);
    Scan scan = new Scan();
    InternalScanner s = r.getScanner(scan);
    List<KeyValue> results = new ArrayList<KeyValue>();
    while (s.next(results))
      ;
    // check if the values in results are in upper case
    s.close();
    Assert.assertTrue(checkIfLowerCase(results));
    // check if we got all 25 results back
    Assert.assertEquals(25, results.size());
  }

  public boolean checkIfLowerCase(List<KeyValue> result) {
    for (KeyValue kv : result) {
      String currValue = new String(kv.getValue());
      String uppercase = currValue.toUpperCase();
      if (!currValue.equals(uppercase)) {
        return false;
      }
    }
    return true;
  }
}
