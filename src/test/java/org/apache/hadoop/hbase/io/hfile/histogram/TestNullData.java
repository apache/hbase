package org.apache.hadoop.hbase.io.hfile.histogram;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram.Bucket;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNullData {
  private static final byte[] TABLE =
      Bytes.toBytes("TestHFileHistogramE2ESingleStore");
  private static final byte[] FAMILY = Bytes.toBytes("family");
  private HBaseTestingUtility util = new HBaseTestingUtility();
  private final int numBuckets = 100;

  @Before
  public void setUp() throws Exception {
    util.getConfiguration().setInt(HFileHistogram.HFILEHISTOGRAM_BINCOUNT,
        numBuckets);
    util.getConfiguration().setBoolean(HConstants.USE_HFILEHISTOGRAM, false);
    util.startMiniCluster(3);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testNullData() throws IOException {
    HTable table = util.createTable(TABLE, FAMILY);
    util.loadTable(table, FAMILY);
    util.flush(TABLE);
    assertTrue(util.getHBaseCluster().getRegions(TABLE).size() == 1);
    HRegion region = util.getHBaseCluster().getRegions(TABLE).get(0);
    List<Bucket> hist = table.getHistogram(region.getStartKey());
    assertTrue(hist == null);
  }

}
