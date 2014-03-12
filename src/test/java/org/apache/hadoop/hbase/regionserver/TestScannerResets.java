package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;

import org.junit.Test;

public class TestScannerResets extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestBlocksRead.class);

  private HBaseConfiguration getConf() {
    HBaseConfiguration conf = new HBaseConfiguration();

    // disable compactions in this test.
    conf.setInt("hbase.hstore.compactionThreshold", 10000);
    return conf;
  }

  HRegion region = null;
  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final String DIR = TEST_UTIL.getTestDir() + "/TestScannerResets/";

  /**
   * @see org.apache.hadoop.hbase.HBaseTestCase#setUp()
   */
  @SuppressWarnings("deprecation")
  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }

  @SuppressWarnings("deprecation")
  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    EnvironmentEdgeManagerTestHelper.reset();
  }

  private void initHRegion(byte[] tableName, String callingMethod,
      HBaseConfiguration conf, String family) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor familyDesc;
    familyDesc = new HColumnDescriptor(Bytes.toBytes(family))
        .setBlocksize(1);
    htd.addFamily(familyDesc);
    HRegionInfo info = new HRegionInfo(htd, null, null, false);
    Path path = new Path(DIR + callingMethod);
    region = HRegion.createHRegion(info, path, conf);
  }

  private void putData(String family, String row, String col, long version)
      throws IOException {
      putData(Bytes.toBytes(family), row, col, version, version);
  }

  // generates a value to put for a row/col/version.
  private static byte[] genValue(String row, String col, long version) {
    return Bytes.toBytes("Value:" + row + "#" + col + "#" + version);
  }

  private void putData(byte[] cf, String row, String col, long versionStart,
      long versionEnd) throws IOException {
    byte columnBytes[] = Bytes.toBytes(col);
    Put put = new Put(Bytes.toBytes(row));

    for (long version = versionStart; version <= versionEnd; version++) {
      put.add(cf, columnBytes, version, genValue(row, col, version));
    }
    region.put(put);
  }

  private static void verifyData(KeyValue kv, String expectedRow,
      String expectedCol, long expectedVersion) {
    assertEquals("RowCheck", expectedRow, Bytes.toString(kv.getRow()));
    assertEquals("ColumnCheck", expectedCol, Bytes.toString(kv.getQualifier()));
    assertEquals("TSCheck", expectedVersion, kv.getTimestamp());
    assertEquals("ValueCheck",
        Bytes.toString(genValue(expectedRow, expectedCol, expectedVersion)),
        Bytes.toString(kv.getValue()));
  }

  // how many data blocks in the specified CF were accessed so far...
  private static long getBlkAccessCount(byte[] cf) {
    return HRegion.getNumericMetric(SchemaMetrics.CF_PREFIX
        + Bytes.toString(cf) + "." + SchemaMetrics.BLOCK_TYPE_PREFIX
        + "Data.fsBlockReadCnt");
  }

  private void scanRange(String family, int minInclusiveTS, int maxExclusiveTS,
                         boolean flushInBetween, int expBlocks) throws IOException {
    byte[] cf = Bytes.toBytes(family);
    long blocksStart = getBlkAccessCount(cf);

    // Setup a scan to read items in time range [6..9).
    Scan s = new Scan();
    s.setTimeRange(minInclusiveTS, maxExclusiveTS);
    InternalScanner scanner = region.getScanner(s);
    int idx = minInclusiveTS;
    List<KeyValue> kvs;
    do {
      kvs = new ArrayList<KeyValue>();
      scanner.next(kvs);
      if (kvs.size() == 0)
        break;
      assertEquals(1, kvs.size());
      verifyData(kvs.get(0), "row" + idx, "col" + idx, idx);
      if (flushInBetween && (idx == minInclusiveTS)) { // first iteration?
        // On the first iteration, after a next() has been done,
        // put some new KVs and flush a new file. This should
        // cause scanner to be reset to a new set of store
        // files.
        // File 3: Timestamp range [9..9]
        putData(family, "row9", "col9", 9);
        region.flushcache();
      }
      idx++;
    } while (true);
    assertEquals("ending index", idx, maxExclusiveTS);

    long blocksEnd = getBlkAccessCount(cf);
    
    assertEquals("Blocks Read Check: ", expBlocks, blocksEnd - blocksStart);
    System.out.println("Blocks Read = "
        + (blocksEnd - blocksStart) + "Expected = " + expBlocks);
  }

  @Test
  public void testScannerReset() throws Exception {
    byte[] TABLE = Bytes.toBytes("testScannerReset");
    String FAMILY = "cf1";
    HBaseConfiguration conf = getConf();
    initHRegion(TABLE, getName(), conf, FAMILY);

    // File1: Timestamp range [1..3]
    putData(FAMILY, "row1", "col1", 1);
    putData(FAMILY, "row2", "col2", 2);
    putData(FAMILY, "row3", "col3", 3);
    region.flushcache();

    // File2: Timestamp range of keys [4..8]
    putData(FAMILY, "row4", "col4", 4);
    putData(FAMILY, "row5", "col5", 5);    
    putData(FAMILY, "row6", "col6", 6);
    putData(FAMILY, "row7", "col7", 7);
    putData(FAMILY, "row8", "col8", 8);
    region.flushcache();

    // Scan values in time range [1..4).
    // with flushing in between being FALSE.
    //
    // Expected blocks read = 3 because only File1
    // contains data in relevant time range we are
    // interested in.
    scanRange(FAMILY, 1, 4, false, 3);

    // Scan values in time range [1..4).
    // with flushing in between being TRUE.
    //
    // Expected blocks read = 4 because only File1
    // contains data in relevant time range even with
    // flushes in between. Note: File1 only has 3 blocks.
    // But we expect to read 1 extra block because reset
    // of the scanner stack will cause an extra seek into
    // the File1. However, due to bug HBASE-4823, this
    // test reads 10 blocks-- 4 blocks in File1, 5 in File2,
    // and 1 in File3 (the flushed file).
    scanRange(FAMILY, 1, 4, true, 4);
  }
}
