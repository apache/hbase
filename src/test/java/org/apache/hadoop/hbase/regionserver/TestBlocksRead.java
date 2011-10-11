package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;

import org.junit.Test;

public class TestBlocksRead extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestBlocksRead.class);

  private HBaseConfiguration getConf() {
    HBaseConfiguration conf = new HBaseConfiguration();

    // disable compactions in this test.
    conf.setInt("hbase.hstore.compactionThreshold", 10000);
    return conf;
  }

  HRegion region = null;
  private final String DIR = HBaseTestingUtility.getTestDir() +
    "/TestHRegion/";

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

  private void initHRegion (byte [] tableName, String callingMethod,
      HBaseConfiguration conf, byte [] ... families)
    throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for(byte [] family : families) {
	HColumnDescriptor familyDesc =  new HColumnDescriptor(
          family,
          HColumnDescriptor.DEFAULT_VERSIONS,
          HColumnDescriptor.DEFAULT_COMPRESSION,
          HColumnDescriptor.DEFAULT_IN_MEMORY,
          HColumnDescriptor.DEFAULT_BLOCKCACHE,
          1, // small block size deliberate; each kv on its own block
          HColumnDescriptor.DEFAULT_TTL,
          HColumnDescriptor.DEFAULT_BLOOMFILTER,
          HColumnDescriptor.DEFAULT_REPLICATION_SCOPE);
      htd.addFamily(familyDesc);
    }
    HRegionInfo info = new HRegionInfo(htd, null, null, false);
    Path path = new Path(DIR + callingMethod);
    region = HRegion.createHRegion(info, path, conf);
  }

  private void putData(byte[] cf, String row, String col, long version)
  throws IOException {
    putData(cf, row, col, version, version);
  }

  // generates a value to put for a row/col/version.
  private static byte[] genValue(String row, String col, long version) {
    return Bytes.toBytes("Value:" + row + "#" + col + "#" + version);
  }

  private void putData(byte[] cf, String row, String col,
                       long versionStart, long versionEnd)
  throws IOException {
    byte columnBytes[] = Bytes.toBytes(col);
    Put put = new Put(Bytes.toBytes(row));

    for (long version = versionStart; version <= versionEnd; version++) {
      put.add(cf, columnBytes, version, genValue(row, col, version));
    }
    region.put(put);
  }

  private KeyValue[] getData(byte[] cf, String row, List<String> columns,
                             int expBlocks)
    throws IOException {

    long blocksStart = getBlkAccessCount(cf);
    Get get = new Get(Bytes.toBytes(row));

    for (String column : columns) {
      get.addColumn(cf, Bytes.toBytes(column));
    }

    KeyValue[] kvs = region.get(get, null).raw();
    long blocksEnd = getBlkAccessCount(cf);
    if (expBlocks != -1) {
      assertEquals("Blocks Read Check", expBlocks, blocksEnd - blocksStart);
    }
    System.out.println("Blocks Read = " + (blocksEnd - blocksStart) +
                       "Expected = " + expBlocks);
    return kvs;
  }

  private KeyValue[] getData(byte[] cf, String row, String column,
                             int expBlocks)
    throws IOException {
    return getData(cf, row, Arrays.asList(column), expBlocks);
  }

  private void deleteFamily(byte[] cf, String row, long version)
    throws IOException {
    Delete del = new Delete(Bytes.toBytes(row));
    del.deleteFamily(cf, version);
    region.delete(del, null, true);
  }

  private void deleteFamily(byte[] cf, String row, String column, long version)
    throws IOException {
    Delete del = new Delete(Bytes.toBytes(row));
    del.deleteColumns(cf, Bytes.toBytes(column), version);
    region.delete(del, null, true);
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

  private static long getBlkAccessCount(byte[] cf) {
    return HRegion.getNumericMetric("cf." + Bytes.toString(cf)  + "."
        + "bt.Data.fsBlockReadCnt");
  }

  /**
   *
   * Test from client side for get with maxResultPerCF set
   *
   * @throws Exception
   */
  @Test
  public void testBlocksRead() throws Exception {
    byte [] TABLE = Bytes.toBytes("testLazySeek");
    byte [] FAMILY = Bytes.toBytes("cf1");
    byte [][] FAMILIES = new byte[][] { FAMILY };
    KeyValue kvs[];

    HBaseConfiguration conf = getConf();
    initHRegion(TABLE, getName(), conf, FAMILIES);

    putData(FAMILY, "row", "col1", 1);
    putData(FAMILY, "row", "col2", 2);
    putData(FAMILY, "row", "col3", 3);
    putData(FAMILY, "row", "col4", 4);
    putData(FAMILY, "row", "col5", 5);
    putData(FAMILY, "row", "col6", 6);
    putData(FAMILY, "row", "col7", 7);
    region.flushcache();

    // Expected block reads: 1 (after fixes for HBASE-4433 & HBASE-4434).
    // The top block contains our answer (including DeleteFamily if present).
    // So we should only be reading 1 block
    kvs = getData(FAMILY, "row", "col1", 1);
    assertEquals(1, kvs.length);
    verifyData(kvs[0], "row", "col1", 1);

    // Expected block reads: 2
    // The first one should be able to process any Delete marker for the
    // row if present, and then get col1. The second one will get col2
    kvs = getData(FAMILY, "row", Arrays.asList("col1", "col2"), 2);
    assertEquals(2, kvs.length);
    verifyData(kvs[0], "row", "col1", 1);
    verifyData(kvs[1], "row", "col2", 2);

    // Expected block reads: 3
    // The first one should be able to process any Delete marker at the top of the
    // row. This will take us to the block containing col1. But that's not a column
    // we are interested in. The two more seeks will be for col2 and col3.
    kvs = getData(FAMILY, "row", Arrays.asList("col2", "col3"), 3);
    assertEquals(2, kvs.length);
    verifyData(kvs[0], "row", "col2", 2);
    verifyData(kvs[1], "row", "col3", 3);

    // Expected block reads: 3
    // Unfortunately, this is a common case when KVs are large, and occupy 1 block each.
    // This issue has been reported as HBASE-4443.
    // Explanation of 3 seeks:
    //  * The first one should be able to process any Delete marker at the top
    //    of the row.
    //  * The second one will be block containing col4, because we search for
    //    row/col5/TS=Long.MAX_VAL.
    //    This will land us in the previous block, and not the block containing row/col5.
    //  * The final block we read will be the actual block that contains our data.
    // When HBASE-4443 is fixed, the number of expected blocks here should be dropped to 2.
    // If we also do some special handling to avoid looking for deletes at the top of the
    // row, then this case can also work in 1 block read.
    kvs = getData(FAMILY, "row", Arrays.asList("col5"), 3);
    assertEquals(1, kvs.length);
    verifyData(kvs[0], "row", "col5", 5);
  }
}
