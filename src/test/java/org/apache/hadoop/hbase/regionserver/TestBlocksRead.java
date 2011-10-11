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
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
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
          StoreFile.BloomType.ROWCOL.toString(),
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
    return HRegion.getNumericMetric(SchemaMetrics.CF_PREFIX
        + Bytes.toString(cf) + "." + SchemaMetrics.BLOCK_TYPE_PREFIX
        + "Data.fsBlockReadCnt");
  }

  /**
   * Test # of blocks read for some simple seek cases.
   * @throws Exception
   */
  @Test
  public void testBlocksRead() throws Exception {
    byte [] TABLE = Bytes.toBytes("testBlocksRead");
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

    // Expected block reads: 2
    // Unfortunately, this is a common case when KVs are large, and occupy
    // 1 block each.
    // This issue has been reported as HBASE-4443.
    // Since HBASE-4469 is fixed now, the seek for delete family marker has been
    // optimized.
    // Explanation of 2 seeks:
    //  * The 1st one will be block containing col4, because we search for
    //    row/col5/TS=Long.MAX_VAL.
    //    This will land us in the previous block, and not the block containing
    //    row/col5.
    //  * The final block we read will be the actual block that contains our data.
    // When HBASE-4443 is fixed, the number of expected blocks here should be
    //  dropped to 1.
    kvs = getData(FAMILY, "row", Arrays.asList("col5"), 2);
    assertEquals(1, kvs.length);
    verifyData(kvs[0], "row", "col5", 5);
  }

  /**
   * Test # of blocks read (targetted at some of the cases Lazy Seek optimizes).
   * @throws Exception
   */
  @Test
  public void testLazySeekBlocksRead() throws Exception {
    byte [] TABLE = Bytes.toBytes("testLazySeekBlocksRead");
    byte [] FAMILY = Bytes.toBytes("cf1");
    byte [][] FAMILIES = new byte[][] { FAMILY };
    KeyValue kvs[];

    HBaseConfiguration conf = getConf();
    initHRegion(TABLE, getName(), conf, FAMILIES);

    // File 1
    putData(FAMILY, "row", "col1", 1);
    putData(FAMILY, "row", "col2", 2);
    region.flushcache();

    // File 2
    putData(FAMILY, "row", "col1", 3);
    putData(FAMILY, "row", "col2", 4);
    region.flushcache();

    // Expected blocks read: 1.
    // Since HBASE-4469 is fixed now, the seek for delete family marker has been
    // optimized.
    // File 2's top block is also the KV we are
    // interested. So only 1 seek is needed.
    kvs = getData(FAMILY, "row", Arrays.asList("col1"), 1);
    assertEquals(1, kvs.length);
    verifyData(kvs[0], "row", "col1", 3);

    // Expected blocks read: 2
    //
    // Since HBASE-4469 is fixed now, the seek for delete family marker has been
    // optimized. File 2's top block has the "col1" KV we are
    // interested. We also need "col2" which is in a block
    // of its own. So, we need that block as well.
    //
    kvs = getData(FAMILY, "row", Arrays.asList("col1", "col2"), 2);
    assertEquals(2, kvs.length);
    verifyData(kvs[0], "row", "col1", 3);
    verifyData(kvs[1], "row", "col2", 4);

    // File 3: Add another column
    putData(FAMILY, "row", "col3", 5);
    region.flushcache();

    // Expected blocks read: 1
    //
    // Since HBASE-4469 is fixed now, the seek for delete family marker has been
    // optimized. File 3's top block has the "col3" KV we are
    // interested. So only 1 seek is needed.
    //
    kvs = getData(FAMILY, "row", "col3", 1);
    assertEquals(1, kvs.length);
    verifyData(kvs[0], "row", "col3", 5);

    // Get a column from older file.
    // Expected blocks read: 3
    //
    // Since HBASE-4469 is fixed now, the seek for delete family marker has been
    // optimized.  File 2's top block has the "col1" KV we are
    // interested.
    //
    // Also no need to seek to file 3 since the row-col bloom filter is enabled.
    // Only 1 seek in File 2 is needed.
    //
    kvs = getData(FAMILY, "row", Arrays.asList("col1"), 1);
    assertEquals(1, kvs.length);
    verifyData(kvs[0], "row", "col1", 3);

    // File 4: Delete the entire row.
    deleteFamily(FAMILY, "row", 6);
    region.flushcache();

    // Expected blocks read: 6. Why? [TODO]
    // With lazy seek, would have expected this to be lower.
    // At least is shouldn't be worse than before.
    kvs = getData(FAMILY, "row", "col1", 5);
    assertEquals(0, kvs.length);
    kvs = getData(FAMILY, "row", "col2", 5);
    assertEquals(0, kvs.length);
    kvs = getData(FAMILY, "row", "col3", 2);
    assertEquals(0, kvs.length);
    kvs = getData(FAMILY, "row", Arrays.asList("col1", "col2", "col3"), 6);
    assertEquals(0, kvs.length);

    // File 5: Delete with post data timestamp and insert some older
    // date in new files.
    deleteFamily(FAMILY, "row", 10);
    region.flushcache();
    putData(FAMILY, "row", "col1", 7);
    putData(FAMILY, "row", "col2", 8);
    putData(FAMILY, "row", "col3", 9);
    region.flushcache();

    // Expected blocks read: 10. Why? [TODO]
    // With lazy seek, would have expected this to be lower.
    // At least is shouldn't be worse than before.
    //
    kvs = getData(FAMILY, "row", Arrays.asList("col1", "col2", "col3"), 10);
    assertEquals(0, kvs.length);

    // File 6: Put back new data
    putData(FAMILY, "row", "col1", 11);
    putData(FAMILY, "row", "col2", 12);
    putData(FAMILY, "row", "col3", 13);
    region.flushcache();

    // Expected blocks read: 9. Why? [TOD0]
    //
    // [Would have expected this to be 8.
    //  Six to go to the top of each file for delete marker. On file 6, the
    //  top block would serve "col1". And we should need two more to
    //  serve col2 and col3 from file 6.
    //
    kvs = getData(FAMILY, "row", Arrays.asList("col1", "col2", "col3"), 5);
    assertEquals(3, kvs.length);
    verifyData(kvs[0], "row", "col1", 11);
    verifyData(kvs[1], "row", "col2", 12);
    verifyData(kvs[2], "row", "col3", 13);
  }
}
