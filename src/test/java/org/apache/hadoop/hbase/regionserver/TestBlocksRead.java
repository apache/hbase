package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class TestBlocksRead extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestBlocksRead.class);
  static final BloomType[] BLOOM_TYPE = new BloomType[] {BloomType.ROWCOL,
      BloomType.ROW, BloomType.NONE};

  private static BlockCache blockCache;

  private HBaseConfiguration getConf() {
    HBaseConfiguration conf = new HBaseConfiguration();

    // disable compactions in this test.
    conf.setInt("hbase.hstore.compactionThreshold", 10000);
    return conf;
  }

  HRegion region = null;
  
  private final HBaseTestingUtility testUtil = new HBaseTestingUtility();
  private final String DIR = testUtil.getTestDir() + "/TestHRegion/";

  /**
   * @see org.apache.hadoop.hbase.HBaseTestCase#setUp()
   */
  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    EnvironmentEdgeManagerTestHelper.reset();
  }

  private void initHRegion(byte[] tableName, String callingMethod,
      HBaseConfiguration conf, String family, boolean versions) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor familyDesc;
    for (int i = 0; i < BLOOM_TYPE.length; i++) {
      BloomType bloomType = BLOOM_TYPE[i];
      familyDesc = new HColumnDescriptor(family + "_" + bloomType)
          .setBlocksize(1)
          .setBloomFilterType(BLOOM_TYPE[i]);
      if (versions)
        familyDesc.setMaxVersions(Integer.MAX_VALUE);
      htd.addFamily(familyDesc);
    }

    HRegionInfo info = new HRegionInfo(htd, null, null, false);
    Path path = new Path(DIR + callingMethod);
    region = HRegion.createHRegion(info, path, conf);
    blockCache = new CacheConfig(conf).getBlockCache();
  }



  private void putData(String family, String row, String col, long version)
      throws IOException {
    for (int i = 0; i < BLOOM_TYPE.length; i++) {
      putData(Bytes.toBytes(family + "_" + BLOOM_TYPE[i]), row, col, version,
          version);
    }
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

  private KeyValue[] getData(String family, String row, List<String> columns,
      int expBlocks) throws IOException {
    return getData(family, row, columns, expBlocks, expBlocks, expBlocks);
  }

  private KeyValue[] getData(String family, String row, List<String> columns,
      long timestamp, int expBlocks) throws IOException {
    return getData(family, row, columns, timestamp, expBlocks, expBlocks, expBlocks);
  }

  private KeyValue[] getData(String family, String row, List<String> columns,
      long timestamp, int expBlocksRowCol, int expBlocksRow, int expBlocksNone)
      throws IOException {
    int[] expBlocks = new int[] { expBlocksRowCol, expBlocksRow, expBlocksNone };
    KeyValue[] kvs = null;

    for (int i = 0; i < BLOOM_TYPE.length; i++) {
      BloomType bloomType = BLOOM_TYPE[i];
      byte[] cf = Bytes.toBytes(family + "_" + bloomType);
      long blocksStart = getBlkAccessCount(cf);
      Get get = new Get(Bytes.toBytes(row));

      for (String column : columns) {
        get.addColumn(cf, Bytes.toBytes(column));
        get.setTimeStamp(timestamp);
      }

      kvs = region.get(get, null).raw();
      long blocksEnd = getBlkAccessCount(cf);
      if (expBlocks[i] != -1) {
        assertEquals("Blocks Read Check for Bloom: " + bloomType, expBlocks[i],
            blocksEnd - blocksStart);
      }
      System.out.println("Blocks Read for Bloom: " + bloomType + " = "
          + (blocksEnd - blocksStart) + "Expected = " + expBlocks[i]);
    }
    return kvs;
  }

  private KeyValue[] getData(String family, String row, List<String> columns,
      int expBlocksRowCol, int expBlocksRow, int expBlocksNone)
      throws IOException {
    int[] expBlocks = new int[] { expBlocksRowCol, expBlocksRow, expBlocksNone };
    KeyValue[] kvs = null;

    for (int i = 0; i < BLOOM_TYPE.length; i++) {
      BloomType bloomType = BLOOM_TYPE[i];
      byte[] cf = Bytes.toBytes(family + "_" + bloomType);
      long blocksStart = getBlkAccessCount(cf);
      Get get = new Get(Bytes.toBytes(row));

      for (String column : columns) {
        get.addColumn(cf, Bytes.toBytes(column));
      }

      kvs = region.get(get, null).raw();
      long blocksEnd = getBlkAccessCount(cf);
      if (expBlocks[i] != -1) {
        assertEquals("Blocks Read Check for Bloom: " + bloomType, expBlocks[i],
            blocksEnd - blocksStart);
      }
      System.out.println("Blocks Read for Bloom: " + bloomType + " = "
          + (blocksEnd - blocksStart) + "Expected = " + expBlocks[i]);
    }
    return kvs;
  }

  private KeyValue[] getData(String family, String row, String column,
      int expBlocks) throws IOException {
    return getData(family, row, Arrays.asList(column), expBlocks, expBlocks,
        expBlocks);
  }

  private KeyValue[] getData(String family, String row, String column,
      int expBlocksRowCol, int expBlocksRow, int expBlocksNone)
      throws IOException {
    return getData(family, row, Arrays.asList(column), expBlocksRowCol,
        expBlocksRow, expBlocksNone);
  }

  private void deleteFamily(String family, String row, long version)
      throws IOException {
    Delete del = new Delete(Bytes.toBytes(row));
    del.deleteFamily(Bytes.toBytes(family + "_ROWCOL"), version);
    del.deleteFamily(Bytes.toBytes(family + "_ROW"), version);
    del.deleteFamily(Bytes.toBytes(family + "_NONE"), version);
    region.delete(del, null, true);
  }

  public void deleteColumn(String family, String qualifier, String row,
      long version) throws IOException {
    Delete del = new Delete(Bytes.toBytes(row));
    for (int i=0; i<BLOOM_TYPE.length; i++) {
      del.deleteColumn(Bytes.toBytes(family + BLOOM_TYPE[i]),
          Bytes.toBytes(qualifier), version);
    }
    region.delete(del, null, true);
  }

  public void deleteColumn(String family, String qualifier, String row)
      throws IOException {
    Delete del = new Delete(Bytes.toBytes(row));
    for (int i=0; i<BLOOM_TYPE.length; i++) {
      del.deleteColumns(Bytes.toBytes(family + "_" + BLOOM_TYPE[i]),
          Bytes.toBytes(qualifier));
    }
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

  private static long getBlkCount() {
    return blockCache.getBlockCount();
  }

  /**
   * Test # of blocks read for some simple seek cases.
   *
   * @throws Exception
   */
  @Test
  public void testBlocksRead() throws Exception {
    byte[] TABLE = Bytes.toBytes("testBlocksRead");
    String FAMILY = "cf1";
    KeyValue kvs[];
    HBaseConfiguration conf = getConf();
    initHRegion(TABLE, getName(), conf, FAMILY, false);

    putData(FAMILY, "row", "col1", 1);
    putData(FAMILY, "row", "col2", 2);
    putData(FAMILY, "row", "col3", 3);
    putData(FAMILY, "row", "col4", 4);
    putData(FAMILY, "row", "col5", 5);
    putData(FAMILY, "row", "col6", 6);
    putData(FAMILY, "row", "col7", 7);

    region.flushcache();

    // Expected block reads: 1
    // The top block has the KV we are
    // interested. So only 1 seek is needed.
    kvs = getData(FAMILY, "row", "col1", 1);
    assertEquals(1, kvs.length);
    verifyData(kvs[0], "row", "col1", 1);

    // Expected block reads: 2
    // The top block and next block has the KVs we are
    // interested. So only 2 seek is needed.
    kvs = getData(FAMILY, "row", Arrays.asList("col1", "col2"), 2);
    assertEquals(2, kvs.length);
    verifyData(kvs[0], "row", "col1", 1);
    verifyData(kvs[1], "row", "col2", 2);

    // Expected block reads: 3
    // The first 2 seeks is to find out col2. [HBASE-4443]
    // One additional seek for col3
    // So 3 seeks are needed.
    kvs = getData(FAMILY, "row", Arrays.asList("col2", "col3"), 3);
    assertEquals(2, kvs.length);
    verifyData(kvs[0], "row", "col2", 2);
    verifyData(kvs[1], "row", "col3", 3);

    // Expected block reads: 2. [HBASE-4443]
    kvs = getData(FAMILY, "row", Arrays.asList("col5"), 2);
    assertEquals(1, kvs.length);
    verifyData(kvs[0], "row", "col5", 5);
  }

  /**
   * Test # of blocks read (targetted at some of the cases Lazy Seek optimizes).
   *
   * @throws Exception
   */
  @Test
  public void testLazySeekBlocksRead() throws Exception {
    byte[] TABLE = Bytes.toBytes("testLazySeekBlocksRead");
    String FAMILY = "cf1";
    KeyValue kvs[];
    HBaseConfiguration conf = getConf();
    initHRegion(TABLE, getName(), conf, FAMILY, false);

    // File 1
    putData(FAMILY, "row", "col1", 1);
    putData(FAMILY, "row", "col2", 2);
    region.flushcache();

    // File 2
    putData(FAMILY, "row", "col1", 3);
    putData(FAMILY, "row", "col2", 4);
    region.flushcache();

    // Expected blocks read: 1.
    // File 2's top block is also the KV we are
    // interested. So only 1 seek is needed.
    kvs = getData(FAMILY, "row", Arrays.asList("col1"), 1);
    assertEquals(1, kvs.length);
    verifyData(kvs[0], "row", "col1", 3);

    // Expected blocks read: 2
    // File 2's top block has the "col1" KV we are
    // interested. We also need "col2" which is in a block
    // of its own. So, we need that block as well.
    kvs = getData(FAMILY, "row", Arrays.asList("col1", "col2"), 2);
    assertEquals(2, kvs.length);
    verifyData(kvs[0], "row", "col1", 3);
    verifyData(kvs[1], "row", "col2", 4);

    // File 3: Add another column
    putData(FAMILY, "row", "col3", 5);
    region.flushcache();

    // Expected blocks read: 1
    // File 3's top block has the "col3" KV we are
    // interested. So only 1 seek is needed.
    kvs = getData(FAMILY, "row", "col3", 1);
    assertEquals(1, kvs.length);
    verifyData(kvs[0], "row", "col3", 5);

    // Get a column from older file.
    // For ROWCOL Bloom filter: Expected blocks read: 1.
    // For ROW Bloom filter: Expected blocks read: 2.
    // For NONE Bloom filter: Expected blocks read: 2.
    kvs = getData(FAMILY, "row", Arrays.asList("col1"), 1, 2, 2);
    assertEquals(1, kvs.length);
    verifyData(kvs[0], "row", "col1", 3);

    // File 4: Delete the entire row.
    deleteFamily(FAMILY, "row", 6);
    region.flushcache();

    // For ROWCOL Bloom filter: Expected blocks read: 2.
    // For ROW Bloom filter: Expected blocks read: 3.
    // For NONE Bloom filter: Expected blocks read: 3.
    kvs = getData(FAMILY, "row", "col1", 2, 3, 3);
    assertEquals(0, kvs.length);
    kvs = getData(FAMILY, "row", "col2", 3, 4, 4);
    assertEquals(0, kvs.length);
    kvs = getData(FAMILY, "row", "col3", 2);
    assertEquals(0, kvs.length);
    kvs = getData(FAMILY, "row", Arrays.asList("col1", "col2", "col3"), 4);
    assertEquals(0, kvs.length);

    // File 5: Delete with post data timestamp and insert some older
    // date in new files.
    deleteFamily(FAMILY, "row", 10);
    region.flushcache();
    putData(FAMILY, "row", "col1", 7);
    putData(FAMILY, "row", "col2", 8);
    putData(FAMILY, "row", "col3", 9);
    region.flushcache();

    // Expected blocks read: 5. [HBASE-4585]
    kvs = getData(FAMILY, "row", Arrays.asList("col1", "col2", "col3"), 5);
    assertEquals(0, kvs.length);

    // File 6: Put back new data
    putData(FAMILY, "row", "col1", 11);
    putData(FAMILY, "row", "col2", 12);
    putData(FAMILY, "row", "col3", 13);
    region.flushcache();


    // Expected blocks read: 5. [HBASE-4585]
    kvs = getData(FAMILY, "row", Arrays.asList("col1", "col2", "col3"), 5);
    assertEquals(3, kvs.length);
    verifyData(kvs[0], "row", "col1", 11);
    verifyData(kvs[1], "row", "col2", 12);
    verifyData(kvs[2], "row", "col3", 13);
  }

  @Test
  public void testLazySeekBlocksReadWithDelete() throws Exception {
    byte[] TABLE = Bytes.toBytes("testLazySeekBlocksReadWithDelete");
    String FAMILY = "cf1";
    KeyValue kvs[];
    HBaseConfiguration conf = getConf();
    initHRegion(TABLE, getName(), conf, FAMILY, true);

    deleteFamily(FAMILY, "row", 200);
    for (int i = 0; i < 100; i++) {
      putData(FAMILY, "row", "col" + i, i);
    }
    putData(FAMILY, "row", "col99", 201);

    region.flushcache();
    kvs = getData(FAMILY, "row", Arrays.asList("col0"), 2);
    assertEquals(0, kvs.length);

    kvs = getData(FAMILY, "row", Arrays.asList("col99"), 2);
    assertEquals(1, kvs.length);
    verifyData(kvs[0], "row", "col99", 201);
  }

  @Test
  public void testDeleteColBloomFilterWithoutDeletesWithFlushCache() throws IOException{
    HRegionServer.numOptimizedSeeks.set(0);
    byte[] TABLE = Bytes.toBytes("testDeleteColBloomFilterWithoutDeletes");
    String FAMILY = "cf1";
    KeyValue kvs[];
    HBaseConfiguration conf = getConf();
    conf.setBoolean("io.storefile.delete.column.bloom.enabled", true);
    initHRegion(TABLE, getName(), conf, FAMILY, true);
    if (!conf.getBoolean(BloomFilterFactory.IO_STOREFILE_DELETECOLUMN_BLOOM_ENABLED, false)) {
      System.out.println("ignoring this test since the delete bloom filter is not enabled...");
      return;
    }
    for (int i = 1; i < 8; i++) {
      for (int j = 1; j < 6; j++) {
        putData(FAMILY, "row", "col"+i, j);
      }
    }
    region.flushcache();

    Scan scan = new Scan();
    scan.setMaxVersions(5);
    InternalScanner s = region.getScanner(scan);
    List<KeyValue> results = new ArrayList<KeyValue>(10);
    while (s.next(results))
      ;
    s.close();
    for (KeyValue kv : results) {
      System.out.println(kv.toString());
    }
    for (int i = 1; i < 8; i++) {
      for (int j = 1; j < 6; j++) {
        if (i == 1 && j == 5) {
          /**
           * Since this is the top KV (KV with smallest column and highest
           * timestamp, we just need one seek
           */
          kvs = getData(FAMILY, "row", Arrays.asList("col" + i), j, 1);
          verifyData(kvs[0], "row", "col" + i, j);
          assertEquals(1, kvs.length);
        } else {
          /**
           * We first go on the KV with max timestamp, then land on the actual
           * KV, which is 2 blocks read
           */
          kvs = getData(FAMILY, "row", Arrays.asList("col" + i), j, 2);
          verifyData(kvs[0], "row", "col" + i, j);
          assertEquals(1, kvs.length);
        }
      }
    }
    int optimizedSeeks = HRegionServer.numOptimizedSeeks.get();
    /**
     * Since we have 7 columns, 3 col families and 5 versions in total, and no
     * deletes, then we should get 7 * 3 * 5 optimized reads (all of them should
     * be optimized)
     **/
    assertEquals(7 * 3 * 5, optimizedSeeks);
  }

  @Test
  public void testDeleteColBloomFilterWithDeletesWithoutFlushCache() throws IOException{
    HRegionServer.numOptimizedSeeks.set(0);
    byte[] TABLE = Bytes.toBytes("testDeleteColBloomFilterWithDeletes");
    String FAMILY = "cf1";
    KeyValue kvs[];
    HBaseConfiguration conf = getConf();
    conf.setBoolean("io.storefile.delete.column.bloom.enabled", true);
    initHRegion(TABLE, getName(), conf, FAMILY, true);
    if (!conf.getBoolean(BloomFilterFactory.IO_STOREFILE_DELETECOLUMN_BLOOM_ENABLED, false)) {
      System.out.println("ignoring this test since the delete bloom filter is not enabled...");
      return;
    }
    for (int i = 1; i < 8; i++) {
      for (int j = 1; j < 6; j++) {
        putData(FAMILY, "row", "col"+i, j);
      }
    }

    deleteColumn(FAMILY, "col2", "row");
    deleteColumn(FAMILY, "col5", "row");
    deleteColumn(FAMILY, "col7", "row");

    kvs = getData(FAMILY, "row",  Arrays.asList("col2"), 5, 0);
    assertTrue(kvs.length == 0);
    kvs = getData(FAMILY, "row", Arrays.asList("col3"), 3, 0);
    verifyData(kvs[0], "row", "col3", 3);
    assertEquals(1, kvs.length);
    kvs = getData(FAMILY, "row", Arrays.asList("col6"), 4, 0 );
    verifyData(kvs[0], "row", "col6", 4);
    assertEquals(1, kvs.length);
    kvs = getData(FAMILY, "row",  Arrays.asList("col7"), 5, 0);
    assertTrue(kvs.length == 0);
    kvs = getData(FAMILY, "row",  Arrays.asList("col5"), 5, 0);
    assertTrue(kvs.length == 0);

    /**
     * Since we don't do flush we read just from the memstore, we are not
     * supposed to update the number of optimized seeks
     **/
    int optimizedSeeks = HRegionServer.numOptimizedSeeks.get();
    assertEquals(0, optimizedSeeks);
  }

  @Test
  public void testDeleteColBloomFilterWithDeletesWithFlushCache() throws IOException{
    HRegionServer.numOptimizedSeeks.set(0);
    byte[] TABLE = Bytes.toBytes("testDeleteColBloomFilterWithDeletes");
    String FAMILY = "cf1";
    KeyValue kvs[];
    HBaseConfiguration conf = getConf();
    conf.setBoolean("io.storefile.delete.column.bloom.enabled", true);
    initHRegion(TABLE, getName(), conf, FAMILY, true);
    if (!conf.getBoolean(BloomFilterFactory.IO_STOREFILE_DELETECOLUMN_BLOOM_ENABLED, false)) {
      System.out.println("ignoring this test since the delete bloom filter is not enabled...");
      return;
    }
    for (int i = 1; i < 8; i++) {
      for (int j = 1; j < 6; j++) {
        putData(FAMILY, "row", "col"+i, j);
      }
    }

    deleteColumn(FAMILY, "col2", "row");
    deleteColumn(FAMILY, "col5", "row");
    deleteColumn(FAMILY, "col7", "row");
    region.flushcache();

    /**
     * only the seeks for the KVs for which we don't have any deletes should be
     * optimized, and since we have 3 col families we will have number of seeks
     *
     **/
    kvs = getData(FAMILY, "row",  Arrays.asList("col2"), 5, 3);
    assertTrue(kvs.length == 0);
    kvs = getData(FAMILY, "row",  Arrays.asList("col3"), 3, 2);
    verifyData(kvs[0], "row", "col3", 3);
    assertEquals(1, kvs.length);
    kvs = getData(FAMILY, "row",  Arrays.asList("col6"), 4, 2 );
    verifyData(kvs[0], "row", "col6", 4);
    assertEquals(1, kvs.length);
    kvs = getData(FAMILY, "row",  Arrays.asList("col7"), 5, 2);
    assertTrue(kvs.length == 0);
    kvs = getData(FAMILY, "row",  Arrays.asList("col5"), 5, 3);
    assertTrue(kvs.length == 0);
    int numSeeks = HRegionServer.numOptimizedSeeks.get();
    assertEquals(6, numSeeks);
  }

  /**
   * This test will make a number of puts, and then do flush, then do another
   * series of puts. Then we will test the number of blocks read while doing get
   * @throws IOException
   */
  @Test
  public void testDeleteColBloomFilterWithoutDeletes() throws IOException {
    HRegionServer.numOptimizedSeeks.set(0);
    byte[] TABLE = Bytes.toBytes("testDeleteColBloomFilterWithDeletes");
    String FAMILY = "cf1";
    KeyValue kvs[];
    HBaseConfiguration conf = getConf();
    conf.setBoolean("io.storefile.delete.column.bloom.enabled", true);
    initHRegion(TABLE, getName(), conf, FAMILY, true);
    if (!conf.getBoolean(BloomFilterFactory.IO_STOREFILE_DELETECOLUMN_BLOOM_ENABLED, false)) {
      System.out.println("ignoring this test since the delete bloom filter is not enabled...");
      return;
    }
    int i, j;
    for (i = 1; i < 8; i++) {
      for (j = 1; j < 6; j++) {
        putData(FAMILY, "row", "col" + i, j);
      }
    }
    region.flushcache();

    /** Do some more puts and don't flush them **/
    for (i = 5; i < 10; i++) {
      for (j = 6; j < 10; j++) {
        putData(FAMILY, "row", "col" + i, j);
      }
    }

    /** Check the number of blocks read for the puts which are flushed **/
    for (i = 1; i < 8; i++) {
      for (j = 1; j < 6; j++) {
        if (i == 1 && j == 5) {
          /**
           * Since this is the top KV (KV with smallest column and highest
           * timestamp, we just need one seek
           */
          kvs = getData(FAMILY, "row", Arrays.asList("col" + i), j, 1);
          verifyData(kvs[0], "row", "col" + i, j);
          assertEquals(1, kvs.length);
        } else {
          /**
           * We first go on the KV with max timestamp, then land on the actual
           * KV, which is 2 blocks read
           */
          kvs = getData(FAMILY, "row", Arrays.asList("col" + i), j, 2);
          verifyData(kvs[0], "row", "col" + i, j);
          assertEquals(1, kvs.length);
        }
      }
    }
    /** Check the number of blocks read for the puts which were not flushed **/
    for (i = 6; i < 10; i++) {
      for (j = 6; j < 10; j++) {
        kvs = getData(FAMILY, "row", Arrays.asList("col" + i), j, 0);
        verifyData(kvs[0], "row", "col" + i, j);
      }
    }

    int optimizedSeeks = HRegionServer.numOptimizedSeeks.get();
    /**
     * since there are no deletes, the additional gets for puts which are not
     * flushed will be counted as optimized too
     **/
    int actualSeeks = 7 * 5 * 3 + (2 * 3 + 1) * 4 * 3;
    assertEquals(actualSeeks, optimizedSeeks);
  }

  /**
   * Test # of blocks read to ensure disabling cache-fill on Scan works.
   * @throws Exception
   */
  @Test
  public void testBlocksStoredWhenCachingDisabled() throws Exception {
    byte [] TABLE = Bytes.toBytes("testBlocksReadWhenCachingDisabled");
    String FAMILY = "cf1";

    HBaseConfiguration conf = getConf();
    initHRegion(TABLE, getName(), conf, FAMILY, false);

    putData(FAMILY, "row", "col1", 1);
    putData(FAMILY, "row", "col2", 2);
    region.flushcache();

    // Execute a scan with caching turned off
    // Expected blocks stored: 0
    long blocksStart = getBlkCount();
    Scan scan = new Scan();
    scan.setCacheBlocks(false);
    InternalScanner rs = region.getScanner(scan);
    List<KeyValue> result = new ArrayList<KeyValue>();
    rs.next(result);
    assertEquals(2 * BLOOM_TYPE.length, result.size());
    rs.close();
    long blocksEnd = getBlkCount();

    assertEquals(blocksStart, blocksEnd);

    // Execute with caching turned on
    // Expected blocks stored: 2
    blocksStart = blocksEnd;
    scan.setCacheBlocks(true);
    rs = region.getScanner(scan);
    result = new ArrayList<KeyValue>(2);
    rs.next(result);
    assertEquals(2 * BLOOM_TYPE.length, result.size());
    rs.close();
    blocksEnd = getBlkCount();

    assertEquals(2 * BLOOM_TYPE.length, blocksEnd - blocksStart);
  }

  @Test
  public void testDeleteColBloomFilterWithoutDeletesWithFlushCacheOneColumn() throws IOException{
    HRegionServer.numOptimizedSeeks.set(0);
    byte[] TABLE = Bytes.toBytes("testDeleteColBloomFilterWithoutDeletesWithFlushCacheOneColumn");
    String FAMILY = "cf1";
    String col = "";
    KeyValue kvs[];
    HBaseConfiguration conf = getConf();
    conf.setBoolean("io.storefile.delete.column.bloom.enabled", true);
    initHRegion(TABLE, getName(), conf, FAMILY, true);
    if (!conf.getBoolean(BloomFilterFactory.IO_STOREFILE_DELETECOLUMN_BLOOM_ENABLED, false)) {
      System.out.println("ignoring this test since the delete bloom filter is not enabled...");
      return;
    }
    for (int i = 1; i < 100; i++) {
      putData(FAMILY, "row", col, i);
    }
    region.flushcache();
    for (int i = 1; i < 100; i++) {
        if (i == 99) {
          kvs = getData(FAMILY, "row", Arrays.asList(col), i, 1);
        } else if (i == 98){
          kvs = getData(FAMILY, "row", Arrays.asList(col), i, 2);
        } else {
          kvs = getData(FAMILY, "row", Arrays.asList(col), i, 3);
        }
        verifyData(kvs[0], "row", col, i);
        assertEquals(1, kvs.length);
    }
  }

  /**
   * One row, two columns, adding data for both columns, afterwards deleting the
   * second column. We should not get any data and optimized seeks should not
   * changed when we try to do get against the deleted column
   *
   * @throws IOException
   */
  @Test
  public void testOptimizedSeeksWithAndWithoutDeletes() throws IOException {
    HRegionServer.numOptimizedSeeks.set(0);
    byte[] TABLE = Bytes.toBytes("testOptimizedSeeksWithAndWithoutDeletes");
    String FAMILY = "cf1";
    HBaseConfiguration conf = getConf();
    KeyValue kvs[];
    conf.setBoolean("io.storefile.delete.column.bloom.enabled", true);
    initHRegion(TABLE, getName(), conf, FAMILY, true);
    if (!conf.getBoolean(
        BloomFilterFactory.IO_STOREFILE_DELETECOLUMN_BLOOM_ENABLED, false)) {
      System.out
          .println("ignoring this test since the delete bloom filter is not enabled...");
      return;
    }
    for (int col = 1; col < 2; col++) {
      for (int i = 1; i < 5; i++) {
        putData(FAMILY, "row", "col" + col, i);
      }
    }
    deleteColumn(FAMILY, "col2", "row");
    region.flushcache();

    int previousOptimizedSeeks = 0;
    for (int col = 1; col < 2; col++) {
      for (int i = 1; i < 5; i++) {
        if (col == 2) {
          if (previousOptimizedSeeks != HRegionServer.numOptimizedSeeks.get()) {
            assertFalse("num optimized seeks increased during querying deleted column!", false);
          }
          kvs = getData(FAMILY, "row", Arrays.asList("col2"), i,  0);
          assertTrue(kvs.length == 0);
        } else {
          if (i == 4) {
            kvs = getData(FAMILY, "row", Arrays.asList("col" + col), i, 1);
          } else {
            kvs = getData(FAMILY, "row", Arrays.asList("col" + col), i, 2);
          }
          verifyData(kvs[0], "row", "col"+col, i);
          assertEquals(1, kvs.length);
        }
        previousOptimizedSeeks = HRegionServer.numOptimizedSeeks.get();
      }
    }
    assertEquals(4 * 3, HRegionServer.numOptimizedSeeks.get());
  }
}
