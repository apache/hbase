package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics.BlockMetricType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFromClientSide4 {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();
  private static byte[] ROW = Bytes.toBytes("testRow");
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte[] VALUE = Bytes.toBytes("testValue");

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt(
      HConstants.MAX_PRELOAD_BLOCKS_KEPT_IN_CACHE, 256);
    TEST_UTIL.startMiniCluster(3);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    // Nothing to do.
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    // Nothing to do.
  }

  public static enum ResultScannerType {
    CLIENT_SCANNER, CLIENT_LOCAL_SCANNER
  }

  class PreloadScanVerify implements Runnable {
    HTable ht;
    List<KeyValue> kvListExp;
    boolean toLog;
    boolean preload;

    public PreloadScanVerify(HTable ht, List<KeyValue> kvListExp,
        boolean toLog, boolean preload) {
      this.ht = ht;
      this.kvListExp = kvListExp;
      this.toLog = toLog;
      this.preload = preload;
    }

    public void run() {
      Scan scan = new Scan();
      scan.setPreloadBlocks(true);
      try {
        ResultScanner scanner = ht.getScanner(scan);
        List<KeyValue> kvListScan;
        Result result;
        kvListScan = new ArrayList<KeyValue>();
        while ((result = scanner.next()) != null) {
          for (KeyValue kv : result.list()) {
            kvListScan.add(kv);
          }
        }
        result = new Result(kvListScan);
        verifyResult(result, kvListExp, toLog, "Testing scan with preloading");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testMultiThreadSinglePreloading() throws Exception {
    String tn = "testMultiThreadSinglePreloading";
    String fn = tn + "family";
    byte[] TABLE = Bytes.toBytes(tn);
    byte[] CFAMILY = Bytes.toBytes(fn);
    byte[][] ROWS = makeNAsciiFixedLength(ROW, 10000, 6);
    byte[][] FAMILIES = makeNAscii(CFAMILY, 1);
    byte[][] QUALIFIERS = makeNAscii(QUALIFIER, 10);
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);
    Put put;
    boolean toLog = false;
    List<KeyValue> kvListExp;
    kvListExp = new ArrayList<KeyValue>();
    for (int r = 0; r < ROWS.length; r++) {
      put = new Put(ROWS[r]);
      for (int c = 0; c < FAMILIES.length; c++)
        for (int q = 0; q < QUALIFIERS.length; q++) {
          KeyValue kv =
              new KeyValue(ROWS[r], FAMILIES[c], QUALIFIERS[q], 1, VALUE);
          put.add(kv);
          kvListExp.add(kv);
        }
      ht.put(put);
    }
    TEST_UTIL.flush();
    int scannerCount = 10;
    Thread[] scanners = new Thread[scannerCount];
    scanners[0] = new Thread(new PreloadScanVerify(ht, kvListExp, toLog, true));
    scanners[0].start();
    scanners[0].join();
    for (int i = 1; i < scannerCount; i++)
      scanners[i] =
          new Thread(new PreloadScanVerify(ht, kvListExp, toLog, false));
    for (int i = 1; i < scannerCount; i++)
      scanners[i].start();
    for (int i = 1; i < scannerCount; i++)
      scanners[i].join();
    SchemaMetrics metrics = SchemaMetrics.getInstance(tn, fn + "0");
    int hits =
        Integer.parseInt(metrics.getBlockMetric(BlockMetricType.CACHE_HIT,
          BlockCategory.DATA, false));
    int misses =
        Integer.parseInt(metrics.getBlockMetric(BlockMetricType.CACHE_MISS,
          BlockCategory.DATA, false));
    int preloaderMiss =
        Integer.parseInt(metrics.getBlockMetric(
          BlockMetricType.PRELOAD_CACHE_MISS, BlockCategory.DATA, false));
    // All the three values should be positive
    assertTrue(misses > 0 && hits > 0 && preloaderMiss > 0);
    // The blocks read by the preloader should always be less than or equal to
    // the total number of misses.
    assertTrue(preloaderMiss <= misses);
  }

  @Test
  public void testSelectSubsetOfColumns() throws Exception {
    String tn = "testSelectSubsetOfColumns";
    String fn = tn + "family";
    byte[] TABLE = Bytes.toBytes(tn);
    byte[] CFAMILY = Bytes.toBytes(fn);
    byte[][] ROWS = makeNAsciiFixedLength(ROW, 10000, 6);
    byte[][] FAMILIES = makeNAscii(CFAMILY, 1);
    byte[][] QUALIFIERS = makeNAscii(QUALIFIER, 10);
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);
    Put put;
    Scan scan;
    Result result;
    boolean toLog = false;
    List<KeyValue> kvListExp, kvListScan;
    kvListExp = new ArrayList<KeyValue>();
    for (int r = 0; r < ROWS.length; r++) {
      put = new Put(ROWS[r], r % 5);
      for (int c = 0; c < FAMILIES.length; c++)
        for (int q = 0; q < QUALIFIERS.length; q++) {
          KeyValue kv =
              new KeyValue(ROWS[r], FAMILIES[c], QUALIFIERS[q], r % 5, VALUE);
          put.add(kv);
          if (r % 5 >= 1 && r % 5 < 4 && q == 5) {
            kvListExp.add(kv);
          }
        }
      ht.put(put);
    }
    TEST_UTIL.flush();
    scan = new Scan();
    scan.setTimeRange(1, 4);
    scan.addColumn(FAMILIES[0], QUALIFIERS[5]);
    scan.setPreloadBlocks(true);
    ResultScanner scanner = ht.getScanner(scan);
    kvListScan = new ArrayList<KeyValue>();
    while ((result = scanner.next()) != null) {
      for (KeyValue kv : result.list()) {
        kvListScan.add(kv);
      }
    }
    SchemaMetrics metrics = SchemaMetrics.getInstance(tn, fn + "0");
    int hits =
        Integer.parseInt(metrics.getBlockMetric(BlockMetricType.CACHE_HIT,
          BlockCategory.DATA, false));
    int misses =
        Integer.parseInt(metrics.getBlockMetric(BlockMetricType.CACHE_MISS,
          BlockCategory.DATA, false));
    int preloaderMiss =
        Integer.parseInt(metrics.getBlockMetric(
          BlockMetricType.PRELOAD_CACHE_MISS, BlockCategory.DATA, false));
    result = new Result(kvListScan);
    // Verify that the KVs are correct
    verifyResult(result, kvListExp, toLog, "Test scan subset of columns");
    // All the three values should be positive
    assertTrue(misses > 0 && hits > 0 && preloaderMiss > 0);
    // Check that at least nine tenth of the blocks read by the preloader is used by the scanner
    assertTrue(9 * preloaderMiss <= 10 * hits);
    // Check that the cache misses ratio is at most 1/10
    assertTrue(misses * 10 <= hits + misses);
  }

  @Test
  public void testScanFullTable() throws Exception {
    String tn = "testScanFullTable";
    String fn = tn + "family";
    byte[] TABLE = Bytes.toBytes(tn);
    byte[] CFAMILY = Bytes.toBytes(fn);
    byte[][] ROWS = makeNAsciiFixedLength(ROW, 10000, 6);
    byte[][] FAMILIES = makeNAscii(CFAMILY, 1);
    byte[][] QUALIFIERS = makeNAscii(QUALIFIER, 10);
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);
    Put put;
    Scan scan;
    Result result;
    boolean toLog = false;
    List<KeyValue> kvListExp, kvListScan;
    kvListExp = new ArrayList<KeyValue>();
    for (int r = 0; r < ROWS.length; r++) {
      put = new Put(ROWS[r], 0);
      for (int c = 0; c < FAMILIES.length; c++)
        for (int q = 0; q < QUALIFIERS.length; q++) {
          KeyValue kv =
              new KeyValue(ROWS[r], FAMILIES[c], QUALIFIERS[q], 0, VALUE);
          put.add(kv);
          kvListExp.add(kv);
        }
      ht.put(put);
    }
    TEST_UTIL.flush();
    scan = new Scan();
    scan.setPreloadBlocks(true);
    ResultScanner scanner = ht.getScanner(scan);
    kvListScan = new ArrayList<KeyValue>();
    while ((result = scanner.next()) != null) {
      for (KeyValue kv : result.list()) {
        kvListScan.add(kv);
      }
    }
    SchemaMetrics metrics = SchemaMetrics.getInstance(tn, fn + "0");
    int hits =
        Integer.parseInt(metrics.getBlockMetric(BlockMetricType.CACHE_HIT,
          BlockCategory.DATA, false));
    int misses =
        Integer.parseInt(metrics.getBlockMetric(BlockMetricType.CACHE_MISS,
          BlockCategory.DATA, false));
    int preloaderMiss =
        Integer.parseInt(metrics.getBlockMetric(
          BlockMetricType.PRELOAD_CACHE_MISS, BlockCategory.DATA, false));
    result = new Result(kvListScan);
    // Verify that the KVs are correct
    verifyResult(result, kvListExp, toLog, "Test scan subset of columns");
    // All the three values should be positive
    assertTrue(misses > 0 && hits > 0 && preloaderMiss > 0);
    // Check that at least nine tenth of the blocks read by the preloader is used by the scanner
    assertTrue(9 * preloaderMiss <= 10 * hits);
    // The blocks read by the preloader should always be less than or equal to
    // the total number of misses.
    assertTrue(preloaderMiss <= misses);
  }

  @Test
  public void testScanNoPreload() throws Exception {
    String tn = "testScanNoPreload";
    String fn = tn + "family";
    byte[] TABLE = Bytes.toBytes(tn);
    byte[] CFAMILY = Bytes.toBytes(fn);
    byte[][] ROWS = makeNAsciiFixedLength(ROW, 10000, 6);
    byte[][] FAMILIES = makeNAscii(CFAMILY, 1);
    byte[][] QUALIFIERS = makeNAscii(QUALIFIER, 10);
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);
    Put put;
    Scan scan;
    Result result;
    boolean toLog = false;
    List<KeyValue> kvListExp, kvListScan;
    kvListExp = new ArrayList<KeyValue>();
    for (int r = 0; r < ROWS.length; r++) {
      put = new Put(ROWS[r], 0);
      for (int c = 0; c < FAMILIES.length; c++)
        for (int q = 0; q < QUALIFIERS.length; q++) {
          KeyValue kv =
              new KeyValue(ROWS[r], FAMILIES[c], QUALIFIERS[q], 0, VALUE);
          put.add(kv);
          kvListExp.add(kv);
        }
      ht.put(put);
    }
    TEST_UTIL.flush();
    scan = new Scan();
    ResultScanner scanner = ht.getScanner(scan);
    kvListScan = new ArrayList<KeyValue>();
    while ((result = scanner.next()) != null) {
      for (KeyValue kv : result.list()) {
        kvListScan.add(kv);
      }
    }
    SchemaMetrics metrics = SchemaMetrics.getInstance(tn, fn + "0");
    int hits =
        Integer.parseInt(metrics.getBlockMetric(BlockMetricType.CACHE_HIT,
          BlockCategory.DATA, false));
    int misses =
        Integer.parseInt(metrics.getBlockMetric(BlockMetricType.CACHE_MISS,
          BlockCategory.DATA, false));
    result = new Result(kvListScan);
    // Verify that the KVs are correct
    verifyResult(result, kvListExp, toLog, "Test scan subset of columns");
    // All the three values should be positive
    assertTrue(misses + hits > 0);
    // Check that the cache hit ratio is at most 1/10
    assertTrue(hits * 10 <= hits + misses);
  }

  private void verifyResult(Result result, List<KeyValue> kvList,
      boolean toLog, String msg) {
    int i = 0;

    LOG.info(msg);
    LOG.info("Exp cnt: " + kvList.size());
    LOG.info("True cnt is: " + result.size());
    assertEquals(kvList.size(), result.size());

    if (kvList.size() == 0) return;

    for (KeyValue kv : result.sorted()) {
      KeyValue kvExp = kvList.get(i++);
      if (toLog) {
        LOG.info("get kv is: " + kv.toString());
        LOG.info("exp kv is: " + kvExp.toString());
      }
      assertTrue("Not equal", kvExp.equals(kv));
    }

  }

  private byte[][] makeNAscii(byte[] base, int n) {
    byte[][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      byte[] tail = Bytes.toBytes(Integer.toString(i));
      ret[i] = Bytes.add(base, tail);
    }
    return ret;
  }

  private byte[][] makeNAsciiFixedLength(byte[] base, int n, int len) {
    byte[][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      String s = i + "";
      while (s.length() < len)
        s = "0" + s;
      byte[] tail = Bytes.toBytes(s);
      ret[i] = Bytes.add(base, tail);
    }
    return ret;
  }
}
