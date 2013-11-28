package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestFromClientSide2 {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] ROW = Bytes.toBytes("testRow");
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
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
    CLIENT_SCANNER,
    CLIENT_LOCAL_SCANNER
  }

  /**
   * Test from client side for batch of scan
   *
   * @throws Exception
   */
  @Test
  public void testScanBatch() throws Exception {
    testScanBatch(ResultScannerType.CLIENT_SCANNER);
  }

  /*
   * Test for Client Side Scan
   */
  @Test
  public void testScanBatchLocalScan() throws Exception {
    testScanBatch(ResultScannerType.CLIENT_LOCAL_SCANNER);
  }

  public void testScanBatch(ResultScannerType version) throws IOException {
    byte [] TABLE = Bytes.toBytes("testScanBatch" + version.name());
    byte [][] QUALIFIERS = makeNAscii(QUALIFIER, 8);

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILY);

    Put put;
    Scan scan;
    Delete delete;
    Result result;
    ResultScanner scanner;
    boolean toLog = true;
    List<KeyValue> kvListExp;

    // table: row, family, c0:0, c1:1, ... , c7:7
    put = new Put(ROW);
    for (int i=0; i < QUALIFIERS.length; i++) {
      KeyValue kv = new KeyValue(ROW, FAMILY, QUALIFIERS[i], i, VALUE);
      put.add(kv);
    }
    ht.put(put);

    // table: row, family, c0:0, c1:1, ..., c6:2, c6:6 , c7:7
    put = new Put(ROW);
    KeyValue kv = new KeyValue(ROW, FAMILY, QUALIFIERS[6], 2, VALUE);
    put.add(kv);
    ht.put(put);

    // delete upto ts: 3
    delete = new Delete(ROW);
    delete.deleteFamily(FAMILY, 3);
    ht.delete(delete);
    if (version == ResultScannerType.CLIENT_LOCAL_SCANNER) {
      TEST_UTIL.flush();
    }
    // without batch
    scan = new Scan(ROW);
    scan.setMaxVersions();
    if (version == ResultScannerType.CLIENT_LOCAL_SCANNER) {
      scanner = ht.getLocalScanner(scan);
    } else {
      scanner = ht.getScanner(scan);
    }
    // c4:4, c5:5, c6:6, c7:7
    kvListExp = new ArrayList<KeyValue>();
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[4], 4, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[5], 5, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[6], 6, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[7], 7, VALUE));
    result = scanner.next();
    verifyResult(result, kvListExp, toLog, "Testing first batch of scan");
    scanner.close();

    // with batch
    scan = new Scan(ROW);
    scan.setMaxVersions();
    scan.setBatch(2);
    if (version == ResultScannerType.CLIENT_LOCAL_SCANNER) {
      scanner = ht.getLocalScanner(scan);
    } else {
      scanner = ht.getScanner(scan);
    }

    // First batch: c4:4, c5:5
    kvListExp = new ArrayList<KeyValue>();
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[4], 4, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[5], 5, VALUE));
    result = scanner.next();
    verifyResult(result, kvListExp, toLog, "Testing first batch of scan");

    // Second batch: c6:6, c7:7
    kvListExp = new ArrayList<KeyValue>();
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[6], 6, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[7], 7, VALUE));
    result = scanner.next();
    verifyResult(result, kvListExp, toLog, "Testing second batch of scan");

  }

  /**
   * Test from client side for get with maxResultPerCF set
   *
   * @throws Exception
   */
  @Test
  public void testGetMaxResults() throws Exception {
    byte [] TABLE = Bytes.toBytes("testGetMaxResults");
    byte [][] FAMILIES = makeNAscii(FAMILY, 3);
    byte [][] QUALIFIERS = makeNAscii(QUALIFIER, 20);

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);

    Get get;
    Put put;
    Result result;
    boolean toLog = true;
    List<KeyValue> kvListExp;

    kvListExp = new ArrayList<KeyValue>();
    // Insert one CF for row[0]
    put = new Put(ROW);
    for (int i=0; i < 10; i++) {
      KeyValue kv = new KeyValue(ROW, FAMILIES[0], QUALIFIERS[i], 1, VALUE);
      put.add(kv);
      kvListExp.add(kv);
    }
    ht.put(put);

    get = new Get(ROW);
    result = ht.get(get);
    verifyResult(result, kvListExp, toLog, "Testing without setting maxResults");

    get = new Get(ROW);
    get.setMaxResultsPerColumnFamily(2);
    result = ht.get(get);
    kvListExp = new ArrayList<KeyValue>();
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[0], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[1], 1, VALUE));
    verifyResult(result, kvListExp, toLog, "Testing basic setMaxResults");

    // Filters: ColumnRangeFilter
    get = new Get(ROW);
    get.setMaxResultsPerColumnFamily(5);
    get.setFilter(new ColumnRangeFilter(QUALIFIERS[2], true, QUALIFIERS[5],
                                        true));
    result = ht.get(get);
    kvListExp = new ArrayList<KeyValue>();
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[2], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[3], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[4], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[5], 1, VALUE));
    verifyResult(result, kvListExp, toLog, "Testing single CF with CRF");

    // Insert two more CF for row[0]
    // 20 columns for CF2, 10 columns for CF1
    put = new Put(ROW);
    for (int i=0; i < QUALIFIERS.length; i++) {
      KeyValue kv = new KeyValue(ROW, FAMILIES[2], QUALIFIERS[i], 1, VALUE);
      put.add(kv);
    }
    ht.put(put);

    put = new Put(ROW);
    for (int i=0; i < 10; i++) {
      KeyValue kv = new KeyValue(ROW, FAMILIES[1], QUALIFIERS[i], 1, VALUE);
      put.add(kv);
    }
    ht.put(put);

    get = new Get(ROW);
    get.setMaxResultsPerColumnFamily(12);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    result = ht.get(get);
    kvListExp = new ArrayList<KeyValue>();
    //Exp: CF1:q0, ..., q9, CF2: q0, q1, q10, q11, ..., q19
    for (int i=0; i < 10; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[i], 1, VALUE));
    }
    for (int i=0; i < 2; i++) {
        kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[i], 1, VALUE));
      }
    for (int i=10; i < 20; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[i], 1, VALUE));
    }
    verifyResult(result, kvListExp, toLog, "Testing multiple CFs");

    // Filters: ColumnRangeFilter and ColumnPrefixFilter
    get = new Get(ROW);
    get.setMaxResultsPerColumnFamily(3);
    get.setFilter(new ColumnRangeFilter(QUALIFIERS[2], true, null, true));
    result = ht.get(get);
    kvListExp = new ArrayList<KeyValue>();
    for (int i=2; i < 5; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[i], 1, VALUE));
    }
    for (int i=2; i < 5; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[i], 1, VALUE));
    }
    for (int i=2; i < 5; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[i], 1, VALUE));
    }
    verifyResult(result, kvListExp, toLog, "Testing multiple CFs + CRF");

    get = new Get(ROW);
    get.setMaxResultsPerColumnFamily(7);
    get.setFilter(new ColumnPrefixFilter(QUALIFIERS[1]));
    result = ht.get(get);
    kvListExp = new ArrayList<KeyValue>();
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[1], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[1], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[1], 1, VALUE));
    for (int i=10; i < 16; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[i], 1, VALUE));
    }
    verifyResult(result, kvListExp, toLog, "Testing multiple CFs + PFF");

  }

  /**
   * Test from client side for scan with maxResultPerCF set
   *
   * @throws Exception
   */
  @Test
  public void testScanMaxResults() throws Exception {
    byte [] TABLE = Bytes.toBytes("testScanLimit");
    byte [][] ROWS= makeNAscii(ROW, 2);
    byte [][] FAMILIES = makeNAscii(FAMILY, 3);
    byte [][] QUALIFIERS = makeNAscii(QUALIFIER, 10);

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);

    Put put;
    Scan scan;
    Result result;
    boolean toLog = true;
    List<KeyValue> kvListExp, kvListScan;

    kvListExp = new ArrayList<KeyValue>();

    for (int r=0; r < ROWS.length; r++) {
      put = new Put(ROWS[r]);
      for (int c=0; c < FAMILIES.length; c++) {
        for (int q=0; q < QUALIFIERS.length; q++) {
          KeyValue kv = new KeyValue(ROWS[r], FAMILIES[c], QUALIFIERS[q], 1, VALUE);
          put.add(kv);
          if (q < 4) {
            kvListExp.add(kv);
          }
        }
      }
      ht.put(put);
    }

    scan = new Scan();
    scan.setMaxResultsPerColumnFamily(4);
    ResultScanner scanner = ht.getScanner(scan);
    kvListScan = new ArrayList<KeyValue>();
    while ((result = scanner.next()) != null) {
      for (KeyValue kv : result.list()) {
        kvListScan.add(kv);
      }
    }
    result = new Result(kvListScan);
    verifyResult(result, kvListExp, toLog, "Testing scan with maxResults");

  }

  /**
   * Test from client side for get with rowOffset
   *
   * @throws Exception
   */
  @Test
  public void testGetRowOffset() throws Exception {
    byte [] TABLE = Bytes.toBytes("testGetRowOffset");
    byte [][] FAMILIES = makeNAscii(FAMILY, 3);
    byte [][] QUALIFIERS = makeNAscii(QUALIFIER, 20);

    HTable ht = TEST_UTIL.createTable(TABLE, FAMILIES);

    Get get;
    Put put;
    Result result;
    boolean toLog = true;
    List<KeyValue> kvListExp;

    // Insert one CF for row
    kvListExp = new ArrayList<KeyValue>();
    put = new Put(ROW);
    for (int i=0; i < 10; i++) {
      KeyValue kv = new KeyValue(ROW, FAMILIES[0], QUALIFIERS[i], 1, VALUE);
      put.add(kv);
      // skipping first two kvs
      if (i < 2) continue;
      kvListExp.add(kv);
    }
    ht.put(put);

    //setting offset to 2
    get = new Get(ROW);
    get.setRowOffsetPerColumnFamily(2);
    result = ht.get(get);
    verifyResult(result, kvListExp, toLog, "Testing basic setRowOffset");

    //setting offset to 20
    get = new Get(ROW);
    get.setRowOffsetPerColumnFamily(20);
    result = ht.get(get);
    kvListExp = new ArrayList<KeyValue>();
    verifyResult(result, kvListExp, toLog, "Testing offset > #kvs");

    //offset + maxResultPerCF
    get = new Get(ROW);
    get.setRowOffsetPerColumnFamily(4);
    get.setMaxResultsPerColumnFamily(5);
    result = ht.get(get);
    kvListExp = new ArrayList<KeyValue>();
    for (int i=4; i < 9; i++) {
      kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[i], 1, VALUE));
    }
    verifyResult(result, kvListExp, toLog,
      "Testing offset + setMaxResultsPerCF");

    // Filters: ColumnRangeFilter
    get = new Get(ROW);
    get.setRowOffsetPerColumnFamily(1);
    get.setFilter(new ColumnRangeFilter(QUALIFIERS[2], true, QUALIFIERS[5],
                                        true));
    result = ht.get(get);
    kvListExp = new ArrayList<KeyValue>();
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[3], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[4], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[0], QUALIFIERS[5], 1, VALUE));
    verifyResult(result, kvListExp, toLog, "Testing offset with CRF");

    // Insert into two more CFs for row
    // 10 columns for CF2, 10 columns for CF1
    for(int j=2; j > 0; j--) {
      put = new Put(ROW);
      for (int i=0; i < 10; i++) {
        KeyValue kv = new KeyValue(ROW, FAMILIES[j], QUALIFIERS[i], 1, VALUE);
        put.add(kv);
      }
      ht.put(put);
    }

    get = new Get(ROW);
    get.setRowOffsetPerColumnFamily(4);
    get.setMaxResultsPerColumnFamily(2);
    get.addFamily(FAMILIES[1]);
    get.addFamily(FAMILIES[2]);
    result = ht.get(get);
    kvListExp = new ArrayList<KeyValue>();
    //Exp: CF1:q4, q5, CF2: q4, q5
    kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[4], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[1], QUALIFIERS[5], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[4], 1, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILIES[2], QUALIFIERS[5], 1, VALUE));
    verifyResult(result, kvListExp, toLog,
       "Testing offset + multiple CFs + maxResults");

  }

  private void verifyResult(Result result, List<KeyValue> kvList, boolean toLog,
    String msg) {
    int i =0;

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

  private byte [][] makeNAscii(byte [] base, int n) {
    byte [][] ret = new byte[n][];
    for(int i=0;i<n;i++) {
      byte [] tail = Bytes.toBytes(Integer.toString(i));
      ret[i] = Bytes.add(base, tail);
    }
    return ret;
  }

}
