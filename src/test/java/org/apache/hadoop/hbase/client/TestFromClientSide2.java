package org.apache.hadoop.hbase.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
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


  /**
   * Test from client side for batch of scan
   *
   * @throws Exception
   */
  @Test
  public void testScanBatch() throws Exception {
    byte [] TABLE = Bytes.toBytes("testScanBatch");
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

    // without batch
    scan = new Scan(ROW);
    scan.setMaxVersions();
    scanner = ht.getScanner(scan);

    // c4:4, c5:5, c6:6, c7:7
    kvListExp = new ArrayList<KeyValue>();
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[4], 4, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[5], 5, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[6], 6, VALUE));
    kvListExp.add(new KeyValue(ROW, FAMILY, QUALIFIERS[7], 7, VALUE));
    result = scanner.next();
    verifyResult(result, kvListExp, toLog, "Testing first batch of scan");

    // with batch
    scan = new Scan(ROW);
    scan.setMaxVersions();
    scan.setBatch(2);
    scanner = ht.getScanner(scan);

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

  private void verifyResult(Result result, List<KeyValue> kvList, boolean toLog, String msg) {
    int i =0;

    LOG.info(msg);
    LOG.info("Exp cnt: " + kvList.size());
    LOG.info("True cnt is: " + result.size());
    assertEquals(kvList.size(), result.size());

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
