package org.apache.hadoop.hbase.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestClientLocalScanner2 {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] FAMILY2 = Bytes.toBytes("testFamily2");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");
  private static int SLAVES = 3;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * This test is to ensure that the MVCC default value that was chosen while
   * creating stores is appropriate. The Memstore TS being non zero in the
   * localScanner was checked by logging and verifying the logs.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testMVCCEffect() throws IOException, InterruptedException{
    final byte [] name = Bytes.toBytes("testMVCCEffect");
    final HTable t = TEST_UTIL.createTable(name, FAMILY);
    final AtomicInteger rowCnt = new AtomicInteger(0);
    final int numRows = 1000;
    final AtomicInteger tmp = new AtomicInteger(0);
    for (int i=0; i<numRows; i++) {
      byte[] row = Bytes.toBytes("row" + i);
      Put p = new Put(row);
      p.add(FAMILY, FAMILY, row);
      t.put(p);
    }
    t.flushCommits();
    TEST_UTIL.flush();
    Thread scanThread = new Thread() {
      public void run() {
        try {
          ResultScanner scanner = t.getScanner(new Scan());
          for (Result r : scanner) {
            rowCnt.incrementAndGet();
          }
          LOG.debug("rowCnt : " + rowCnt.get());
        } catch (IOException e) {
          LOG.error("Caught IOException" + e.getMessage());
        }
      }
    };
    Thread putThread = new Thread() {
      public void run() {
        try {
          final HTable tmpTable = new HTable(TEST_UTIL.getConfiguration(), name);
          for (int i=numRows; i<2*numRows; i++) {
            byte[] row = Bytes.toBytes("row" + i);
            Put p = new Put(row);
            p.add(FAMILY, QUALIFIER, VALUE);
            tmpTable.put(p);
            tmp.incrementAndGet();
            if ((i+1) % 1000 == 0) {
              tmpTable.flushCommits();
            }
          }
        } catch (IOException e) {
          LOG.error("Caught IOException" + e.getMessage());
        }
      }
    };
    scanThread.start();
    putThread.start();
    putThread.join();
    scanThread.join();

    t.flushCommits();
    TEST_UTIL.flush();
    Threads.sleep(5000);
    LOG.debug("rowCnt : " + rowCnt.get());
    assertTrue(rowCnt.get() == numRows);
    ResultScanner scanner = t.getLocalScanner(new Scan());
    for (Result r : scanner) {
      rowCnt.incrementAndGet();
    }
    LOG.debug("rowCnt : " + rowCnt.get());
    assertTrue(rowCnt.get() == (3*numRows));
  }

}
