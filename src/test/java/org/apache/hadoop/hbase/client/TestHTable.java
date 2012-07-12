package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHTable {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
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

  @Test
  public void testHTableMultiPutThreadPool() throws Exception {
    byte [] TABLE = Bytes.toBytes("testHTableMultiputThreadPool");
    final int NUM_REGIONS = 10;
    HTable ht = TEST_UTIL.createTable(TABLE, new byte[][]{FAMILY},
        3, Bytes.toBytes("aaaaa"), Bytes.toBytes("zzzzz"), NUM_REGIONS);
    byte [][] ROWS = ht.getStartKeys();
    ThreadPoolExecutor pool = (ThreadPoolExecutor)HTable.multiActionThreadPool;
    int previousPoolSize = pool.getPoolSize();
    int previousLargestPoolSize = pool.getLargestPoolSize();
    long previousCompletedTaskCount = pool.getCompletedTaskCount();

    for (int i = 0; i < NUM_REGIONS; i++) {
      Put put = new Put(ROWS[i]);
      put.add(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      ht.flushCommits();
    }

    // verify that HTable does NOT use thread pool for single put requests
    assertEquals(1, pool.getCorePoolSize());
    assertEquals(previousPoolSize, pool.getPoolSize());
    assertEquals(previousLargestPoolSize, pool.getLargestPoolSize());
    assertEquals(previousCompletedTaskCount, pool.getCompletedTaskCount());

    ArrayList<Put> multiput = new ArrayList<Put>();
    for (int i = 0; i < NUM_REGIONS; i++) {
      Put put = new Put(ROWS[i]);
      put.add(FAMILY, QUALIFIER, VALUE);
      multiput.add(put);
    }
    ht.put(multiput);
    ht.flushCommits();

    // verify that HTable does use thread pool for multi put requests.
    assertTrue((SLAVES >= pool.getLargestPoolSize())
      && (pool.getLargestPoolSize() >= previousLargestPoolSize));
    assertEquals(SLAVES,
        (pool.getCompletedTaskCount() - previousCompletedTaskCount));
  }
}
