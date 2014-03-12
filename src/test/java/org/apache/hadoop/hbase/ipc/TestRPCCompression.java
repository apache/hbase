package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRPCCompression {
  final Log LOG = LogFactory.getLog(getClass());
  private static HBaseTestingUtility TEST_UTIL;
  private static int SLAVES = 1;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // start the cluster
    Configuration conf = HBaseConfiguration.create();
    TEST_UTIL = new HBaseTestingUtility(conf);
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
  public void testCompressedRPC() throws Exception {
    byte[] TABLE = Bytes.toBytes("testRPCCompression");
    byte[][] FAMILIES = new byte[][] { Bytes.toBytes("foo") };
    byte [] QUALIFIER = Bytes.toBytes("testQualifier");
    byte [] VALUE = Bytes.toBytes("testValue");

    // create a table
    TEST_UTIL.createTable(TABLE, FAMILIES);
    LOG.debug("Created table " + new String(TABLE));
    
    // open the table with compressed RPC
    Configuration conf = HBaseConfiguration.create();
    String zkPortStr = TEST_UTIL.getConfiguration().get(
        "hbase.zookeeper.property.clientPort");
    conf.setInt("hbase.zookeeper.property.clientPort", 
        Integer.parseInt(zkPortStr));
    conf.set(HConstants.HBASE_RPC_COMPRESSION_KEY, 
        Compression.Algorithm.GZ.getName());
    HTable table = new HTable(conf, TABLE);

    // put some values
    byte [][] ROWS = { Bytes.toBytes("a"), Bytes.toBytes("b") };
    for (int i = 0; i < ROWS.length; i++) {
      Put put = new Put(ROWS[i]);
      put.add(FAMILIES[0], QUALIFIER, VALUE);
      table.put(put);
    }
    LOG.debug("Wrote some puts to table " + new String(TABLE));

    // flush the table
    table.flushCommits();
    LOG.debug("Flushed table " + new String(TABLE));

    // read back the values
    for (int i = 0; i < ROWS.length; i++) {
      Get get = new Get(ROWS[i]);
      get.addColumn(FAMILIES[0], QUALIFIER);
      Result result = table.get(get);
      
      assertEquals(new String(VALUE), 
    		  new String(result.getValue(FAMILIES[0], QUALIFIER)));
    }
    LOG.debug("Read and verified from table " + new String(TABLE));
  }
}
