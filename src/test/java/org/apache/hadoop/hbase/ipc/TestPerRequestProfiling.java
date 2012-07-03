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

public class TestPerRequestProfiling {
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
  public void testPerRequestProfiling() throws Exception {
    byte[] TABLE = Bytes.toBytes("testPerRequestProfiling");
    byte[][] FAMILIES = new byte[][] { Bytes.toBytes("foo") };
    byte [] QUALIFIER = Bytes.toBytes("testQualifier");
    byte [] VALUE = Bytes.toBytes("testValue");

    // create a table
    TEST_UTIL.createTable(TABLE, FAMILIES);
    LOG.debug("Created table " + new String(TABLE));
    
    // open the table
    Configuration conf = HBaseConfiguration.create();
    String zkPortStr = TEST_UTIL.getConfiguration().get(
        "hbase.zookeeper.property.clientPort");
    conf.setInt("hbase.zookeeper.property.clientPort", 
        Integer.parseInt(zkPortStr));
    HTable table = new HTable(conf, TABLE);

    LOG.debug("Testing with profiling off");
    // put some values with profiling off
    byte [][] ROWS = { Bytes.toBytes("a"), Bytes.toBytes("b") };
    for (int i = 0; i < ROWS.length; i++) {
      Put put = new Put(ROWS[i]);
      put.add(FAMILIES[0], QUALIFIER, VALUE);
      table.put(put);
      // autoflush is on by default, or else move this check after flush
      assertTrue (table.getProfilingData () == null);
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
      
      assertTrue (table.getProfilingData () == null);
    }
    LOG.debug("Read and verified from table " + new String(TABLE));
    
    // turn profiling on and repeat test
    table.setProfiling(true);
    
    LOG.debug("Testing with profiling on");
    for (int i = 0; i < ROWS.length; i++) {
      Put put = new Put(ROWS[i]);
      put.add(FAMILIES[0], QUALIFIER, VALUE);
      table.put(put);
      // autoflush is on by default, or else move this check after flush
      assertTrue (table.getProfilingData () != null);
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
      
      assertTrue (table.getProfilingData () != null);
    }
    LOG.debug("Read and verified from table " + new String(TABLE));
    
    // turn profiling back off and repeat test to make sure
    // profiling data gets cleared
    table.setProfiling(false);
    
    LOG.debug("Testing with profiling off");
    for (int i = 0; i < ROWS.length; i++) {
      Put put = new Put(ROWS[i]);
      put.add(FAMILIES[0], QUALIFIER, VALUE);
      table.put(put);
      // autoflush is on by default, or else move this check after flush
      assertTrue (table.getProfilingData () == null);
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
      
      assertTrue (table.getProfilingData () == null);
    }
    LOG.debug("Read and verified from table " + new String(TABLE));
  }
}
