package org.apache.hadoop.hbase.ipc;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
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
      assertNull(table.getProfilingData());
    }
    LOG.debug("Wrote some puts to table " + new String(TABLE));

    // flush the table
    table.flushCommits();
    LOG.debug("Flushed table " + new String(TABLE));

    // read back the values
    for (int i = 0; i < ROWS.length; i++) {
      Get get = new Get.Builder(ROWS[i]).addColumn(FAMILIES[0], QUALIFIER)
          .create();
      Result result = table.get(get);
      
      assertEquals(new String(VALUE), 
          new String(result.getValue(FAMILIES[0], QUALIFIER)));
      assertNull(table.getProfilingData());
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
      assertNotNull(table.getProfilingData());
    }
    LOG.debug("Wrote some puts to table " + new String(TABLE));

    // flush the table
    table.flushCommits();
    LOG.debug("Flushed table " + new String(TABLE));

    // read back the values
    for (int i = 0; i < ROWS.length; i++) {
      Get get = new Get.Builder(ROWS[i]).addColumn(FAMILIES[0], QUALIFIER)
          .create();
      Result result = table.get(get);
      
      assertEquals(new String(VALUE), 
          new String(result.getValue(FAMILIES[0], QUALIFIER)));
      assertNotNull(table.getProfilingData());
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
      assertNull(table.getProfilingData());
    }
    LOG.debug("Wrote some puts to table " + new String(TABLE));

    // flush the table
    table.flushCommits();
    LOG.debug("Flushed table " + new String(TABLE));

    // read back the values
    for (int i = 0; i < ROWS.length; i++) {
      Get get = new Get.Builder(ROWS[i]).addColumn(FAMILIES[0], QUALIFIER).create();
      Result result = table.get(get);
      
      assertEquals(new String(VALUE), 
          new String(result.getValue(FAMILIES[0], QUALIFIER)));
      assertNull(table.getProfilingData());
    }
    LOG.debug("Read and verified from table " + new String(TABLE));
  }
  
  @Test
  public void testProfilingData() throws Exception {
    LOG.debug("Testing ProfilingData object");
    
    ProfilingData pData1 = new ProfilingData();
    pData1.addString("testStringKey", "testStringVal");
    pData1.addBoolean("testBooleanKey", false);
    pData1.addInt("testIntKey", 1);
    pData1.addLong("testLongKey", 2);
    pData1.addFloat("testFloatKey", 3);
    assertTrue(pData1.getString("testStringKey").equals("testStringVal"));
    assertTrue(pData1.getBoolean("testBooleanKey") == false);
    assertTrue(pData1.getInt("testIntKey") == 1);
    assertTrue(pData1.getLong("testLongKey") == 2);
    assertTrue(pData1.getFloat("testFloatKey") == 3);
    pData1.incInt("testIntKey", 3);
    pData1.incInt("testIntKey");
    pData1.decInt("testIntKey", 5);
    pData1.decInt("testIntKey");
    pData1.incLong("testLongKey", 3);
    pData1.incLong("testLongKey");
    pData1.decLong("testLongKey", 5);
    pData1.decLong("testLongKey");
    pData1.incFloat("testFloatKey", 3);
    pData1.decFloat("testFloatKey", 5);
    assertTrue(pData1.getInt("testIntKey") == -1);
    assertTrue(pData1.getLong("testLongKey") == 0);
    assertTrue(pData1.getFloat("testFloatKey") == 1);
    
    ProfilingData pData2 = new ProfilingData();
    pData2.addString("testStringKey", "2");
    pData2.addString("testStringKey2", "testStringVal");
    pData2.addBoolean("testBooleanKey", true);
    pData2.addBoolean("testBooleanKey2", false);
    pData2.addInt("testIntKey", 4);
    pData2.addInt("testIntKey2", 1);
    pData2.addLong("testLongKey", 5);
    pData2.addLong("testLongKey2", 2);
    pData2.addFloat("testFloatKey", 6);
    pData2.addFloat("testFloatKey2", 3);
    
    pData1.merge(pData2);
    assertTrue(pData1.getString("testStringKey").equals("testStringVal" + 
          ProfilingData.STRING_MERGE_SEPARATOR + "2"));
    assertTrue(pData1.getBoolean("testBooleanKey") == true);
    assertTrue(pData1.getInt("testIntKey") == 3);
    assertTrue(pData1.getLong("testLongKey") == 5);
    assertTrue(pData1.getFloat("testFloatKey") == 7);
    assertTrue(pData1.getString("testStringKey2").equals("testStringVal"));
    assertTrue(pData1.getBoolean("testBooleanKey2") == false);
    assertTrue(pData1.getInt("testIntKey2") == 1);
    assertTrue(pData1.getLong("testLongKey2") == 2);
    assertTrue(pData1.getFloat("testFloatKey2") == 3);
    
    LOG.debug("Finished testing ProfilingData object");
  }
}
