package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHRegionServerFileSystemFailure {
  private static final Log LOG = LogFactory
      .getLog(TestHRegionServerFileSystemFailure.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte[][] FAMILIES = { Bytes.toBytes("f1"),
      Bytes.toBytes("f2"), Bytes.toBytes("f3"), Bytes.toBytes("f4") };
  private static final int nLoaders = 10;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private static class TableLoader extends Thread {
    private final HTable table;

    public TableLoader(HTable table) {
      this.table = table;
    }

    @Override
    public void run() {
      while (true) {
        try {
          for (int i = 0; i < FAMILIES.length; i++) {
            byte[] columnFamily = FAMILIES[i];
            TEST_UTIL.loadTable(table, columnFamily);
          }
        } catch (IOException e) {
          LOG.warn(e);
          break;
        }
      }
    }
  }

  @Test
  public void testHRegionServerFileSystemFailure() throws Exception {
    // Build some data.
    byte[] tableName = Bytes.toBytes("testCloseHRegion");
    TEST_UTIL.createTable(tableName, FAMILIES);
    HTable table = new HTable(tableName);
    for (int i = 0; i < FAMILIES.length; i++) {
      byte[] columnFamily = FAMILIES[i];
      TEST_UTIL.createMultiRegions(table, columnFamily);
    }

    for (int i = 0; i < nLoaders; i++) {
      new TableLoader(table).start();
    }

    // Wait for loaders to build up some data.
    Thread.sleep(10000);

    // Pick a regionserver.
    Configuration conf = TEST_UTIL.getConfiguration();
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);

    // Bring down HDFS.
    TEST_UTIL.shutdownMiniDFSCluster();

    // Verify checkFileSystem returns false and doesn't throw Exceptions.
    assertFalse(server.checkFileSystem());
  }
}
