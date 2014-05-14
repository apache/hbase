package org.apache.hadoop.hbase.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

@Category(MediumTests.class)
public class TestAlterTableLocking {

  private final Log LOG = LogFactory.getLog(TestAlterTableLocking.class);

  private final HBaseTestingUtility testingUtility = new HBaseTestingUtility();

  private static final byte[] TABLENAME = Bytes.toBytes("TestTable");
  private static final byte[] FAMILYNAME = Bytes.toBytes("cf");
  private static final int NUM_REGIONS = 20;
  private static final int NUM_VERSIONS = 1;
  private static final byte[] START_KEY = Bytes.toBytes(0);
  private static final byte[] END_KEY = Bytes.toBytes(200);
  private static final int WAIT_INTERVAL = 1000;
  private static final int MAX_CONCURRENT_REGIONS_CLOSED = 2;
  private static final int WAIT_FOR_TABLE = 30 * 1000;
  private static final Pair<Integer, Integer> ALTER_DONE = Pair.newPair(0,0);

  @Before
  public void setup() throws IOException, InterruptedException {
    //Start mini cluster
    testingUtility.startMiniCluster(1);
    testingUtility.createTable(
        TABLENAME,
        new byte[][]{FAMILYNAME},
        NUM_VERSIONS,
        START_KEY,
        END_KEY,
        NUM_REGIONS
    );
    testingUtility.waitTableAvailable(TABLENAME, WAIT_FOR_TABLE);
  }

  @After
  public void tearDown() throws IOException {
    testingUtility.shutdownMiniCluster();
  }

  @Test
  public void testTryLock() {
    HBaseAdmin admin = null;
    try {
      admin = testingUtility.getHBaseAdmin();
    } catch (MasterNotRunningException e) {
      LOG.error("Master Not running. " + e.getStackTrace());
      Assert.fail("Master for test cluster is not running.");
    }

    try {
      admin.alterTable(TABLENAME,null,null,null, WAIT_INTERVAL, MAX_CONCURRENT_REGIONS_CLOSED);

      boolean threwIOException = false;
      try {
        //Should throw exception if locking is working.
        admin.alterTable(TABLENAME, null, null, null, WAIT_INTERVAL, MAX_CONCURRENT_REGIONS_CLOSED);
      } catch (IOException e) {
        //No simultaneous alterTable allowed.
        threwIOException = true;
      }
      Assert.assertTrue("TableLock Failed to stop a second " +
          "Alter Table op from starting.", threwIOException);

    } catch (MasterNotRunningException e) {
      LOG.error("Master for test cluster is not running. " + e.getStackTrace());
      Assert.fail("Master for test cluster is not running.");
    } catch (IOException e) {
      LOG.error("An exception was thrown while trying to alter a table. " + e.getStackTrace());
      Assert.fail("An exception was encountered while trying to run the Alter Table command.");
    }

    try {
      while (!admin.getAlterStatus(TABLENAME).equals(ALTER_DONE)) {
        try {
          Assert.assertTrue(tableIsLocked(TABLENAME));
          Thread.sleep(WAIT_INTERVAL);
        } catch (InterruptedException e) {
          LOG.error("Interrupted while sleeping after table edit. " + e.getStackTrace());
        }
      }
    } catch (IOException e) {
      LOG.error("An IOException was encountered while waiting for the " +
          "Alter Table command to finish reopening regions." + e.getStackTrace());
      Assert.fail("An IOException was encountered while reopening regions.");
    }

    Assert.assertFalse("The table lock was not released after the " +
        "Alter Table command was finished.", tableIsLocked(TABLENAME));

  }

  private boolean tableIsLocked(byte[] tableName) {
    return testingUtility.getHBaseCluster().getMaster().isTableLocked(tableName);
  }
}
