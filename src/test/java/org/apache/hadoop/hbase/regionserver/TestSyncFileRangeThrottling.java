package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.util.InjectionEventCore;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.nativeio.NativeIO;

import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestSyncFileRangeThrottling {
  private static final Log LOG = LogFactory
      .getLog(TestSyncFileRangeThrottling.class);
  protected static HBaseTestingUtility TEST_UTIL;
  private static volatile boolean syncFileRangeInvoked = false;
  private static volatile int flags = 0;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(HConstants.HBASE_ENABLE_SYNCFILERANGE_THROTTLING_KEY, true);
    TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster(1);
    InjectionHandler.set(new TestSyncFileRangeThrottlingHandler());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public static class TestSyncFileRangeThrottlingHandler extends
      InjectionHandler {
    protected void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEventCore.NATIVEIO_SYNC_FILE_RANGE) {
        flags = (Integer) args[0];
        syncFileRangeInvoked = true;
      }
    }
  }

  @Test
  public void testBasic() throws Exception {
    byte[] family = "family".getBytes();
    HTable table = TEST_UTIL
        .createTable("test".getBytes(), family);
    TEST_UTIL.loadTable2(table, family);
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    for (HRegion region : server.getOnlineRegions()) {
      if (!new String(region.getTableDesc().getName()).equals(new String(
          table.getTableName()))) {
        LOG.info("Skipping since name is : "
            + new String(region.getTableDesc().getName()));
        continue;
      }
      Store store = region.getStore(family);
      region.flushcache();
      store.compactRecentForTesting(-1);
    }
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < 30000 && !syncFileRangeInvoked) {
      Thread.sleep(1000);
    }
    assertTrue(syncFileRangeInvoked);
    int expectedFlags = NativeIO.SYNC_FILE_RANGE_WAIT_AFTER
        | NativeIO.SYNC_FILE_RANGE_WRITE;
    assertEquals(expectedFlags, flags);
  }

}
