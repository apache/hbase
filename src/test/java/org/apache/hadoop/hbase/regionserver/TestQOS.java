package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.UnstableTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hdfs.util.InjectionEvent;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.InjectionEventI;
import org.apache.hadoop.util.InjectionHandler;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestQOS {
  private static final Log LOG = LogFactory.getLog(TestQOS.class);
  protected static HBaseTestingUtility TEST_UTIL;
  private static volatile int classOfServiceR;
  private static volatile int classOfServiceW;
  private static volatile int priorityW;
  private static volatile int priorityR;
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(HConstants.HBASE_ENABLE_QOS_KEY, true);
    TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster(1);
    InjectionHandler.set(new TestQOSHandler());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public static class TestQOSHandler extends
      InjectionHandler {
    @Override
    protected void _processEvent(InjectionEventI event, Object... args) {
      if (event == InjectionEvent.DATANODE_WRITE_BLOCK
          || event == InjectionEvent.DATANODE_READ_BLOCK) {
        if (NativeCodeLoader.isNativeCodeLoaded()) {
          try {
            int value = NativeIO.ioprioGetIfPossible();
            // See ioprio.h for explanation on the following lines
            if (event ==  InjectionEvent.DATANODE_READ_BLOCK) {
              priorityR = value & ((1 << 13) - 1);
              classOfServiceR = value >> 13;
            } else {
              priorityW = value & ((1 << 13) - 1);
              classOfServiceW = value >> 13;
            }
          } catch (Exception e) {

          }
        }
      }
    }
  }

  // Marked as unstable and recorded in #4053465
  @Category(UnstableTests.class)
  @Test
  public void testBasic() throws Exception {
    byte[] family = "family".getBytes();
    HTable table = TEST_UTIL.createTable("test".getBytes(), family);
    TEST_UTIL.loadTable2(table, family);
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    byte[] regionName = null;
    for (HRegion region : server.getOnlineRegions()) {
      if (!new String(region.getTableDesc().getName()).equals(new String(table
          .getTableName()))) {
        LOG.info("Skipping since name is : "
            + new String(region.getTableDesc().getName()));
        continue;
      }
      Store store = region.getStore(family);
      region.flushcache();
      regionName = region.getRegionName();
      if (NativeCodeLoader.isNativeCodeLoaded()) {
        LOG.info("Verifying priorities");
        assertEquals(HConstants.IOPRIO_CLASSOF_SERVICE, classOfServiceW);
        assertEquals(HConstants.FLUSH_PRIORITY, priorityW);
      }
      store.compactRecentForTesting(-1);
      if (NativeCodeLoader.isNativeCodeLoaded()) {
        assertEquals(HConstants.IOPRIO_CLASSOF_SERVICE, classOfServiceW);
        assertEquals(HConstants.COMPACT_PRIORITY, priorityW);
      }
    }

    // Roll the log so that we start a new block for the HLog and
    // capture the IOPRIO injection.
    server.getLog(0).rollWriter();
    Put p = new Put("row".getBytes());
    p.add("family".getBytes(), "qualifier".getBytes(), "value".getBytes());
    table.put(p);
    table.flushCommits();
    LOG.info("DONE WAL WRITE");
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      assertEquals(HConstants.IOPRIO_CLASSOF_SERVICE, classOfServiceW);
      assertEquals(HConstants.HLOG_PRIORITY, priorityW);
    }
    // Flush the region so that the read hits the datanode.
    server.flushRegion(regionName);
    LOG.info("FINISHED FLUSH");
    Get g = new Get("row".getBytes());
    g.addColumn("family".getBytes(), "qualifier".getBytes());
    Result r = table.get(g);
    LOG.info("FINISHED GET");
    assertTrue(Arrays.equals("value".getBytes(),
        r.getFamilyMap("family".getBytes()).get("qualifier".getBytes())));
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      assertEquals(HConstants.IOPRIO_CLASSOF_SERVICE, classOfServiceR);
      assertEquals(HConstants.PREAD_PRIORITY, priorityR);
    }
  }
}
