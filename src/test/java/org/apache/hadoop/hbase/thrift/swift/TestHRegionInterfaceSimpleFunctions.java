package org.apache.hadoop.hbase.thrift.swift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.executor.HBaseEventHandler;
import org.apache.hadoop.hbase.executor.RegionTransitionEventData;
import org.apache.hadoop.hbase.ipc.HBaseRPCOptions;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.ThriftHRegionInterface;
import org.apache.hadoop.hbase.ipc.thrift.HBaseThriftRPC;
import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestHRegionInterfaceSimpleFunctions {
  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final int SLAVES = 1;
  private static final byte[] TABLENAME =
      Bytes.toBytes("testSimpleHRegionInterfaceFunctions");
  private static final byte[] FAMILY = Bytes.toBytes("testFamily");

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(
        HConstants.REGION_SERVER_WRITE_THRIFT_INFO_TO_META, true);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.CLIENT_TO_RS_USE_THRIFT,
        true);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.MASTER_TO_RS_USE_THRIFT,
        true);
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout=180000)
  public void testSimpleHRegionInterfaceFunctions()
      throws IOException, InterruptedException, ExecutionException, TimeoutException, ThriftHBaseException {
    HTable table = TEST_UTIL.createTable(TABLENAME, FAMILY);
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    ThriftHRegionInterface.Async thriftServer = TEST_UTIL.getHBaseCluster().getThriftRegionServer(0);
    TEST_UTIL.loadTable(table, FAMILY);

    HServerInfo info = server.getHServerInfo();
    HRegionInfo regionInfo = null;
    for (HRegion r : server.getOnlineRegions()) {
      if (Bytes.BYTES_COMPARATOR.compare(r.getTableDesc().getName(),
          TABLENAME) == 0) {
        // Since we have only 1 slave here we can afford to do this
        regionInfo = r.getRegionInfo();
        break;
      }
    }

    InetSocketAddress addr =
        new InetSocketAddress(info.getHostname(), server.getThriftServerPort());
    HRegionInterface client = (HRegionInterface) HBaseThriftRPC
        .getClient(addr, TEST_UTIL.getConfiguration(), ThriftHRegionInterface.Async.class, HBaseRPCOptions.DEFAULT);

    // tGetClosestRowBefore
    Result r = null;
    r = client.getClosestRowBefore(regionInfo.getRegionName(),
        Bytes.toBytes("abfd"), FAMILY);
    assertTrue(Bytes.BYTES_COMPARATOR
        .compare(Bytes.toBytes("abf"), r.getValue(FAMILY, null)) == 0);

    // tGetCurrentTimeMillis
    long currentTime = client.getCurrentTimeMillis();
    assertTrue(currentTime <= EnvironmentEdgeManager.currentTimeMillis());

    // tGetLastFlushTime
    long lastFlushTime = client.getLastFlushTime(regionInfo.getRegionName());
    assertTrue(currentTime <= EnvironmentEdgeManager.currentTimeMillis());

    // tFlushRegion
    client.flushRegion(regionInfo.getRegionName());
    long curTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - curTime > (180*1000)) {
      long curFlushTime = client.getLastFlushTime(regionInfo.getRegionName());
      if (curFlushTime > lastFlushTime) {
        lastFlushTime = curFlushTime;
        break;
      }
    }

    // tFlushRegionIfOlderThanTS
    client.flushRegion(regionInfo.getRegionName(),
        EnvironmentEdgeManager.currentTimeMillis());
    curTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - curTime > (180*1000)) {
      long curFlushTime = client.getLastFlushTime(regionInfo.getRegionName());
      if (curFlushTime > lastFlushTime) {
        lastFlushTime = curFlushTime;
        break;
      }
    }

    // tGetLastFlushTimes
    MapWritable flushTimes = client.getLastFlushTimes();
    assertEquals(flushTimes.size(), 3);
    for (Entry<Writable, Writable> entry : flushTimes.entrySet()) {
      if (Bytes.BYTES_COMPARATOR
          .compare(((BytesWritable)entry.getKey()).getBytes(), regionInfo.getRegionName()) == 0) {
        assertTrue(lastFlushTime <= ((LongWritable)entry.getValue()).get());
      }
    }

    // tGetStartCode
    assertEquals(client.getStartCode(), server.getStartCode());

    // tGetStoreFileList
    assertEquals(client.getStoreFileList(regionInfo.getRegionName(), FAMILY),
        server.getStoreFileList(regionInfo.getRegionName(), FAMILY));

    // tGetHLogsList
    assertEquals(client.getHLogsList(false), server.getHLogsList(false));

    // getReginoAssignment
    List<HRegionInfo> infos = thriftServer.getRegionsAssignment().get();
    assertFalse(infos.isEmpty());
  }

  /**
   * Test checkAndPut, checkAndDelete, incrementColumnValue through Thrift interface
   */
  @Test
  public void testAtomicMutation() throws Exception {
    HTable table = TEST_UTIL.createTable(TABLENAME, FAMILY);
    ThriftHRegionInterface.Async thriftServer = TEST_UTIL.getHBaseCluster().getThriftRegionServer(0);

    byte[] row = Bytes.toBytes("test-row");
    byte[] invalidValue = Bytes.toBytes("test-row2");
    HRegionInfo regionInfo = table.getRegionLocation(row).getRegionInfo();
    byte[] regionName = regionInfo.getRegionName();
    Put put = new Put(row);
    put.add(FAMILY, null, row);
    assertTrue(thriftServer.checkAndPut(regionName, row, FAMILY, null, null, put).get());
    assertFalse(thriftServer.checkAndPut(
        regionInfo.getRegionName(), row, FAMILY, null, invalidValue, put).get());
    Delete delete = new Delete(row);
    delete.deleteFamily(FAMILY);
    assertFalse(thriftServer.checkAndDelete(regionName, row, FAMILY, null, invalidValue, delete).get());
    assertTrue(thriftServer.checkAndDelete(regionName, row, FAMILY, null, row, delete).get());

    put = new Put(row);
    put.add(FAMILY, null, Bytes.toBytes(1L));
    thriftServer.put(regionName, put).get();
    long result = thriftServer.incrementColumnValue(regionName, row, FAMILY, null, 100L, false).get();
    assertEquals(101L, result);
  }

  /**
   * Test setNumHDFSQuorumReadThreads, setHDFSQuorumReadTimeoutMillis through Thrift interface
   */
  @Test
  public void testQuorumConfigurationChanges() throws Exception {
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    ThriftHRegionInterface.Async thriftServer = TEST_UTIL.getHBaseCluster().getThriftRegionServer(0);

    int threads = server.getQuorumReadThreadsMax() + 1;
    long timeout = server.getQuorumReadTimeoutMillis() + 1;

    thriftServer.setNumHDFSQuorumReadThreads(threads).get();
    thriftServer.setHDFSQuorumReadTimeoutMillis(timeout).get();

    assertEquals(threads, server.getQuorumReadThreadsMax());
    assertEquals(timeout, server.getQuorumReadTimeoutMillis());
  }

  /**
   * Test closeRegion through Thrift interface.
   */
  @Test
  public void testCloseRegion() throws Exception {
    HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    ThriftHRegionInterface.Async thriftServer = TEST_UTIL.getHBaseCluster().getThriftRegionServer(0);

    HRegion[] region = server.getOnlineRegionsAsArray();
    HRegionInfo regionInfo = region[0].getRegionInfo();

    // Some initializtion relevant to zk.
    ZooKeeperWrapper zkWrapper = server.getZooKeeperWrapper();
    String regionZNode = zkWrapper.getZNode(
        zkWrapper.getRegionInTransitionZNode(), regionInfo.getEncodedName());

    thriftServer.closeRegion(regionInfo, true).get();

    byte[] data = zkWrapper.readZNode(regionZNode, new Stat());
    RegionTransitionEventData rsData = new RegionTransitionEventData();
    Writables.getWritable(data, rsData);

    // Verify region is closed.
    assertNull(server.getOnlineRegion(regionInfo.getRegionName()));
    assertEquals(HBaseEventHandler.HBaseEventType.RS2ZK_REGION_CLOSED, rsData.getHbEvent());
  }

  /**
   * Test stop, getStopReason, updateConfiguration through Thrift interface
   * @throws Exception
   */
  @Test
  public void testStopRegionServer() throws Exception {
    ThriftHRegionInterface.Async thriftServer = TEST_UTIL.getHBaseCluster().getThriftRegionServer(0);
    thriftServer.updateConfiguration().get();

    String why = "test reason";
    thriftServer.stop(why);
    assertEquals(thriftServer.getStopReason().get(), why);
  }
}
