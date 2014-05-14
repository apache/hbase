package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(MediumTests.class)
public class TestMultipleHLogs {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static int USER_REGION_NUM = 3;
  private final static int TOTAL_REGION_NUM = USER_REGION_NUM + 2;
  private static Configuration conf;
  private static FileSystem fs;
  private static Path dir;
  private static MiniDFSCluster cluster;

  private static Path hbaseDir;
  private static Path oldLogDir;
  
  @Before
  public void setUp() throws Exception {}
  
  @After
  public void tearDown() throws Exception {}
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // quicker heartbeat interval for faster DN death notification
    TEST_UTIL.getConfiguration().setInt("heartbeat.recheck.interval", 5000);
    TEST_UTIL.getConfiguration().setInt("dfs.heartbeat.interval", 1);
    TEST_UTIL.getConfiguration().setInt("dfs.socket.timeout", 5000);
    // faster failover with cluster.shutdown();fs.close() idiom
    TEST_UTIL.getConfiguration().setInt("ipc.client.connect.max.retries", 1);
    TEST_UTIL.getConfiguration().setInt("dfs.client.block.recovery.retries", 1);
    
    TEST_UTIL.getConfiguration().setInt(HConstants.HLOG_CNT_PER_SERVER, TOTAL_REGION_NUM);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.HLOG_FORMAT_BACKWARD_COMPATIBILITY, false);
    TEST_UTIL.startMiniCluster(1);
  }
  
  @Test
  public void testMultipleHLogs() throws IOException, InterruptedException {
    final byte[] CF = Bytes.toBytes("cf");
    final byte[] QAULIFIER = Bytes.toBytes("qaulifier");
    final byte[] VALUE = Bytes.toBytes("VALUE");
    final int actualStartKey = 0;
    final int actualEndKey = Integer.MAX_VALUE;
    final int keysPerRegion = (actualEndKey - actualStartKey) / USER_REGION_NUM;
    final int splitStartKey = actualStartKey + keysPerRegion;
    final int splitEndKey = actualEndKey - keysPerRegion;
    final String keyFormat = "%08x";
    final HTable table = TEST_UTIL.createTable(Bytes.toBytes("testMultipleHLogs"),
        new byte[][]{CF},
        1,
        Bytes.toBytes(String.format(keyFormat, splitStartKey)),
        Bytes.toBytes(String.format(keyFormat, splitEndKey)),
        USER_REGION_NUM);
    // Put some data for each Region
    for (byte[] row : table.getStartKeys()) {
      Put p = new Put(row);
      p.add(CF, QAULIFIER, VALUE);
      table.put(p);
      table.flushCommits();
    }
    HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    
    final Path logDir = new Path(FSUtils.getRootDir(TEST_UTIL.getConfiguration()),
        HLog.getHLogDirectoryName(regionServer.getServerInfo().getServerName()));
    FileStatus[] files = TEST_UTIL.getDFSCluster().getFileSystem().listStatus(logDir);
    assertEquals(TOTAL_REGION_NUM, files.length);
    assertEquals(TOTAL_REGION_NUM, regionServer.getTotalHLogCnt());
    assertEquals(TOTAL_REGION_NUM, regionServer.getOnlineRegions().size());
    
    for (HRegion region : regionServer.getOnlineRegions()) {
      HLog hlog = region.getLog();
      hlog.rollWriter();
      assertEquals(1, hlog.getNumLogFiles());
    }
    
    for (FileStatus fileStatus : files) {
      assertTrue(HLog.validateHLogFilename(fileStatus.getPath().getName()));
    }
  }
}
