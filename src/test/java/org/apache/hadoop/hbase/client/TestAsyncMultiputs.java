package org.apache.hadoop.hbase.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.StringBytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAsyncMultiputs {
  private static final Log LOG = LogFactory.getLog(TestAsyncMultiputs.class);
  private static final HBaseTestingUtility util = new HBaseTestingUtility();
  private static int NUM_REGIONSERVERS = 1;

  @BeforeClass
  public static void setUp() throws Exception {
    util.startMiniCluster(NUM_REGIONSERVERS);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownMiniCluster();
  }

  /**
   * Test intended to check if the wal gives the control
   * back when writers are calling append.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testMultithreadedCallbackFromHLog() throws IOException, InterruptedException {
    final HLog log = new HLog(util.getConfiguration(),
        new Path("/tmp/testMultithreadedCallback"));
    int numThreads = 100;
    ExecutorService executor = Executors.newCachedThreadPool();
    for (int i = 0; i < numThreads; i++) {
      final int threadId = i;
      executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          HTableDescriptor desc =
              new HTableDescriptor(Bytes.toBytes("table" + threadId));
          HRegionInfo info = new HRegionInfo(desc, new byte[0], new byte[0]);
          WALEdit edits = new WALEdit();
          for (byte[] row : getRandomRows(1, 10)) {
            edits.add(new KeyValue(row));
          }
          try {
            log.append(info, desc.getName(), edits,
                System.currentTimeMillis()).get(1, TimeUnit.MINUTES);
          } catch(Exception e) {
            fail();
          }
          return null;
        }
      });
    }
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.MINUTES);
  }

  @Test
  public void testMultiputSucceeds() throws IOException {
    String tableName = "testMultiputSucceeds";
    byte[] tableNameBytes = Bytes.toBytes(tableName);
    String cf = "cf";
    byte[][] families = new byte[][] { Bytes.toBytes(cf) };
    LOG.debug("Creating the table " + tableName);
    int numRegions = 100;
    HRegionServer server = createTableAndGiveMeTheRegionServer(tableNameBytes,
        families, numRegions);
    MultiPut multiput = getRandomMultiPut(tableNameBytes, 1000, families[0],
        server.getServerInfo().getServerAddress());
    server.multiPut(multiput);
    for (HRegion region : server.getOnlineRegionsAsArray()) {
      server.flushRegion(region.getRegionName());
    }
    for (Entry<byte[], List<Put>> entry : multiput.getPuts().entrySet()) {
      byte[] regionName = entry.getKey();
      for (Put p : entry.getValue()) {
        Get g = new Get(p.getRow());
        Result r = server.get(regionName, g);
        assertNotNull(g);
        assertTrue(Bytes.equals(r.getValue(families[0], null), p.getRow()));
        assertTrue(Bytes.equals(r.getRow(), p.getRow()));
      }
    }
  }

  public HRegionServer createTableAndGiveMeTheRegionServer(
      byte[] tableName, byte[][] families, int numregions) throws IOException {
    LOG.debug("Creating table " + Bytes.toString(tableName) + " with "
        + numregions + " regions");
    util.createTable(tableName, families, 1, Bytes.toBytes("aaa"),
        Bytes.toBytes("zzz"), numregions);
    assertTrue(NUM_REGIONSERVERS == 1);
    HRegionServer rs = util.getRSForFirstRegionInTable(tableName);
    LOG.debug("Found the regionserver. Returning" + rs);
    return rs;
  }

  public static MultiPut getRandomMultiPut(byte[] tableName, int numPuts, byte[] cf,
      HServerAddress addr) throws IOException {
    return getRandomMultiPut(tableName, numPuts, cf, addr, util);
  }

  /**
   * Creates random puts with rows between aaa.. to zzz..
   * @param tableName
   * @param numPuts
   * @param cf
   * @param addr
   * @param util : The hbase testing utility to use.
   * @return
   * @throws IOException
   */
  public static MultiPut getRandomMultiPut(byte[] tableName, int numPuts, byte[] cf,
      HServerAddress addr, HBaseTestingUtility util) throws IOException {
    MultiPut multiput = new MultiPut(addr);
    List<Put> puts = getRandomPuts(numPuts, cf);
    LOG.debug("Creating " + numPuts + " puts to insert into the multiput");
    HConnection conn = util.getConnection();
    for (Put put : puts) {
      byte[] regionName = conn.getRegionLocation(new StringBytes(tableName),
          put.getRow(), false).getRegionInfo().getRegionName();
      multiput.add(regionName, put);
    }
    return multiput;
  }

  /**
   * Creates puts with rows between aaa... and zzz..
   * @param numPuts
   * @param cf
   * @return
   */
  public static List<Put> getRandomPuts(int numPuts, byte[] cf) {
    List<Put> ret = new ArrayList<Put> (numPuts);
    List<byte[]> rows = getRandomRows(numPuts, 3);
    for (byte[] row : rows) {
      Put p = new Put(row);
      p.add(cf, null, row);
      ret.add(p);
    }
    return ret;
  }

  public static List<byte[]> getRandomRows(int numRows, int rowlen) {
    Random rand = new Random();
    List<byte[]> ret = new ArrayList<byte[]>(numRows);
    for (int i = 0; i < numRows; i++) {
      byte[] row = new byte[rowlen];
      for (int j = 0; j < rowlen; j++) {
        row[j] = (byte) ('a' + (byte) rand.nextInt('z' - 'a'));
      }
      ret.add(row);
    }
    return ret;
  }

}
