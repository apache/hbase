package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.MultiPut;
import org.apache.hadoop.hbase.client.TestAsyncMultiputs;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.TestHLog.FailingSequenceFileLogWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestAsyncMultiputsFailingWal {
  private final Log LOG = LogFactory.getLog(TestAsyncMultiputsFailingWal.class);

  @Test
  public void testFailedMultiput() throws Exception {
    String tableName = "testFailedMultiput";
    byte[] tableNameBytes = Bytes.toBytes(tableName);
    String cf = "cf";
    byte[][] families = new byte[][] { Bytes.toBytes(cf) };
    LOG.debug("Creating the table " + tableName);
    int numRegions = 3;
    HBaseTestingUtility localutil = new HBaseTestingUtility();
    localutil.startMiniCluster();

    LOG.debug("Creating table " + tableName + " with "
        + numRegions + " regions");
    localutil.createTable(tableNameBytes, families, 1, Bytes.toBytes("aaa"),
        Bytes.toBytes("zzz"), numRegions);
    HRegionServer rs = localutil.getRSForFirstRegionInTable(tableNameBytes);
    LOG.debug("Found the regionserver. Returning" + rs);

    HLog.logWriterClass = null;
    assertTrue(rs.getLogCount() == 1);
    HLog log = rs.getLog(0);
    log.getConfiguration().set("hbase.regionserver.hlog.writer.impl",
            FailingSequenceFileLogWriter.class.getName());
    log.rollWriter();
    MultiPut multiput = TestAsyncMultiputs.getRandomMultiPut(
        tableNameBytes, 1000, families[0],
        rs.getServerInfo().getServerAddress(), localutil);

    try {
      FailingSequenceFileLogWriter.setFailureMode(true);
      rs.multiPut(multiput);
      FailingSequenceFileLogWriter.setFailureMode(false);
    } catch (Exception e) {
      LOG.debug("Excepted to see an exception", e);
      return;
    }
    fail();
  }


}
