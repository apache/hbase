package org.apache.hadoop.hbase.consensus.log;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class TestTransactionLogCreator {

  @Test
  public void testNewLogfileCreation() throws Exception {
    String logDir = "/tmp/testNewLogfileCreation";
    File logDirFile = new File(logDir);
    if (logDirFile.exists()) {
      FileUtils.deleteDirectory(logDirFile);
    }
    logDirFile.mkdir();
    Configuration conf = new Configuration();
    conf.set(HConstants.RAFT_TRANSACTION_LOG_DIRECTORY_KEY, logDir);
    TransactionLogManager logManager =
        new TransactionLogManager(conf, "test", HConstants.UNDEFINED_TERM_INDEX);
    logManager.initialize(null);
    // Wait for new logs to be created
    Thread.sleep(1000);
    File currentLogDir = new File(logDir + "/test/current/");
    File[] files = currentLogDir.listFiles();
    int expected = HConstants.RAFT_MAX_NUM_NEW_LOGS;
    assertEquals("# of new log files", expected, files.length);

    logManager.forceRollLog();
    Thread.sleep(1000);
    files = currentLogDir.listFiles();
    expected++;
    assertEquals("# of new log files", expected, files.length);
  }
}
