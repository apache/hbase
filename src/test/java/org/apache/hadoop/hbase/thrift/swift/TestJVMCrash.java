package org.apache.hadoop.hbase.thrift.swift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.Test;

public class TestJVMCrash {
  private HBaseTestingUtility hbaseTestingUtility = new HBaseTestingUtility();
  private final Log LOG = LogFactory.getLog(TestJVMCrash.class);
  @Test
  public void testRepeatedHBaseTestingUtilityRuns() throws Exception {
    for (int i = 0; i < 10; i++) {
      LOG.debug("Iteration : " + i);
      hbaseTestingUtility.startMiniCluster(2);
      hbaseTestingUtility.shutdownMiniCluster();
    }
  }
}
