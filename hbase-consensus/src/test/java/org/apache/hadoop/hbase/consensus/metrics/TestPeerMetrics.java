package org.apache.hadoop.hbase.consensus.metrics;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestPeerMetrics {
  PeerMetrics metrics;

  @Before
  public void setUp() {
    metrics = new PeerMetrics("TestTable.region", "proc1", "peer1", null);
  }

  @Test
  public void shoudReturnName() {
    String expectedName = String.format("%s:type=%s,name=%s,proc=%s,peer=%s",
            "org.apache.hadoop.hbase.consensus", PeerMetrics.TYPE,
            "TestTable.region", "proc1", "peer1");
    assertEquals(expectedName, metrics.getMBeanName());
  }
}
