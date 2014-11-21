package org.apache.hadoop.hbase.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A test utility for Unit Testing RAFT protocol by itself.
 */
//@RunWith(value = Parameterized.class)
public class SimulatedNetworkTester {
  private static final Logger LOG = LoggerFactory.getLogger(
          SimulatedNetworkTester.class);
  private final LocalTestBed testbed;

  @Before
  public void setUpBeforeClass() throws Exception {
    testbed.start();
  }

  @After
  public void tearDownAfterClass() throws Exception {
    testbed.stop();
  }

  /**
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return RaftTestDataProvider.getRaftBasicLogTestData();
  }
  */

  public SimulatedNetworkTester() {
    testbed = new LocalTestBed();
  }

  /**
   * This test function is to test the protocol is able to make progress within a period of time
   * based on the on-disk transaction log
   */
  @Test(timeout=1000000)
  public void testConsensusProtocol() throws Exception {
    testbed.runConsensusProtocol();
  }
}
