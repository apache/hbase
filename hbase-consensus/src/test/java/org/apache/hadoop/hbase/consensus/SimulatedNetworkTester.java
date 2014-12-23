package org.apache.hadoop.hbase.consensus;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


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
