package org.apache.hadoop.hbase.consensus.metrics;

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
