/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertEquals;

@Category(SmallTests.class)
public class TestHDFSBlocksDistribution {
  @Test
  public void testAddHostsAndBlockWeight() throws Exception {
    HDFSBlocksDistribution distribution = new HDFSBlocksDistribution();
    distribution.addHostsAndBlockWeight(null, 100);
    assertEquals("Expecting no hosts weights", 0, distribution.getHostAndWeights().size());
    distribution.addHostsAndBlockWeight(new String[0], 100);
    assertEquals("Expecting no hosts weights", 0, distribution.getHostAndWeights().size());
    distribution.addHostsAndBlockWeight(new String[] {"test"}, 101);
    assertEquals("Should be one host", 1, distribution.getHostAndWeights().size());
    distribution.addHostsAndBlockWeight(new String[] {"test"}, 202);
    assertEquals("Should be one host", 1, distribution.getHostAndWeights().size());
    assertEquals("test host should have weight 303", 303,
        distribution.getHostAndWeights().get("test").getWeight());
    distribution.addHostsAndBlockWeight(new String[] {"testTwo"}, 222);
    assertEquals("Should be two hosts", 2, distribution.getHostAndWeights().size());
    assertEquals("Total weight should be 525", 525, distribution.getUniqueBlocksTotalWeight());
  }

  public class MockHDFSBlocksDistribution extends HDFSBlocksDistribution {
    public Map<String,HostAndWeight> getHostAndWeights() {
      HashMap<String, HostAndWeight> map = new HashMap<String, HostAndWeight>();
      map.put("test", new HostAndWeight(null, 100));
      return map;
    }

  }

  @Test
  public void testAdd() throws Exception {
    HDFSBlocksDistribution distribution = new HDFSBlocksDistribution();
    distribution.add(new MockHDFSBlocksDistribution());
    assertEquals("Expecting no hosts weights", 0, distribution.getHostAndWeights().size());
    distribution.addHostsAndBlockWeight(new String[]{"test"}, 10);
    assertEquals("Should be one host", 1, distribution.getHostAndWeights().size());
    distribution.add(new MockHDFSBlocksDistribution());
    assertEquals("Should be one host", 1, distribution.getHostAndWeights().size());
    assertEquals("Total weight should be 10", 10, distribution.getUniqueBlocksTotalWeight());
  }
}
