/*
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.DNS;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestHDFSBlocksDistribution {

  @Test
  public void testAddHostsAndBlockWeight() throws Exception {
    HDFSBlocksDistribution distribution = new HDFSBlocksDistribution();
    distribution.addHostsAndBlockWeight(null, 100);
    assertEquals(0, distribution.getHostAndWeights().size(), "Expecting no hosts weights");
    distribution.addHostsAndBlockWeight(new String[0], 100);
    assertEquals(0, distribution.getHostAndWeights().size(), "Expecting no hosts weights");
    distribution.addHostsAndBlockWeight(new String[] { "test" }, 101);
    assertEquals(1, distribution.getHostAndWeights().size(), "Should be one host");
    distribution.addHostsAndBlockWeight(new String[] { "test" }, 202);
    assertEquals(1, distribution.getHostAndWeights().size(), "Should be one host");
    assertEquals(303, distribution.getHostAndWeights().get("test").getWeight(),
      "test host should have weight 303");
    distribution.addHostsAndBlockWeight(new String[] { "testTwo" }, 222);
    assertEquals(2, distribution.getHostAndWeights().size(), "Should be two hosts");
    assertEquals(525, distribution.getUniqueBlocksTotalWeight(), "Total weight should be 525");
    distribution.addHostsAndBlockWeight(new String[] { "test" }, 100,
      new StorageType[] { StorageType.SSD });
    assertEquals(403, distribution.getHostAndWeights().get("test").getWeight(),
      "test host should have weight 403");
    assertEquals(100, distribution.getHostAndWeights().get("test").getWeightForSsd(),
      "test host should have weight for ssd 100");
  }

  private static final class MockHDFSBlocksDistribution extends HDFSBlocksDistribution {

    @Override
    public Map<String, HostAndWeight> getHostAndWeights() {
      HashMap<String, HostAndWeight> map = new HashMap<>();
      map.put("test", new HostAndWeight(null, 100, 0));
      return map;
    }
  }

  @Test
  public void testAdd() throws Exception {
    HDFSBlocksDistribution distribution = new HDFSBlocksDistribution();
    distribution.add(new MockHDFSBlocksDistribution());
    assertEquals(0, distribution.getHostAndWeights().size(), "Expecting no hosts weights");
    distribution.addHostsAndBlockWeight(new String[] { "test" }, 10);
    assertEquals(1, distribution.getHostAndWeights().size(), "Should be one host");
    distribution.add(new MockHDFSBlocksDistribution());
    assertEquals(1, distribution.getHostAndWeights().size(), "Should be one host");
    assertEquals(10, distribution.getUniqueBlocksTotalWeight(), "Total weight should be 10");
  }

  @Test
  public void testLocalHostCompatibility() throws Exception {
    String currentHost = DNS.getDefaultHost("default", "default");
    HDFSBlocksDistribution distribution = new HDFSBlocksDistribution();
    assertEquals(0.0, distribution.getBlockLocalityIndex(currentHost), 0.01,
      "Locality should be 0.0");
    distribution.addHostsAndBlockWeight(new String[] { "localhost" }, 10);
    assertEquals(1, distribution.getHostAndWeights().size(), "Should be one host");
    assertEquals(0.0, distribution.getBlockLocalityIndex("test"), 0.01, "Locality should be 0.0");
    assertNotEquals(0.0, distribution.getBlockLocalityIndex(currentHost), 0.01,
      "Locality should be 0.0");
  }

}
