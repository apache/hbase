/**
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
package org.apache.hadoop.hbase.mapreduce;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({MapReduceTests.class, LargeTests.class})
public class TestHBaseMRTestingUtility {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseMRTestingUtility.class);

  @Test
  public void testMRYarnConfigsPopulation() throws IOException {
    Map<String, String> dummyProps = new HashMap<>();
    dummyProps.put("mapreduce.jobtracker.address", "dummyhost:11234");
    dummyProps.put("yarn.resourcemanager.address", "dummyhost:11235");
    dummyProps.put("mapreduce.jobhistory.address", "dummyhost:11236");
    dummyProps.put("yarn.resourcemanager.scheduler.address", "dummyhost:11237");
    dummyProps.put("mapreduce.jobhistory.webapp.address", "dummyhost:11238");
    dummyProps.put("yarn.resourcemanager.webapp.address", "dummyhost:11239");

    HBaseTestingUtility hbt = new HBaseTestingUtility();

    // populate the mr props to the Configuration instance
    for (Map.Entry<String, String> entry : dummyProps.entrySet()) {
      hbt.getConfiguration().set(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String,String> entry : dummyProps.entrySet()) {
      assertTrue("The Configuration for key " + entry.getKey() +" and value: " + entry.getValue() +
          " is not populated correctly", hbt.getConfiguration().get(entry.getKey()).equals(entry.getValue()));
    }

    hbt.startMiniMapReduceCluster();

    // Confirm that MiniMapReduceCluster overwrites the mr properties and updates the Configuration
    for (Map.Entry<String,String> entry : dummyProps.entrySet()) {
      assertFalse("The MR prop: " + entry.getValue() + " is not overwritten when map reduce mini"+
          "cluster is started", hbt.getConfiguration().get(entry.getKey()).equals(entry.getValue()));
    }

    hbt.shutdownMiniMapReduceCluster();
  }
}
