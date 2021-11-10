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
package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, SmallTests.class })
public class TestDoubleArrayCost {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDoubleArrayCost.class);

  @Test
  public void testComputeCost() {
    DoubleArrayCost cost = new DoubleArrayCost();

    cost.prepare(100);
    cost.applyCostsChange(costs -> {
      for (int i = 0; i < 100; i++) {
        costs[i] = 10;
      }
    });
    assertEquals(0, cost.cost(), 0.01);

    cost.prepare(101);
    cost.applyCostsChange(costs -> {
      for (int i = 0; i < 100; i++) {
        costs[i] = 0;
      }
      costs[100] = 100;
    });
    assertEquals(1, cost.cost(), 0.01);

    cost.prepare(200);
    cost.applyCostsChange(costs -> {
      for (int i = 0; i < 100; i++) {
        costs[i] = 0;
        costs[i + 100] = 100;
      }
      costs[100] = 100;
    });
    assertEquals(0.0708, cost.cost(), 0.01);
  }
}
