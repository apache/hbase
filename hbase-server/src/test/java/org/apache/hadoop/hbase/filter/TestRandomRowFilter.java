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
package org.apache.hadoop.hbase.filter;

import static org.junit.Assert.*;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({FilterTests.class, SmallTests.class})
public class TestRandomRowFilter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRandomRowFilter.class);

  protected RandomRowFilter quarterChanceFilter;

  @Before
  public void setUp() throws Exception {
    quarterChanceFilter = new RandomRowFilter(0.25f);
  }

  /**
   * Tests basics
   *
   * @throws Exception
   */
  @Test
  public void testBasics() throws Exception {
    int included = 0;
    int max = 1000000;
    for (int i = 0; i < max; i++) {
      if (!quarterChanceFilter.filterRowKey(KeyValueUtil.createFirstOnRow(Bytes.toBytes("row")))) {
        included++;
      }
    }
    // Now let's check if the filter included the right number of rows;
    // since we're dealing with randomness, we must have a include an epsilon
    // tolerance.
    int epsilon = max / 100;
    assertTrue("Roughly 25% should pass the filter", Math.abs(included - max
        / 4) < epsilon);
  }

  /**
   * Tests serialization
   *
   * @throws Exception
   */
  @Test
  public void testSerialization() throws Exception {
    RandomRowFilter newFilter = serializationTest(quarterChanceFilter);
    // use epsilon float comparison
    assertTrue("float should be equal", Math.abs(newFilter.getChance()
        - quarterChanceFilter.getChance()) < 0.000001f);
  }

  private RandomRowFilter serializationTest(RandomRowFilter filter)
      throws Exception {
    // Decompose filter to bytes.
    byte[] buffer = filter.toByteArray();

    // Recompose filter.
    RandomRowFilter newFilter = RandomRowFilter.parseFrom(buffer);

    return newFilter;
  }

}

