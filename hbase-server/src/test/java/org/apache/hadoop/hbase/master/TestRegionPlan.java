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
package org.apache.hadoop.hbase.master;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({MasterTests.class, SmallTests.class})
public class TestRegionPlan {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionPlan.class);

  private final ServerName SRC = ServerName.valueOf("source", 1234, 2345);
  private final ServerName DEST = ServerName.valueOf("dest", 1234, 2345);
  @Rule
  public TestName name = new TestName();

  @Test
  public void testCompareTo() {
    RegionInfo hri = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName())).build();
    RegionPlan a = new RegionPlan(hri, null, null);
    RegionPlan b = new RegionPlan(hri, null, null);
    assertEquals(0, a.compareTo(b));
    a = new RegionPlan(hri, SRC, null);
    b = new RegionPlan(hri, null, null);
    assertEquals(1, a.compareTo(b));
    a = new RegionPlan(hri, null, null);
    b = new RegionPlan(hri, SRC, null);
    assertEquals(-1, a.compareTo(b));
    a = new RegionPlan(hri, SRC, null);
    b = new RegionPlan(hri, SRC, null);
    assertEquals(0, a.compareTo(b));
    a = new RegionPlan(hri, SRC, null);
    b = new RegionPlan(hri, SRC, DEST);
    assertEquals(-1, a.compareTo(b));
    a = new RegionPlan(hri, SRC, DEST);
    b = new RegionPlan(hri, SRC, DEST);
    assertEquals(0, a.compareTo(b));
  }

  @Test
  public void testEqualsWithNulls() {
    RegionInfo hri = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName())).build();
    RegionPlan a = new RegionPlan(hri, null, null);
    RegionPlan b = new RegionPlan(hri, null, null);
    assertTrue(a.equals(b));
    a = new RegionPlan(hri, SRC, null);
    b = new RegionPlan(hri, null, null);
    assertFalse(a.equals(b));
    a = new RegionPlan(hri, SRC, null);
    b = new RegionPlan(hri, SRC, null);
    assertTrue(a.equals(b));
    a = new RegionPlan(hri, SRC, null);
    b = new RegionPlan(hri, SRC, DEST);
    assertFalse(a.equals(b));
  }

  @Test
  public void testEquals() {
    RegionInfo hri = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName())).build();

    // Identity equality
    RegionPlan plan = new RegionPlan(hri, SRC, DEST);
    assertEquals(plan.hashCode(), new RegionPlan(hri, SRC, DEST).hashCode());
    assertEquals(plan, new RegionPlan(hri, SRC, DEST));

    // HRI is used for equality
    RegionInfo other =
        RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName() + "other")).build();
    assertNotEquals(plan.hashCode(), new RegionPlan(other, SRC, DEST).hashCode());
    assertNotEquals(plan, new RegionPlan(other, SRC, DEST));
  }
}
