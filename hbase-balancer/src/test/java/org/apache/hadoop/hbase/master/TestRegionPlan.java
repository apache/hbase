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
package org.apache.hadoop.hbase.master;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(MasterTests.TAG)
@Tag(SmallTests.TAG)
public class TestRegionPlan {

  private final ServerName SRC = ServerName.valueOf("source", 1234, 2345);
  private final ServerName DEST = ServerName.valueOf("dest", 1234, 2345);

  private String methodName;

  @BeforeEach
  public void setUp(TestInfo testInfo) {
    methodName = testInfo.getTestMethod().get().getName();
  }

  @Test
  public void testCompareTo() {
    RegionInfo hri = RegionInfoBuilder.newBuilder(TableName.valueOf(methodName)).build();
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
    RegionInfo hri = RegionInfoBuilder.newBuilder(TableName.valueOf(methodName)).build();
    RegionPlan a = new RegionPlan(hri, null, null);
    RegionPlan b = new RegionPlan(hri, null, null);
    assertEquals(a, b);
    a = new RegionPlan(hri, SRC, null);
    b = new RegionPlan(hri, null, null);
    assertNotEquals(a, b);
    a = new RegionPlan(hri, SRC, null);
    b = new RegionPlan(hri, SRC, null);
    assertEquals(a, b);
    a = new RegionPlan(hri, SRC, null);
    b = new RegionPlan(hri, SRC, DEST);
    assertNotEquals(a, b);
  }

  @Test
  public void testEquals() {
    RegionInfo hri = RegionInfoBuilder.newBuilder(TableName.valueOf(methodName)).build();

    // Identity equality
    RegionPlan plan = new RegionPlan(hri, SRC, DEST);
    assertEquals(plan.hashCode(), new RegionPlan(hri, SRC, DEST).hashCode());
    assertEquals(plan, new RegionPlan(hri, SRC, DEST));

    // HRI is used for equality
    RegionInfo other =
      RegionInfoBuilder.newBuilder(TableName.valueOf(methodName + "other")).build();
    assertNotEquals(plan.hashCode(), new RegionPlan(other, SRC, DEST).hashCode());
    assertNotEquals(plan, new RegionPlan(other, SRC, DEST));
  }
}
