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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.TableName;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestRegionPlan {
  @Test
  public void test() {
    HRegionInfo hri = new HRegionInfo(TableName.valueOf("table"));
    ServerName source = ServerName.valueOf("source", 1234, 2345);
    ServerName dest = ServerName.valueOf("dest", 1234, 2345);
    
    // Identiy equality
    RegionPlan plan = new RegionPlan(hri, source, dest);
    assertEquals(plan.hashCode(), new RegionPlan(hri, source, dest).hashCode());
    assertEquals(plan, new RegionPlan(hri, source, dest));

    // Source and destination not used for equality
    assertEquals(plan.hashCode(), new RegionPlan(hri, dest, source).hashCode());
    assertEquals(plan, new RegionPlan(hri, dest, source));

    // HRI is used for equality
    HRegionInfo other = new HRegionInfo(TableName.valueOf("other"));
    assertNotEquals(plan.hashCode(), new RegionPlan(other, source, dest).hashCode());
    assertNotEquals(plan, new RegionPlan(other, source, dest));
  }
}
