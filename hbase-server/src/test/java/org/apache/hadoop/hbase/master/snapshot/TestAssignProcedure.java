/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.master.snapshot;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.assignment.AssignProcedure;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

import static junit.framework.TestCase.assertTrue;


@Category({RegionServerTests.class, SmallTests.class})
public class TestAssignProcedure {
  @Rule public TestName name = new TestName();
  @Rule public final TestRule timeout = CategoryBasedTimeout.builder().
      withTimeout(this.getClass()).
      withLookingForStuckThread(true).
      build();

  @Test
  public void testSimpleComparator() {
    List<AssignProcedure> procedures = new ArrayList<AssignProcedure>();
    RegionInfo user1 = RegionInfoBuilder.newBuilder(TableName.valueOf("user_space1")).build();
    procedures.add(new AssignProcedure(user1));
    RegionInfo user2 = RegionInfoBuilder.newBuilder(TableName.valueOf("user_space2")).build();
    procedures.add(new AssignProcedure(RegionInfoBuilder.FIRST_META_REGIONINFO));
    procedures.add(new AssignProcedure(user2));
    RegionInfo system = RegionInfoBuilder.newBuilder(TableName.NAMESPACE_TABLE_NAME).build();
    procedures.add(new AssignProcedure(system));
    procedures.sort(AssignProcedure.COMPARATOR);
    assertTrue(procedures.get(0).isMeta());
    assertTrue(procedures.get(1).getRegionInfo().getTable().equals(TableName.NAMESPACE_TABLE_NAME));
  }

  @Test
  public void testComparatorWithMetas() {
    List<AssignProcedure> procedures = new ArrayList<AssignProcedure>();
    RegionInfo user1 = RegionInfoBuilder.newBuilder(TableName.valueOf("user_space1")).build();
    procedures.add(new AssignProcedure(user1));
    RegionInfo meta2 = RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).
        setStartKey(Bytes.toBytes("002")).build();
    procedures.add(new AssignProcedure(meta2));
    RegionInfo meta1 = RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).
        setStartKey(Bytes.toBytes("001")).build();
    procedures.add(new AssignProcedure(meta1));
    procedures.add(new AssignProcedure(RegionInfoBuilder.FIRST_META_REGIONINFO));
    RegionInfo meta0 = RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).
        setStartKey(Bytes.toBytes("000")).build();
    procedures.add(new AssignProcedure(meta0));
    RegionInfo user2 = RegionInfoBuilder.newBuilder(TableName.valueOf("user_space2")).build();
    procedures.add(new AssignProcedure(user2));
    RegionInfo system = RegionInfoBuilder.newBuilder(TableName.NAMESPACE_TABLE_NAME).build();
    procedures.add(new AssignProcedure(system));
    procedures.sort(AssignProcedure.COMPARATOR);
    assertTrue(procedures.get(0).getRegionInfo().equals(RegionInfoBuilder.FIRST_META_REGIONINFO));
    assertTrue(procedures.get(1).getRegionInfo().equals(meta0));
    assertTrue(procedures.get(2).getRegionInfo().equals(meta1));
    assertTrue(procedures.get(3).getRegionInfo().equals(meta2));
    assertTrue(procedures.get(4).getRegionInfo().getTable().equals(TableName.NAMESPACE_TABLE_NAME));
  }
}
