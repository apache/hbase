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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.function.Function;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestRowCacheCanCacheRow {
  private static final byte[] CF1 = "cf1".getBytes();
  private static final byte[] CF2 = "cf2".getBytes();
  private static final byte[] ROW_KEY = "row".getBytes();
  private static final TableName TABLE_NAME = TableName.valueOf("test");

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRowCacheCanCacheRow.class);

  @Test
  public void testRowCacheEnabled() {
    Region region = Mockito.mock(Region.class);
    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(CF1).build();
    TableDescriptor td;

    Get get = new Get(ROW_KEY);
    get.addFamily(CF1);

    td = TableDescriptorBuilder.newBuilder(TABLE_NAME).setRowCacheEnabled(true).setColumnFamily(cfd)
      .build();
    Mockito.when(region.getTableDescriptor()).thenReturn(td);
    Assert.assertTrue(RowCacheService.canCacheRow(get, region));

    // Disable row cache, expect false
    td = TableDescriptorBuilder.newBuilder(TABLE_NAME).setColumnFamily(cfd)
      .setRowCacheEnabled(false).build();
    Mockito.when(region.getTableDescriptor()).thenReturn(td);
    Assert.assertFalse(RowCacheService.canCacheRow(get, region));
  }

  @Test
  public void testRetrieveAllCells() {
    Region region = Mockito.mock(Region.class);
    ColumnFamilyDescriptor cfd1 = ColumnFamilyDescriptorBuilder.newBuilder(CF1).build();
    ColumnFamilyDescriptor cfd2 = ColumnFamilyDescriptorBuilder.newBuilder(CF2).build();
    TableDescriptor td = TableDescriptorBuilder.newBuilder(TABLE_NAME).setRowCacheEnabled(true)
      .setColumnFamily(cfd1).setColumnFamily(cfd2).build();
    Mockito.when(region.getTableDescriptor()).thenReturn(td);

    // Not all CFs, expect false
    Get get = new Get(ROW_KEY);
    get.addFamily(CF1);
    Assert.assertFalse(RowCacheService.canCacheRow(get, region));

    // All CFs, expect true
    get.addFamily(CF2);
    Assert.assertTrue(RowCacheService.canCacheRow(get, region));

    // Not all qualifiers, expect false
    get.addColumn(CF1, "q1".getBytes());
    Assert.assertFalse(RowCacheService.canCacheRow(get, region));
  }

  @Test
  public void testTtl() {
    ColumnFamilyDescriptor cfd1;
    ColumnFamilyDescriptor cfd2;
    TableDescriptor td;
    Region region = Mockito.mock(Region.class);
    Get get = new Get(ROW_KEY);
    get.addFamily(CF1);
    get.addFamily(CF2);

    // Ttl is set, expect false
    cfd1 = ColumnFamilyDescriptorBuilder.newBuilder(CF1).setTimeToLive(1).build();
    cfd2 = ColumnFamilyDescriptorBuilder.newBuilder(CF2).build();
    td = TableDescriptorBuilder.newBuilder(TABLE_NAME).setRowCacheEnabled(true)
      .setColumnFamily(cfd1).setColumnFamily(cfd2).build();
    Mockito.when(region.getTableDescriptor()).thenReturn(td);
    Assert.assertFalse(RowCacheService.canCacheRow(get, region));

    // Ttl is not set, expect true
    cfd1 = ColumnFamilyDescriptorBuilder.newBuilder(CF1).build();
    td = TableDescriptorBuilder.newBuilder(TABLE_NAME).setRowCacheEnabled(true)
      .setColumnFamily(cfd1).setColumnFamily(cfd2).build();
    Mockito.when(region.getTableDescriptor()).thenReturn(td);
    Assert.assertTrue(RowCacheService.canCacheRow(get, region));
  }

  @Test
  public void testFilter() {
    testWith(
      get -> get.setFilter(new RowFilter(CompareOperator.EQUAL, new BinaryComparator(ROW_KEY))));
  }

  @Test
  public void testCacheBlock() {
    testWith(get -> get.setCacheBlocks(false));
  }

  @Test
  public void testAttribute() {
    testWith(get -> get.setAttribute("test", "value".getBytes()));
  }

  @Test
  public void testCheckExistenceOnly() {
    testWith(get -> get.setCheckExistenceOnly(true));
  }

  @Test
  public void testColumnFamilyTimeRange() {
    testWith(get -> get.setColumnFamilyTimeRange(CF1, 1000, 2000));
  }

  @Test
  public void testConsistency() {
    testWith(get -> get.setConsistency(Consistency.TIMELINE));
  }

  @Test
  public void testAuthorizations() {
    testWith(get -> get.setAuthorizations(new Authorizations("foo")));
  }

  @Test
  public void testId() {
    testWith(get -> get.setId("test"));
  }

  @Test
  public void testIsolationLevel() {
    testWith(get -> get.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED));
  }

  @Test
  public void testMaxResultsPerColumnFamily() {
    testWith(get -> get.setMaxResultsPerColumnFamily(2));
  }

  @Test
  public void testReplicaId() {
    testWith(get -> get.setReplicaId(1));
  }

  @Test
  public void testRowOffsetPerColumnFamily() {
    testWith(get -> get.setRowOffsetPerColumnFamily(1));
  }

  @Test
  public void testTimeRange() {
    testWith(get -> {
      try {
        return get.setTimeRange(1, 2);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void testTimestamp() {
    testWith(get -> get.setTimestamp(1));
  }

  private static void testWith(Function<Get, Get> func) {
    Region region = Mockito.mock(Region.class);
    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(CF1).build();
    TableDescriptor td = TableDescriptorBuilder.newBuilder(TABLE_NAME).setRowCacheEnabled(true)
      .setColumnFamily(cfd).build();
    Mockito.when(region.getTableDescriptor()).thenReturn(td);

    Get get = new Get(ROW_KEY);
    get.addFamily(CF1);
    Assert.assertTrue(RowCacheService.canCacheRow(get, region));

    // expect false
    Get unused = func.apply(get);
    Assert.assertFalse(RowCacheService.canCacheRow(get, region));
  }
}
