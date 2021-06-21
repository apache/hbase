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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagBuilderFactory;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TestFromClientSide;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test coprocessor methods
 * {@link RegionObserver#postIncrementBeforeWAL(ObserverContext, Mutation, List)} and
 * {@link RegionObserver#postAppendBeforeWAL(ObserverContext, Mutation, List)}. These methods may
 * change the cells which will be applied to memstore and WAL. So add unit test for the case which
 * change the cell's column family and tags.
 */
@Category({CoprocessorTests.class, MediumTests.class})
public class TestPostIncrementAndAppendBeforeWAL {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestPostIncrementAndAppendBeforeWAL.class);

  @Rule
  public TestName name = new TestName();

  private static final Logger LOG = LoggerFactory.getLogger(TestFromClientSide.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static Connection connection;

  private static final byte [] ROW = Bytes.toBytes("row");
  private static final String CF1 = "cf1";
  private static final byte[] CF1_BYTES = Bytes.toBytes(CF1);
  private static final String CF2 = "cf2";
  private static final byte[] CF2_BYTES = Bytes.toBytes(CF2);
  private static final String CF_NOT_EXIST = "cf_not_exist";
  private static final byte[] CF_NOT_EXIST_BYTES = Bytes.toBytes(CF_NOT_EXIST);
  private static final byte[] CQ1 = Bytes.toBytes("cq1");
  private static final byte[] CQ2 = Bytes.toBytes("cq2");
  private static final byte[] VALUE = Bytes.toBytes("value");
  private static final byte[] VALUE2 = Bytes.toBytes("valuevalue");
  private static final String USER = "User";
  private static final Permission PERMS =
    Permission.newBuilder().withActions(Permission.Action.READ).build();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    UTIL.startMiniCluster();
    connection = UTIL.getConnection();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    connection.close();
    UTIL.shutdownMiniCluster();
  }

  private void createTableWithCoprocessor(TableName tableName, String coprocessor)
      throws IOException {
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF1_BYTES).build())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF2_BYTES).build())
        .setCoprocessor(coprocessor).build();
    connection.getAdmin().createTable(tableDesc);
  }

  @Test
  public void testChangeCellWithDifferntColumnFamily() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    createTableWithCoprocessor(tableName,
      ChangeCellWithDifferntColumnFamilyObserver.class.getName());

    try (Table table = connection.getTable(tableName)) {
      Increment increment = new Increment(ROW).addColumn(CF1_BYTES, CQ1, 1);
      table.increment(increment);
      Get get = new Get(ROW).addColumn(CF2_BYTES, CQ1);
      Result result = table.get(get);
      assertEquals(1, result.size());
      assertEquals(1, Bytes.toLong(result.getValue(CF2_BYTES, CQ1)));

      Append append = new Append(ROW).addColumn(CF1_BYTES, CQ2, VALUE);
      table.append(append);
      get = new Get(ROW).addColumn(CF2_BYTES, CQ2);
      result = table.get(get);
      assertEquals(1, result.size());
      assertTrue(Bytes.equals(VALUE, result.getValue(CF2_BYTES, CQ2)));
    }
  }

  @Test
  public void testChangeCellWithNotExistColumnFamily() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    createTableWithCoprocessor(tableName,
      ChangeCellWithNotExistColumnFamilyObserver.class.getName());

    try (Table table = connection.getTable(tableName)) {
      try {
        Increment increment = new Increment(ROW).addColumn(CF1_BYTES, CQ1, 1);
        table.increment(increment);
        fail("should throw NoSuchColumnFamilyException");
      } catch (Exception e) {
        assertTrue(e instanceof NoSuchColumnFamilyException);
      }
      try {
        Append append = new Append(ROW).addColumn(CF1_BYTES, CQ2, VALUE);
        table.append(append);
        fail("should throw NoSuchColumnFamilyException");
      } catch (Exception e) {
        assertTrue(e instanceof NoSuchColumnFamilyException);
      }
    }
  }

  @Test
  public void testIncrementTTLWithACLTag() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    createTableWithCoprocessor(tableName, ChangeCellWithACLTagObserver.class.getName());
    try (Table table = connection.getTable(tableName)) {
      // Increment without TTL
      Increment firstIncrement = new Increment(ROW).addColumn(CF1_BYTES, CQ1, 1)
        .setACL(USER, PERMS);
      Result result = table.increment(firstIncrement);
      assertEquals(1, result.size());
      assertEquals(1, Bytes.toLong(result.getValue(CF1_BYTES, CQ1)));

      // Check if the new cell can be read
      Get get = new Get(ROW).addColumn(CF1_BYTES, CQ1);
      result = table.get(get);
      assertEquals(1, result.size());
      assertEquals(1, Bytes.toLong(result.getValue(CF1_BYTES, CQ1)));

      // Increment with TTL
      Increment secondIncrement = new Increment(ROW).addColumn(CF1_BYTES, CQ1, 1).setTTL(1000)
        .setACL(USER, PERMS);
      result = table.increment(secondIncrement);

      // We should get value 2 here
      assertEquals(1, result.size());
      assertEquals(2, Bytes.toLong(result.getValue(CF1_BYTES, CQ1)));

      // Wait 4s to let the second increment expire
      Thread.sleep(4000);
      get = new Get(ROW).addColumn(CF1_BYTES, CQ1);
      result = table.get(get);

      // The value should revert to 1
      assertEquals(1, result.size());
      assertEquals(1, Bytes.toLong(result.getValue(CF1_BYTES, CQ1)));
    }
  }

  @Test
  public void testAppendTTLWithACLTag() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    createTableWithCoprocessor(tableName, ChangeCellWithACLTagObserver.class.getName());
    try (Table table = connection.getTable(tableName)) {
      // Append without TTL
      Append firstAppend = new Append(ROW).addColumn(CF1_BYTES, CQ2, VALUE).setACL(USER, PERMS);
      Result result = table.append(firstAppend);
      assertEquals(1, result.size());
      assertTrue(Bytes.equals(VALUE, result.getValue(CF1_BYTES, CQ2)));

      // Check if the new cell can be read
      Get get = new Get(ROW).addColumn(CF1_BYTES, CQ2);
      result = table.get(get);
      assertEquals(1, result.size());
      assertTrue(Bytes.equals(VALUE, result.getValue(CF1_BYTES, CQ2)));

      // Append with TTL
      Append secondAppend = new Append(ROW).addColumn(CF1_BYTES, CQ2, VALUE).setTTL(1000)
        .setACL(USER, PERMS);
      result = table.append(secondAppend);

      // We should get "valuevalue""
      assertEquals(1, result.size());
      assertTrue(Bytes.equals(VALUE2, result.getValue(CF1_BYTES, CQ2)));

      // Wait 4s to let the second append expire
      Thread.sleep(4000);
      get = new Get(ROW).addColumn(CF1_BYTES, CQ2);
      result = table.get(get);

      // The value should revert to "value"
      assertEquals(1, result.size());
      assertTrue(Bytes.equals(VALUE, result.getValue(CF1_BYTES, CQ2)));
    }
  }

  private static boolean checkAclTag(byte[] acl, Cell cell) {
    Iterator<Tag> iter = PrivateCellUtil.tagsIterator(cell);
    while (iter.hasNext()) {
      Tag tag = iter.next();
      if (tag.getType() == TagType.ACL_TAG_TYPE) {
        Tag temp = TagBuilderFactory.create().
          setTagType(TagType.ACL_TAG_TYPE).setTagValue(acl).build();
        return Tag.matchingValue(tag, temp);
      }
    }
    return false;
  }

  public static class ChangeCellWithDifferntColumnFamilyObserver
      implements RegionCoprocessor, RegionObserver {
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public List<Pair<Cell, Cell>> postIncrementBeforeWAL(
        ObserverContext<RegionCoprocessorEnvironment> ctx, Mutation mutation,
        List<Pair<Cell, Cell>> cellPairs) throws IOException {
      return cellPairs.stream()
          .map(
            pair -> new Pair<>(pair.getFirst(), newCellWithDifferentColumnFamily(pair.getSecond())))
          .collect(Collectors.toList());
    }

    private Cell newCellWithDifferentColumnFamily(Cell cell) {
      return ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
          .setRow(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
          .setFamily(CF2_BYTES, 0, CF2_BYTES.length).setQualifier(CellUtil.cloneQualifier(cell))
          .setTimestamp(cell.getTimestamp()).setType(cell.getType().getCode())
          .setValue(CellUtil.cloneValue(cell)).build();
    }

    @Override
    public List<Pair<Cell, Cell>> postAppendBeforeWAL(
        ObserverContext<RegionCoprocessorEnvironment> ctx, Mutation mutation,
        List<Pair<Cell, Cell>> cellPairs) throws IOException {
      return cellPairs.stream()
          .map(
            pair -> new Pair<>(pair.getFirst(), newCellWithDifferentColumnFamily(pair.getSecond())))
          .collect(Collectors.toList());
    }
  }

  public static class ChangeCellWithNotExistColumnFamilyObserver
      implements RegionCoprocessor, RegionObserver {
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public List<Pair<Cell, Cell>> postIncrementBeforeWAL(
        ObserverContext<RegionCoprocessorEnvironment> ctx, Mutation mutation,
        List<Pair<Cell, Cell>> cellPairs) throws IOException {
      return cellPairs.stream()
          .map(
            pair -> new Pair<>(pair.getFirst(), newCellWithNotExistColumnFamily(pair.getSecond())))
          .collect(Collectors.toList());
    }

    private Cell newCellWithNotExistColumnFamily(Cell cell) {
      return ExtendedCellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
          .setRow(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
          .setFamily(CF_NOT_EXIST_BYTES, 0, CF_NOT_EXIST_BYTES.length)
          .setQualifier(CellUtil.cloneQualifier(cell)).setTimestamp(cell.getTimestamp())
          .setType(cell.getType().getCode()).setValue(CellUtil.cloneValue(cell)).build();
    }

    @Override
    public List<Pair<Cell, Cell>> postAppendBeforeWAL(
        ObserverContext<RegionCoprocessorEnvironment> ctx, Mutation mutation,
        List<Pair<Cell, Cell>> cellPairs) throws IOException {
      return cellPairs.stream()
          .map(
            pair -> new Pair<>(pair.getFirst(), newCellWithNotExistColumnFamily(pair.getSecond())))
          .collect(Collectors.toList());
    }
  }

  public static class ChangeCellWithACLTagObserver extends AccessController {
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public List<Pair<Cell, Cell>> postIncrementBeforeWAL(
        ObserverContext<RegionCoprocessorEnvironment> ctx, Mutation mutation,
        List<Pair<Cell, Cell>> cellPairs) throws IOException {
      List<Pair<Cell, Cell>> result = super.postIncrementBeforeWAL(ctx, mutation, cellPairs);
      for (Pair<Cell, Cell> pair : result) {
        if (mutation.getACL() != null && !checkAclTag(mutation.getACL(), pair.getSecond())) {
          throw new DoNotRetryIOException("Unmatched ACL tag.");
        }
      }
      return result;
    }

    @Override
    public List<Pair<Cell, Cell>> postAppendBeforeWAL(
        ObserverContext<RegionCoprocessorEnvironment> ctx, Mutation mutation,
        List<Pair<Cell, Cell>> cellPairs) throws IOException {
      List<Pair<Cell, Cell>> result = super.postAppendBeforeWAL(ctx, mutation, cellPairs);
      for (Pair<Cell, Cell> pair : result) {
        if (mutation.getACL() != null && !checkAclTag(mutation.getACL(), pair.getSecond())) {
          throw new DoNotRetryIOException("Unmatched ACL tag.");
        }
      }
      return result;
    }
  }
}
