/**
 *
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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.InvalidMutationDurabilityException;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestInvalidMutationDurabilityException {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestInvalidMutationDurabilityException.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NOT_REPLICATE = TableName.valueOf("TableNotReplicate");

  private static TableName TABLE_NEED_REPLICATE = TableName.valueOf("TableNeedReplicate");

  private static byte[] CF = Bytes.toBytes("cf");

  private static byte[] CQ = Bytes.toBytes("cq");

  private static Table tableNotReplicate;

  private static Table tableNeedReplicate;

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster();
    UTIL.getAdmin().createTable(TableDescriptorBuilder.newBuilder(TABLE_NOT_REPLICATE)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF).build()).build());
    UTIL.getAdmin().createTable(TableDescriptorBuilder.newBuilder(TABLE_NEED_REPLICATE)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF)
            .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build()).build());
    tableNotReplicate = UTIL.getConnection().getTable(TABLE_NOT_REPLICATE);
    tableNeedReplicate = UTIL.getConnection().getTable(TABLE_NEED_REPLICATE);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.getAdmin().disableTable(TABLE_NOT_REPLICATE);
    UTIL.getAdmin().disableTable(TABLE_NEED_REPLICATE);
    UTIL.getAdmin().deleteTable(TABLE_NOT_REPLICATE);
    UTIL.getAdmin().deleteTable(TABLE_NEED_REPLICATE);
    UTIL.shutdownMiniCluster();
  }

  private Put newPutWithSkipWAL() {
    Put put = new Put(Bytes.toBytes("row"));
    put.addColumn(CF, CQ, Bytes.toBytes("value"));
    put.setDurability(Durability.SKIP_WAL);
    return put;
  }

  @Test
  public void testPutToTableNotReplicate() throws Exception {
    tableNotReplicate.put(newPutWithSkipWAL());
  }

  @Test(expected = InvalidMutationDurabilityException.class)
  public void testPutToTableNeedReplicate() throws Exception {
    tableNeedReplicate.put(newPutWithSkipWAL());
  }

  private Delete newDeleteWithSkipWAL() {
    Delete delete = new Delete(Bytes.toBytes("row"));
    delete.addColumn(CF, CQ);
    delete.setDurability(Durability.SKIP_WAL);
    return delete;
  }

  @Test
  public void testDeleteToTableNotReplicate() throws Exception {
    tableNotReplicate.delete(newDeleteWithSkipWAL());
  }

  @Test(expected = InvalidMutationDurabilityException.class)
  public void testDeleteToTableNeedReplicate() throws Exception {
    tableNeedReplicate.delete(newDeleteWithSkipWAL());
  }

  private Append newAppendWithSkipWAL() {
    Append append = new Append(Bytes.toBytes("row"));
    append.addColumn(CF, CQ, Bytes.toBytes("value"));
    append.setDurability(Durability.SKIP_WAL);
    return append;
  }

  @Test
  public void testAppendToTableNotReplicate() throws Exception {
    tableNotReplicate.append(newAppendWithSkipWAL());
  }

  @Test(expected = InvalidMutationDurabilityException.class)
  public void testAppendToTableNeedReplicate() throws Exception {
    tableNeedReplicate.append(newAppendWithSkipWAL());
  }

  private Increment newIncrementWithSkipWAL() {
    Increment increment = new Increment(Bytes.toBytes("row"));
    increment.addColumn(CF, CQ, 1);
    increment.setDurability(Durability.SKIP_WAL);
    return increment;
  }

  @Test
  public void testIncrementToTableNotReplicate() throws Exception {
    tableNotReplicate.increment(newIncrementWithSkipWAL());
  }

  @Test(expected = InvalidMutationDurabilityException.class)
  public void testIncrementToTableNeedReplicate() throws Exception {
    tableNeedReplicate.increment(newIncrementWithSkipWAL());
  }

  @Test
  public void testCheckWithMutateToTableNotReplicate() throws Exception {
    tableNotReplicate.checkAndMutate(Bytes.toBytes("row"), CF).qualifier(CQ).ifNotExists()
        .thenPut(newPutWithSkipWAL());
  }

  @Test(expected = InvalidMutationDurabilityException.class)
  public void testCheckWithMutateToTableNeedReplicate() throws Exception {
    tableNeedReplicate.checkAndMutate(Bytes.toBytes("row"), CF).qualifier(CQ).ifNotExists()
        .thenPut(newPutWithSkipWAL());
  }
}
