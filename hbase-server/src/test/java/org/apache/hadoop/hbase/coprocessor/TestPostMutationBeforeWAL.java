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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TestFromClientSide;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test coprocessor methods
 * {@link RegionObserver#postMutationBeforeWAL(ObserverContext, RegionObserver.MutationType,
 * Mutation, Cell, Cell)}.
 * This method will change the cells which will be applied to
 * memstore and WAL. So add unit test for the case which change the cell's tags .
 */
@Category({ CoprocessorTests.class, MediumTests.class })
public class TestPostMutationBeforeWAL {

  @Rule
  public TestName name = new TestName();

  private static final Log LOG = LogFactory.getLog(TestFromClientSide.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static Connection connection;

  private static final byte [] ROW = Bytes.toBytes("row");
  private static final String CF1 = "cf1";
  private static final byte[] CF1_BYTES = Bytes.toBytes(CF1);
  private static final String CF2 = "cf2";
  private static final byte[] CF2_BYTES = Bytes.toBytes(CF2);
  private static final byte[] CQ1 = Bytes.toBytes("cq1");
  private static final byte[] CQ2 = Bytes.toBytes("cq2");
  private static final byte[] VALUE = Bytes.toBytes("value");
  private static final byte[] VALUE2 = Bytes.toBytes("valuevalue");
  private static final String USER = "User";
  private static final Permission PERMS = new Permission(Permission.Action.READ);

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
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    tableDesc.addFamily(new HColumnDescriptor(CF1_BYTES));
    tableDesc.addFamily(new HColumnDescriptor(CF2_BYTES));
    tableDesc.addCoprocessor(coprocessor);
    connection.getAdmin().createTable(tableDesc);
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
      Append firstAppend = new Append(ROW).add(CF1_BYTES, CQ2, VALUE).setACL(USER, PERMS);
      Result result = table.append(firstAppend);
      assertEquals(1, result.size());
      assertTrue(Bytes.equals(VALUE, result.getValue(CF1_BYTES, CQ2)));

      // Check if the new cell can be read
      Get get = new Get(ROW).addColumn(CF1_BYTES, CQ2);
      result = table.get(get);
      assertEquals(1, result.size());
      assertTrue(Bytes.equals(VALUE, result.getValue(CF1_BYTES, CQ2)));

      // Append with TTL
      Append secondAppend = new Append(ROW).add(CF1_BYTES, CQ2, VALUE).setTTL(1000)
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
    Iterator<Tag> iter = CellUtil.tagsIterator(cell.getTagsArray(),
      cell.getTagsOffset(), cell.getTagsLength());
    while (iter.hasNext()) {
      Tag tag = iter.next();
      if (tag.getType() == TagType.ACL_TAG_TYPE) {
        return Bytes.equals(acl, tag.getValue());
      }
    }
    return false;
  }

  public static class ChangeCellWithACLTagObserver extends AccessController {

    @Override
    public Cell postMutationBeforeWAL(ObserverContext<RegionCoprocessorEnvironment> ctx,
        MutationType mutationType, Mutation mutation, Cell oldCell, Cell newCell)
        throws IOException {
      Cell result = super.postMutationBeforeWAL(ctx, mutationType, mutation, oldCell, newCell);
      if (mutation.getACL() != null && !checkAclTag(mutation.getACL(), result)) {
        throw new DoNotRetryIOException("Unmatched ACL tag.");
      }
      return result;
    }
  }
}
