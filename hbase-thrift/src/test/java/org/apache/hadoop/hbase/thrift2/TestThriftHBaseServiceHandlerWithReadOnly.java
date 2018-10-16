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
package org.apache.hadoop.hbase.thrift2;

import static java.nio.ByteBuffer.wrap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.thrift2.generated.TAppend;
import org.apache.hadoop.hbase.thrift2.generated.TColumnIncrement;
import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.TCompareOp;
import org.apache.hadoop.hbase.thrift2.generated.TDelete;
import org.apache.hadoop.hbase.thrift2.generated.TGet;
import org.apache.hadoop.hbase.thrift2.generated.TIOError;
import org.apache.hadoop.hbase.thrift2.generated.TIncrement;
import org.apache.hadoop.hbase.thrift2.generated.TMutation;
import org.apache.hadoop.hbase.thrift2.generated.TPut;
import org.apache.hadoop.hbase.thrift2.generated.TRowMutations;
import org.apache.hadoop.hbase.thrift2.generated.TScan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ClientTests.class, MediumTests.class})
public class TestThriftHBaseServiceHandlerWithReadOnly {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestThriftHBaseServiceHandlerWithReadOnly.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  // Static names for tables, columns, rows, and values
  private static byte[] tableAname = Bytes.toBytes("tableA");
  private static byte[] familyAname = Bytes.toBytes("familyA");
  private static byte[] familyBname = Bytes.toBytes("familyB");
  private static byte[] qualifierAname = Bytes.toBytes("qualifierA");
  private static byte[] qualifierBname = Bytes.toBytes("qualifierB");
  private static byte[] valueAname = Bytes.toBytes("valueA");
  private static byte[] valueBname = Bytes.toBytes("valueB");
  private static HColumnDescriptor[] families = new HColumnDescriptor[] {
      new HColumnDescriptor(familyAname).setMaxVersions(3),
      new HColumnDescriptor(familyBname).setMaxVersions(2)
  };

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.getConfiguration().setBoolean("hbase.thrift.readonly", true);
    UTIL.getConfiguration().set("hbase.client.retries.number", "3");
    UTIL.startMiniCluster();
    Admin admin = UTIL.getAdmin();
    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableAname));
    for (HColumnDescriptor family : families) {
      tableDescriptor.addFamily(family);
    }
    admin.createTable(tableDescriptor);
    admin.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws Exception {

  }

  private ThriftHBaseServiceHandler createHandler() throws TException {
    try {
      Configuration conf = UTIL.getConfiguration();
      return new ThriftHBaseServiceHandler(conf, UserProvider.instantiate(conf));
    } catch (IOException ie) {
      throw new TException(ie);
    }
  }

  @Test
  public void testExistsWithReadOnly() throws TException {

    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testExists");
    ByteBuffer table = wrap(tableAname);
    TGet get = new TGet(wrap(rowName));

    boolean exceptionCaught = false;
    try {
      handler.exists(table, get);
    } catch (TIOError e) {
      exceptionCaught = true;
    } finally {
      assertFalse(exceptionCaught);
    }
  }

  @Test
  public void testExistsAllWithReadOnly() throws TException {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName1 = Bytes.toBytes("testExistsAll1");
    byte[] rowName2 = Bytes.toBytes("testExistsAll2");
    ByteBuffer table = wrap(tableAname);

    List<TGet> gets = new ArrayList<>();
    gets.add(new TGet(wrap(rowName1)));
    gets.add(new TGet(wrap(rowName2)));

    boolean exceptionCaught = false;
    try {
      handler.existsAll(table, gets);
    } catch (TIOError e) {
      exceptionCaught = true;
    } finally {
      assertFalse(exceptionCaught);
    }
  }

  @Test
  public void testGetWithReadOnly() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testGet");
    ByteBuffer table = wrap(tableAname);

    TGet get = new TGet(wrap(rowName));

    boolean exceptionCaught = false;
    try {
      handler.get(table, get);
    } catch (TIOError e) {
      exceptionCaught = true;
    } finally {
      assertFalse(exceptionCaught);
    }
  }

  @Test
  public void testGetMultipleWithReadOnly() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = wrap(tableAname);
    byte[] rowName1 = Bytes.toBytes("testGetMultiple1");
    byte[] rowName2 = Bytes.toBytes("testGetMultiple2");

    List<TGet> gets = new ArrayList<>(2);
    gets.add(new TGet(wrap(rowName1)));
    gets.add(new TGet(wrap(rowName2)));

    boolean exceptionCaught = false;
    try {
      handler.getMultiple(table, gets);
    } catch (TIOError e) {
      exceptionCaught = true;
    } finally {
      assertFalse(exceptionCaught);
    }
  }

  @Test
  public void testPutWithReadOnly() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = wrap(tableAname);
    byte[] rowName = Bytes.toBytes("testPut");

    List<TColumnValue> columnValues = new ArrayList<>(2);
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(valueAname)));
    columnValues.add(new TColumnValue(wrap(familyBname), wrap(qualifierBname), wrap(valueBname)));
    TPut put = new TPut(wrap(rowName), columnValues);

    boolean exceptionCaught = false;
    try {
      handler.put(table, put);
    } catch (TIOError e) {
      exceptionCaught = true;
      assertTrue(e.getCause() instanceof DoNotRetryIOException);
      assertEquals("Thrift Server is in Read-only mode.", e.getMessage());
    } finally {
      assertTrue(exceptionCaught);
    }
  }

  @Test
  public void testCheckAndPutWithReadOnly() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testCheckAndPut");
    ByteBuffer table = wrap(tableAname);

    List<TColumnValue> columnValuesA = new ArrayList<>(1);
    TColumnValue columnValueA = new TColumnValue(wrap(familyAname), wrap(qualifierAname),
        wrap(valueAname));
    columnValuesA.add(columnValueA);
    TPut putA = new TPut(wrap(rowName), columnValuesA);
    putA.setColumnValues(columnValuesA);

    List<TColumnValue> columnValuesB = new ArrayList<>(1);
    TColumnValue columnValueB = new TColumnValue(wrap(familyBname), wrap(qualifierBname),
        wrap(valueBname));
    columnValuesB.add(columnValueB);
    TPut putB = new TPut(wrap(rowName), columnValuesB);
    putB.setColumnValues(columnValuesB);

    boolean exceptionCaught = false;
    try {
      handler.checkAndPut(table, wrap(rowName), wrap(familyAname),
          wrap(qualifierAname), wrap(valueAname), putB);
    } catch (TIOError e) {
      exceptionCaught = true;
      assertTrue(e.getCause() instanceof DoNotRetryIOException);
      assertEquals("Thrift Server is in Read-only mode.", e.getMessage());
    } finally {
      assertTrue(exceptionCaught);
    }
  }

  @Test
  public void testPutMultipleWithReadOnly() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = wrap(tableAname);
    byte[] rowName1 = Bytes.toBytes("testPutMultiple1");
    byte[] rowName2 = Bytes.toBytes("testPutMultiple2");

    List<TColumnValue> columnValues = new ArrayList<>(2);
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(valueAname)));
    columnValues.add(new TColumnValue(wrap(familyBname), wrap(qualifierBname), wrap(valueBname)));
    List<TPut> puts = new ArrayList<>(2);
    puts.add(new TPut(wrap(rowName1), columnValues));
    puts.add(new TPut(wrap(rowName2), columnValues));

    boolean exceptionCaught = false;
    try {
      handler.putMultiple(table, puts);
    } catch (TIOError e) {
      exceptionCaught = true;
      assertTrue(e.getCause() instanceof DoNotRetryIOException);
      assertEquals("Thrift Server is in Read-only mode.", e.getMessage());
    } finally {
      assertTrue(exceptionCaught);
    }
  }

  @Test
  public void testDeleteWithReadOnly() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testDelete");
    ByteBuffer table = wrap(tableAname);

    TDelete delete = new TDelete(wrap(rowName));

    boolean exceptionCaught = false;
    try {
      handler.deleteSingle(table, delete);
    } catch (TIOError e) {
      exceptionCaught = true;
      assertTrue(e.getCause() instanceof DoNotRetryIOException);
      assertEquals("Thrift Server is in Read-only mode.", e.getMessage());
    } finally {
      assertTrue(exceptionCaught);
    }
  }

  @Test
  public void testDeleteMultipleWithReadOnly() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = wrap(tableAname);
    byte[] rowName1 = Bytes.toBytes("testDeleteMultiple1");
    byte[] rowName2 = Bytes.toBytes("testDeleteMultiple2");

    List<TDelete> deletes = new ArrayList<>(2);
    deletes.add(new TDelete(wrap(rowName1)));
    deletes.add(new TDelete(wrap(rowName2)));

    boolean exceptionCaught = false;
    try {
      handler.deleteMultiple(table, deletes);
    } catch (TIOError e) {
      exceptionCaught = true;
      assertTrue(e.getCause() instanceof DoNotRetryIOException);
      assertEquals("Thrift Server is in Read-only mode.", e.getMessage());
    } finally {
      assertTrue(exceptionCaught);
    }
  }

  @Test
  public void testCheckAndMutateWithReadOnly() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = wrap(tableAname);
    ByteBuffer row = wrap(Bytes.toBytes("row"));
    ByteBuffer family = wrap(familyAname);
    ByteBuffer qualifier = wrap(qualifierAname);
    ByteBuffer value = wrap(valueAname);

    List<TColumnValue> columnValuesB = new ArrayList<>(1);
    TColumnValue columnValueB = new TColumnValue(family, wrap(qualifierBname), wrap(valueBname));
    columnValuesB.add(columnValueB);
    TPut putB = new TPut(row, columnValuesB);
    putB.setColumnValues(columnValuesB);

    TRowMutations tRowMutations = new TRowMutations(row,
        Arrays.<TMutation> asList(TMutation.put(putB)));

    boolean exceptionCaught = false;
    try {
      handler.checkAndMutate(table, row, family, qualifier, TCompareOp.EQUAL, value,
          tRowMutations);
    } catch (TIOError e) {
      exceptionCaught = true;
      assertTrue(e.getCause() instanceof DoNotRetryIOException);
      assertEquals("Thrift Server is in Read-only mode.", e.getMessage());
    } finally {
      assertTrue(exceptionCaught);
    }
  }

  @Test
  public void testCheckAndDeleteWithReadOnly() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testCheckAndDelete");
    ByteBuffer table = wrap(tableAname);

    TDelete delete = new TDelete(wrap(rowName));

    boolean exceptionCaught = false;
    try {
      handler.checkAndDelete(table, wrap(rowName), wrap(familyAname),
          wrap(qualifierAname), wrap(valueAname), delete);
    } catch (TIOError e) {
      exceptionCaught = true;
      assertTrue(e.getCause() instanceof DoNotRetryIOException);
      assertEquals("Thrift Server is in Read-only mode.", e.getMessage());
    } finally {
      assertTrue(exceptionCaught);
    }
  }

  @Test
  public void testIncrementWithReadOnly() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testIncrement");
    ByteBuffer table = wrap(tableAname);

    List<TColumnIncrement> incrementColumns = new ArrayList<>(1);
    incrementColumns.add(new TColumnIncrement(wrap(familyAname), wrap(qualifierAname)));
    TIncrement increment = new TIncrement(wrap(rowName), incrementColumns);

    boolean exceptionCaught = false;
    try {
      handler.increment(table, increment);
    } catch (TIOError e) {
      exceptionCaught = true;
      assertTrue(e.getCause() instanceof DoNotRetryIOException);
      assertEquals("Thrift Server is in Read-only mode.", e.getMessage());
    } finally {
      assertTrue(exceptionCaught);
    }
  }

  @Test
  public void testAppendWithReadOnly() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testAppend");
    ByteBuffer table = wrap(tableAname);
    byte[] v1 = Bytes.toBytes("42");

    List<TColumnValue> appendColumns = new ArrayList<>(1);
    appendColumns.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(v1)));
    TAppend append = new TAppend(wrap(rowName), appendColumns);

    boolean exceptionCaught = false;
    try {
      handler.append(table, append);
    } catch (TIOError e) {
      exceptionCaught = true;
      assertTrue(e.getCause() instanceof DoNotRetryIOException);
      assertEquals("Thrift Server is in Read-only mode.", e.getMessage());
    } finally {
      assertTrue(exceptionCaught);
    }
  }

  @Test
  public void testMutateRowWithReadOnly() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testMutateRow");
    ByteBuffer table = wrap(tableAname);

    List<TColumnValue> columnValuesA = new ArrayList<>(1);
    TColumnValue columnValueA = new TColumnValue(wrap(familyAname), wrap(qualifierAname),
        wrap(valueAname));
    columnValuesA.add(columnValueA);
    TPut putA = new TPut(wrap(rowName), columnValuesA);
    putA.setColumnValues(columnValuesA);

    TDelete delete = new TDelete(wrap(rowName));

    List<TMutation> mutations = new ArrayList<>(2);
    TMutation mutationA = TMutation.put(putA);
    mutations.add(mutationA);
    TMutation mutationB = TMutation.deleteSingle(delete);
    mutations.add(mutationB);
    TRowMutations tRowMutations = new TRowMutations(wrap(rowName),mutations);

    boolean exceptionCaught = false;
    try {
      handler.mutateRow(table,tRowMutations);
    } catch (TIOError e) {
      exceptionCaught = true;
      assertTrue(e.getCause() instanceof DoNotRetryIOException);
      assertEquals("Thrift Server is in Read-only mode.", e.getMessage());
    } finally {
      assertTrue(exceptionCaught);
    }
  }

  @Test
  public void testScanWithReadOnly() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = wrap(tableAname);

    TScan scan = new TScan();
    boolean exceptionCaught = false;
    try {
      int scanId = handler.openScanner(table, scan);
      handler.getScannerRows(scanId, 10);
      handler.closeScanner(scanId);
    } catch (TIOError e) {
      exceptionCaught = true;
    } finally {
      assertFalse(exceptionCaught);
    }
  }
}
