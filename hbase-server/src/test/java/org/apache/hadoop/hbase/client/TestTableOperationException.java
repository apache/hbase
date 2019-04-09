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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MediumTests.class, ClientTests.class})
public class TestTableOperationException {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTableOperationException.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName TABLE_DONOT_RETRY = TableName.valueOf("TableDoNotRetry");

  private static TableName TABLE_RETRY = TableName.valueOf("TableRetry");

  private static Table tableDoNotRetry;

  private static Table tableRetry;

  private static byte[] CF = Bytes.toBytes("cf");

  private static byte[] CQ = Bytes.toBytes("cq");

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    UTIL.startMiniCluster();
    UTIL.getAdmin().createTable(TableDescriptorBuilder.newBuilder(TABLE_DONOT_RETRY)
        .setCoprocessor(ThrowDoNotRetryIOExceptionCoprocessor.class.getName())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF).build()).build());
    UTIL.getAdmin().createTable(TableDescriptorBuilder.newBuilder(TABLE_RETRY)
        .setCoprocessor(ThrowIOExceptionCoprocessor.class.getName())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF).build()).build());
    tableDoNotRetry = UTIL.getConnection().getTable(TABLE_DONOT_RETRY);
    tableRetry = UTIL.getConnection().getTable(TABLE_RETRY);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.getAdmin().disableTable(TABLE_DONOT_RETRY);
    UTIL.getAdmin().disableTable(TABLE_RETRY);
    UTIL.getAdmin().deleteTable(TABLE_DONOT_RETRY);
    UTIL.getAdmin().deleteTable(TABLE_RETRY);
    UTIL.shutdownMiniCluster();
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testGetWithDoNotRetryIOException() throws Exception {
    tableDoNotRetry.get(new Get(Bytes.toBytes("row")).addColumn(CF, CQ));
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testPutWithDoNotRetryIOException() throws Exception {
    tableDoNotRetry.put(new Put(Bytes.toBytes("row")).addColumn(CF, CQ, Bytes.toBytes("value")));
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testDeleteWithDoNotRetryIOException() throws Exception {
    tableDoNotRetry.delete(new Delete(Bytes.toBytes("row")).addColumn(CF, CQ));
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testAppendWithDoNotRetryIOException() throws Exception {
    tableDoNotRetry
        .append(new Append(Bytes.toBytes("row")).addColumn(CF, CQ, Bytes.toBytes("value")));
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testIncrementWithDoNotRetryIOException() throws Exception {
    tableDoNotRetry.increment(new Increment(Bytes.toBytes("row")).addColumn(CF, CQ, 1));
  }

  @Test(expected = RetriesExhaustedException.class)
  public void testGetWithIOException() throws Exception {
    tableRetry.get(new Get(Bytes.toBytes("row")).addColumn(CF, CQ));
  }

  @Test(expected = RetriesExhaustedException.class)
  public void testPutWithIOException() throws Exception {
    tableRetry.put(new Put(Bytes.toBytes("row")).addColumn(CF, CQ, Bytes.toBytes("value")));
  }

  @Test(expected = RetriesExhaustedException.class)
  public void testDeleteWithIOException() throws Exception {
    tableRetry.delete(new Delete(Bytes.toBytes("row")).addColumn(CF, CQ));
  }

  @Test(expected = RetriesExhaustedException.class)
  public void testAppendWithIOException() throws Exception {
    tableRetry.append(new Append(Bytes.toBytes("row")).addColumn(CF, CQ, Bytes.toBytes("value")));
  }

  @Test(expected = RetriesExhaustedException.class)
  public void testIncrementWithIOException() throws Exception {
    tableRetry.increment(new Increment(Bytes.toBytes("row")).addColumn(CF, CQ, 1));
  }

  public static class ThrowDoNotRetryIOExceptionCoprocessor
      implements RegionCoprocessor, RegionObserver {

    public ThrowDoNotRetryIOExceptionCoprocessor() {
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e, final Get get,
        final List<Cell> results) throws IOException {
      throw new DoNotRetryIOException("Call failed and don't retry");
    }

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put,
        final WALEdit edit, final Durability durability) throws IOException {
      throw new DoNotRetryIOException("Call failed and don't retry");
    }

    @Override
    public void preDelete(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Delete delete, final WALEdit edit, final Durability durability) throws IOException {
      throw new DoNotRetryIOException("Call failed and don't retry");
    }

    @Override
    public Result preIncrement(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Increment increment) throws IOException {
      throw new DoNotRetryIOException("Call failed and don't retry");
    }

    @Override
    public Result preAppend(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Append append) throws IOException {
      throw new DoNotRetryIOException("Call failed and don't retry");
    }
  }

  public static class ThrowIOExceptionCoprocessor
      implements RegionCoprocessor, RegionObserver {

    public ThrowIOExceptionCoprocessor() {
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e, final Get get,
        final List<Cell> results) throws IOException {
      throw new IOException("Call failed and retry");
    }

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put,
        final WALEdit edit, final Durability durability) throws IOException {
      throw new IOException("Call failed and retry");
    }

    @Override
    public void preDelete(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Delete delete, final WALEdit edit, final Durability durability) throws IOException {
      throw new IOException("Call failed and retry");
    }

    @Override
    public Result preIncrement(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Increment increment) throws IOException {
      throw new IOException("Call failed and retry");
    }

    @Override
    public Result preAppend(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Append append) throws IOException {
      throw new IOException("Call failed and retry");
    }
  }
}
