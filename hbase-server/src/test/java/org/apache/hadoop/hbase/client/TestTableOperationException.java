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
package org.apache.hadoop.hbase.client;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtil;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MediumTests.TAG)
@Tag(ClientTests.TAG)
public class TestTableOperationException {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static TableName TABLE_DONOT_RETRY = TableName.valueOf("TableDoNotRetry");

  private static TableName TABLE_RETRY = TableName.valueOf("TableRetry");

  private static Table tableDoNotRetry;

  private static Table tableRetry;

  private static byte[] CF = Bytes.toBytes("cf");

  private static byte[] CQ = Bytes.toBytes("cq");

  @BeforeAll
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    UTIL.startMiniCluster();
    UTIL.getAdmin()
      .createTable(TableDescriptorBuilder.newBuilder(TABLE_DONOT_RETRY)
        .setCoprocessor(ThrowDoNotRetryIOExceptionCoprocessor.class.getName())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF).build()).build());
    UTIL.getAdmin()
      .createTable(TableDescriptorBuilder.newBuilder(TABLE_RETRY)
        .setCoprocessor(ThrowIOExceptionCoprocessor.class.getName())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF).build()).build());
    tableDoNotRetry = UTIL.getConnection().getTable(TABLE_DONOT_RETRY);
    tableRetry = UTIL.getConnection().getTable(TABLE_RETRY);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    UTIL.getAdmin().disableTable(TABLE_DONOT_RETRY);
    UTIL.getAdmin().disableTable(TABLE_RETRY);
    UTIL.getAdmin().deleteTable(TABLE_DONOT_RETRY);
    UTIL.getAdmin().deleteTable(TABLE_RETRY);
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testGetWithDoNotRetryIOException() throws Exception {
    assertThrows(DoNotRetryIOException.class,
      () -> tableDoNotRetry.get(new Get(Bytes.toBytes("row")).addColumn(CF, CQ)));
  }

  @Test
  public void testPutWithDoNotRetryIOException() throws Exception {
    assertThrows(DoNotRetryIOException.class, () -> tableDoNotRetry
      .put(new Put(Bytes.toBytes("row")).addColumn(CF, CQ, Bytes.toBytes("value"))));
  }

  @Test
  public void testDeleteWithDoNotRetryIOException() throws Exception {
    assertThrows(DoNotRetryIOException.class,
      () -> tableDoNotRetry.delete(new Delete(Bytes.toBytes("row")).addColumn(CF, CQ)));
  }

  @Test
  public void testAppendWithDoNotRetryIOException() throws Exception {
    assertThrows(DoNotRetryIOException.class, () -> tableDoNotRetry
      .append(new Append(Bytes.toBytes("row")).addColumn(CF, CQ, Bytes.toBytes("value"))));
  }

  @Test
  public void testIncrementWithDoNotRetryIOException() throws Exception {
    assertThrows(DoNotRetryIOException.class,
      () -> tableDoNotRetry.increment(new Increment(Bytes.toBytes("row")).addColumn(CF, CQ, 1)));
  }

  @Test
  public void testGetWithIOException() throws Exception {
    assertThrows(RetriesExhaustedException.class,
      () -> tableRetry.get(new Get(Bytes.toBytes("row")).addColumn(CF, CQ)));
  }

  @Test
  public void testPutWithIOException() throws Exception {
    assertThrows(RetriesExhaustedException.class, () -> tableRetry
      .put(new Put(Bytes.toBytes("row")).addColumn(CF, CQ, Bytes.toBytes("value"))));
  }

  @Test
  public void testDeleteWithIOException() throws Exception {
    assertThrows(RetriesExhaustedException.class,
      () -> tableRetry.delete(new Delete(Bytes.toBytes("row")).addColumn(CF, CQ)));
  }

  @Test
  public void testAppendWithIOException() throws Exception {
    assertThrows(RetriesExhaustedException.class, () -> tableRetry
      .append(new Append(Bytes.toBytes("row")).addColumn(CF, CQ, Bytes.toBytes("value"))));
  }

  @Test
  public void testIncrementWithIOException() throws Exception {
    assertThrows(RetriesExhaustedException.class,
      () -> tableRetry.increment(new Increment(Bytes.toBytes("row")).addColumn(CF, CQ, 1)));
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
    public void preGetOp(final ObserverContext<? extends RegionCoprocessorEnvironment> e,
      final Get get, final List<Cell> results) throws IOException {
      throw new DoNotRetryIOException("Call failed and don't retry");
    }

    @Override
    public void prePut(final ObserverContext<? extends RegionCoprocessorEnvironment> e,
      final Put put, final WALEdit edit, final Durability durability) throws IOException {
      throw new DoNotRetryIOException("Call failed and don't retry");
    }

    @Override
    public void preDelete(final ObserverContext<? extends RegionCoprocessorEnvironment> e,
      final Delete delete, final WALEdit edit, final Durability durability) throws IOException {
      throw new DoNotRetryIOException("Call failed and don't retry");
    }

    @Override
    public Result preIncrement(final ObserverContext<? extends RegionCoprocessorEnvironment> e,
      final Increment increment) throws IOException {
      throw new DoNotRetryIOException("Call failed and don't retry");
    }

    @Override
    public Result preAppend(final ObserverContext<? extends RegionCoprocessorEnvironment> e,
      final Append append) throws IOException {
      throw new DoNotRetryIOException("Call failed and don't retry");
    }
  }

  public static class ThrowIOExceptionCoprocessor implements RegionCoprocessor, RegionObserver {

    public ThrowIOExceptionCoprocessor() {
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preGetOp(final ObserverContext<? extends RegionCoprocessorEnvironment> e,
      final Get get, final List<Cell> results) throws IOException {
      throw new IOException("Call failed and retry");
    }

    @Override
    public void prePut(final ObserverContext<? extends RegionCoprocessorEnvironment> e,
      final Put put, final WALEdit edit, final Durability durability) throws IOException {
      throw new IOException("Call failed and retry");
    }

    @Override
    public void preDelete(final ObserverContext<? extends RegionCoprocessorEnvironment> e,
      final Delete delete, final WALEdit edit, final Durability durability) throws IOException {
      throw new IOException("Call failed and retry");
    }

    @Override
    public Result preIncrement(final ObserverContext<? extends RegionCoprocessorEnvironment> e,
      final Increment increment) throws IOException {
      throw new IOException("Call failed and retry");
    }

    @Override
    public Result preAppend(final ObserverContext<? extends RegionCoprocessorEnvironment> e,
      final Append append) throws IOException {
      throw new IOException("Call failed and retry");
    }
  }
}
