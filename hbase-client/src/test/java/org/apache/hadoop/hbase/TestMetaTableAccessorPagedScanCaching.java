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
package org.apache.hadoop.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Asserts the single-RPC promise of the paginated meta-scan path
 * ({@link MetaTableAccessor#scanMetaForTableRegions(Connection, MetaTableAccessor.Visitor, TableName, byte[], int, CatalogReplicaMode)})
 * by capturing the {@link Scan} dispatched against the meta {@link Table} and asserting
 * {@code scan.getCaching() == rowLimit}. ScannerNext RPC count is
 * {@code ceil(rowsRequested / scan.getCaching())}, so {@code caching == rowLimit} is sufficient to
 * prove a single ScannerNext RPC.
 * <p/>
 * The configured {@code hbase.meta.scanner.caching} is set to a value smaller than {@code rowLimit}
 * so the paged-vs-unbounded branches in {@code MetaTableAccessor#getMetaScan} are distinguishable.
 */
@Tag(ClientTests.TAG)
@Tag(SmallTests.TAG)
public class TestMetaTableAccessorPagedScanCaching {

  private static final TableName USER_TABLE = TableName.valueOf("LocatorPaged");
  private static final int META_CACHING = 2;
  private static final int NUM_REGIONS = 5;

  private static final MetaTableAccessor.Visitor NOOP_VISITOR = result -> true;

  private Connection connection;
  private Table metaTable;

  @BeforeEach
  public void setUp() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt(HConstants.HBASE_META_SCANNER_CACHING, META_CACHING);

    connection = mock(Connection.class);
    metaTable = mock(Table.class);
    ResultScanner scanner = mock(ResultScanner.class);

    when(connection.getConfiguration()).thenReturn(conf);
    when(connection.getTable(TableName.META_TABLE_NAME)).thenReturn(metaTable);
    when(metaTable.getScanner(any(Scan.class))).thenReturn(scanner);
    when(scanner.next()).thenReturn(null);
  }

  @Test
  public void testPagedScanCachingEqualsLimitWhenLimitWithinCaching() throws IOException {
    int rowLimit = META_CACHING;
    MetaTableAccessor.scanMetaForTableRegions(connection, NOOP_VISITOR, USER_TABLE, null, rowLimit,
      CatalogReplicaMode.NONE);
    assertEquals(rowLimit, capturedScan().getCaching());
  }

  @Test
  public void testPagedScanCachingEqualsLimitWhenLimitExceedsCaching() throws IOException {
    int rowLimit = NUM_REGIONS;
    MetaTableAccessor.scanMetaForTableRegions(connection, NOOP_VISITOR, USER_TABLE, null, rowLimit,
      CatalogReplicaMode.NONE);
    assertEquals(rowLimit, capturedScan().getCaching());
  }

  @Test
  public void testUnboundedPathStillUsesConfiguredCaching() throws IOException {
    MetaTableAccessor.scanMetaForTableRegions(connection, NOOP_VISITOR, USER_TABLE);
    Scan scan = capturedScan();
    assertEquals(META_CACHING, scan.getCaching());
    assertEquals(Integer.MAX_VALUE, scan.getLimit());
  }

  private Scan capturedScan() throws IOException {
    ArgumentCaptor<Scan> scanCaptor = ArgumentCaptor.forClass(Scan.class);
    verify(metaTable).getScanner(scanCaptor.capture());
    return scanCaptor.getValue();
  }
}
