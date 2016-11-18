/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test class for {@link RegionServerSpaceQuotaManager}.
 */
@Category(SmallTests.class)
public class TestRegionServerSpaceQuotaManager {

  private RegionServerSpaceQuotaManager quotaManager;
  private Connection conn;
  private Table quotaTable;
  private ResultScanner scanner;

  @Before
  @SuppressWarnings("unchecked")
  public void setup() throws Exception {
    quotaManager = mock(RegionServerSpaceQuotaManager.class);
    conn = mock(Connection.class);
    quotaTable = mock(Table.class);
    scanner = mock(ResultScanner.class);
    // Call the real getViolationPoliciesToEnforce()
    when(quotaManager.getViolationPoliciesToEnforce()).thenCallRealMethod();
    // Mock out creating a scanner
    when(quotaManager.getConnection()).thenReturn(conn);
    when(conn.getTable(QuotaUtil.QUOTA_TABLE_NAME)).thenReturn(quotaTable);
    when(quotaTable.getScanner(any(Scan.class))).thenReturn(scanner);
    // Mock out the static method call with some indirection
    doAnswer(new Answer<Void>(){
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        Result result = invocation.getArgumentAt(0, Result.class);
        Map<TableName,SpaceViolationPolicy> policies = invocation.getArgumentAt(1, Map.class);
        QuotaTableUtil.extractViolationPolicy(result, policies);
        return null;
      }
    }).when(quotaManager).extractViolationPolicy(any(Result.class), any(Map.class));
  }

  @Test
  public void testMissingAllColumns() {
    List<Result> results = new ArrayList<>();
    results.add(Result.create(Collections.emptyList()));
    when(scanner.iterator()).thenReturn(results.iterator());
    try {
      quotaManager.getViolationPoliciesToEnforce();
      fail("Expected an IOException, but did not receive one.");
    } catch (IOException e) {
      // Expected an error because we had no cells in the row.
      // This should only happen due to programmer error.
    }
  }

  @Test
  public void testMissingDesiredColumn() {
    List<Result> results = new ArrayList<>();
    // Give a column that isn't the one we want
    Cell c = new KeyValue(toBytes("t:inviolation"), toBytes("q"), toBytes("s"), new byte[0]);
    results.add(Result.create(Collections.singletonList(c)));
    when(scanner.iterator()).thenReturn(results.iterator());
    try {
      quotaManager.getViolationPoliciesToEnforce();
      fail("Expected an IOException, but did not receive one.");
    } catch (IOException e) {
      // Expected an error because we were missing the column we expected in this row.
      // This should only happen due to programmer error.
    }
  }

  @Test
  public void testParsingError() {
    List<Result> results = new ArrayList<>();
    Cell c = new KeyValue(toBytes("t:inviolation"), toBytes("u"), toBytes("v"), new byte[0]);
    results.add(Result.create(Collections.singletonList(c)));
    when(scanner.iterator()).thenReturn(results.iterator());
    try {
      quotaManager.getViolationPoliciesToEnforce();
      fail("Expected an IOException, but did not receive one.");
    } catch (IOException e) {
      // We provided a garbage serialized protobuf message (empty byte array), this should
      // in turn throw an IOException
    }
  }
}
