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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.IOException;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Run Append tests that use the HBase clients;
 */
@Tag(MediumTests.TAG)
@Tag(ClientTests.TAG)
public class TestAppendFromClientSide {

  protected final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static byte[] ROW = Bytes.toBytes("testRow");
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");

  @BeforeAll
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
  }

  @AfterAll
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testAppendWithCustomTimestamp(TestInfo testInfo) throws IOException {
    TableName TABLENAME = TableName.valueOf(testInfo.getTestMethod().get().getName());
    Table table = TEST_UTIL.createTable(TABLENAME, FAMILY);
    long timestamp = 999;
    Append append = new Append(ROW);
    append.add(ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(ROW)
      .setFamily(FAMILY).setQualifier(QUALIFIER).setTimestamp(timestamp)
      .setType(KeyValue.Type.Put.getCode()).setValue(Bytes.toBytes(100L)).build());
    Result r = table.append(append);
    assertEquals(1, r.size());
    assertEquals(timestamp, r.rawCells()[0].getTimestamp());
    r = table.get(new Get(ROW));
    assertEquals(1, r.size());
    assertEquals(timestamp, r.rawCells()[0].getTimestamp());
    r = table.append(append);
    assertEquals(1, r.size());
    assertNotEquals(timestamp, r.rawCells()[0].getTimestamp());
    r = table.get(new Get(ROW));
    assertEquals(1, r.size());
    assertNotEquals(timestamp, r.rawCells()[0].getTimestamp());
  }
}
