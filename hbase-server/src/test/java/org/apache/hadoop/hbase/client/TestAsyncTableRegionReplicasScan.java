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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

@Tag(MediumTests.TAG)
@Tag(ClientTests.TAG)
@HBaseParameterizedTestTemplate
public class TestAsyncTableRegionReplicasScan extends AbstractTestAsyncTableRegionReplicasRead {

  private static int ROW_COUNT = 1000;

  public TestAsyncTableRegionReplicasScan(Supplier<AsyncTable<?>> getTable) {
    super(getTable);
  }

  private static byte[] getRow(int i) {
    return Bytes.toBytes(String.format("%s-%03d", Bytes.toString(ROW), i));
  }

  private static byte[] getValue(int i) {
    return Bytes.toBytes(String.format("%s-%03d", Bytes.toString(VALUE), i));
  }

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    startClusterAndCreateTable();
    AsyncTable<?> table = ASYNC_CONN.getTable(TABLE_NAME);
    for (int i = 0; i < ROW_COUNT; i++) {
      table.put(new Put(getRow(i)).addColumn(FAMILY, QUALIFIER, getValue(i))).get();
    }
    waitUntilAllReplicasHaveRow(getRow(ROW_COUNT - 1));
  }

  @Override
  protected void readAndCheck(AsyncTable<?> table, int replicaId) throws IOException {
    Scan scan = new Scan().setConsistency(Consistency.TIMELINE).setCaching(1);
    if (replicaId >= 0) {
      scan.setReplicaId(replicaId);
    }
    try (ResultScanner scanner = table.getScanner(scan)) {
      for (int i = 0; i < ROW_COUNT; i++) {
        Result result = scanner.next();
        assertNotNull(result);
        assertArrayEquals(getValue(i), result.getValue(FAMILY, QUALIFIER));
      }
    }
  }
}
