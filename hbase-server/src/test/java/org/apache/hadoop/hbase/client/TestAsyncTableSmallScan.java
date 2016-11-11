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

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTableSmallScan extends AbstractTestAsyncTableScan {

  @Test
  public void testScanWithLimit() throws InterruptedException, ExecutionException {
    AsyncTable table = ASYNC_CONN.getTable(TABLE_NAME);
    int start = 111;
    int stop = 888;
    int limit = 300;
    List<Result> results = table.smallScan(new Scan(Bytes.toBytes(String.format("%03d", start)))
        .setStopRow(Bytes.toBytes(String.format("%03d", stop))).setSmall(true),
      limit).get();
    assertEquals(limit, results.size());
    IntStream.range(0, limit).forEach(i -> {
      Result result = results.get(i);
      int actualIndex = start + i;
      assertEquals(String.format("%03d", actualIndex), Bytes.toString(result.getRow()));
      assertEquals(actualIndex, Bytes.toInt(result.getValue(FAMILY, CQ1)));
    });
  }

  @Test
  public void testReversedScanWithLimit() throws InterruptedException, ExecutionException {
    AsyncTable table = ASYNC_CONN.getTable(TABLE_NAME);
    int start = 888;
    int stop = 111;
    int limit = 300;
    List<Result> results = table.smallScan(
      new Scan(Bytes.toBytes(String.format("%03d", start)))
          .setStopRow(Bytes.toBytes(String.format("%03d", stop))).setSmall(true).setReversed(true),
      limit).get();
    assertEquals(limit, results.size());
    IntStream.range(0, limit).forEach(i -> {
      Result result = results.get(i);
      int actualIndex = start - i;
      assertEquals(String.format("%03d", actualIndex), Bytes.toString(result.getRow()));
      assertEquals(actualIndex, Bytes.toInt(result.getValue(FAMILY, CQ1)));
    });
  }

  @Override
  protected Scan createScan() {
    return new Scan().setSmall(true);
  }

  @Override
  protected List<Result> doScan(AsyncTable table, Scan scan) throws Exception {
    return table.smallScan(scan).get();
  }
}
