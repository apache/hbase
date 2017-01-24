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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTableScanAll extends AbstractTestAsyncTableScan {

  @Parameter(0)
  public String tableType;

  @Parameter(1)
  public Supplier<AsyncTableBase> getTable;

  @Parameter(2)
  public String scanType;

  @Parameter(3)
  public Supplier<Scan> scanCreator;

  private static RawAsyncTable getRawTable() {
    return ASYNC_CONN.getRawTable(TABLE_NAME);
  }

  private static AsyncTable getTable() {
    return ASYNC_CONN.getTable(TABLE_NAME, ForkJoinPool.commonPool());
  }

  private static Scan createNormalScan() {
    return new Scan();
  }

  // test if we can handle partial result when open scanner.
  private static Scan createSmallResultSizeScan() {
    return new Scan().setMaxResultSize(1);
  }

  @Parameters(name = "{index}: table={0}, scan={2}")
  public static List<Object[]> params() {
    Supplier<AsyncTableBase> rawTable = TestAsyncTableScanAll::getRawTable;
    Supplier<AsyncTableBase> normalTable = TestAsyncTableScanAll::getTable;
    Supplier<Scan> normalScan = TestAsyncTableScanAll::createNormalScan;
    Supplier<Scan> smallResultSizeScan = TestAsyncTableScanAll::createSmallResultSizeScan;
    return Arrays.asList(new Object[] { "raw", rawTable, "normal", normalScan },
      new Object[] { "raw", rawTable, "smallResultSize", smallResultSizeScan },
      new Object[] { "normal", normalTable, "normal", normalScan },
      new Object[] { "normal", normalTable, "smallResultSize", smallResultSizeScan });
  }

  @Test
  public void testScanWithLimit() throws InterruptedException, ExecutionException {
    int start = 111;
    int stop = 888;
    int limit = 300;
    List<Result> results = getTable.get()
        .scanAll(scanCreator.get().withStartRow(Bytes.toBytes(String.format("%03d", start)))
            .withStopRow(Bytes.toBytes(String.format("%03d", stop))).setLimit(limit)
            .setReadType(ReadType.PREAD))
        .get();
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
    int start = 888;
    int stop = 111;
    int limit = 300;
    List<Result> results = getTable.get()
        .scanAll(scanCreator.get().withStartRow(Bytes.toBytes(String.format("%03d", start)))
            .withStopRow(Bytes.toBytes(String.format("%03d", stop))).setLimit(limit)
            .setReadType(ReadType.PREAD).setReversed(true))
        .get();
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
    return scanCreator.get();
  }

  @Override
  protected List<Result> doScan(Scan scan) throws Exception {
    return getTable.get().scanAll(scan).get();
  }
}
