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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
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

  @Parameters(name = "{index}: table={0}, scan={2}")
  public static List<Object[]> params() {
    Supplier<AsyncTableBase> rawTable = TestAsyncTableScanAll::getRawTable;
    Supplier<AsyncTableBase> normalTable = TestAsyncTableScanAll::getTable;
    return getScanCreater().stream()
        .flatMap(p -> Arrays.asList(new Object[] { "raw", rawTable, p.getFirst(), p.getSecond() },
          new Object[] { "normal", normalTable, p.getFirst(), p.getSecond() }).stream())
        .collect(Collectors.toList());
  }

  @Override
  protected Scan createScan() {
    return scanCreator.get();
  }

  @Override
  protected List<Result> doScan(Scan scan) throws Exception {
    List<Result> results = getTable.get().scanAll(scan).get();
    if (scan.getBatch() > 0) {
      results = convertFromBatchResult(results);
    }
    return results;
  }
}
