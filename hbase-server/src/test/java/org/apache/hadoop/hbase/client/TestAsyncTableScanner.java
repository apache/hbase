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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;

import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncTableScanner extends AbstractTestAsyncTableScan {

  @Parameter
  public Supplier<Scan> scanCreater;

  @Parameters
  public static List<Object[]> params() {
    return Arrays.asList(new Supplier<?>[] { TestAsyncTableScanner::createNormalScan },
      new Supplier<?>[] { TestAsyncTableScanner::createBatchScan },
      new Supplier<?>[] { TestAsyncTableScanner::createSmallResultSizeScan },
      new Supplier<?>[] { TestAsyncTableScanner::createBatchSmallResultSizeScan });
  }

  private static Scan createNormalScan() {
    return new Scan();
  }

  private static Scan createBatchScan() {
    return new Scan().setBatch(1);
  }

  // set a small result size for testing flow control
  private static Scan createSmallResultSizeScan() {
    return new Scan().setMaxResultSize(1);
  }

  private static Scan createBatchSmallResultSizeScan() {
    return new Scan().setBatch(1).setMaxResultSize(1);
  }

  @Override
  protected Scan createScan() {
    return scanCreater.get();
  }

  @Override
  protected List<Result> doScan(Scan scan) throws Exception {
    AsyncTable table = ASYNC_CONN.getTable(TABLE_NAME, ForkJoinPool.commonPool());
    List<Result> results = new ArrayList<>();
    try (ResultScanner scanner = table.getScanner(scan)) {
      for (Result result; (result = scanner.next()) != null;) {
        results.add(result);
      }
    }
    if (scan.getBatch() > 0) {
      results = convertFromBatchResult(results);
    }
    return results;
  }

}
