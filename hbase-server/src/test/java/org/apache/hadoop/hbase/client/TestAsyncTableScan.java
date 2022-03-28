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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.apache.hbase.thirdparty.com.google.common.base.Throwables;

@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncTableScan extends AbstractTestAsyncTableScan {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncTableScan.class);

  @Parameter(0)
  public String scanType;

  @Parameter(1)
  public Supplier<Scan> scanCreater;

  @Parameters(name = "{index}: scan={0}")
  public static List<Object[]> params() {
    return getScanCreatorParams();
  }

  @Override
  protected Scan createScan() {
    return scanCreater.get();
  }

  @Override
  protected List<Result> doScan(Scan scan, int closeAfter) throws Exception {
    AsyncTable<ScanResultConsumer> table =
        ASYNC_CONN.getTable(TABLE_NAME, ForkJoinPool.commonPool());
    List<Result> results;
    if (closeAfter > 0) {
      // these tests batch settings with the sample data result in each result being
      // split in two. so we must allow twice the expected results in order to reach
      // our true limit. see convertFromBatchResult for details.
      if (scan.getBatch() > 0) {
        closeAfter = closeAfter * 2;
      }
      LimitedScanResultConsumer consumer = new LimitedScanResultConsumer(closeAfter);
      table.scan(scan, consumer);
      results = consumer.getAll();
    } else {
      SimpleScanResultConsumer consumer = new SimpleScanResultConsumer();
      table.scan(scan, consumer);
      results = consumer.getAll();
    }
    if (scan.getBatch() > 0) {
      results = convertFromBatchResult(results);
    }
    return results;
  }

  private static class LimitedScanResultConsumer implements ScanResultConsumer {

    private final int limit;

    public LimitedScanResultConsumer(int limit) {
      this.limit = limit;
    }

    private final List<Result> results = new ArrayList<>();

    private Throwable error;

    private boolean finished = false;

    @Override
    public synchronized boolean onNext(Result result) {
      results.add(result);
      return results.size() < limit;
    }

    @Override
    public synchronized void onError(Throwable error) {
      this.error = error;
      finished = true;
      notifyAll();
    }

    @Override
    public synchronized void onComplete() {
      finished = true;
      notifyAll();
    }

    public synchronized List<Result> getAll() throws Exception {
      while (!finished) {
        wait();
      }
      if (error != null) {
        Throwables.propagateIfPossible(error, Exception.class);
        throw new Exception(error);
      }
      return results;
    }
  }

}
