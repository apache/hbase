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

import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.function.Supplier;

import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ MediumTests.class, ClientTests.class })
public class TestRawAsyncTableScan extends AbstractTestAsyncTableScan {

  private static final class SimpleRawScanResultConsumer implements RawScanResultConsumer {

    private final Queue<Result> queue = new ArrayDeque<>();

    private boolean finished;

    private Throwable error;

    @Override
    public synchronized boolean onNext(Result[] results) {
      for (Result result : results) {
        queue.offer(result);
      }
      notifyAll();
      return true;
    }

    @Override
    public boolean onHeartbeat() {
      return true;
    }

    @Override
    public synchronized void onError(Throwable error) {
      finished = true;
      this.error = error;
      notifyAll();
    }

    @Override
    public synchronized void onComplete() {
      finished = true;
      notifyAll();
    }

    public synchronized Result take() throws IOException, InterruptedException {
      for (;;) {
        if (!queue.isEmpty()) {
          return queue.poll();
        }
        if (finished) {
          if (error != null) {
            Throwables.propagateIfPossible(error, IOException.class);
            throw new IOException(error);
          } else {
            return null;
          }
        }
        wait();
      }
    }
  }

  @Parameter
  public Supplier<Scan> scanCreater;

  @Parameters
  public static List<Object[]> params() {
    return Arrays.asList(new Supplier<?>[] { TestRawAsyncTableScan::createNormalScan },
      new Supplier<?>[] { TestRawAsyncTableScan::createBatchScan });
  }

  private static Scan createNormalScan() {
    return new Scan();
  }

  private static Scan createBatchScan() {
    return new Scan().setBatch(1);
  }

  @Override
  protected Scan createScan() {
    return scanCreater.get();
  }

  @Override
  protected List<Result> doScan(Scan scan) throws Exception {
    SimpleRawScanResultConsumer scanConsumer = new SimpleRawScanResultConsumer();
    ASYNC_CONN.getRawTable(TABLE_NAME).scan(scan, scanConsumer);
    List<Result> results = new ArrayList<>();
    for (Result result; (result = scanConsumer.take()) != null;) {
      results.add(result);
    }
    if (scan.getBatch() > 0) {
      results = convertFromBatchResult(results);
    }
    return results;
  }
}
