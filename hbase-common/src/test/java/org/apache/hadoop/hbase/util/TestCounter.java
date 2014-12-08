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
package org.apache.hadoop.hbase.util;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestCounter {
  private static final int[] THREAD_COUNTS = {1, 10, 100};
  private static final int DATA_COUNT = 1000000;

  private interface Operation {
    void execute();
  }

  @Test
  public void testIncrement() throws Exception {
    for(int threadCount : THREAD_COUNTS) {
      final Counter counter = new Counter();

      execute(new Operation() {
        @Override
        public void execute() {
          counter.increment();
        }
      }, threadCount);

      Assert.assertEquals(threadCount * (long)DATA_COUNT, counter.get());
    }
  }

  @Test
  public void testIncrementAndGet() throws Exception {
    for(int threadCount: THREAD_COUNTS) {
      final Counter counter = new Counter();

      execute(new Operation() {
        @Override
        public void execute() {
          counter.increment();
          counter.get();
        }
      }, threadCount);

      Assert.assertEquals(threadCount * (long)DATA_COUNT, counter.get());
    }
  }

  private static void execute(final Operation op, int threadCount)
      throws InterruptedException {

    final CountDownLatch prepareLatch = new CountDownLatch(threadCount);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch endLatch = new CountDownLatch(threadCount);

    class OperationThread extends Thread {
      @Override
      public void run() {
        try {
          prepareLatch.countDown();
          startLatch.await();

          for(int i=0; i<DATA_COUNT; i++) {
            op.execute();
          }

          endLatch.countDown();

        } catch(Exception e) {}
      }
    }

    for(int j=0; j<threadCount; j++) {
      new OperationThread().start();
    }

    prepareLatch.await();
    startLatch.countDown();
    endLatch.await();
  }
}
