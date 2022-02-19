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
package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This is a hammer test that verifies MultiVersionConcurrencyControl in a
 * multiple writer single reader scenario.
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestMultiVersionConcurrencyControl {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMultiVersionConcurrencyControl.class);

  static class Writer implements Runnable {
    final AtomicBoolean finished;
    final MultiVersionConcurrencyControl mvcc;
    final AtomicBoolean status;

    Writer(AtomicBoolean finished, MultiVersionConcurrencyControl mvcc, AtomicBoolean status) {
      this.finished = finished;
      this.mvcc = mvcc;
      this.status = status;
    }

    public boolean failed = false;

    @Override
    public void run() {
      while (!finished.get()) {
        MultiVersionConcurrencyControl.WriteEntry e =
            mvcc.begin();
        // System.out.println("Begin write: " + e.getWriteNumber());
        // 10 usec - 500usec (including 0)
        int sleepTime = ThreadLocalRandom.current().nextInt(500);
        // 500 * 1000 = 500,000ns = 500 usec
        // 1 * 100 = 100ns = 1usec
        try {
          if (sleepTime > 0) Thread.sleep(0, sleepTime * 1000);
        } catch (InterruptedException e1) {
        }
        try {
          mvcc.completeAndWait(e);
        } catch (RuntimeException ex) {
          // got failure
          System.out.println(ex.toString());
          ex.printStackTrace();
          status.set(false);
          return;
          // Report failure if possible.
        }
      }
    }
  }

  @Test
  public void testParallelism() throws Exception {
    final MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();

    final AtomicBoolean finished = new AtomicBoolean(false);

    // fail flag for the reader thread
    final AtomicBoolean readerFailed = new AtomicBoolean(false);
    final AtomicLong failedAt = new AtomicLong();
    Runnable reader = new Runnable() {
      @Override
      public void run() {
        long prev = mvcc.getReadPoint();
        while (!finished.get()) {
          long newPrev = mvcc.getReadPoint();
          if (newPrev < prev) {
            // serious problem.
            System.out.println("Reader got out of order, prev: " + prev + " next was: " + newPrev);
            readerFailed.set(true);
            // might as well give up
            failedAt.set(newPrev);
            return;
          }
        }
      }
    };

    // writer thread parallelism.
    int n = 20;
    Thread[] writers = new Thread[n];
    AtomicBoolean[] statuses = new AtomicBoolean[n];
    Thread readThread = new Thread(reader);

    for (int i = 0; i < n; ++i) {
      statuses[i] = new AtomicBoolean(true);
      writers[i] = new Thread(new Writer(finished, mvcc, statuses[i]));
      writers[i].start();
    }
    readThread.start();

    try {
      Thread.sleep(10 * 1000);
    } catch (InterruptedException ex) {
    }

    finished.set(true);

    readThread.join();
    for (int i = 0; i < n; ++i) {
      writers[i].join();
    }

    // check failure.
    Assert.assertFalse(readerFailed.get());
    for (int i = 0; i < n; ++i) {
      Assert.assertTrue(statuses[i].get());
    }
  }
}
