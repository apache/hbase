/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.atomic.AtomicBoolean;

@Category(SmallTests.class)
public class TestThreads {
  private static final Log LOG = LogFactory.getLog(TestThreads.class);

  private static final int SLEEP_TIME_MS = 3000;
  private static final int TOLERANCE_MS = (int) (0.10 * SLEEP_TIME_MS);

  private final AtomicBoolean wasInterrupted = new AtomicBoolean(false);

  @Test(timeout=60000)
  public void testSleepWithoutInterrupt() throws InterruptedException {
    Thread sleeper = new Thread(new Runnable() {
      @Override
      public void run() {
        LOG.debug("Sleeper thread: sleeping for " + SLEEP_TIME_MS);
        Threads.sleepWithoutInterrupt(SLEEP_TIME_MS);
        LOG.debug("Sleeper thread: finished sleeping");
        wasInterrupted.set(Thread.currentThread().isInterrupted());
      }
    });
    LOG.debug("Starting sleeper thread (" + SLEEP_TIME_MS + " ms)");
    sleeper.start();
    long startTime = System.currentTimeMillis();
    LOG.debug("Main thread: sleeping for 200 ms");
    Threads.sleep(200);

    LOG.debug("Interrupting the sleeper thread and sleeping for 500 ms");
    sleeper.interrupt();
    Threads.sleep(500);

    LOG.debug("Interrupting the sleeper thread and sleeping for 800 ms");
    sleeper.interrupt();
    Threads.sleep(800);

    LOG.debug("Interrupting the sleeper thread again");
    sleeper.interrupt();
    sleeper.join();

    assertTrue("sleepWithoutInterrupt did not preserve the thread's " +
        "interrupted status", wasInterrupted.get());

    long timeElapsed = System.currentTimeMillis() - startTime;
    // We expect to wait at least SLEEP_TIME_MS, but we can wait more if there is a GC.
    assertTrue("Elapsed time " + timeElapsed + " ms is out of the expected " +
        " sleep time of " + SLEEP_TIME_MS, SLEEP_TIME_MS - timeElapsed < TOLERANCE_MS);
    LOG.debug("Target sleep time: " + SLEEP_TIME_MS + ", time elapsed: " +
        timeElapsed);
  }
}
