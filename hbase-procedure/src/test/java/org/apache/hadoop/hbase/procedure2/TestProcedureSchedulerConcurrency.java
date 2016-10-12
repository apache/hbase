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

package org.apache.hadoop.hbase.procedure2;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.NoopProcedure;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Threads;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, MediumTests.class})
public class TestProcedureSchedulerConcurrency {
  private static final Log LOG = LogFactory.getLog(TestProcedureEvents.class);

  private SimpleProcedureScheduler procSched;

  @Before
  public void setUp() throws IOException {
    procSched = new SimpleProcedureScheduler();
    procSched.start();
  }

  @After
  public void tearDown() throws IOException {
    procSched.stop();
  }

  @Test(timeout=60000)
  public void testConcurrentWaitWake() throws Exception {
    testConcurrentWaitWake(false);
  }

  @Test(timeout=60000)
  public void testConcurrentWaitWakeBatch() throws Exception {
    testConcurrentWaitWake(true);
  }

  private void testConcurrentWaitWake(final boolean useWakeBatch) throws Exception {
    final int WAIT_THRESHOLD = 2500;
    final int NPROCS = 20;
    final int NRUNS = 500;

    final ProcedureScheduler sched = procSched;
    for (long i = 0; i < NPROCS; ++i) {
      sched.addBack(new TestProcedureWithEvent(i));
    }

    final Thread[] threads = new Thread[4];
    final AtomicInteger waitCount = new AtomicInteger(0);
    final AtomicInteger wakeCount = new AtomicInteger(0);

    final ConcurrentSkipListSet<TestProcedureWithEvent> waitQueue =
      new ConcurrentSkipListSet<TestProcedureWithEvent>();
    threads[0] = new Thread() {
      @Override
      public void run() {
        long lastUpdate = 0;
        while (true) {
          final int oldWakeCount = wakeCount.get();
          if (useWakeBatch) {
            ProcedureEvent[] ev = new ProcedureEvent[waitQueue.size()];
            for (int i = 0; i < ev.length; ++i) {
              ev[i] = waitQueue.pollFirst().getEvent();
              LOG.debug("WAKE BATCH " + ev[i] + " total=" + wakeCount.get());
            }
            sched.wakeEvents(ev.length, ev);
            wakeCount.addAndGet(ev.length);
          } else {
            int size = waitQueue.size();
            while (size-- > 0) {
              ProcedureEvent ev = waitQueue.pollFirst().getEvent();
              sched.wakeEvent(ev);
              LOG.debug("WAKE " + ev + " total=" + wakeCount.get());
              wakeCount.incrementAndGet();
            }
          }
          if (wakeCount.get() != oldWakeCount) {
            lastUpdate = System.currentTimeMillis();
          } else if (wakeCount.get() >= NRUNS &&
              (System.currentTimeMillis() - lastUpdate) > WAIT_THRESHOLD) {
            break;
          }
          Threads.sleepWithoutInterrupt(25);
        }
      }
    };

    for (int i = 1; i < threads.length; ++i) {
      threads[i] = new Thread() {
        @Override
        public void run() {
          while (true) {
            TestProcedureWithEvent proc = (TestProcedureWithEvent)sched.poll();
            if (proc == null) continue;

            sched.suspendEvent(proc.getEvent());
            waitQueue.add(proc);
            sched.waitEvent(proc.getEvent(), proc);
            LOG.debug("WAIT " + proc.getEvent());
            if (waitCount.incrementAndGet() >= NRUNS) {
              break;
            }
          }
        }
      };
    }

    for (int i = 0; i < threads.length; ++i) {
      threads[i].start();
    }
    for (int i = 0; i < threads.length; ++i) {
      threads[i].join();
    }

    sched.clear();
  }

  public static class TestProcedureWithEvent extends NoopProcedure<Void> {
    private final ProcedureEvent event;

    public TestProcedureWithEvent(long procId) {
      setProcId(procId);
      event = new ProcedureEvent("test-event procId=" + procId);
    }

    public ProcedureEvent getEvent() {
      return event;
    }
  }
}
