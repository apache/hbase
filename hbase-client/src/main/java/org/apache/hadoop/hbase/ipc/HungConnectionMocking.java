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
package org.apache.hadoop.hbase.ipc;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class HungConnectionMocking extends OutputStream {

  private static final Logger LOG = LoggerFactory.getLogger(HungConnectionMocking.class);

  private static final DataOutputStream INSTANCE =
    new DataOutputStream(new HungConnectionMocking());

  private static final AtomicBoolean IS_PAUSED = new AtomicBoolean(false);
  private static final AtomicBoolean SHOULD_PAUSE_READ = new AtomicBoolean(false);
  private static final AtomicBoolean SHOULD_MOCK_WRITE = new AtomicBoolean(false);
  private static final AtomicBoolean IS_SLEEPING = new AtomicBoolean(false);

  private static LongAdder THREAD_REPLACED = null;
  private static LongAdder THREAD_ENDED_AFTER_INTERRUPT = null;

  public static void reset() {
    IS_PAUSED.set(false);
    SHOULD_PAUSE_READ.set(false);
    SHOULD_MOCK_WRITE.set(false);
    IS_SLEEPING.set(false);
    THREAD_REPLACED = new LongAdder();
    THREAD_ENDED_AFTER_INTERRUPT = new LongAdder();
  }

  public static void incrThreadReplacedCount() {
    if (THREAD_REPLACED != null) {
      THREAD_REPLACED.increment();
    }
  }

  public static void incrThreadEndedCount() {
    if (THREAD_ENDED_AFTER_INTERRUPT != null) {
      THREAD_ENDED_AFTER_INTERRUPT.increment();
    }
  }

  public static long getThreadReplacedCount() {
    return THREAD_REPLACED == null ? 0 : THREAD_REPLACED.sum();
  }

  public static long getThreadEndedCount() {
    return THREAD_ENDED_AFTER_INTERRUPT == null ? 0 : THREAD_ENDED_AFTER_INTERRUPT.sum();
  }

  public static DataOutputStream maybeMockStream(DataOutputStream defaultStream) {
    if (SHOULD_MOCK_WRITE.get()) {
      return INSTANCE;
    } else {
      return defaultStream;
    }
  }

  public static void enableMockedWrite() {
    SHOULD_MOCK_WRITE.set(true);
  }

  public static void disableMockedWrite() {
    SHOULD_MOCK_WRITE.set(false);
  }

  public static void pauseReads() {
    SHOULD_PAUSE_READ.set(true);
  }

  public static void unpauseReads() {
    synchronized (SHOULD_PAUSE_READ) {
      SHOULD_PAUSE_READ.set(false);
      SHOULD_PAUSE_READ.notifyAll();
    }
  }

  public static boolean isSleeping() {
    return IS_SLEEPING.get();
  }

  public static void awaitSleepingState() {
    synchronized (IS_SLEEPING) {
      while (!IS_SLEEPING.get()) {
        LOG.info("Waiting for stream to enter sleeping state");
        try {
          IS_SLEEPING.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  public static void maybePauseRead() {
    if (SHOULD_PAUSE_READ.get()) {
      synchronized (SHOULD_PAUSE_READ) {
        while (SHOULD_PAUSE_READ.get()) {
          synchronized (IS_PAUSED) {
            IS_PAUSED.set(true);
            IS_PAUSED.notifyAll();
          }
          LOG.info("Pausing read");
          try {
            SHOULD_PAUSE_READ.wait();
          } catch (InterruptedException e) {
            // ignore because we expect to be interrupted
          }
        }
      }
      LOG.info("Done pausing read");
    }
  }

  public static void awaitPausedReadState() {
    synchronized (IS_PAUSED) {
      while (!IS_PAUSED.get()) {
        LOG.info("Waiting for read to be paused");
        try {
          IS_PAUSED.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public void write(int b) throws IOException {
    try {
      LOG.info("Sleeping");
      synchronized (IS_SLEEPING) {
        IS_SLEEPING.set(true);
        IS_SLEEPING.notifyAll();
      }
      Thread.sleep(60_000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    LOG.info("Done - throwing exception");
    throw new SocketTimeoutException("from test");

  }
}
