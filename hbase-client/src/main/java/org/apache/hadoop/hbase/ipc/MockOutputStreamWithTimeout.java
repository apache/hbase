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
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class MockOutputStreamWithTimeout extends OutputStream {

  private static final Logger LOG = LoggerFactory.getLogger(MockOutputStreamWithTimeout.class);

  private static final DataOutputStream INSTANCE =
    new DataOutputStream(new MockOutputStreamWithTimeout());

  private static final AtomicBoolean SHOULD_FAIL = new AtomicBoolean(false);
  private static final AtomicBoolean IS_SLEEPING = new AtomicBoolean(false);

  public static DataOutputStream maybeMock(DataOutputStream defaultStream) {
    if (SHOULD_FAIL.get()) {
      return INSTANCE;
    } else {
      return defaultStream;
    }
  }

  public static void enable() {
    SHOULD_FAIL.set(true);
  }

  public static void disable() {
    SHOULD_FAIL.set(false);
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
