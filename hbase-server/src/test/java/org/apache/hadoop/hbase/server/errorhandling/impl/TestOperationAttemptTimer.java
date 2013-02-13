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
package org.apache.hadoop.hbase.server.errorhandling.impl;

import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionListener;
import org.apache.hadoop.hbase.server.errorhandling.OperationAttemptTimer;
import org.apache.hadoop.hbase.server.errorhandling.exception.OperationAttemptTimeoutException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test the {@link OperationAttemptTimer} to ensure we fulfill contracts
 */
@Category(SmallTests.class)
@SuppressWarnings("unchecked")
public class TestOperationAttemptTimer {

  private static final Log LOG = LogFactory.getLog(TestOperationAttemptTimer.class);

  @Test(timeout = 1000)
  public void testTimerTrigger() {
    final long time = 10000000;
    ExceptionListener<Exception> listener = Mockito.mock(ExceptionListener.class);
    OperationAttemptTimer timer = new OperationAttemptTimer(listener, time);
    timer.start();
    timer.trigger();
    Mockito.verify(listener, Mockito.times(1)).receiveError(Mockito.anyString(),
      Mockito.any(OperationAttemptTimeoutException.class));
  }

  @Test
  public void testTimerPassesOnErrorInfo() {
    final long time = 10;
    ExceptionListener<Exception> listener = Mockito.mock(ExceptionListener.class);
    final Object[] data = new Object[] { "data" };
    OperationAttemptTimer timer = new OperationAttemptTimer(listener, time, data);
    timer.start();
    timer.trigger();
    Mockito.verify(listener).receiveError(Mockito.anyString(),
      Mockito.any(OperationAttemptTimeoutException.class), Mockito.eq(data[0]));
  }

  @Test(timeout = 1000)
  public void testStartAfterComplete() throws InterruptedException {
    final long time = 10;
    ExceptionListener<Exception> listener = Mockito.mock(ExceptionListener.class);
    OperationAttemptTimer timer = new OperationAttemptTimer(listener, time);
    timer.complete();
    try {
      timer.start();
      fail("Timer should fail to start after complete.");
    } catch (IllegalStateException e) {
      LOG.debug("Correctly failed timer: " + e.getMessage());
    }
    Thread.sleep(time + 1);
    Mockito.verifyZeroInteractions(listener);
  }

  @Test(timeout = 1000)
  public void testStartAfterTrigger() throws InterruptedException {
    final long time = 10;
    ExceptionListener<Exception> listener = Mockito.mock(ExceptionListener.class);
    OperationAttemptTimer timer = new OperationAttemptTimer(listener, time);
    timer.trigger();
    try {
      timer.start();
      fail("Timer should fail to start after complete.");
    } catch (IllegalStateException e) {
      LOG.debug("Correctly failed timer: " + e.getMessage());
    }
    Thread.sleep(time * 2);
    Mockito.verify(listener, Mockito.times(1)).receiveError(Mockito.anyString(),
      Mockito.any(OperationAttemptTimeoutException.class));
    Mockito.verifyNoMoreInteractions(listener);
  }
}
