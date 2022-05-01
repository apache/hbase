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
package org.apache.hadoop.hbase.errorhandling;

import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the {@link TimeoutExceptionInjector} to ensure we fulfill contracts
 */
@Category({ MasterTests.class, SmallTests.class })
public class TestTimeoutExceptionInjector {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTimeoutExceptionInjector.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestTimeoutExceptionInjector.class);

  /**
   * Test that a manually triggered timer fires an exception.
   */
  @Test
  public void testTimerTrigger() {
    final long time = 10000000; // pick a value that is very far in the future
    ForeignExceptionListener listener = Mockito.mock(ForeignExceptionListener.class);
    TimeoutExceptionInjector timer = new TimeoutExceptionInjector(listener, time);
    timer.start();
    timer.trigger();
    Mockito.verify(listener, Mockito.times(1)).receive(Mockito.any());
  }

  /**
   * Test that a manually triggered exception with data fires with the data in receiveError.
   */
  @Test
  public void testTimerPassesOnErrorInfo() {
    final long time = 1000000;
    ForeignExceptionListener listener = Mockito.mock(ForeignExceptionListener.class);
    TimeoutExceptionInjector timer = new TimeoutExceptionInjector(listener, time);
    timer.start();
    timer.trigger();
    Mockito.verify(listener).receive(Mockito.any());
  }

  /**
   * Demonstrate TimeoutExceptionInjector semantics -- completion means no more exceptions passed to
   * error listener.
   */
  @Test
  public void testStartAfterComplete() throws InterruptedException {
    final long time = 10;
    ForeignExceptionListener listener = Mockito.mock(ForeignExceptionListener.class);
    TimeoutExceptionInjector timer = new TimeoutExceptionInjector(listener, time);
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

  /**
   * Demonstrate TimeoutExceptionInjector semantics -- triggering fires exception and completes the
   * timer.
   */
  @Test
  public void testStartAfterTrigger() throws InterruptedException {
    final long time = 10;
    ForeignExceptionListener listener = Mockito.mock(ForeignExceptionListener.class);
    TimeoutExceptionInjector timer = new TimeoutExceptionInjector(listener, time);
    timer.trigger();
    try {
      timer.start();
      fail("Timer should fail to start after complete.");
    } catch (IllegalStateException e) {
      LOG.debug("Correctly failed timer: " + e.getMessage());
    }
    Thread.sleep(time * 2);
    Mockito.verify(listener, Mockito.times(1)).receive(Mockito.any());
    Mockito.verifyNoMoreInteractions(listener);
  }
}
