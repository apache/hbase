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
package org.apache.hadoop.hbase.errorhandling;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test that we propagate errors through an dispatcher exactly once via different failure
 * injection mechanisms.
 */
@Category(SmallTests.class)
public class TestForeignExceptionDispatcher {
  private static final Log LOG = LogFactory.getLog(TestForeignExceptionDispatcher.class);

  /**
   * Exception thrown from the test
   */
  final ForeignException EXTEXN = new ForeignException("FORTEST", new IllegalArgumentException("FORTEST"));
  final ForeignException EXTEXN2 = new ForeignException("FORTEST2", new IllegalArgumentException("FORTEST2"));

  /**
   * Tests that a dispatcher only dispatches only the first exception, and does not propagate
   * subsequent exceptions.
   */
  @Test
  public void testErrorPropagation() {
    ForeignExceptionListener listener1 = Mockito.mock(ForeignExceptionListener.class);
    ForeignExceptionListener listener2 = Mockito.mock(ForeignExceptionListener.class);
    ForeignExceptionDispatcher dispatcher = new ForeignExceptionDispatcher();

    // add the listeners
    dispatcher.addListener(listener1);
    dispatcher.addListener(listener2);

    // create an artificial error
    dispatcher.receive(EXTEXN);

    // make sure the listeners got the error
    Mockito.verify(listener1, Mockito.times(1)).receive(EXTEXN);
    Mockito.verify(listener2, Mockito.times(1)).receive(EXTEXN);

    // make sure that we get an exception
    try {
      dispatcher.rethrowException();
      fail("Monitor should have thrown an exception after getting error.");
    } catch (ForeignException ex) {
      assertTrue("Got an unexpected exception:" + ex, ex.getCause() == EXTEXN.getCause());
      LOG.debug("Got the testing exception!");
    }

    // push another error, which should be not be passed to listeners
    dispatcher.receive(EXTEXN2);
    Mockito.verify(listener1, Mockito.never()).receive(EXTEXN2);
    Mockito.verify(listener2, Mockito.never()).receive(EXTEXN2);
  }

  @Test
  public void testSingleDispatcherWithTimer() {
    ForeignExceptionListener listener1 = Mockito.mock(ForeignExceptionListener.class);
    ForeignExceptionListener listener2 = Mockito.mock(ForeignExceptionListener.class);

    ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher();

    // add the listeners
    monitor.addListener(listener1);
    monitor.addListener(listener2);

    TimeoutExceptionInjector timer = new TimeoutExceptionInjector(monitor, 1000);
    timer.start();
    timer.trigger();

    assertTrue("Monitor didn't get timeout", monitor.hasException());

    // verify that that we propagated the error
    Mockito.verify(listener1).receive(Mockito.any(ForeignException.class));
    Mockito.verify(listener2).receive(Mockito.any(ForeignException.class));
  }

  /**
   * Test that the dispatcher can receive an error via the timer mechanism.
   */
  @Test
  public void testAttemptTimer() {
    ForeignExceptionListener listener1 = Mockito.mock(ForeignExceptionListener.class);
    ForeignExceptionListener listener2 = Mockito.mock(ForeignExceptionListener.class);
    ForeignExceptionDispatcher orchestrator = new ForeignExceptionDispatcher();

    // add the listeners
    orchestrator.addListener(listener1);
    orchestrator.addListener(listener2);

    // now create a timer and check for that error
    TimeoutExceptionInjector timer = new TimeoutExceptionInjector(orchestrator, 1000);
    timer.start();
    timer.trigger();
    // make sure that we got the timer error
    Mockito.verify(listener1, Mockito.times(1)).receive(Mockito.any(ForeignException.class));
    Mockito.verify(listener2, Mockito.times(1)).receive(Mockito.any(ForeignException.class));
  }
}