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

import static org.junit.Assert.assertTrue;
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
 * Test using the single error dispatcher
 */
@SuppressWarnings("unchecked")
@Category(SmallTests.class)
public class TestSingleExceptionDispatcher {

  private static final Log LOG = LogFactory.getLog(TestSingleExceptionDispatcher.class);
  @Test
  public void testErrorPropagation() {
    ExceptionListener<Exception> listener1 = Mockito.mock(ExceptionListener.class);
    ExceptionListener<Exception> listener2 = Mockito.mock(ExceptionListener.class);

    ExceptionDispatcher<? extends ExceptionListener<Exception>, Exception> monitor = new ExceptionDispatcher<ExceptionListener<Exception>, Exception>();

    // add the listeners
    monitor.addErrorListener(monitor.genericVisitor, listener1);
    monitor.addErrorListener(monitor.genericVisitor, listener2);

    // create an artificial error
    String message = "Some error";
    Exception expected = new ExceptionForTesting("error");
    Object info = "info1";
    monitor.receiveError(message, expected, info);

    // make sure the listeners got the error
    Mockito.verify(listener1).receiveError(message, expected, info);
    Mockito.verify(listener2).receiveError(message, expected, info);

    // make sure that we get an exception
    try {
      monitor.failOnError();
      fail("Monitor should have thrown an exception after getting error.");
    } catch (Exception e) {
      assertTrue("Got an unexpected exception:" + e, e instanceof ExceptionForTesting);
      LOG.debug("Got the testing exception!");
    }
    // push another error, but this shouldn't be passed to the listeners
    monitor.receiveError("another error", new ExceptionForTesting("hello"),
      "shouldn't be found");
    // make sure we don't re-propagate the error
    Mockito.verifyNoMoreInteractions(listener1, listener2);
  }

  @Test
  public void testSingleDispatcherWithTimer() {
    ExceptionListener<Exception> listener1 = Mockito.mock(ExceptionListener.class);
    ExceptionListener<Exception> listener2 = Mockito.mock(ExceptionListener.class);

    ExceptionDispatcher<? extends ExceptionListener<Exception>, Exception> monitor = new ExceptionDispatcher<ExceptionListener<Exception>, Exception>();

    // add the listeners
    monitor.addErrorListener(monitor.genericVisitor, listener1);
    monitor.addErrorListener(monitor.genericVisitor, listener2);

    Object info = "message";
    OperationAttemptTimer timer = new OperationAttemptTimer(monitor, 1000, info);
    timer.start();
    timer.trigger();

    assertTrue("Monitor didn't get timeout", monitor.checkForError());

    // verify that that we propagated the error
    Mockito.verify(listener1).receiveError(Mockito.anyString(),
      Mockito.any(OperationAttemptTimeoutException.class), Mockito.eq(info));
    Mockito.verify(listener2).receiveError(Mockito.anyString(),
      Mockito.any(OperationAttemptTimeoutException.class), Mockito.eq(info));
  }

  @Test
  public void testAddListenerWithoutVisitor() {
    SimpleErrorListener<Exception> listener = new SimpleErrorListener<Exception>();
    ExceptionDispatcher<SimpleErrorListener<Exception>, Exception> monitor = new ExceptionDispatcher<SimpleErrorListener<Exception>, Exception>();
    try {
      monitor.addErrorListener(listener);
      fail("Monitor needs t have a visitor for adding generically typed listeners");
    } catch (UnsupportedOperationException e) {
      LOG.debug("Correctly failed to add listener without visitor: " + e.getMessage());
    }
  }
}