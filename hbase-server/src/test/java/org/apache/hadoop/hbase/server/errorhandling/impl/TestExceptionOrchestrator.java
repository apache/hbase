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

import java.util.Arrays;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionListener;
import org.apache.hadoop.hbase.server.errorhandling.OperationAttemptTimer;
import org.apache.hadoop.hbase.server.errorhandling.exception.OperationAttemptTimeoutException;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test that we propagate errors through an orchestrator as expected
 */
@Category(SmallTests.class)
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestExceptionOrchestrator {

  @Test
  public void testErrorPropagation() {

    ExceptionListener listener1 = Mockito.mock(ExceptionListener.class);
    ExceptionListener listener2 = Mockito.mock(ExceptionListener.class);

    ExceptionOrchestrator<Exception> orchestrator = new ExceptionOrchestrator<Exception>();

    // add the listeners
    orchestrator.addErrorListener(orchestrator.genericVisitor, listener1);
    orchestrator.addErrorListener(orchestrator.genericVisitor, listener2);

    // create an artificial error
    String message = "Some error";
    Object[] info = new Object[] { "info1" };
    Exception e = new ExceptionForTesting("error");

    orchestrator.receiveError(message, e, info);

    // make sure the listeners got the error
    Mockito.verify(listener1, Mockito.times(1)).receiveError(message, e, info);
    Mockito.verify(listener2, Mockito.times(1)).receiveError(message, e, info);

    // push another error, which should be passed to listeners
    message = "another error";
    e = new ExceptionForTesting("hello");
    info[0] = "info2";
    orchestrator.receiveError(message, e, info);
    Mockito.verify(listener1, Mockito.times(1)).receiveError(message, e, info);
    Mockito.verify(listener2, Mockito.times(1)).receiveError(message, e, info);

    // now create a timer and check for that error
    info[0] = "timer";
    OperationAttemptTimer timer = new OperationAttemptTimer(orchestrator, 1000, info);
    timer.start();
    timer.trigger();
    // make sure that we got the timer error
    Mockito.verify(listener1, Mockito.times(1)).receiveError(Mockito.anyString(),
      Mockito.any(OperationAttemptTimeoutException.class),
      Mockito.argThat(new VarArgMatcher<Object>(Object.class, info)));
    Mockito.verify(listener2, Mockito.times(1)).receiveError(Mockito.anyString(),
      Mockito.any(OperationAttemptTimeoutException.class),
      Mockito.argThat(new VarArgMatcher<Object>(Object.class, info)));
  }

  /**
   * Matcher that matches var-args elements
   * @param <T> Type of args to match
   */
  private static class VarArgMatcher<T> extends BaseMatcher<T> {

    private T[] expected;
    private Class<T> clazz;
    private String reason;

    /**
     * Setup the matcher to expect args of the given type
     * @param clazz type of args to expect
     * @param expected expected arguments
     */
    public VarArgMatcher(Class<T> clazz, T... expected) {
      this.expected = expected;
      this.clazz = clazz;
    }

    @Override
    public boolean matches(Object arg0) {
      // null check early exit
      if (expected == null && arg0 == null) return true;

      // single arg matching
      if (clazz.isAssignableFrom(arg0.getClass())) {
        if (expected.length == 1) {
          if (arg0.equals(expected[0])) return true;
          reason = "single argument received, but didn't match argument";
        } else {
          reason = "single argument received, but expected array of args, size = "
              + expected.length;
        }
      } else if (arg0.getClass().isArray()) {
        // array matching
        try {
          T[] arg = (T[]) arg0;
          if (Arrays.equals(expected, arg)) return true;
          reason = "Array of args didn't match expected";
        } catch (Exception e) {
          reason = "Exception while matching arguments:" + e.getMessage();
        }
      } else reason = "Objet wasn't the same as passed class or not an array";

      // nothing worked - fail
      return false;
    }

    @Override
    public void describeTo(Description arg0) {
      arg0.appendText(reason);
    }

  }
}