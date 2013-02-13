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

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionCheckable;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionListener;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionVisitor;
import org.apache.hadoop.hbase.server.errorhandling.FaultInjector;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test that we can correctly inject faults for testing
 */
@Category(SmallTests.class)
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestFaultInjecting {

  private static final Log LOG = LogFactory.getLog(TestFaultInjecting.class);
  public static final ExceptionVisitor<ExceptionListener> VISITOR = new ExceptionVisitor<ExceptionListener>() {

    @Override
    public void visit(ExceptionListener listener, String message, Exception e, Object... info) {
      listener.receiveError(message, e, info);
    }
  };

  @Test
  public void testSimpleFaultInjection() {
    ExceptionDispatcherFactory<ExceptionListener> factory = Mockito
        .spy(new ExceptionDispatcherFactory<ExceptionListener>(TestFaultInjecting.VISITOR));
    ExceptionDispatcher<ExceptionListener, Exception> dispatcher = new ExceptionDispatcher<ExceptionListener, Exception>();
    Mockito.when(factory.buildErrorHandler(VISITOR)).thenReturn(dispatcher);
    String info = "info";
    ExceptionOrchestratorFactory.addFaultInjector(new StringFaultInjector(info));
    ExceptionCheckable<Exception> monitor = factory.createErrorHandler();
    // make sure we wrap the dispatcher with the fault injection
    assertNotSame(dispatcher, monitor);

    // test that we actually inject a fault
    assertTrue("Monitor didn't get an injected error", monitor.checkForError());
    try {
      monitor.failOnError();
      fail("Monitor didn't get an exception from the fault injected in the factory.");
    } catch (ExceptionForTesting e) {
      LOG.debug("Correctly got an exception from the test!");
    } catch (Exception e) {
      fail("Got an unexpected exception:" + e);
    }
  }

  /**
   * Fault injector that will always throw a string error
   */
  public static class StringFaultInjector implements FaultInjector<ExceptionForTesting> {
    private final String info;

    public StringFaultInjector(String info) {
      this.info = info;
    }

    @Override
    public Pair<ExceptionForTesting, Object[]> injectFault(StackTraceElement[] trace) {
      if (ExceptionTestingUtils.stackContainsClass(trace, TestFaultInjecting.class)) {
        return new Pair<ExceptionForTesting, Object[]>(new ExceptionForTesting(
            "injected!"), new String[] { info });
      }
      return null;
    }
  }
}