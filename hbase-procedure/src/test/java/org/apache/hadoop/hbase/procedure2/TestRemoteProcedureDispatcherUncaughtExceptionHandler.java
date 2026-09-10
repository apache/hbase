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
package org.apache.hadoop.hbase.procedure2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Make sure the {@link UncaughtExceptionHandler} will be called when there are unchecked exceptions
 * thrown in the task.
 * <p/>
 * See HBASE-21875 and HBASE-21890 for more details.
 */
@Tag(MasterTests.TAG)
@Tag(SmallTests.TAG)
public class TestRemoteProcedureDispatcherUncaughtExceptionHandler {

  private static HBaseCommonTestingUtil UTIL = new HBaseCommonTestingUtil();

  private static final class ExceptionHandler implements UncaughtExceptionHandler {

    private Throwable error;

    @Override
    public synchronized void uncaughtException(Thread t, Throwable e) {
      this.error = e;
      notifyAll();
    }

    public synchronized void get() throws Throwable {
      while (error == null) {
        wait();
      }
      throw error;
    }
  }

  private static final class Dispatcher extends RemoteProcedureDispatcher<Void, Integer> {

    private final UncaughtExceptionHandler handler;

    public Dispatcher(UncaughtExceptionHandler handler) {
      super(UTIL.getConfiguration());
      this.handler = handler;
    }

    @Override
    protected UncaughtExceptionHandler getUncaughtExceptionHandler() {
      return handler;
    }

    @Override
    protected void remoteDispatch(Integer key, Set<RemoteProcedure> operations) {
    }

    @Override
    protected void abortPendingOperations(Integer key, Set<RemoteProcedure> operations) {
    }
  }

  private ExceptionHandler handler;

  private Dispatcher dispatcher;

  @BeforeEach
  public void setUp() {
    handler = new ExceptionHandler();
    dispatcher = new Dispatcher(handler);
    dispatcher.start();
  }

  @AfterEach
  public void tearDown() {
    dispatcher.stop();
    dispatcher = null;
    handler = null;
  }

  @Test
  public void testSubmit() throws Throwable {
    String message = "inject error";
    dispatcher.submitTask(new Runnable() {

      @Override
      public void run() {
        throw new RuntimeException(message);
      }
    });
    RuntimeException exception = assertThrows(RuntimeException.class, () -> handler.get());
    assertEquals(message, exception.getMessage());
  }

  @Test
  public void testDelayedSubmit() throws Throwable {
    String message = "inject error";
    dispatcher.submitTask(new Runnable() {

      @Override
      public void run() {
        throw new RuntimeException(message);
      }
    }, 100, TimeUnit.MILLISECONDS);
    RuntimeException exception = assertThrows(RuntimeException.class, () -> handler.get());
    assertEquals(message, exception.getMessage());
  }
}
