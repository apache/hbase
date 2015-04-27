/**
 *
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

package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetProcedureResultRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetProcedureResultResponse;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({ClientTests.class, SmallTests.class})
public class TestProcedureFuture {
  private static class TestFuture extends HBaseAdmin.ProcedureFuture<Void> {
    private boolean postOperationResultCalled = false;
    private boolean waitOperationResultCalled = false;
    private boolean getProcedureResultCalled = false;
    private boolean convertResultCalled = false;

    public TestFuture(final HBaseAdmin admin, final Long procId) {
      super(admin, procId);
    }

    public boolean wasPostOperationResultCalled() {
      return postOperationResultCalled;
    }

    public boolean wasWaitOperationResultCalled() {
      return waitOperationResultCalled;
    }

    public boolean wasGetProcedureResultCalled() {
      return getProcedureResultCalled;
    }

    public boolean wasConvertResultCalled() {
      return convertResultCalled;
    }

    @Override
    protected GetProcedureResultResponse getProcedureResult(
        final GetProcedureResultRequest request) throws IOException {
      getProcedureResultCalled = true;
      return GetProcedureResultResponse.newBuilder()
              .setState(GetProcedureResultResponse.State.FINISHED)
              .build();
    }

    @Override
    protected Void convertResult(final GetProcedureResultResponse response) throws IOException {
      convertResultCalled = true;
      return null;
    }

    @Override
    protected Void waitOperationResult(final long deadlineTs)
        throws IOException, TimeoutException {
      waitOperationResultCalled = true;
      return null;
    }

    @Override
    protected Void postOperationResult(final Void result, final long deadlineTs)
        throws IOException, TimeoutException {
      postOperationResultCalled = true;
      return result;
    }
  }

  /**
   * When a master return a result with procId,
   * we are skipping the waitOperationResult() call,
   * since we are getting the procedure result.
   */
  @Test(timeout=60000)
  public void testWithProcId() throws Exception {
    HBaseAdmin admin = Mockito.mock(HBaseAdmin.class);
    TestFuture f = new TestFuture(admin, 100L);
    f.get(1, TimeUnit.MINUTES);

    assertTrue("expected getProcedureResult() to be called", f.wasGetProcedureResultCalled());
    assertTrue("expected convertResult() to be called", f.wasConvertResultCalled());
    assertFalse("unexpected waitOperationResult() called", f.wasWaitOperationResultCalled());
    assertTrue("expected postOperationResult() to be called", f.wasPostOperationResultCalled());
  }

  /**
   * Verify that the spin loop for the procedure running works.
   */
  @Test(timeout=60000)
  public void testWithProcIdAndSpinning() throws Exception {
    final AtomicInteger spinCount = new AtomicInteger(0);
    HBaseAdmin admin = Mockito.mock(HBaseAdmin.class);
    TestFuture f = new TestFuture(admin, 100L) {
      @Override
      protected GetProcedureResultResponse getProcedureResult(
          final GetProcedureResultRequest request) throws IOException {
        boolean done = spinCount.incrementAndGet() >= 10;
        return GetProcedureResultResponse.newBuilder()
              .setState(done ? GetProcedureResultResponse.State.FINISHED :
                GetProcedureResultResponse.State.RUNNING)
              .build();
      }
    };
    f.get(1, TimeUnit.MINUTES);

    assertEquals(10, spinCount.get());
    assertTrue("expected convertResult() to be called", f.wasConvertResultCalled());
    assertFalse("unexpected waitOperationResult() called", f.wasWaitOperationResultCalled());
    assertTrue("expected postOperationResult() to be called", f.wasPostOperationResultCalled());
  }

  /**
   * When a master return a result without procId,
   * we are skipping the getProcedureResult() call.
   */
  @Test(timeout=60000)
  public void testWithoutProcId() throws Exception {
    HBaseAdmin admin = Mockito.mock(HBaseAdmin.class);
    TestFuture f = new TestFuture(admin, null);
    f.get(1, TimeUnit.MINUTES);

    assertFalse("unexpected getProcedureResult() called", f.wasGetProcedureResultCalled());
    assertFalse("unexpected convertResult() called", f.wasConvertResultCalled());
    assertTrue("expected waitOperationResult() to be called", f.wasWaitOperationResultCalled());
    assertTrue("expected postOperationResult() to be called", f.wasPostOperationResultCalled());
  }

  /**
   * When a new client with procedure support tries to ask an old-master without proc-support
   * the procedure result we get a DoNotRetryIOException (which is an UnsupportedOperationException)
   * The future should trap that and fallback to the waitOperationResult().
   *
   * This happens when the operation calls happens on a "new master" but while we are waiting
   * the operation to be completed, we failover on an "old master".
   */
  @Test(timeout=60000)
  public void testOnServerWithNoProcedureSupport() throws Exception {
    HBaseAdmin admin = Mockito.mock(HBaseAdmin.class);
    TestFuture f = new TestFuture(admin, 100L) {
      @Override
      protected GetProcedureResultResponse getProcedureResult(
        final GetProcedureResultRequest request) throws IOException {
        super.getProcedureResult(request);
        throw new DoNotRetryIOException(new UnsupportedOperationException("getProcedureResult"));
      }
    };
    f.get(1, TimeUnit.MINUTES);

    assertTrue("expected getProcedureResult() to be called", f.wasGetProcedureResultCalled());
    assertFalse("unexpected convertResult() called", f.wasConvertResultCalled());
    assertTrue("expected waitOperationResult() to be called", f.wasWaitOperationResultCalled());
    assertTrue("expected postOperationResult() to be called", f.wasPostOperationResultCalled());
  }
}