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

package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CountDownLatch;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.client.VersionInfoUtil;
import org.apache.hadoop.hbase.procedure2.Procedure;

/**
 * Latch used by the Master to have the prepare() sync behaviour for old
 * clients, that can only get exceptions in a synchronous way.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class ProcedurePrepareLatch {
  private static final NoopLatch noopLatch = new NoopLatch();

  /**
   * Create a latch if the client does not have async proc support.
   * This uses the default 1.1 version.
   * @return a CompatibilityLatch or a NoopLatch if the client has async proc support
   */
  public static ProcedurePrepareLatch createLatch() {
    // don't use the latch if we have procedure support (default 1.1)
    return createLatch(1, 1);
  }

  /**
   * Create a latch if the client does not have async proc support
   * @param major major version with async proc support
   * @param minor minor version with async proc support
   * @return a CompatibilityLatch or a NoopLatch if the client has async proc support
   */
  public static ProcedurePrepareLatch createLatch(int major, int minor) {
    // don't use the latch if we have procedure support
    return hasProcedureSupport(major, minor) ? noopLatch : new CompatibilityLatch();
  }

  /**
   * Creates a latch which blocks.
   */
  public static ProcedurePrepareLatch createBlockingLatch() {
    return new CompatibilityLatch();
  }

  /**
   * Returns the singleton latch which does nothing.
   */
  public static ProcedurePrepareLatch getNoopLatch() {
    return noopLatch;
  }

  private static boolean hasProcedureSupport(int major, int minor) {
    return VersionInfoUtil.currentClientHasMinimumVersion(major, minor);
  }

  protected abstract void countDown(final Procedure proc);
  public abstract void await() throws IOException;

  public static void releaseLatch(final ProcedurePrepareLatch latch, final Procedure proc) {
    if (latch != null) {
      latch.countDown(proc);
    }
  }

  private static class NoopLatch extends ProcedurePrepareLatch {
    @Override
    protected void countDown(final Procedure proc) {}
    @Override
    public void await() throws IOException {}
  }

  protected static class CompatibilityLatch extends ProcedurePrepareLatch {
    private final CountDownLatch latch = new CountDownLatch(1);

    private IOException exception = null;

    @Override
    protected void countDown(final Procedure proc) {
      if (proc.hasException()) {
        exception = MasterProcedureUtil.unwrapRemoteIOException(proc);
      }
      latch.countDown();
    }

    @Override
    public void await() throws IOException {
      try {
        latch.await();
      } catch (InterruptedException e) {
        throw (InterruptedIOException)new InterruptedIOException().initCause(e);
      }

      if (exception != null) {
        throw exception;
      }
    }
  }
}
