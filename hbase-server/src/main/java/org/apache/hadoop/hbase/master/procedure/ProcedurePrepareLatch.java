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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.VersionInfo;

/**
 * Latch used by the Master to have the prepare() sync behaviour for old
 * clients, that can only get exceptions in a synchronous way.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class ProcedurePrepareLatch {
  private static final NoopLatch noopLatch = new NoopLatch();

  public static ProcedurePrepareLatch createLatch() {
    // don't use the latch if we have procedure support
    return hasProcedureSupport() ? noopLatch : new CompatibilityLatch();
  }

  public static boolean hasProcedureSupport() {
    return currentClientHasMinimumVersion(1, 1);
  }

  private static boolean currentClientHasMinimumVersion(int major, int minor) {
    RpcCallContext call = RpcServer.getCurrentCall();
    VersionInfo versionInfo = call != null ? call.getClientVersionInfo() : null;
    if (versionInfo != null) {
      String[] components = versionInfo.getVersion().split("\\.");

      int clientMajor = components.length > 0 ? Integer.parseInt(components[0]) : 0;
      if (clientMajor != major) {
        return clientMajor > major;
      }

      int clientMinor = components.length > 1 ? Integer.parseInt(components[1]) : 0;
      return clientMinor >= minor;
    }
    return false;
  }

  protected abstract void countDown(final Procedure proc);
  public abstract void await() throws IOException;

  protected static void releaseLatch(final ProcedurePrepareLatch latch, final Procedure proc) {
    if (latch != null) {
      latch.countDown(proc);
    }
  }

  private static class NoopLatch extends ProcedurePrepareLatch {
    protected void countDown(final Procedure proc) {}
    public void await() throws IOException {}
  }

  protected static class CompatibilityLatch extends ProcedurePrepareLatch {
    private final CountDownLatch latch = new CountDownLatch(1);

    private IOException exception = null;

    protected void countDown(final Procedure proc) {
      if (proc.hasException()) {
        exception = proc.getException().unwrapRemoteException();
      }
      latch.countDown();
    }

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
