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

package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

@InterfaceAudience.Private
public class TimeLimitedRpcController implements RpcController {

  /**
   * The time, in ms before the call should expire.
   */
  protected volatile Integer callTimeout;
  protected volatile boolean cancelled = false;
  protected final AtomicReference<RpcCallback<Object>> cancellationCb =
      new AtomicReference<RpcCallback<Object>>(null);

  protected final AtomicReference<RpcCallback<IOException>> failureCb =
      new AtomicReference<RpcCallback<IOException>>(null);

  private IOException exception;

  public int getCallTimeout() {
    if (callTimeout != null) {
      return callTimeout;
    } else {
      return 0;
    }
  }

  public void setCallTimeout(int callTimeout) {
    this.callTimeout = callTimeout;
  }

  public boolean hasCallTimeout(){
    return callTimeout != null;
  }

  @Override
  public String errorText() {
    if (exception != null) {
      return exception.getMessage();
    } else {
      return null;
    }
  }

  /**
   * For use in async rpc clients
   * @return true if failed
   */
  @Override
  public boolean failed() {
    return this.exception != null;
  }

  @Override
  public boolean isCanceled() {
    return cancelled;
  }

  @Override
  public void notifyOnCancel(RpcCallback<Object> cancellationCb) {
    this.cancellationCb.set(cancellationCb);
    if (this.cancelled) {
      cancellationCb.run(null);
    }
  }

  /**
   * Notify a callback on error.
   * For use in async rpc clients
   *
   * @param failureCb the callback to call on error
   */
  public void notifyOnFail(RpcCallback<IOException> failureCb) {
    this.failureCb.set(failureCb);
    if (this.exception != null) {
      failureCb.run(this.exception);
    }
  }

  @Override
  public void reset() {
    exception = null;
    cancelled = false;
    failureCb.set(null);
    cancellationCb.set(null);
    callTimeout = null;
  }

  @Override
  public void setFailed(String reason) {
    this.exception = new IOException(reason);
    if (this.failureCb.get() != null) {
      this.failureCb.get().run(this.exception);
    }
  }

  /**
   * Set failed with an exception to pass on.
   * For use in async rpc clients
   *
   * @param e exception to set with
   */
  public void setFailed(IOException e) {
    this.exception = e;
    if (this.failureCb.get() != null) {
      this.failureCb.get().run(this.exception);
    }
  }

  @Override
  public void startCancel() {
    cancelled = true;
    if (cancellationCb.get() != null) {
      cancellationCb.get().run(null);
    }
  }
}
