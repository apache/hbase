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

  public Integer getCallTimeout() {
    return callTimeout;
  }

  public void setCallTimeout(int callTimeout) {
    this.callTimeout = callTimeout;
  }

  public boolean hasCallTimeout(){
    return callTimeout != null;
  }

  @Override
  public String errorText() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean failed() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCanceled() {
    return cancelled;
  }

  @Override
  public void notifyOnCancel(RpcCallback<Object> cancellationCb) {
    this.cancellationCb.set(cancellationCb);
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFailed(String arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void startCancel() {
    cancelled = true;
    if (cancellationCb.get() != null) {
      cancellationCb.get().run(null);
    }
  }
}
