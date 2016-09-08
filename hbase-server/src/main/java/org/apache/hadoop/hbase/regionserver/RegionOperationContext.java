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
package org.apache.hadoop.hbase.regionserver;

import com.google.protobuf.RpcController;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;

/**
 * In each operation of AsyncRegion, we pass a context object with information of the request.
 * We can pass deadline of this request to AsyncRegion implementation to drop timeout request and
 * not waste time on timed out requests.
 * We can add listeners to watch the event of completion/failure of this operation, which helps us
 * make operation of AsyncRegion non-blocking. It is important for Staged Event-Driven Architecture
 * (SEDA), see HBASE-16583 for details.
 * The context is RPC-free, don't add RPC related code. In RPC we should use listener to deal with
 * the result.
 * @param <T> The type of result, Void if the operation has no result.
 */
@InterfaceAudience.Private
public class RegionOperationContext<T> {

  private long deadline = Long.MAX_VALUE;
  private List<OperationListener<T>> listeners;

  public long getDeadline() {
    return deadline;
  }

  public void setDeadline(long deadline) {
    this.deadline = deadline;
  }

  public RegionOperationContext() {
    listeners = new ArrayList<>();
  }

  public RegionOperationContext(RegionOperationContext<T> context) {
    this.deadline = context.deadline;
    this.listeners = new ArrayList<>(context.listeners);
  }

  public RegionOperationContext(RpcController controller) {
    if (controller instanceof HBaseRpcController) {
      this.deadline = ((HBaseRpcController) controller).getDeadline();
    }
  }

  public synchronized void addListener(OperationListener<T> listener) {
    listeners.add(listener);
  }

  /**
   * We will call this only in one thread, so no need to lock.
   */
  public void error(Throwable error) {
    for (OperationListener<T> listener : listeners) {
      listener.failed(error);
    }
  }

  /**
   * We will call this only in one thread, so no need to lock.
   */
  public void done(T result) {
    for (OperationListener<T> listener : listeners) {
      listener.completed(result);
    }
  }

}
