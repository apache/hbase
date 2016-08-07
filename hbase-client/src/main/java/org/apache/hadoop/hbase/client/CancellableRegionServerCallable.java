/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import java.io.IOException;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;

/**
 * This class is used to unify HTable calls with AsyncProcess Framework. HTable can use
 * AsyncProcess directly though this class. Also adds global timeout tracking on top of
 * RegionServerCallable and implements Cancellable.
 */
@InterfaceAudience.Private
abstract class CancellableRegionServerCallable<T> extends RegionServerCallable<T> implements
Cancellable {
  private final RetryingTimeTracker tracker = new RetryingTimeTracker();

  CancellableRegionServerCallable(Connection connection, TableName tableName, byte[] row,
      RpcControllerFactory rpcControllerFactory) {
    super(connection, rpcControllerFactory, tableName, row);
  }

  /* Override so can mess with the callTimeout.
   * (non-Javadoc)
   * @see org.apache.hadoop.hbase.client.RegionServerCallable#rpcCall(int)
   */
  @Override
  public T call(int callTimeout) throws IOException {
    // It is expected (it seems) that tracker.start can be called multiple times (on each trip
    // through the call when retrying). Also, we can call start and no need of a stop.
    this.tracker.start();
    int remainingTime = tracker.getRemainingTime(callTimeout);
    if (remainingTime == 0) {
      throw new DoNotRetryIOException("Timeout for mutate row");
    }
    return super.call(remainingTime);
  }

  @Override
  public void cancel() {
    getRpcController().startCancel();
  }

  @Override
  public boolean isCancelled() {
    return getRpcController().isCanceled();
  }
}