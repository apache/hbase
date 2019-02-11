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
import java.io.InterruptedIOException;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;

/**
 * This class is used to unify HTable calls with AsyncProcess Framework. HTable can use
 * AsyncProcess directly though this class. Also adds global timeout tracking on top of
 * RegionServerCallable and implements Cancellable.
 * Global timeout tracking conflicts with logic in RpcRetryingCallerImpl's callWithRetries. So you
 * can only use this callable in AsyncProcess which only uses callWithoutRetries and retries in its
 * own implementation.
 */
@InterfaceAudience.Private
abstract class CancellableRegionServerCallable<T> extends ClientServiceCallable<T> implements
    Cancellable {
  private final RetryingTimeTracker tracker;
  private final int rpcTimeout;

  CancellableRegionServerCallable(ConnectionImplementation connection, TableName tableName,
      byte[] row, RpcController rpcController, int rpcTimeout, RetryingTimeTracker tracker,
      int priority) {
    super(connection, tableName, row, rpcController, priority);
    this.rpcTimeout = rpcTimeout;
    this.tracker = tracker;
  }

  /* Override so can mess with the callTimeout.
   * (non-Javadoc)
   * @see org.apache.hadoop.hbase.client.RegionServerCallable#rpcCall(int)
   */
  @Override
  public T call(int operationTimeout) throws IOException {
    if (isCancelled()) return null;
    if (Thread.interrupted()) {
      throw new InterruptedIOException();
    }
    // It is expected (it seems) that tracker.start can be called multiple times (on each trip
    // through the call when retrying). Also, we can call start and no need of a stop.
    this.tracker.start();
    int remainingTime = tracker.getRemainingTime(operationTimeout);
    if (remainingTime <= 1) {
      // "1" is a special return value in RetryingTimeTracker, see its implementation.
      throw new DoNotRetryIOException("Operation rpcTimeout");
    }
    return super.call(Math.min(rpcTimeout, remainingTime));
  }

  @Override
  public void prepare(boolean reload) throws IOException {
    if (isCancelled()) return;
    if (Thread.interrupted()) {
      throw new InterruptedIOException();
    }
    super.prepare(reload);
  }

  @Override
  protected void setStubByServiceName(ServerName serviceName) throws IOException {
    setStub(getConnection().getClient(serviceName));
  }

  @Override
  public void cancel() {
    getRpcController().startCancel();
  }

  @Override
  public boolean isCancelled() {
    return getRpcController().isCanceled();
  }

  protected ClientProtos.MultiResponse doMulti(ClientProtos.MultiRequest request)
  throws org.apache.hbase.thirdparty.com.google.protobuf.ServiceException {
    return getStub().multi(getRpcController(), request);
  }

  protected ClientProtos.ScanResponse doScan(ClientProtos.ScanRequest request)
  throws org.apache.hbase.thirdparty.com.google.protobuf.ServiceException {
    return getStub().scan(getRpcController(), request);
  }

  protected ClientProtos.PrepareBulkLoadResponse doPrepareBulkLoad(
      ClientProtos.PrepareBulkLoadRequest request)
  throws org.apache.hbase.thirdparty.com.google.protobuf.ServiceException {
    return getStub().prepareBulkLoad(getRpcController(), request);
  }

  protected ClientProtos.BulkLoadHFileResponse doBulkLoadHFile(
      ClientProtos.BulkLoadHFileRequest request)
  throws org.apache.hbase.thirdparty.com.google.protobuf.ServiceException {
    return getStub().bulkLoadHFile(getRpcController(), request);
  }

  protected ClientProtos.CleanupBulkLoadResponse doCleanupBulkLoad(
      ClientProtos.CleanupBulkLoadRequest request)
  throws org.apache.hbase.thirdparty.com.google.protobuf.ServiceException {
    return getStub().cleanupBulkLoad(getRpcController(), request);
  }
}
