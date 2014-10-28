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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import java.io.IOException;

/**
 * An {@link RpcRetryingCaller} that will update the per-region stats for the call on return,
 * if stats are available
 */
@InterfaceAudience.Private
public class StatsTrackingRpcRetryingCaller<T> implements RpcRetryingCaller<T> {
  private final ServerStatisticTracker stats;
  private final RpcRetryingCaller<T> delegate;

  public StatsTrackingRpcRetryingCaller(RpcRetryingCaller<T> delegate,
      ServerStatisticTracker stats) {
    this.delegate = delegate;
    this.stats = stats;
  }

  @Override
  public void cancel() {
    delegate.cancel();
  }

  @Override
  public T callWithRetries(RetryingCallable<T> callable, int callTimeout)
      throws IOException, RuntimeException {
    T result = delegate.callWithRetries(callable, callTimeout);
    return updateStatsAndUnwrap(result, callable);
  }

  @Override
  public T callWithoutRetries(RetryingCallable<T> callable, int callTimeout)
      throws IOException, RuntimeException {
    T result = delegate.callWithRetries(callable, callTimeout);
    return updateStatsAndUnwrap(result, callable);
  }

  private T updateStatsAndUnwrap(T result, RetryingCallable<T> callable) {
    // don't track stats about requests that aren't to regionservers
    if (!(callable instanceof RegionServerCallable)) {
      return result;
    }

    // mutli-server callables span multiple regions, so they don't have a location,
    // but they are region server callables, so we have to handle them when we process the
    // result, not in here
    if (callable instanceof MultiServerCallable) {
      return result;
    }

    // update the stats for the single server callable
    RegionServerCallable<T> regionCallable = (RegionServerCallable) callable;
    HRegionLocation location = regionCallable.getLocation();
    return ResultStatsUtil.updateStats(result, stats, location);
  }
}