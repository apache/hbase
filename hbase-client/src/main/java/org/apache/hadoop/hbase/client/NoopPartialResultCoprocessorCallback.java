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
package org.apache.hadoop.hbase.client;

import java.time.Duration;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class NoopPartialResultCoprocessorCallback<S, R>
  implements AsyncTable.PartialResultCoprocessorCallback<S, R> {

  private final AsyncTable.CoprocessorCallback<R> delegate;

  public NoopPartialResultCoprocessorCallback(AsyncTable.CoprocessorCallback<R> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void onRegionComplete(RegionInfo region, R resp) {
    delegate.onRegionComplete(region, resp);
  }

  @Override
  public void onRegionError(RegionInfo region, Throwable error) {
    delegate.onRegionError(region, error);
  }

  @Override
  public void onComplete() {
    delegate.onComplete();
  }

  @Override
  public void onError(Throwable error) {
    delegate.onError(error);
  }

  @Override
  public ServiceCaller<S, R> getNextCallable(R response, RegionInfo region) {
    return null;
  }

  @Override
  public Duration getWaitInterval(R response, RegionInfo region) {
    return Duration.ZERO;
  }
}
