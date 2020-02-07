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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.LossyCounting;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Private
public class MetricsUserAggregateImpl implements MetricsUserAggregate{

  /** Provider for mapping principal names to Users */
  private final UserProvider userProvider;

  private final MetricsUserAggregateSource source;
  private final LossyCounting userMetricLossyCounting;

  public MetricsUserAggregateImpl(Configuration conf) {
    source = CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
        .getUserAggregate();
    userMetricLossyCounting = new LossyCounting<MetricsUserSource>("userMetrics",
        (LossyCounting.LossyCountingListener<MetricsUserSource>) key -> source.deregister(key));
    this.userProvider = UserProvider.instantiate(conf);
  }

  /**
   * Returns the active user to which authorization checks should be applied.
   * If we are in the context of an RPC call, the remote user is used,
   * otherwise the currently logged in user is used.
   */
  private String getActiveUser() {
    Optional<User> user = RpcServer.getRequestUser();
    if (!user.isPresent()) {
      // for non-rpc handling, fallback to system user
      try {
        user = Optional.of(userProvider.getCurrent());
      } catch (IOException ignore) {
      }
    }
    return user.isPresent() ? user.get().getShortName() : null;
  }

  @VisibleForTesting
  MetricsUserAggregateSource getSource() {
    return source;
  }

  @Override
  public void updatePut(long t) {
    String user = getActiveUser();
    if (user != null) {
      getOrCreateMetricsUser(user).updatePut(t);
    }
  }

  @Override
  public void updateDelete(long t) {
    String user = getActiveUser();
    if (user != null) {
      getOrCreateMetricsUser(user).updateDelete(t);
    }
  }

  @Override
  public void updateGet(long t) {
    String user = getActiveUser();
    if (user != null) {
      getOrCreateMetricsUser(user).updateGet(t);
    }
  }

  @Override
  public void updateIncrement(long t) {
    String user = getActiveUser();
    if (user != null) {
      getOrCreateMetricsUser(user).updateIncrement(t);
    }
  }

  @Override
  public void updateAppend(long t) {
    String user = getActiveUser();
    if (user != null) {
      getOrCreateMetricsUser(user).updateAppend(t);
    }
  }

  @Override
  public void updateReplay(long t) {
    String user = getActiveUser();
    if (user != null) {
      getOrCreateMetricsUser(user).updateReplay(t);
    }
  }

  @Override
  public void updateScanTime(long t) {
    String user = getActiveUser();
    if (user != null) {
      getOrCreateMetricsUser(user).updateScanTime(t);
    }
  }

  private MetricsUserSource getOrCreateMetricsUser(String user) {
    MetricsUserSource userSource = source.getOrCreateMetricsUser(user);
    userMetricLossyCounting.add(userSource);
    return userSource;
  }
}
