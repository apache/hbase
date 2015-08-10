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

import com.google.common.base.Preconditions;

import java.util.concurrent.ThreadPoolExecutor;

public class MetricsConnectionWrapperImpl implements MetricsConnectionWrapper {

  private final ConnectionImplementation conn;

  public MetricsConnectionWrapperImpl(ConnectionImplementation connection) {
    Preconditions.checkNotNull(connection);
    this.conn = connection;
  }

  @Override public String getId() {
    return conn.toString();
  }

  @Override public String getUserName() {
    return conn.user == null ? "" : conn.user.toString();
  }

  @Override public String getClusterId() {
    return conn.clusterId;
  }

  @Override public String getZookeeperQuorum() {
    try {
      return conn.getKeepAliveZooKeeperWatcher().getQuorum();
    } catch (Exception e) {
      return "";
    }
  }

  @Override public String getZookeeperBaseNode() {
    try {
      return conn.getKeepAliveZooKeeperWatcher().getBaseZNode();
    } catch (Exception e) {
      return "";
    }
  }

  @Override public int getMetaLookupPoolActiveCount() {
    if (conn.getCurrentMetaLookupPool() == null) {
      return 0;
    }
    ThreadPoolExecutor tpe = (ThreadPoolExecutor) conn.getCurrentMetaLookupPool();
    return tpe.getActiveCount();
  }

  @Override public int getMetaLookupPoolLargestPoolSize() {
    if (conn.getCurrentMetaLookupPool() == null) {
      return 0;
    }
    ThreadPoolExecutor tpe = (ThreadPoolExecutor) conn.getCurrentMetaLookupPool();
    return tpe.getLargestPoolSize();
  }

  @Override public String getBatchPoolId() {
    if (conn.getCurrentBatchPool() == null) {
      return "";
    }
    return Integer.toHexString(conn.getCurrentBatchPool().hashCode());
  }

  @Override public int getBatchPoolActiveCount() {
    if (conn.getCurrentBatchPool() == null) {
      return 0;
    }
    ThreadPoolExecutor tpe = (ThreadPoolExecutor) conn.getCurrentBatchPool();
    return tpe.getActiveCount();
  }

  @Override public int getBatchPoolLargestPoolSize() {
    if (conn.getCurrentBatchPool() == null) {
      return 0;
    }
    ThreadPoolExecutor tpe = (ThreadPoolExecutor) conn.getCurrentBatchPool();
    return tpe.getLargestPoolSize();
  }
}
