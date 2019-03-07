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

import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.FutureUtils;

/**
 * Simple cluster registry inserted in place of our usual zookeeper based one.
 */
class SimpleRegistry extends DoNothingAsyncRegistry {

  private final ServerName metaHost;

  volatile boolean closed = false;

  private static final String META_HOST_CONFIG_NAME = "hbase.client.simple-registry.meta.host";

  private static final String DEFAULT_META_HOST = "meta.example.org.16010,12345";

  public static void setMetaHost(Configuration conf, ServerName metaHost) {
    conf.set(META_HOST_CONFIG_NAME, metaHost.getServerName());
  }

  public SimpleRegistry(Configuration conf) {
    super(conf);
    this.metaHost = ServerName.valueOf(conf.get(META_HOST_CONFIG_NAME, DEFAULT_META_HOST));
  }

  @Override
  public CompletableFuture<RegionLocations> getMetaRegionLocation() {
    if (closed) {
      return FutureUtils.failedFuture(new DoNotRetryIOException("Client already closed"));
    } else {
      return CompletableFuture.completedFuture(new RegionLocations(
        new HRegionLocation(RegionInfoBuilder.FIRST_META_REGIONINFO, metaHost)));
    }
  }

  @Override
  public CompletableFuture<String> getClusterId() {
    if (closed) {
      return FutureUtils.failedFuture(new DoNotRetryIOException("Client already closed"));
    } else {
      return CompletableFuture.completedFuture(HConstants.CLUSTER_ID_DEFAULT);
    }
  }

  @Override
  public CompletableFuture<Integer> getCurrentNrHRS() {
    if (closed) {
      return FutureUtils.failedFuture(new DoNotRetryIOException("Client already closed"));
    } else {
      return CompletableFuture.completedFuture(1);
    }
  }

  @Override
  public void close() {
    closed = true;
  }
}