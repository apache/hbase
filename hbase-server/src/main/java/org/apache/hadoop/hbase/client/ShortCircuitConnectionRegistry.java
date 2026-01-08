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

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A {@link ConnectionRegistry} implementation used at server side, where we could use the
 * {@link ConnectionRegistryEndpoint} directly, without any rpcs.
 */
@InterfaceAudience.Private
class ShortCircuitConnectionRegistry implements ConnectionRegistry {

  private final ConnectionRegistryEndpoint endpoint;

  public ShortCircuitConnectionRegistry(ConnectionRegistryEndpoint endpoint) {
    this.endpoint = endpoint;
  }

  @Override
  public CompletableFuture<RegionLocations> getMetaRegionLocations() {
    CompletableFuture<RegionLocations> future = new CompletableFuture<>();
    List<HRegionLocation> locs = endpoint.getMetaLocations();
    if (locs.isEmpty()) {
      future.completeExceptionally(new IOException("no meta location available"));
    } else {
      future.complete(new RegionLocations(locs));
    }
    return future;
  }

  @Override
  public CompletableFuture<String> getClusterId() {
    return CompletableFuture.completedFuture(endpoint.getClusterId());
  }

  @Override
  public CompletableFuture<ServerName> getActiveMaster() {
    CompletableFuture<ServerName> future = new CompletableFuture<>();
    Optional<ServerName> activeMaster = endpoint.getActiveMaster();
    if (activeMaster.isPresent()) {
      future.complete(activeMaster.get());
    } else {
      future.completeExceptionally(new IOException("no active master available"));
    }
    return future;
  }

  @Override
  public CompletableFuture<org.apache.hadoop.hbase.TableName> getMetaTableName() {
    return CompletableFuture.completedFuture(endpoint.getMetaTableName());
  }

  @Override
  public String getConnectionString() {
    return "short-circuit";
  }

  @Override
  public void close() {
    // nothing
  }
}
