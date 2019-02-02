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
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;

/**
 * Can be overridden in UT if you only want to implement part of the methods in
 * {@link AsyncRegistry}.
 */
public class DummyAsyncRegistry implements AsyncRegistry {

  public static final String REGISTRY_IMPL_CONF_KEY = AsyncRegistryFactory.REGISTRY_IMPL_CONF_KEY;

  @Override
  public CompletableFuture<RegionLocations> getMetaRegionLocation() {
    return null;
  }

  @Override
  public CompletableFuture<String> getClusterId() {
    return null;
  }

  @Override
  public CompletableFuture<Integer> getCurrentNrHRS() {
    return null;
  }

  @Override
  public CompletableFuture<ServerName> getMasterAddress() {
    return null;
  }

  @Override
  public CompletableFuture<Integer> getMasterInfoPort() {
    return null;
  }

  @Override
  public void close() {
  }
}
