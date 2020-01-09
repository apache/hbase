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

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Implementations hold cluster information such as this cluster's id, location of hbase:meta, etc..
 * Internal use only.
 */
@InterfaceAudience.Private
interface AsyncRegistry extends Closeable {
  /**
   * A completed CompletableFuture to host default hbase:meta table state (ENABLED).
   */
  TableState ENABLED_META_TABLE_STATE =
    new TableState(TableName.META_TABLE_NAME, TableState.State.ENABLED);
  CompletableFuture<TableState> COMPLETED_GET_META_TABLE_STATE =
    CompletableFuture.completedFuture(ENABLED_META_TABLE_STATE);

  /**
   * Get the location of meta region.
   */
  CompletableFuture<RegionLocations> getMetaRegionLocation();

  /**
   * The hbase:meta table state.
   */
  default CompletableFuture<TableState> getMetaTableState() {
    return COMPLETED_GET_META_TABLE_STATE;
  }

  /**
   * Should only be called once.
   * <p>
   * The upper layer should store this value somewhere as it will not be change any more.
   */
  CompletableFuture<String> getClusterId();

  /**
   * Get the address of HMaster.
   */
  CompletableFuture<ServerName> getMasterAddress();

  /**
   * Closes this instance and releases any system resources associated with it
   */
  @Override
  void close();
}
