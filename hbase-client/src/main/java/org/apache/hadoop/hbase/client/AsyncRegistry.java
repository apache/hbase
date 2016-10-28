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

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Implementations hold cluster information such as this cluster's id, location of hbase:meta, etc..
 * All stuffs that may be related to zookeeper at client side are placed here.
 * <p>
 * Most methods are executed asynchronously except getClusterId. It will be executed synchronously
 * and should be called only once when initialization.
 * <p>
 * Internal use only.
 */
@InterfaceAudience.Private
interface AsyncRegistry extends Closeable {

  /**
   * Get the location of meta region.
   */
  CompletableFuture<RegionLocations> getMetaRegionLocation();

  /**
   * Should only be called once.
   * <p>
   * The upper layer should store this value somewhere as it will not be change any more.
   */
  String getClusterId();

  /**
   * Get the number of 'running' regionservers.
   */
  CompletableFuture<Integer> getCurrentNrHRS();

  /**
   * Get the address of HMaster.
   */
  CompletableFuture<ServerName> getMasterAddress();

  /**
   * Get the info port of HMaster.
   */
  CompletableFuture<Integer> getMasterInfoPort();

  /**
   * Closes this instance and releases any system resources associated with it
   */
  @Override
  void close();
}
