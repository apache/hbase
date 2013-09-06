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

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HRegionLocation;

/**
 * Cluster registry.
 * Implemenations hold cluster information such as this cluster's id, location of hbase:meta, etc.
 */
interface Registry {
  /**
   * @param connection
   */
  void init(HConnection connection);

  /**
   * @return Meta region location
   * @throws IOException
   */
  HRegionLocation getMetaRegionLocation() throws IOException;

  /**
   * @return Cluster id.
   */
  String getClusterId();

  /**
   * @param enabled Return true if table is enabled
   * @throws IOException
   */
  boolean isTableOnlineState(TableName tableName, boolean enabled) throws IOException;

  /**
   * @return Count of 'running' regionservers
   * @throws IOException
   */
  int getCurrentNrHRS() throws IOException;
}