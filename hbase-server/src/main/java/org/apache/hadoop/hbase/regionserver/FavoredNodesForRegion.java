/**
 *
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

import java.net.InetSocketAddress;
import java.util.List;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ServerName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Abstraction that allows different modules in RegionServer to update/get
 * the favored nodes information for regions. 
 */
@InterfaceAudience.Private
public interface FavoredNodesForRegion {
  /**
   * Used to update the favored nodes mapping when required.
   * @param encodedRegionName
   * @param favoredNodes
   */
  void updateRegionFavoredNodesMapping(String encodedRegionName, List<ServerName> favoredNodes);

  /**
   * Get the favored nodes mapping for this region. Used when the HDFS create API
   * is invoked to pass in favored nodes hints for new region files.
   * @param encodedRegionName
   * @return array containing the favored nodes' InetSocketAddresses
   */
  InetSocketAddress[] getFavoredNodesForRegion(String encodedRegionName);
}
