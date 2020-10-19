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

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * There are two modes with meta replica support.
 *   HighAvailable    - Client sends requests to the primary meta region first, within a
 *                      configured amount of time, if  there is no response coming back,
 *                      client sends requests to all replica regions and takes the first
 *                      response.
 *
 *   LoadBalance      - Client sends requests to meta replica regions in a round-robin mode,
 *                      if results from replica regions are stale, next time, client sends requests for
 *                      these stable locations to the primary meta region. In this mode, scan
 *                      requests are load balanced across all replica regions.
 */
enum MetaReplicaMode {
  None,
  HighAvailable,
  LoadBalance
}

/**
 * A Meta replica chooser decides which meta replica to go for scan requests.
 */
@InterfaceAudience.Private
public interface MetaReplicaLoadBalanceReplicaChooser {

  void updateCacheOnError(final HRegionLocation loc, final int fromMetaReplicaId);
  int chooseReplicaToGo(final TableName tablename, final byte[] row,
    final RegionLocateType locateType);


}
