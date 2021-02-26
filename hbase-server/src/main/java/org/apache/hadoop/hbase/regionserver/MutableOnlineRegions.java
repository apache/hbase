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

import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interface to Map of online regions.  In the  Map, the key is the region's
 * encoded name and the value is an {@link Region} instance.
 */
@InterfaceAudience.Private
public interface MutableOnlineRegions extends OnlineRegions {

  /**
   * Add to online regions.
   * @param r
   */
  void addRegion(final HRegion r);

  /**
   * Removes the given Region from the list of onlineRegions.
   * @param r Region to remove.
   * @param destination Destination, if any, null otherwise.
   * @return True if we removed a region from online list.
   */
  boolean removeRegion(final HRegion r, ServerName destination);
}
