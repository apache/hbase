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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;

/**
 * Interface to Map of online regions.  In the  Map, the key is the region's
 * encoded name and the value is an {@link Region} instance.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface OnlineRegions extends Server {
  /**
   * Add to online regions.
   * @param r
   */
  void addToOnlineRegions(final Region r);

  /**
   * This method removes Region corresponding to hri from the Map of onlineRegions.
   *
   * @param r Region to remove.
   * @param destination Destination, if any, null otherwise.
   * @return True if we removed a region from online list.
   */
  boolean removeFromOnlineRegions(final Region r, ServerName destination);

  /**
   * Return {@link Region} instance.
   * Only works if caller is in same context, in same JVM. Region is not
   * serializable.
   * @param encodedRegionName
   * @return Region for the passed encoded <code>encodedRegionName</code> or
   * null if named region is not member of the online regions.
   */
  Region getFromOnlineRegions(String encodedRegionName);

   /**
    * Get all online regions of a table in this RS.
    * @param tableName
    * @return List of Region
    * @throws java.io.IOException
    */
   List<Region> getOnlineRegions(TableName tableName) throws IOException;
}
