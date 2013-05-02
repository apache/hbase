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
package org.apache.hadoop.hbase.master;

import java.util.Map;

import org.apache.hadoop.hbase.HServerLoad;

/**
 * This is the JMX management interface for Hbase master information
 */
public interface MXBean {

  /**
   * Required for MXBean implementation
   */
  public static interface RegionsInTransitionInfo {
    /**
     * Name of region in transition
     */
    public String getRegionName();
    /**
     * Current transition state
     */
    public String getRegionState();
    /**
     * Get Region Server name
     */
    public String getRegionServerName();
    /**
     * Get last update time
     */
    public long getLastUpdateTime();
  }

  /**
   * Get ServerName
   */
  public String getServerName();

  /**
   * Get Average Load
   * @return Average Load
   */
  public double getAverageLoad();

  /**
   * Get the Cluster ID
   * @return Cluster ID
   */
  public String getClusterId();

  /**
   * Get the Zookeeper Quorum Info
   * @return Zookeeper Quorum Info
   */
  public String getZookeeperQuorum();

  /**
   * Get the co-processors
   * @return Co-processors
   */
  public String[] getCoprocessors();

  /**
   * Get hbase master start time
   * @return Start time of master in milliseconds
   */
  public long getMasterStartTime();

  /**
   * Get the hbase master active time
   * @return Time in milliseconds when master became active
   */
  public long getMasterActiveTime();

  /**
   * Whether this master is the active master
   * @return True if this is the active master
   */
  public boolean getIsActiveMaster();

  /**
   * Get the live region servers
   * @return Live region servers
   */
  public Map<String, HServerLoad> getRegionServers();

  /**
   * Get the dead region servers
   * @return Dead region Servers
   */
  public String[] getDeadRegionServers();

  /**
   * Get information on regions in transition
   * @return Regions in transition
   */
  public RegionsInTransitionInfo[] getRegionsInTransition();

}
