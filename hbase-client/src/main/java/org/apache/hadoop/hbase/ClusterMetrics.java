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

package org.apache.hadoop.hbase;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Metrics information on the HBase cluster.
 * <p>
 * <tt>ClusterMetrics</tt> provides clients with information such as:
 * <ul>
 * <li>The count and names of region servers in the cluster.</li>
 * <li>The count and names of dead region servers in the cluster.</li>
 * <li>The name of the active master for the cluster.</li>
 * <li>The name(s) of the backup master(s) for the cluster, if they exist.</li>
 * <li>The average cluster load.</li>
 * <li>The number of regions deployed on the cluster.</li>
 * <li>The number of requests since last report.</li>
 * <li>Detailed region server loading and resource usage information,
 *  per server and per region.</li>
 * <li>Regions in transition at master</li>
 * <li>The unique cluster ID</li>
 * </ul>
 * <tt>{@link Option}</tt> provides a way to get desired ClusterStatus information.
 * The following codes will get all the cluster information.
 * <pre>
 * {@code
 * // Original version still works
 * Admin admin = connection.getAdmin();
 * ClusterMetrics metrics = admin.getClusterStatus();
 * // or below, a new version which has the same effects
 * ClusterMetrics metrics = admin.getClusterStatus(EnumSet.allOf(Option.class));
 * }
 * </pre>
 * If information about live servers is the only wanted.
 * then codes in the following way:
 * <pre>
 * {@code
 * Admin admin = connection.getAdmin();
 * ClusterMetrics metrics = admin.getClusterStatus(EnumSet.of(Option.LIVE_SERVERS));
 * }
 * </pre>
 */
@InterfaceAudience.Public
public interface ClusterMetrics {

  /**
   * @return the HBase version string as reported by the HMaster
   */
  @Nullable
  String getHBaseVersion();

  /**
   * @return the names of region servers on the dead list
   */
  List<ServerName> getDeadServerNames();

  /**
   * @return the names of region servers on the live list
   */
  Map<ServerName, ServerMetrics> getLiveServerMetrics();

  /**
   * @return the number of regions deployed on the cluster
   */
  default int getRegionCount() {
    return getLiveServerMetrics().entrySet().stream()
        .mapToInt(v -> v.getValue().getRegionMetrics().size()).sum();
  }

  /**
   * @return the number of requests since last report
   */
  default long getRequestCount() {
    return getLiveServerMetrics().entrySet().stream()
        .flatMap(v -> v.getValue().getRegionMetrics().values().stream())
        .mapToLong(RegionMetrics::getRequestCount).sum();
  }

  /**
   * Returns detailed information about the current master {@link ServerName}.
   * @return current master information if it exists
   */
  @Nullable
  ServerName getMasterName();

  /**
   * @return the names of backup masters
   */
  List<ServerName> getBackupMasterNames();

  @InterfaceAudience.Private
  List<RegionState> getRegionStatesInTransition();

  @Nullable
  String getClusterId();

  List<String> getMasterCoprocessorNames();

  default long getLastMajorCompactionTimestamp(TableName table) {
    return getLiveServerMetrics().values().stream()
        .flatMap(s -> s.getRegionMetrics().values().stream())
        .filter(r -> RegionInfo.getTable(r.getRegionName()).equals(table))
        .mapToLong(RegionMetrics::getLastMajorCompactionTimestamp).min().orElse(0);
  }

  default long getLastMajorCompactionTimestamp(byte[] regionName) {
    return getLiveServerMetrics().values().stream()
        .filter(s -> s.getRegionMetrics().containsKey(regionName))
        .findAny()
        .map(s -> s.getRegionMetrics().get(regionName).getLastMajorCompactionTimestamp())
        .orElse(0L);
  }

  @Nullable
  Boolean getBalancerOn();

  int getMasterInfoPort();

  List<ServerName> getServersName();

  /**
   * @return the average cluster load
   */
  default double getAverageLoad() {
    int serverSize = getLiveServerMetrics().size();
    if (serverSize == 0) {
      return 0;
    }
    return (double)getRegionCount() / (double)serverSize;
  }

  /**
   * Provide region states count for given table.
   * e.g howmany regions of give table are opened/closed/rit etc
   *
   * @return map of table to region states count
   */
  Map<TableName, RegionStatesCount> getTableRegionStatesCount();

  /**
   * Provide the list of master tasks
   */
  @Nullable
  List<ServerTask> getMasterTasks();

  /**
   * Kinds of ClusterMetrics
   */
  enum Option {
    /**
     * metrics about hbase version
     */
    HBASE_VERSION,
    /**
     * metrics about cluster id
     */
    CLUSTER_ID,
    /**
     * metrics about balancer is on or not
     */
    BALANCER_ON,
    /**
     * metrics about live region servers
     */
    LIVE_SERVERS,
    /**
     * metrics about dead region servers
     */
    DEAD_SERVERS,
    /**
     * metrics about master name
     */
    MASTER,
    /**
     * metrics about backup masters name
     */
    BACKUP_MASTERS,
    /**
     * metrics about master coprocessors
     */
    MASTER_COPROCESSORS,
    /**
     * metrics about regions in transition
     */
    REGIONS_IN_TRANSITION,
    /**
     * metrics info port
     */
    MASTER_INFO_PORT,
    /**
     * metrics about live region servers name
     */
    SERVERS_NAME,
    /**
     * metrics about table to no of regions status count
     */
    TABLE_TO_REGIONS_COUNT,
    /**
     * metrics about monitored tasks
     */
    TASKS,
  }
}
