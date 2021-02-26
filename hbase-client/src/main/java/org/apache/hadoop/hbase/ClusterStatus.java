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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Objects;

/**
 * Status information on the HBase cluster.
 * <p>
 * <tt>ClusterStatus</tt> provides clients with information such as:
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
 * <tt>{@link ClusterMetrics.Option}</tt> provides a way to get desired ClusterStatus information.
 * The following codes will get all the cluster information.
 * <pre>
 * {@code
 * // Original version still works
 * Admin admin = connection.getAdmin();
 * ClusterStatus status = admin.getClusterStatus();
 * // or below, a new version which has the same effects
 * ClusterStatus status = admin.getClusterStatus(EnumSet.allOf(Option.class));
 * }
 * </pre>
 * If information about live servers is the only wanted.
 * then codes in the following way:
 * <pre>
 * {@code
 * Admin admin = connection.getAdmin();
 * ClusterStatus status = admin.getClusterStatus(EnumSet.of(Option.LIVE_SERVERS));
 * }
 * </pre>
 * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
 *             Use {@link ClusterMetrics} instead.
 */
@InterfaceAudience.Public
@Deprecated
public class ClusterStatus implements ClusterMetrics {

  // TODO: remove this in 3.0
  private static final byte VERSION = 2;

  private final ClusterMetrics metrics;

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   */
  @Deprecated
  public ClusterStatus(final String hbaseVersion, final String clusterid,
      final Map<ServerName, ServerLoad> servers,
      final Collection<ServerName> deadServers,
      final ServerName master,
      final Collection<ServerName> backupMasters,
      final List<RegionState> rit,
      final String[] masterCoprocessors,
      final Boolean balancerOn,
      final int masterInfoPort) {
    // TODO: make this constructor private
    this(ClusterMetricsBuilder.newBuilder().setHBaseVersion(hbaseVersion)
      .setDeadServerNames(new ArrayList<>(deadServers))
      .setLiveServerMetrics(servers.entrySet().stream()
      .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())))
      .setBackerMasterNames(new ArrayList<>(backupMasters)).setBalancerOn(balancerOn)
      .setClusterId(clusterid)
      .setMasterCoprocessorNames(Arrays.asList(masterCoprocessors))
      .setMasterName(master)
      .setMasterInfoPort(masterInfoPort)
      .setRegionsInTransition(rit)
      .build());
  }

  @InterfaceAudience.Private
  public ClusterStatus(ClusterMetrics metrics) {
    this.metrics = metrics;
  }

  /**
   * @return the names of region servers on the dead list
   */
  @Override
  public List<ServerName> getDeadServerNames() {
    return metrics.getDeadServerNames();
  }

  @Override
  public Map<ServerName, ServerMetrics> getLiveServerMetrics() {
    return metrics.getLiveServerMetrics();
  }

  /**
  * @return the number of region servers in the cluster
  * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
  *             Use {@link #getLiveServerMetrics()}.
  */
  @Deprecated
  public int getServersSize() {
    return metrics.getLiveServerMetrics().size();
  }

  /**
   * @return the number of dead region servers in the cluster
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-13656">HBASE-13656</a>).
   *             Use {@link #getDeadServerNames()}.
   */
  @Deprecated
  public int getDeadServers() {
    return getDeadServersSize();
  }

  /**
   * @return the number of dead region servers in the cluster
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getDeadServerNames()}.
   */
  @Deprecated
  public int getDeadServersSize() {
    return metrics.getDeadServerNames().size();
  }

  /**
   * @return the number of regions deployed on the cluster
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getRegionCount()}.
   */
  @Deprecated
  public int getRegionsCount() {
    return getRegionCount();
  }

  /**
   * @return the number of requests since last report
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getRequestCount()} instead.
   */
  @Deprecated
  public int getRequestsCount() {
    return (int) getRequestCount();
  }

  @Nullable
  @Override
  public ServerName getMasterName() {
    return metrics.getMasterName();
  }

  @Override
  public List<ServerName> getBackupMasterNames() {
    return metrics.getBackupMasterNames();
  }

  @Override
  public List<RegionState> getRegionStatesInTransition() {
    return metrics.getRegionStatesInTransition();
  }

  /**
   * @return the HBase version string as reported by the HMaster
   */
  public String getHBaseVersion() {
    return metrics.getHBaseVersion();
  }

  private Map<ServerName, ServerLoad> getLiveServerLoads() {
    return metrics.getLiveServerMetrics().entrySet().stream()
      .collect(Collectors.toMap(e -> e.getKey(), e -> new ServerLoad(e.getValue())));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ClusterStatus)) {
      return false;
    }
    ClusterStatus other = (ClusterStatus) o;
    return Objects.equal(getHBaseVersion(), other.getHBaseVersion()) &&
      Objects.equal(getLiveServerLoads(), other.getLiveServerLoads()) &&
      getDeadServerNames().containsAll(other.getDeadServerNames()) &&
      Arrays.equals(getMasterCoprocessors(), other.getMasterCoprocessors()) &&
      Objects.equal(getMaster(), other.getMaster()) &&
      getBackupMasters().containsAll(other.getBackupMasters()) &&
      Objects.equal(getClusterId(), other.getClusterId()) &&
      getMasterInfoPort() == other.getMasterInfoPort();
  }

  @Override
  public int hashCode() {
    return metrics.hashCode();
  }

  /**
   * @return the object version number
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   */
  @Deprecated
  public byte getVersion() {
    return VERSION;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getLiveServerMetrics()} instead.
   */
  @Deprecated
  public Collection<ServerName> getServers() {
    return metrics.getLiveServerMetrics().keySet();
  }

  /**
   * Returns detailed information about the current master {@link ServerName}.
   * @return current master information if it exists
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getMasterName} instead.
   */
  @Deprecated
  public ServerName getMaster() {
    return metrics.getMasterName();
  }

  /**
   * @return the number of backup masters in the cluster
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getBackupMasterNames} instead.
   */
  @Deprecated
  public int getBackupMastersSize() {
    return metrics.getBackupMasterNames().size();
  }

  /**
   * @return the names of backup masters
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getBackupMasterNames} instead.
   */
  @Deprecated
  public List<ServerName> getBackupMasters() {
    return metrics.getBackupMasterNames();
  }

  /**
   * @param sn
   * @return Server's load or null if not found.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getLiveServerMetrics} instead.
   */
  @Deprecated
  public ServerLoad getLoad(final ServerName sn) {
    ServerMetrics serverMetrics = metrics.getLiveServerMetrics().get(sn);
    return serverMetrics == null ? null : new ServerLoad(serverMetrics);
  }

  public String getClusterId() {
    return metrics.getClusterId();
  }

  @Override
  public List<String> getMasterCoprocessorNames() {
    return metrics.getMasterCoprocessorNames();
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getMasterCoprocessorNames} instead.
   */
  @Deprecated
  public String[] getMasterCoprocessors() {
    List<String> rval = metrics.getMasterCoprocessorNames();
    return rval.toArray(new String[rval.size()]);
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getLastMajorCompactionTimestamp(TableName)} instead.
   */
  @Deprecated
  public long getLastMajorCompactionTsForTable(TableName table) {
    return metrics.getLastMajorCompactionTimestamp(table);
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link #getLastMajorCompactionTimestamp(byte[])} instead.
   */
  @Deprecated
  public long getLastMajorCompactionTsForRegion(final byte[] region) {
    return metrics.getLastMajorCompactionTimestamp(region);
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             No flag in 2.0
   */
  @Deprecated
  public boolean isBalancerOn() {
    return metrics.getBalancerOn() != null && metrics.getBalancerOn();
  }

  @Override
  public Boolean getBalancerOn() {
    return metrics.getBalancerOn();
  }

  @Override
  public int getMasterInfoPort() {
    return metrics.getMasterInfoPort();
  }

  @Override
  public List<ServerName> getServersName() {
    return metrics.getServersName();
  }

  @Override
  public Map<TableName, RegionStatesCount> getTableRegionStatesCount() {
    return metrics.getTableRegionStatesCount();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(1024);
    sb.append("Master: " + metrics.getMasterName());

    int backupMastersSize = getBackupMastersSize();
    sb.append("\nNumber of backup masters: " + backupMastersSize);
    if (backupMastersSize > 0) {
      for (ServerName serverName: metrics.getBackupMasterNames()) {
        sb.append("\n  " + serverName);
      }
    }

    int serversSize = getServersSize();
    int serversNameSize = getServersName().size();
    sb.append("\nNumber of live region servers: "
        + (serversSize > 0 ? serversSize : serversNameSize));
    if (serversSize > 0) {
      for (ServerName serverName : metrics.getLiveServerMetrics().keySet()) {
        sb.append("\n  " + serverName.getServerName());
      }
    } else if (serversNameSize > 0) {
      for (ServerName serverName : getServersName()) {
        sb.append("\n  " + serverName.getServerName());
      }
    }

    int deadServerSize = metrics.getDeadServerNames().size();
    sb.append("\nNumber of dead region servers: " + deadServerSize);
    if (deadServerSize > 0) {
      for (ServerName serverName : metrics.getDeadServerNames()) {
        sb.append("\n  " + serverName);
      }
    }

    sb.append("\nAverage load: " + getAverageLoad());
    sb.append("\nNumber of requests: " + getRequestCount());
    sb.append("\nNumber of regions: " + getRegionsCount());

    int ritSize = metrics.getRegionStatesInTransition().size();
    sb.append("\nNumber of regions in transition: " + ritSize);
    if (ritSize > 0) {
      for (RegionState state: metrics.getRegionStatesInTransition()) {
        sb.append("\n  " + state.toDescriptiveString());
      }
    }
    return sb.toString();
  }
}
