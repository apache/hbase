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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.shaded.com.google.common.base.Objects;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.master.RegionState;

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
 * <tt>{@link Option}</tt> provides a way to get desired ClusterStatus information.
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
 */
@InterfaceAudience.Public
public class ClusterStatus {

  // TODO: remove this in 3.0
  private static final byte VERSION = 2;

  private String hbaseVersion;
  private Map<ServerName, ServerLoad> liveServers;
  private List<ServerName> deadServers;
  private ServerName master;
  private List<ServerName> backupMasters;
  private List<RegionState> intransition;
  private String clusterId;
  private String[] masterCoprocessors;
  private Boolean balancerOn;
  private int masterInfoPort;

  /**
   * Use {@link ClusterStatus.Builder} to construct a ClusterStatus instead.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-15511">HBASE-15511</a>).
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
    this.hbaseVersion = hbaseVersion;
    this.liveServers = servers;
    this.deadServers = new ArrayList<>(deadServers);
    this.master = master;
    this.backupMasters = new ArrayList<>(backupMasters);
    this.intransition = rit;
    this.clusterId = clusterid;
    this.masterCoprocessors = masterCoprocessors;
    this.balancerOn = balancerOn;
    this.masterInfoPort = masterInfoPort;
  }

  private ClusterStatus(final String hbaseVersion, final String clusterid,
      final Map<ServerName, ServerLoad> servers,
      final List<ServerName> deadServers,
      final ServerName master,
      final List<ServerName> backupMasters,
      final List<RegionState> rit,
      final String[] masterCoprocessors,
      final Boolean balancerOn,
      final int masterInfoPort) {
    this.hbaseVersion = hbaseVersion;
    this.liveServers = servers;
    this.deadServers = deadServers;
    this.master = master;
    this.backupMasters = backupMasters;
    this.intransition = rit;
    this.clusterId = clusterid;
    this.masterCoprocessors = masterCoprocessors;
    this.balancerOn = balancerOn;
    this.masterInfoPort = masterInfoPort;
  }

  /**
   * @return the names of region servers on the dead list
   */
  public List<ServerName> getDeadServerNames() {
    if (deadServers == null) {
      return Collections.emptyList();
    }
    return Collections.unmodifiableList(deadServers);
  }

  /**
   * @return the number of region servers in the cluster
   */
  public int getServersSize() {
    return liveServers != null ? liveServers.size() : 0;
  }

  /**
   * @return the number of dead region servers in the cluster
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-13656">HBASE-13656</a>).
   *             Use {@link #getDeadServersSize()}.
   */
  @Deprecated
  public int getDeadServers() {
    return getDeadServersSize();
  }

  /**
   * @return the number of dead region servers in the cluster
   */
  public int getDeadServersSize() {
    return deadServers != null ? deadServers.size() : 0;
  }


  /**
   * @return the average cluster load
   */
  public double getAverageLoad() {
    int load = getRegionsCount();
    int serverSize = getServersSize();
    return serverSize != 0 ? (double)load / (double)serverSize : 0.0;
  }

  /**
   * @return the number of regions deployed on the cluster
   */
  public int getRegionsCount() {
    int count = 0;
    if (liveServers != null && !liveServers.isEmpty()) {
      for (Map.Entry<ServerName, ServerLoad> e: this.liveServers.entrySet()) {
        count += e.getValue().getNumberOfRegions();
      }
    }
    return count;
  }

  /**
   * @return the number of requests since last report
   */
  public int getRequestsCount() {
    int count = 0;
    if (liveServers != null && !liveServers.isEmpty()) {
      for (Map.Entry<ServerName, ServerLoad> e: this.liveServers.entrySet()) {
        count += e.getValue().getNumberOfRequests();
      }
    }
    return count;
  }

  /**
   * @return the HBase version string as reported by the HMaster
   */
  public String getHBaseVersion() {
    return hbaseVersion;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ClusterStatus)) {
      return false;
    }
    ClusterStatus other = (ClusterStatus) o;
    return Objects.equal(getHBaseVersion(), other.getHBaseVersion()) &&
      Objects.equal(this.liveServers, other.liveServers) &&
      getDeadServerNames().containsAll(other.getDeadServerNames()) &&
      Arrays.equals(getMasterCoprocessors(), other.getMasterCoprocessors()) &&
      Objects.equal(getMaster(), other.getMaster()) &&
      getBackupMasters().containsAll(other.getBackupMasters()) &&
      Objects.equal(getClusterId(), other.getClusterId()) &&
      getMasterInfoPort() == other.getMasterInfoPort();
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  public int hashCode() {
    return Objects.hashCode(hbaseVersion, liveServers, deadServers, master, backupMasters,
      clusterId, masterInfoPort);
  }

  /**
   *
   * @return the object version number
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   */
  @Deprecated
  public byte getVersion() {
    return VERSION;
  }

  //
  // Getters
  //

  public Collection<ServerName> getServers() {
    if (liveServers == null) {
      return Collections.emptyList();
    }
    return Collections.unmodifiableCollection(this.liveServers.keySet());
  }

  /**
   * Returns detailed information about the current master {@link ServerName}.
   * @return current master information if it exists
   */
  public ServerName getMaster() {
    return this.master;
  }

  /**
   * @return the number of backup masters in the cluster
   */
  public int getBackupMastersSize() {
    return backupMasters != null ? backupMasters.size() : 0;
  }

  /**
   * @return the names of backup masters
   */
  public List<ServerName> getBackupMasters() {
    if (backupMasters == null) {
      return Collections.emptyList();
    }
    return Collections.unmodifiableList(this.backupMasters);
  }

  /**
   * @param sn
   * @return Server's load or null if not found.
   */
  public ServerLoad getLoad(final ServerName sn) {
    return liveServers != null ? liveServers.get(sn) : null;
  }

  @InterfaceAudience.Private
  public List<RegionState> getRegionsInTransition() {
    if (intransition == null) {
      return Collections.emptyList();
    }
    return Collections.unmodifiableList(intransition);
  }

  public String getClusterId() {
    return clusterId;
  }

  public String[] getMasterCoprocessors() {
    return masterCoprocessors;
  }

  public long getLastMajorCompactionTsForTable(TableName table) {
    long result = Long.MAX_VALUE;
    for (ServerName server : getServers()) {
      ServerLoad load = getLoad(server);
      for (RegionLoad rl : load.getRegionsLoad().values()) {
        if (table.equals(HRegionInfo.getTable(rl.getName()))) {
          result = Math.min(result, rl.getLastMajorCompactionTs());
        }
      }
    }
    return result == Long.MAX_VALUE ? 0 : result;
  }

  public long getLastMajorCompactionTsForRegion(final byte[] region) {
    for (ServerName server : getServers()) {
      ServerLoad load = getLoad(server);
      RegionLoad rl = load.getRegionsLoad().get(region);
      if (rl != null) {
        return rl.getLastMajorCompactionTs();
      }
    }
    return 0;
  }

  public boolean isBalancerOn() {
    return balancerOn != null && balancerOn;
  }

  public Boolean getBalancerOn() {
    return balancerOn;
  }

  public int getMasterInfoPort() {
    return masterInfoPort;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder(1024);
    sb.append("Master: " + master);

    int backupMastersSize = getBackupMastersSize();
    sb.append("\nNumber of backup masters: " + backupMastersSize);
    if (backupMastersSize > 0) {
      for (ServerName serverName: backupMasters) {
        sb.append("\n  " + serverName);
      }
    }

    int serversSize = getServersSize();
    sb.append("\nNumber of live region servers: " + serversSize);
    if (serversSize > 0) {
      for (ServerName serverName: liveServers.keySet()) {
        sb.append("\n  " + serverName.getServerName());
      }
    }

    int deadServerSize = getDeadServersSize();
    sb.append("\nNumber of dead region servers: " + deadServerSize);
    if (deadServerSize > 0) {
      for (ServerName serverName: deadServers) {
        sb.append("\n  " + serverName);
      }
    }

    sb.append("\nAverage load: " + getAverageLoad());
    sb.append("\nNumber of requests: " + getRequestsCount());
    sb.append("\nNumber of regions: " + getRegionsCount());

    int ritSize = (intransition != null) ? intransition.size() : 0;
    sb.append("\nNumber of regions in transition: " + ritSize);
    if (ritSize > 0) {
      for (RegionState state: intransition) {
        sb.append("\n  " + state.toDescriptiveString());
      }
    }
    return sb.toString();
  }

  @InterfaceAudience.Private
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for construct a ClusterStatus.
   */
  @InterfaceAudience.Private
  public static class Builder {
    private String hbaseVersion = null;
    private Map<ServerName, ServerLoad> liveServers = null;
    private List<ServerName> deadServers = null;
    private ServerName master = null;
    private List<ServerName> backupMasters = null;
    private List<RegionState> intransition = null;
    private String clusterId = null;
    private String[] masterCoprocessors = null;
    private Boolean balancerOn = null;
    private int masterInfoPort = -1;

    private Builder() {}

    public Builder setHBaseVersion(String hbaseVersion) {
      this.hbaseVersion = hbaseVersion;
      return this;
    }

    public Builder setLiveServers(Map<ServerName, ServerLoad> liveServers) {
      this.liveServers = liveServers;
      return this;
    }

    public Builder setDeadServers(List<ServerName> deadServers) {
      this.deadServers = deadServers;
      return this;
    }

    public Builder setMaster(ServerName master) {
      this.master = master;
      return this;
    }

    public Builder setBackupMasters(List<ServerName> backupMasters) {
      this.backupMasters = backupMasters;
      return this;
    }

    public Builder setRegionState(List<RegionState> intransition) {
      this.intransition = intransition;
      return this;
    }

    public Builder setClusterId(String clusterId) {
      this.clusterId = clusterId;
      return this;
    }

    public Builder setMasterCoprocessors(String[] masterCoprocessors) {
      this.masterCoprocessors = masterCoprocessors;
      return this;
    }

    public Builder setBalancerOn(Boolean balancerOn) {
      this.balancerOn = balancerOn;
      return this;
    }

    public Builder setMasterInfoPort(int masterInfoPort) {
      this.masterInfoPort = masterInfoPort;
      return this;
    }

    public ClusterStatus build() {
      return new ClusterStatus(hbaseVersion, clusterId, liveServers,
          deadServers, master, backupMasters, intransition, masterCoprocessors,
          balancerOn, masterInfoPort);
    }
  }

  /**
   * Kinds of ClusterStatus
   */
  public enum Option {
    HBASE_VERSION, /** status about hbase version */
    CLUSTER_ID, /** status about cluster id */
    BALANCER_ON, /** status about balancer is on or not */
    LIVE_SERVERS, /** status about live region servers */
    DEAD_SERVERS, /** status about dead region servers */
    MASTER, /** status about master */
    BACKUP_MASTERS, /** status about backup masters */
    MASTER_COPROCESSORS, /** status about master coprocessors */
    REGIONS_IN_TRANSITION, /** status about regions in transition */
    MASTER_INFO_PORT; /** master info port **/
  }
}
