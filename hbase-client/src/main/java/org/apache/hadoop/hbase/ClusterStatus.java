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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.Map;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.io.VersionedWritable;


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
 */
@InterfaceAudience.Public
public class ClusterStatus extends VersionedWritable {
  /**
   * Version for object serialization.  Incremented for changes in serialized
   * representation.
   * <dl>
   *   <dt>0</dt> <dd>Initial version</dd>
   *   <dt>1</dt> <dd>Added cluster ID</dd>
   *   <dt>2</dt> <dd>Added Map of ServerName to ServerLoad</dd>
   *   <dt>3</dt> <dd>Added master and backupMasters</dd>
   * </dl>
   */
  private static final byte VERSION = 2;

  private String hbaseVersion;
  private Map<ServerName, ServerLoad> liveServers;
  private Collection<ServerName> deadServers;
  private ServerName master;
  private Collection<ServerName> backupMasters;
  private Set<RegionState> intransition;
  private String clusterId;
  private String[] masterCoprocessors;
  private Boolean balancerOn;

  public ClusterStatus(final String hbaseVersion, final String clusterid,
      final Map<ServerName, ServerLoad> servers,
      final Collection<ServerName> deadServers,
      final ServerName master,
      final Collection<ServerName> backupMasters,
      final Set<RegionState> rit,
      final String[] masterCoprocessors,
      final Boolean balancerOn) {
    this.hbaseVersion = hbaseVersion;

    this.liveServers = servers;
    this.deadServers = deadServers;
    this.master = master;
    this.backupMasters = backupMasters;
    this.intransition = rit;
    this.clusterId = clusterid;
    this.masterCoprocessors = masterCoprocessors;
    this.balancerOn = balancerOn;
  }

  /**
   * @return the names of region servers on the dead list
   */
  public Collection<ServerName> getDeadServerNames() {
    if (deadServers == null) {
      return Collections.<ServerName>emptyList();
    }
    return Collections.unmodifiableCollection(deadServers);
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
    return (double)load / (double)getServersSize();
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
    return (getVersion() == ((ClusterStatus)o).getVersion()) &&
      getHBaseVersion().equals(((ClusterStatus)o).getHBaseVersion()) &&
      this.liveServers.equals(((ClusterStatus)o).liveServers) &&
      this.deadServers.containsAll(((ClusterStatus)o).deadServers) &&
      Arrays.equals(this.masterCoprocessors,
                    ((ClusterStatus)o).masterCoprocessors) &&
      this.master.equals(((ClusterStatus)o).master) &&
      this.backupMasters.containsAll(((ClusterStatus)o).backupMasters);
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  public int hashCode() {
    return VERSION + hbaseVersion.hashCode() + this.liveServers.hashCode() +
      this.deadServers.hashCode() + this.master.hashCode() +
      this.backupMasters.hashCode();
  }

  /** @return the object version number */
  public byte getVersion() {
    return VERSION;
  }

  //
  // Getters
  //

  public Collection<ServerName> getServers() {
    if (liveServers == null) {
      return Collections.<ServerName>emptyList();
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
  public Collection<ServerName> getBackupMasters() {
    if (backupMasters == null) {
      return Collections.<ServerName>emptyList();
    }
    return Collections.unmodifiableCollection(this.backupMasters);
  }

  /**
   * @param sn
   * @return Server's load or null if not found.
   */
  public ServerLoad getLoad(final ServerName sn) {
    return liveServers != null ? liveServers.get(sn) : null;
  }

  @InterfaceAudience.Private
  public Set<RegionState> getRegionsInTransition() {
    return this.intransition;
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
}
