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

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.quotas.QuotaObserverChore;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;

/**
 * Impl for exposing HMaster Information through JMX
 */
@InterfaceAudience.Private
public class MetricsMasterWrapperImpl implements MetricsMasterWrapper {

  private final HMaster master;

  public MetricsMasterWrapperImpl(final HMaster master) {
    this.master = master;
  }

  @Override
  public double getAverageLoad() {
    return master.getAverageLoad();
  }

  @Override
  public long getSplitPlanCount() {
    return master.getSplitPlanCount();
  }

  @Override
  public long getMergePlanCount() {
    return master.getMergePlanCount();
  }

  @Override
  public long getMasterInitializationTime() {
    return master.getMasterFinishedInitializationTime();
  }

  @Override
  public String getClusterId() {
    return master.getClusterId();
  }

  @Override
  public String getZookeeperQuorum() {
    ZKWatcher zk = master.getZooKeeper();
    if (zk == null) {
      return "";
    }
    return zk.getQuorum();
  }

  @Override
  public String[] getCoprocessors() {
    return master.getMasterCoprocessors();
  }

  @Override
  public long getStartTime() {
    return master.getMasterStartTime();
  }

  @Override
  public long getActiveTime() {
    return master.getMasterActiveTime();
  }

  @Override
  public String getRegionServers() {
    ServerManager serverManager = this.master.getServerManager();
    if (serverManager == null) {
      return "";
    }
    return StringUtils.join(serverManager.getOnlineServers().keySet(), ";");
  }

  @Override
  public int getNumRegionServers() {
    ServerManager serverManager = this.master.getServerManager();
    if (serverManager == null) {
      return 0;
    }
    return serverManager.getOnlineServers().size();
  }

  @Override
  public String getDeadRegionServers() {
    ServerManager serverManager = this.master.getServerManager();
    if (serverManager == null) {
      return "";
    }
    return StringUtils.join(serverManager.getDeadServers().copyServerNames(), ";");
  }


  @Override
  public int getNumDeadRegionServers() {
    ServerManager serverManager = this.master.getServerManager();
    if (serverManager == null) {
      return 0;
    }
    return serverManager.getDeadServers().size();
  }

  @Override
  public String getServerName() {
    ServerName serverName = master.getServerName();
    if (serverName == null) {
      return "";
    }
    return serverName.getServerName();
  }

  @Override
  public boolean getIsActiveMaster() {
    return master.isActiveMaster();
  }

  @Override
  public long getNumWALFiles() {
    return master.getNumWALFiles();
  }

  @Override
  public Map<String,Entry<Long,Long>> getTableSpaceUtilization() {
    QuotaObserverChore quotaChore = master.getQuotaObserverChore();
    if (quotaChore == null) {
      return Collections.emptyMap();
    }
    Map<TableName,SpaceQuotaSnapshot> tableSnapshots = quotaChore.getTableQuotaSnapshots();
    Map<String,Entry<Long,Long>> convertedData = new HashMap<>();
    for (Entry<TableName,SpaceQuotaSnapshot> entry : tableSnapshots.entrySet()) {
      convertedData.put(entry.getKey().toString(), convertSnapshot(entry.getValue()));
    }
    return convertedData;
  }

  @Override
  public Map<String,Entry<Long,Long>> getNamespaceSpaceUtilization() {
    QuotaObserverChore quotaChore = master.getQuotaObserverChore();
    if (quotaChore == null) {
      return Collections.emptyMap();
    }
    Map<String,SpaceQuotaSnapshot> namespaceSnapshots = quotaChore.getNamespaceQuotaSnapshots();
    Map<String,Entry<Long,Long>> convertedData = new HashMap<>();
    for (Entry<String,SpaceQuotaSnapshot> entry : namespaceSnapshots.entrySet()) {
      convertedData.put(entry.getKey(), convertSnapshot(entry.getValue()));
    }
    return convertedData;
  }

  Entry<Long,Long> convertSnapshot(SpaceQuotaSnapshot snapshot) {
    return new SimpleImmutableEntry<Long,Long>(snapshot.getUsage(), snapshot.getLimit());
  }

  @Override
  public PairOfSameType<Integer> getRegionCounts() {
    try {
      if (!master.isInitialized()) {
        return new PairOfSameType<>(0, 0);
      }
      Integer onlineRegionCount = 0;
      Integer offlineRegionCount = 0;

      List<TableDescriptor> descriptors = master.listTableDescriptors(null, null,
          null, false);

      for (TableDescriptor htDesc : descriptors) {
        TableName tableName = htDesc.getTableName();
        Map<RegionState.State, List<RegionInfo>> tableRegions =
            master.getAssignmentManager().getRegionStates()
                .getRegionByStateOfTable(tableName);
        onlineRegionCount += tableRegions.get(RegionState.State.OPEN).size();
        offlineRegionCount += tableRegions.get(RegionState.State.OFFLINE).size();
      }
      return new PairOfSameType<>(onlineRegionCount, offlineRegionCount);
    } catch (IOException e) {
      return new PairOfSameType<>(0, 0);
    }
  }
}
