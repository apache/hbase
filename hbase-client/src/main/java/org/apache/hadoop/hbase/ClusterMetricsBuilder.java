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
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.Option;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FSProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

@InterfaceAudience.Private
public final class ClusterMetricsBuilder {

  public static ClusterStatusProtos.ClusterStatus toClusterStatus(ClusterMetrics metrics) {
    ClusterStatusProtos.ClusterStatus.Builder builder
        = ClusterStatusProtos.ClusterStatus.newBuilder()
        .addAllBackupMasters(metrics.getBackupMasterNames().stream()
            .map(ProtobufUtil::toServerName).collect(Collectors.toList()))
        .addAllDeadServers(metrics.getDeadServerNames().stream()
            .map(ProtobufUtil::toServerName).collect(Collectors.toList()))
        .addAllLiveServers(metrics.getLiveServerMetrics().entrySet().stream()
            .map(s -> ClusterStatusProtos.LiveServerInfo
                .newBuilder()
                .setServer(ProtobufUtil.toServerName(s.getKey()))
                .setServerLoad(ServerMetricsBuilder.toServerLoad(s.getValue()))
                .build())
            .collect(Collectors.toList()))
        .addAllMasterCoprocessors(metrics.getMasterCoprocessorNames().stream()
            .map(n -> HBaseProtos.Coprocessor.newBuilder().setName(n).build())
            .collect(Collectors.toList()))
        .addAllRegionsInTransition(metrics.getRegionStatesInTransition().stream()
            .map(r -> ClusterStatusProtos.RegionInTransition
                .newBuilder()
                .setSpec(HBaseProtos.RegionSpecifier
                    .newBuilder()
                    .setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME)
                    .setValue(UnsafeByteOperations.unsafeWrap(r.getRegion().getRegionName()))
                    .build())
                .setRegionState(r.convert())
                .build())
            .collect(Collectors.toList()))
        .setMasterInfoPort(metrics.getMasterInfoPort())
        .addAllServersName(metrics.getServersName().stream().map(ProtobufUtil::toServerName)
          .collect(Collectors.toList()))
        .addAllTableRegionStatesCount(metrics.getTableRegionStatesCount().entrySet().stream()
          .map(status ->
            ClusterStatusProtos.TableRegionStatesCount.newBuilder()
              .setTableName(ProtobufUtil.toProtoTableName((status.getKey())))
              .setRegionStatesCount(ProtobufUtil.toTableRegionStatesCount(status.getValue()))
              .build())
          .collect(Collectors.toList()));
    if (metrics.getMasterName() != null) {
      builder.setMaster(ProtobufUtil.toServerName((metrics.getMasterName())));
    }
    if (metrics.getMasterTasks() != null) {
      builder.addAllMasterTasks(metrics.getMasterTasks().stream()
        .map(t -> ProtobufUtil.toServerTask(t)).collect(Collectors.toList()));
    }
    if (metrics.getBalancerOn() != null) {
      builder.setBalancerOn(metrics.getBalancerOn());
    }
    if (metrics.getClusterId() != null) {
      builder.setClusterId(new ClusterId(metrics.getClusterId()).convert());
    }
    if (metrics.getHBaseVersion() != null) {
      builder.setHbaseVersion(
          FSProtos.HBaseVersionFileContent.newBuilder()
              .setVersion(metrics.getHBaseVersion()));
    }
    return builder.build();
  }

  public static ClusterMetrics toClusterMetrics(
      ClusterStatusProtos.ClusterStatus proto) {
    ClusterMetricsBuilder builder = ClusterMetricsBuilder.newBuilder();
    builder.setLiveServerMetrics(proto.getLiveServersList().stream()
        .collect(Collectors.toMap(e -> ProtobufUtil.toServerName(e.getServer()),
            ServerMetricsBuilder::toServerMetrics)))
        .setDeadServerNames(proto.getDeadServersList().stream()
            .map(ProtobufUtil::toServerName)
            .collect(Collectors.toList()))
        .setBackerMasterNames(proto.getBackupMastersList().stream()
            .map(ProtobufUtil::toServerName)
            .collect(Collectors.toList()))
        .setRegionsInTransition(proto.getRegionsInTransitionList().stream()
            .map(ClusterStatusProtos.RegionInTransition::getRegionState)
            .map(RegionState::convert)
            .collect(Collectors.toList()))
        .setMasterCoprocessorNames(proto.getMasterCoprocessorsList().stream()
            .map(HBaseProtos.Coprocessor::getName)
            .collect(Collectors.toList()))
        .setServerNames(proto.getServersNameList().stream().map(ProtobufUtil::toServerName)
            .collect(Collectors.toList()))
        .setTableRegionStatesCount(
          proto.getTableRegionStatesCountList().stream()
          .collect(Collectors.toMap(
            e -> ProtobufUtil.toTableName(e.getTableName()),
            e -> ProtobufUtil.toTableRegionStatesCount(e.getRegionStatesCount()))))
        .setMasterTasks(proto.getMasterTasksList().stream()
          .map(t -> ProtobufUtil.getServerTask(t)).collect(Collectors.toList()));
    if (proto.hasClusterId()) {
      builder.setClusterId(ClusterId.convert(proto.getClusterId()).toString());
    }

    if (proto.hasHbaseVersion()) {
      builder.setHBaseVersion(proto.getHbaseVersion().getVersion());
    }

    if (proto.hasMaster()) {
      builder.setMasterName(ProtobufUtil.toServerName(proto.getMaster()));
    }

    if (proto.hasBalancerOn()) {
      builder.setBalancerOn(proto.getBalancerOn());
    }

    if (proto.hasMasterInfoPort()) {
      builder.setMasterInfoPort(proto.getMasterInfoPort());
    }
    return builder.build();
  }

  /**
   * Convert ClusterStatusProtos.Option to ClusterMetrics.Option
   * @param option a ClusterStatusProtos.Option
   * @return converted ClusterMetrics.Option
   */
  public static ClusterMetrics.Option toOption(ClusterStatusProtos.Option option) {
    switch (option) {
      case HBASE_VERSION: return ClusterMetrics.Option.HBASE_VERSION;
      case LIVE_SERVERS: return ClusterMetrics.Option.LIVE_SERVERS;
      case DEAD_SERVERS: return ClusterMetrics.Option.DEAD_SERVERS;
      case REGIONS_IN_TRANSITION: return ClusterMetrics.Option.REGIONS_IN_TRANSITION;
      case CLUSTER_ID: return ClusterMetrics.Option.CLUSTER_ID;
      case MASTER_COPROCESSORS: return ClusterMetrics.Option.MASTER_COPROCESSORS;
      case MASTER: return ClusterMetrics.Option.MASTER;
      case BACKUP_MASTERS: return ClusterMetrics.Option.BACKUP_MASTERS;
      case BALANCER_ON: return ClusterMetrics.Option.BALANCER_ON;
      case SERVERS_NAME: return ClusterMetrics.Option.SERVERS_NAME;
      case MASTER_INFO_PORT: return ClusterMetrics.Option.MASTER_INFO_PORT;
      case TABLE_TO_REGIONS_COUNT: return ClusterMetrics.Option.TABLE_TO_REGIONS_COUNT;
      case TASKS: return ClusterMetrics.Option.TASKS;
      // should not reach here
      default: throw new IllegalArgumentException("Invalid option: " + option);
    }
  }

  /**
   * Convert ClusterMetrics.Option to ClusterStatusProtos.Option
   * @param option a ClusterMetrics.Option
   * @return converted ClusterStatusProtos.Option
   */
  public static ClusterStatusProtos.Option toOption(ClusterMetrics.Option option) {
    switch (option) {
      case HBASE_VERSION: return ClusterStatusProtos.Option.HBASE_VERSION;
      case LIVE_SERVERS: return ClusterStatusProtos.Option.LIVE_SERVERS;
      case DEAD_SERVERS: return ClusterStatusProtos.Option.DEAD_SERVERS;
      case REGIONS_IN_TRANSITION: return ClusterStatusProtos.Option.REGIONS_IN_TRANSITION;
      case CLUSTER_ID: return ClusterStatusProtos.Option.CLUSTER_ID;
      case MASTER_COPROCESSORS: return ClusterStatusProtos.Option.MASTER_COPROCESSORS;
      case MASTER: return ClusterStatusProtos.Option.MASTER;
      case BACKUP_MASTERS: return ClusterStatusProtos.Option.BACKUP_MASTERS;
      case BALANCER_ON: return ClusterStatusProtos.Option.BALANCER_ON;
      case SERVERS_NAME: return Option.SERVERS_NAME;
      case MASTER_INFO_PORT: return ClusterStatusProtos.Option.MASTER_INFO_PORT;
      case TABLE_TO_REGIONS_COUNT: return ClusterStatusProtos.Option.TABLE_TO_REGIONS_COUNT;
      case TASKS: return ClusterStatusProtos.Option.TASKS;
      // should not reach here
      default: throw new IllegalArgumentException("Invalid option: " + option);
    }
  }

  /**
   * Convert a list of ClusterStatusProtos.Option to an enum set of ClusterMetrics.Option
   * @param options the pb options
   * @return an enum set of ClusterMetrics.Option
   */
  public static EnumSet<ClusterMetrics.Option> toOptions(List<ClusterStatusProtos.Option> options) {
    return options.stream().map(ClusterMetricsBuilder::toOption)
        .collect(Collectors.toCollection(() -> EnumSet.noneOf(ClusterMetrics.Option.class)));
  }

  /**
   * Convert an enum set of ClusterMetrics.Option to a list of ClusterStatusProtos.Option
   * @param options the ClusterMetrics options
   * @return a list of ClusterStatusProtos.Option
   */
  public static List<ClusterStatusProtos.Option> toOptions(EnumSet<ClusterMetrics.Option> options) {
    return options.stream().map(ClusterMetricsBuilder::toOption).collect(Collectors.toList());
  }

  public static ClusterMetricsBuilder newBuilder() {
    return new ClusterMetricsBuilder();
  }
  @Nullable
  private String hbaseVersion;
  private List<ServerName> deadServerNames = Collections.emptyList();
  private Map<ServerName, ServerMetrics> liveServerMetrics = new TreeMap<>();
  @Nullable
  private ServerName masterName;
  private List<ServerName> backupMasterNames = Collections.emptyList();
  private List<RegionState> regionsInTransition = Collections.emptyList();
  @Nullable
  private String clusterId;
  private List<String> masterCoprocessorNames = Collections.emptyList();
  @Nullable
  private Boolean balancerOn;
  private int masterInfoPort;
  private List<ServerName> serversName = Collections.emptyList();
  private Map<TableName, RegionStatesCount> tableRegionStatesCount = Collections.emptyMap();
  @Nullable
  private List<ServerTask> masterTasks;

  private ClusterMetricsBuilder() {
  }
  public ClusterMetricsBuilder setHBaseVersion(String value) {
    this.hbaseVersion = value;
    return this;
  }
  public ClusterMetricsBuilder setDeadServerNames(List<ServerName> value) {
    this.deadServerNames = value;
    return this;
  }

  public ClusterMetricsBuilder setLiveServerMetrics(Map<ServerName, ServerMetrics> value) {
    liveServerMetrics.putAll(value);
    return this;
  }

  public ClusterMetricsBuilder setMasterName(ServerName value) {
    this.masterName = value;
    return this;
  }
  public ClusterMetricsBuilder setBackerMasterNames(List<ServerName> value) {
    this.backupMasterNames = value;
    return this;
  }
  public ClusterMetricsBuilder setRegionsInTransition(List<RegionState> value) {
    this.regionsInTransition = value;
    return this;
  }
  public ClusterMetricsBuilder setClusterId(String value) {
    this.clusterId = value;
    return this;
  }
  public ClusterMetricsBuilder setMasterCoprocessorNames(List<String> value) {
    this.masterCoprocessorNames = value;
    return this;
  }
  public ClusterMetricsBuilder setBalancerOn(@Nullable Boolean value) {
    this.balancerOn = value;
    return this;
  }
  public ClusterMetricsBuilder setMasterInfoPort(int value) {
    this.masterInfoPort = value;
    return this;
  }
  public ClusterMetricsBuilder setServerNames(List<ServerName> serversName) {
    this.serversName = serversName;
    return this;
  }
  public ClusterMetricsBuilder setMasterTasks(List<ServerTask> masterTasks) {
    this.masterTasks = masterTasks;
    return this;
  }

  public ClusterMetricsBuilder setTableRegionStatesCount(
      Map<TableName, RegionStatesCount> tableRegionStatesCount) {
    this.tableRegionStatesCount = tableRegionStatesCount;
    return this;
  }

  public ClusterMetrics build() {
    return new ClusterMetricsImpl(
        hbaseVersion,
        deadServerNames,
        liveServerMetrics,
        masterName,
        backupMasterNames,
        regionsInTransition,
        clusterId,
        masterCoprocessorNames,
        balancerOn,
        masterInfoPort,
        serversName,
        tableRegionStatesCount,
        masterTasks
    );
  }
  private static class ClusterMetricsImpl implements ClusterMetrics {
    @Nullable
    private final String hbaseVersion;
    private final List<ServerName> deadServerNames;
    private final Map<ServerName, ServerMetrics> liveServerMetrics;
    @Nullable
    private final ServerName masterName;
    private final List<ServerName> backupMasterNames;
    private final List<RegionState> regionsInTransition;
    @Nullable
    private final String clusterId;
    private final List<String> masterCoprocessorNames;
    @Nullable
    private final Boolean balancerOn;
    private final int masterInfoPort;
    private final List<ServerName> serversName;
    private final Map<TableName, RegionStatesCount> tableRegionStatesCount;
    private final List<ServerTask> masterTasks;

    ClusterMetricsImpl(String hbaseVersion, List<ServerName> deadServerNames,
        Map<ServerName, ServerMetrics> liveServerMetrics,
        ServerName masterName,
        List<ServerName> backupMasterNames,
        List<RegionState> regionsInTransition,
        String clusterId,
        List<String> masterCoprocessorNames,
        Boolean balancerOn,
        int masterInfoPort,
        List<ServerName> serversName,
        Map<TableName, RegionStatesCount> tableRegionStatesCount,
        List<ServerTask> masterTasks) {
      this.hbaseVersion = hbaseVersion;
      this.deadServerNames = Preconditions.checkNotNull(deadServerNames);
      this.liveServerMetrics = Preconditions.checkNotNull(liveServerMetrics);
      this.masterName = masterName;
      this.backupMasterNames = Preconditions.checkNotNull(backupMasterNames);
      this.regionsInTransition = Preconditions.checkNotNull(regionsInTransition);
      this.clusterId = clusterId;
      this.masterCoprocessorNames = Preconditions.checkNotNull(masterCoprocessorNames);
      this.balancerOn = balancerOn;
      this.masterInfoPort = masterInfoPort;
      this.serversName = serversName;
      this.tableRegionStatesCount = Preconditions.checkNotNull(tableRegionStatesCount);
      this.masterTasks = masterTasks;
    }

    @Override
    public String getHBaseVersion() {
      return hbaseVersion;
    }

    @Override
    public List<ServerName> getDeadServerNames() {
      return Collections.unmodifiableList(deadServerNames);
    }

    @Override
    public Map<ServerName, ServerMetrics> getLiveServerMetrics() {
      return Collections.unmodifiableMap(liveServerMetrics);
    }

    @Override
    public ServerName getMasterName() {
      return masterName;
    }

    @Override
    public List<ServerName> getBackupMasterNames() {
      return Collections.unmodifiableList(backupMasterNames);
    }

    @Override
    public List<RegionState> getRegionStatesInTransition() {
      return Collections.unmodifiableList(regionsInTransition);
    }

    @Override
    public String getClusterId() {
      return clusterId;
    }

    @Override
    public List<String> getMasterCoprocessorNames() {
      return Collections.unmodifiableList(masterCoprocessorNames);
    }

    @Override
    public Boolean getBalancerOn() {
      return balancerOn;
    }

    @Override
    public int getMasterInfoPort() {
      return masterInfoPort;
    }

    @Override
    public List<ServerName> getServersName() {
      return Collections.unmodifiableList(serversName);
    }

    @Override
    public Map<TableName, RegionStatesCount> getTableRegionStatesCount() {
      return Collections.unmodifiableMap(tableRegionStatesCount);
    }

    @Override
    public List<ServerTask> getMasterTasks() {
      return masterTasks;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(1024);
      sb.append("Master: " + getMasterName());

      int backupMastersSize = getBackupMasterNames().size();
      sb.append("\nNumber of backup masters: " + backupMastersSize);
      if (backupMastersSize > 0) {
        for (ServerName serverName: getBackupMasterNames()) {
          sb.append("\n  " + serverName);
        }
      }

      int serversSize = getLiveServerMetrics().size();
      int serversNameSize = getServersName().size();
      sb.append("\nNumber of live region servers: "
          + (serversSize > 0 ? serversSize : serversNameSize));
      if (serversSize > 0) {
        for (ServerName serverName : getLiveServerMetrics().keySet()) {
          sb.append("\n  " + serverName.getServerName());
        }
      } else if (serversNameSize > 0) {
        for (ServerName serverName : getServersName()) {
          sb.append("\n  " + serverName.getServerName());
        }
      }

      int deadServerSize = getDeadServerNames().size();
      sb.append("\nNumber of dead region servers: " + deadServerSize);
      if (deadServerSize > 0) {
        for (ServerName serverName : getDeadServerNames()) {
          sb.append("\n  " + serverName);
        }
      }

      sb.append("\nAverage load: " + getAverageLoad());
      sb.append("\nNumber of requests: " + getRequestCount());
      sb.append("\nNumber of regions: " + getRegionCount());

      int ritSize = getRegionStatesInTransition().size();
      sb.append("\nNumber of regions in transition: " + ritSize);
      if (ritSize > 0) {
        for (RegionState state : getRegionStatesInTransition()) {
          sb.append("\n  " + state.toDescriptiveString());
        }
      }
      return sb.toString();
    }
  }
}
