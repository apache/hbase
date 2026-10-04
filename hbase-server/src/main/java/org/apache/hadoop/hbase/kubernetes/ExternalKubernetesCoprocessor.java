/*
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
package org.apache.hadoop.hbase.kubernetes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetricsBuilder;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerMetricsBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.ClientMetaCoprocessor;
import org.apache.hadoop.hbase.coprocessor.ClientMetaCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ClientMetaObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.ObserverRpcCallContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.net.HostAndPort;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public class ExternalKubernetesCoprocessor implements ClientMetaCoprocessor, MasterCoprocessor,
  RegionCoprocessor, ClientMetaObserver, MasterObserver, RegionObserver {
  private static final Logger LOG = LoggerFactory.getLogger(ExternalKubernetesCoprocessor.class);

  private static final String KUBERNETES_HEADER = HConstants.CLIENT_HEADER_PREFIX + "kubernetes";

  private volatile ExternalMapping mapping;

  public ExternalKubernetesCoprocessor() {
  }

  @Override
  public Optional<ClientMetaObserver> getClientMetaObserver() {
    return Optional.of(this);
  }

  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return Optional.of(this);
  }

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    mapping = new ExternalMapping(env.getConfiguration());
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    ExternalMapping current = mapping;

    if (current != null) {
      current.close();
    }
  }

  private static boolean isInternalClient(ObserverContext<?> ctx) {
    Optional<ObserverRpcCallContext> rpcCallContext = ctx.getRpcCallContext();

    if (rpcCallContext.isEmpty()) {
      return true;
    }

    byte[] value = rpcCallContext.get().getAttributes().get(KUBERNETES_HEADER);

    return !Objects.equals(Bytes.toString(value), "true");
  }

  private String map(String hostname) {
    ExternalMapping current = mapping;

    if (current == null) {
      LOG.warn("External hostname mapping is not configured.");
      return hostname;
    }

    String newHostname = current.get(hostname);

    if (newHostname == null) {
      LOG.warn("Missing external hostname mapping for '{}'.", hostname);
      return hostname;
    }

    return newHostname;
  }

  private Address transformAddress(Address address) {
    String newHostname = map(address.getHostName());
    HostAndPort hostAndPort = HostAndPort.fromString(newHostname);

    if (hostAndPort.hasPort()) {
      return Address.fromParts(hostAndPort.getHost(), hostAndPort.getPort());
    } else {
      return Address.fromParts(hostAndPort.getHost(), address.getPort());
    }
  }

  private ServerName transformServerName(ServerName serverName) {
    Address newAddress = transformAddress(serverName.getAddress());
    return ServerName.valueOf(newAddress, serverName.getStartCode());
  }

  private List<ServerName> transformServerNames(List<ServerName> serverNames) {
    if (serverNames == null) {
      return null;
    }

    List<ServerName> newServerNames = new ArrayList<>(serverNames.size());

    for (ServerName serverName : serverNames) {
      ServerName newServerName = transformServerName(serverName);
      newServerNames.add(newServerName);
    }

    return newServerNames;
  }

  private HRegionLocation transformRegionLocation(HRegionLocation regionLocation) {
    ServerName newServerName = transformServerName(regionLocation.getServerName());
    return new HRegionLocation(regionLocation.getRegion(), newServerName,
      regionLocation.getSeqNum());
  }

  @Override
  public ServerName postGetActiveMaster(ObserverContext<ClientMetaCoprocessorEnvironment> ctx,
    ServerName serverName) throws IOException {
    if (isInternalClient(ctx)) {
      return serverName;
    }

    return transformServerName(serverName);
  }

  @Override
  public Map<ServerName, Boolean> postGetMasters(
    ObserverContext<ClientMetaCoprocessorEnvironment> ctx, Map<ServerName, Boolean> serverNames)
    throws IOException {
    if (isInternalClient(ctx)) {
      return serverNames;
    }

    Map<ServerName, Boolean> newServerNames = new LinkedHashMap<>(serverNames.size());

    serverNames
      .forEach((serverName, active) -> newServerNames.put(transformServerName(serverName), active));

    return newServerNames;
  }

  @Override
  public List<HRegionLocation> postGetMetaLocations(
    ObserverContext<ClientMetaCoprocessorEnvironment> ctx, List<HRegionLocation> metaLocations)
    throws IOException {
    if (isInternalClient(ctx)) {
      return metaLocations;
    }

    return metaLocations.stream().map(this::transformRegionLocation).collect(Collectors.toList());
  }

  @Override
  public List<ServerName> postGetBootstrapNodes(
    ObserverContext<ClientMetaCoprocessorEnvironment> ctx, List<ServerName> bootstrapNodes)
    throws IOException {
    if (isInternalClient(ctx)) {
      return bootstrapNodes;
    }

    return bootstrapNodes.stream().map(this::transformServerName).collect(Collectors.toList());
  }

  @Override
  public void postGetClusterMetrics(ObserverContext<MasterCoprocessorEnvironment> ctx,
    ClusterMetricsBuilder metricsBuilder) throws IOException {
    if (isInternalClient(ctx)) {
      return;
    }

    ClusterMetrics metrics = metricsBuilder.build();

    ServerName masterName = metrics.getMasterName();

    if (masterName != null) {
      metricsBuilder.setMasterName(transformServerName(masterName));
    }

    List<ServerName> newBackupMasterNames = transformServerNames(metrics.getBackupMasterNames());
    metricsBuilder.setBackupMasterNames(newBackupMasterNames);

    List<ServerName> newServersName = transformServerNames(metrics.getServersName());
    metricsBuilder.setServerNames(newServersName);

    Map<ServerName, ServerMetrics> liveServerMetrics = metrics.getLiveServerMetrics();
    Map<ServerName, ServerMetrics> newLiveServerMetrics = new TreeMap<>();

    for (ServerMetrics liveServerMetric : liveServerMetrics.values()) {
      ServerName newServerName = transformServerName(liveServerMetric.getServerName());
      ServerMetrics newLiveServerMetric =
        ServerMetricsBuilder.newBuilder(newServerName).setVersion(liveServerMetric.getVersion())
          .setVersionNumber(liveServerMetric.getVersionNumber())
          .setRequestCountPerSecond(liveServerMetric.getRequestCountPerSecond())
          .setReadRequestCount(liveServerMetric.getReadRequestsCount())
          .setWriteRequestCount(liveServerMetric.getWriteRequestsCount())
          .setUsedHeapSize(liveServerMetric.getUsedHeapSize())
          .setMaxHeapSize(liveServerMetric.getMaxHeapSize())
          .setInfoServerPort(liveServerMetric.getInfoServerPort())
          .setReplicationLoadSources(liveServerMetric.getReplicationLoadSourceList())
          .setReplicationLoadSink(liveServerMetric.getReplicationLoadSink())
          .setRegionMetrics(new ArrayList<>(liveServerMetric.getRegionMetrics().values()))
          .setUserMetrics(new ArrayList<>(liveServerMetric.getUserMetrics().values()))
          .setCoprocessorNames(new ArrayList<>(liveServerMetric.getCoprocessorNames()))
          .setReportTimestamp(liveServerMetric.getReportTimestamp())
          .setLastReportTimestamp(liveServerMetric.getLastReportTimestamp())
          .setTasks(liveServerMetric.getTasks())
          .setRegionCachedInfo(liveServerMetric.getRegionCachedInfo()).build();

      newLiveServerMetrics.put(newServerName, newLiveServerMetric);
    }

    metricsBuilder.setLiveServerMetrics(newLiveServerMetrics);

    List<ServerName> newDeadServerNames = transformServerNames(metrics.getDeadServerNames());
    metricsBuilder.setDeadServerNames(newDeadServerNames);

    List<ServerName> newDecommissionedServerNames =
      transformServerNames(metrics.getDecommissionedServerNames());
    metricsBuilder.setDecommissionedServerNames(newDecommissionedServerNames);

    List<ServerName> newUnknownServerNames = transformServerNames(metrics.getUnknownServerNames());
    metricsBuilder.setUnknownServerNames(newUnknownServerNames);

    List<RegionState> regionStatesInTransition = metrics.getRegionStatesInTransition();
    List<RegionState> newRegionStatesInTransition =
      new ArrayList<>(regionStatesInTransition.size());

    for (RegionState regionStateInTransition : regionStatesInTransition) {
      ServerName serverName = regionStateInTransition.getServerName();
      ServerName newServerName = transformServerName(serverName);

      RegionState newRegionState = new RegionState(regionStateInTransition.getRegion(),
        regionStateInTransition.getState(), regionStateInTransition.getStamp(), newServerName,
        regionStateInTransition.getRitDuration());
      newRegionStatesInTransition.add(newRegionState);
    }

    metricsBuilder.setRegionsInTransition(newRegionStatesInTransition);
  }

  private static boolean isInfoFamily(Cell cell) {
    return Bytes.compareTo(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
      HConstants.CATALOG_FAMILY, 0, HConstants.CATALOG_FAMILY.length) == 0;
  }

  private static boolean isServerQualifier(Cell cell) {
    return Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(),
      cell.getQualifierLength(), HConstants.SERVER_QUALIFIER, 0, HConstants.SERVER_QUALIFIER.length)
        == 0;
  }

  private static boolean isSnQualifier(Cell cell) {
    return Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(),
      cell.getQualifierLength(), HConstants.SERVERNAME_QUALIFIER, 0,
      HConstants.SERVERNAME_QUALIFIER.length) == 0;
  }

  private static byte[] copyRow(Cell cell) {
    return Bytes.copy(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
  }

  private Cell transformServerCell(Cell cell) {
    byte[] row = copyRow(cell);

    String value =
      Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    Address address = Address.fromString(value);

    Address newAddress = transformAddress(address);
    byte[] newValue = Bytes.toBytes(newAddress.toString());

    return new KeyValue(row, HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
      cell.getTimestamp(), newValue);
  }

  private Cell transformSnCell(Cell cell) {
    byte[] row = copyRow(cell);

    String value =
      Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    ServerName serverName = ServerName.valueOf(value);

    ServerName newServerName = transformServerName(serverName);
    byte[] newValue = Bytes.toBytes(newServerName.toString());

    return new KeyValue(row, HConstants.CATALOG_FAMILY, HConstants.SERVERNAME_QUALIFIER,
      cell.getTimestamp(), newValue);
  }

  private Cell transformCell(Cell cell) {
    if (!isInfoFamily(cell) || cell.getType() != Cell.Type.Put) {
      return cell;
    }

    if (isServerQualifier(cell)) {
      return transformServerCell(cell);
    } else if (isSnQualifier(cell)) {
      return transformSnCell(cell);
    } else {
      return cell;
    }
  }

  @Override
  public void postGetOp(ObserverContext<? extends RegionCoprocessorEnvironment> ctx, Get get,
    List<Cell> result) throws IOException {
    if (isInternalClient(ctx)) {
      return;
    }

    TableName tableName = ctx.getEnvironment().getRegionInfo().getTable();

    if (!TableName.isMetaTableName(tableName)) {
      return;
    }

    ListIterator<Cell> iterator = result.listIterator();

    while (iterator.hasNext()) {
      Cell cell = iterator.next();
      Cell replacedCell = transformCell(cell);

      if (cell != replacedCell) {
        iterator.set(replacedCell);
      }
    }
  }

  @Override
  public boolean postScannerNext(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
    InternalScanner s, List<Result> result, int limit, boolean hasNext) throws IOException {
    if (isInternalClient(ctx)) {
      return hasNext;
    }

    TableName tableName = ctx.getEnvironment().getRegionInfo().getTable();

    if (!TableName.isMetaTableName(tableName)) {
      return hasNext;
    }

    for (Result r : result) {
      Cell[] cells = r.rawCells();

      for (int i = 0; i < cells.length; i++) {
        cells[i] = transformCell(cells[i]);
      }
    }

    return hasNext;
  }
}
