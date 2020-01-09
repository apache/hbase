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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.client.RegionInfo.DEFAULT_REPLICA_ID;
import static org.apache.hadoop.hbase.client.RegionInfoBuilder.FIRST_META_REGIONINFO;
import static org.apache.hadoop.hbase.client.RegionReplicaUtil.getRegionInfoForDefaultReplica;
import static org.apache.hadoop.hbase.client.RegionReplicaUtil.getRegionInfoForReplica;
import static org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil.lengthOfPBMagic;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;
import static org.apache.hadoop.hbase.zookeeper.ZKMetadata.removeMetaData;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ReadOnlyZKClient;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;

/**
 * Zookeeper based registry implementation.
 */
@InterfaceAudience.Private
class ZKAsyncRegistry implements AsyncRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(ZKAsyncRegistry.class);

  private final ReadOnlyZKClient zk;

  private final ZNodePaths znodePaths;

  /**
   * A znode maintained by MirroringTableStateManager.
   * MirroringTableStateManager is deprecated to be removed in hbase3. It can also be disabled.
   * Make sure it is enabled if you want to alter hbase:meta table in hbase2. In hbase3,
   * TBD how metatable state will be hosted; likely on active hbase master.
   */
  private final String znodeMirroredMetaTableState;


  ZKAsyncRegistry(Configuration conf) {
    this.znodePaths = new ZNodePaths(conf);
    this.znodeMirroredMetaTableState =
      ZNodePaths.joinZNode(this.znodePaths.tableZNode, TableName.META_TABLE_NAME.getNameAsString());
    this.zk = new ReadOnlyZKClient(conf);
  }

  private interface Converter<T> {
    T convert(byte[] data) throws Exception;
  }

  private <T> CompletableFuture<T> getAndConvert(String path, Converter<T> converter) {
    CompletableFuture<T> future = new CompletableFuture<>();
    addListener(zk.get(path), (data, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
        return;
      }
      try {
        future.complete(converter.convert(data));
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
    });
    return future;
  }

  private static String getClusterId(byte[] data) throws DeserializationException {
    if (data == null || data.length == 0) {
      return null;
    }
    data = removeMetaData(data);
    return ClusterId.parseFrom(data).toString();
  }

  @Override
  public CompletableFuture<String> getClusterId() {
    return getAndConvert(znodePaths.clusterIdZNode, ZKAsyncRegistry::getClusterId);
  }

  @VisibleForTesting
  ReadOnlyZKClient getZKClient() {
    return zk;
  }

  private static ZooKeeperProtos.MetaRegionServer getMetaProto(byte[] data) throws IOException {
    if (data == null || data.length == 0) {
      return null;
    }
    data = removeMetaData(data);
    int prefixLen = lengthOfPBMagic();
    return ZooKeeperProtos.MetaRegionServer.parser().parseFrom(data, prefixLen,
      data.length - prefixLen);
  }

  private static void tryComplete(MutableInt remaining, HRegionLocation[] locs,
      CompletableFuture<RegionLocations> future) {
    remaining.decrement();
    if (remaining.intValue() > 0) {
      return;
    }
    future.complete(new RegionLocations(locs));
  }

  private Pair<RegionState.State, ServerName> getStateAndServerName(
      ZooKeeperProtos.MetaRegionServer proto) {
    RegionState.State state;
    if (proto.hasState()) {
      state = RegionState.State.convert(proto.getState());
    } else {
      state = RegionState.State.OPEN;
    }
    HBaseProtos.ServerName snProto = proto.getServer();
    return Pair.newPair(state,
      ServerName.valueOf(snProto.getHostName(), snProto.getPort(), snProto.getStartCode()));
  }

  private void getMetaRegionLocation(CompletableFuture<RegionLocations> future,
      List<String> metaReplicaZNodes) {
    if (metaReplicaZNodes.isEmpty()) {
      future.completeExceptionally(new IOException("No meta znode available"));
    }
    HRegionLocation[] locs = new HRegionLocation[metaReplicaZNodes.size()];
    MutableInt remaining = new MutableInt(locs.length);
    for (String metaReplicaZNode : metaReplicaZNodes) {
      int replicaId = znodePaths.getMetaReplicaIdFromZnode(metaReplicaZNode);
      String path = ZNodePaths.joinZNode(znodePaths.baseZNode, metaReplicaZNode);
      if (replicaId == DEFAULT_REPLICA_ID) {
        addListener(getAndConvert(path, ZKAsyncRegistry::getMetaProto), (proto, error) -> {
          if (error != null) {
            future.completeExceptionally(error);
            return;
          }
          if (proto == null) {
            future.completeExceptionally(new IOException("Meta znode is null"));
            return;
          }
          Pair<RegionState.State, ServerName> stateAndServerName = getStateAndServerName(proto);
          if (stateAndServerName.getFirst() != RegionState.State.OPEN) {
            LOG.warn("hbase:meta region (replicaId={}) is in state {}", replicaId,
                stateAndServerName.getFirst());
          }
          locs[DEFAULT_REPLICA_ID] = new HRegionLocation(
            getRegionInfoForDefaultReplica(FIRST_META_REGIONINFO), stateAndServerName.getSecond());
          tryComplete(remaining, locs, future);
        });
      } else {
        addListener(getAndConvert(path, ZKAsyncRegistry::getMetaProto), (proto, error) -> {
          if (future.isDone()) {
            return;
          }
          if (error != null) {
            LOG.warn("Failed to fetch " + path, error);
            locs[replicaId] = null;
          } else if (proto == null) {
            LOG.warn("hbase:meta znode for replica " + replicaId + " is null");
            locs[replicaId] = null;
          } else {
            Pair<RegionState.State, ServerName> stateAndServerName = getStateAndServerName(proto);
            if (stateAndServerName.getFirst() != RegionState.State.OPEN) {
              LOG.warn("Meta region for replica " + replicaId + " is in state " +
                stateAndServerName.getFirst());
              locs[replicaId] = null;
            } else {
              locs[replicaId] =
                new HRegionLocation(getRegionInfoForReplica(FIRST_META_REGIONINFO, replicaId),
                  stateAndServerName.getSecond());
            }
          }
          tryComplete(remaining, locs, future);
        });
      }
    }
  }

  @Override
  public CompletableFuture<RegionLocations> getMetaRegionLocation() {
    CompletableFuture<RegionLocations> future = new CompletableFuture<>();
    addListener(
      zk.list(znodePaths.baseZNode).thenApply(children -> children.stream().
          filter(c -> znodePaths.isMetaZNodePrefix(c)).collect(Collectors.toList())),
      (metaReplicaZNodes, error) -> {
        if (error != null) {
          future.completeExceptionally(error);
          return;
        }
        getMetaRegionLocation(future, metaReplicaZNodes);
      });
    return future;
  }

  private static ZooKeeperProtos.Master getMasterProto(byte[] data) throws IOException {
    if (data == null || data.length == 0) {
      return null;
    }
    data = removeMetaData(data);
    int prefixLen = lengthOfPBMagic();
    return ZooKeeperProtos.Master.parser().parseFrom(data, prefixLen, data.length - prefixLen);
  }

  @Override
  public CompletableFuture<ServerName> getMasterAddress() {
    return getAndConvert(znodePaths.masterAddressZNode, ZKAsyncRegistry::getMasterProto)
        .thenApply(proto -> {
          if (proto == null) {
            return null;
          }
          HBaseProtos.ServerName snProto = proto.getMaster();
          return ServerName.valueOf(snProto.getHostName(), snProto.getPort(),
            snProto.getStartCode());
        });
  }

  @Override
  public CompletableFuture<TableState> getMetaTableState() {
    return getAndConvert(this.znodeMirroredMetaTableState, ZKAsyncRegistry::getTableState).
      thenApply(state -> {
        return state == null || state.equals(ENABLED_META_TABLE_STATE.getState())?
          ENABLED_META_TABLE_STATE: new TableState(TableName.META_TABLE_NAME, state);
      }).exceptionally(e -> {
        // Handle this case where no znode... Return default ENABLED in this case:
        // Caused by: java.io.IOException: java.util.concurrent.ExecutionException:
        // java.util.concurrent.ExecutionException:
        // org.apache.zookeeper.KeeperException$NoNodeException: KeeperErrorCode = NoNode for
        //  /hbase/table/hbase:meta
        // If not case of above, then rethrow but may need to wrap. See
        // https://stackoverflow.com/questions/55453961/
        //  completablefutureexceptionally-rethrow-checked-exception
        if (e.getCause() instanceof KeeperException.NoNodeException) {
          return ENABLED_META_TABLE_STATE;
        }
        throw e instanceof CompletionException? (CompletionException)e:
          new CompletionException(e);
      });
  }

  /**
   * Get tablestate from data byte array found in the mirroring znode of table state.
   */
  private static TableState.State getTableState(byte[] data) throws DeserializationException {
    if (data == null || data.length == 0) {
      return null;
    }
    try {
      return ProtobufUtil.toTableState(ProtobufUtil.toTableState(removeMetaData(data)));
    } catch (IOException ioe) {
      throw new DeserializationException(ioe);
    }
  }

  @Override
  public void close() {
    zk.close();
  }
}
