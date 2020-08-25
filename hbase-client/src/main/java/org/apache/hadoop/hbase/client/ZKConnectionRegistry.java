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

import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_RPC_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.HBASE_RPC_READ_TIMEOUT_KEY;
import static org.apache.hadoop.hbase.HConstants.HBASE_RPC_TIMEOUT_KEY;
import static org.apache.hadoop.hbase.client.RegionInfo.DEFAULT_REPLICA_ID;
import static org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil.lengthOfPBMagic;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;
import static org.apache.hadoop.hbase.zookeeper.ZKMetadata.removeMetaData;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ReadOnlyZKClient;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ClientMetaService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.LocateMetaRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;

/**
 * Zookeeper based registry implementation.
 */
@InterfaceAudience.Private
class ZKConnectionRegistry implements ConnectionRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(ZKConnectionRegistry.class);

  private final ReadOnlyZKClient zk;

  private final ZNodePaths znodePaths;

  private final AtomicReference<ClientMetaService.Interface> cachedStub = new AtomicReference<>();

  private final AtomicReference<CompletableFuture<ClientMetaService.Interface>> stubMakeFuture =
    new AtomicReference<>();

  // RPC client used to talk to the masters.
  private final RpcClient rpcClient;
  private final RpcControllerFactory rpcControllerFactory;
  private final long readRpcTimeoutNs;
  private final User user;

  ZKConnectionRegistry(Configuration conf) throws IOException {
    this.znodePaths = new ZNodePaths(conf);
    this.zk = new ReadOnlyZKClient(conf);
    // XXX: we pass cluster id as null here since we do not have a cluster id yet, we have to fetch
    // this through the connection registry...
    // This is a problem as we will use the cluster id to determine the authentication method
    rpcClient = RpcClientFactory.createClient(conf, null);
    rpcControllerFactory = RpcControllerFactory.instantiate(conf);
    long rpcTimeoutMs = conf.getLong(HBASE_RPC_TIMEOUT_KEY, DEFAULT_HBASE_RPC_TIMEOUT);
    this.readRpcTimeoutNs =
      TimeUnit.MILLISECONDS.toNanos(conf.getLong(HBASE_RPC_READ_TIMEOUT_KEY, rpcTimeoutMs));
    this.user = User.getCurrent();
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
    return getAndConvert(znodePaths.clusterIdZNode, ZKConnectionRegistry::getClusterId);
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

  private Pair<RegionState.State, ServerName>
    getStateAndServerName(ZooKeeperProtos.MetaRegionServer proto) {
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
        addListener(getAndConvert(path, ZKConnectionRegistry::getMetaProto), (proto, error) -> {
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
            LOG.warn("Meta region is in state " + stateAndServerName.getFirst());
          }
          locs[DEFAULT_REPLICA_ID] = new HRegionLocation(
            RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).setRegionId(1).build(),
            stateAndServerName.getSecond());
          tryComplete(remaining, locs, future);
        });
      } else {
        addListener(getAndConvert(path, ZKConnectionRegistry::getMetaProto), (proto, error) -> {
          if (future.isDone()) {
            return;
          }
          if (error != null) {
            LOG.warn("Failed to fetch " + path, error);
            locs[replicaId] = null;
          } else if (proto == null) {
            LOG.warn("Meta znode for replica " + replicaId + " is null");
            locs[replicaId] = null;
          } else {
            Pair<RegionState.State, ServerName> stateAndServerName = getStateAndServerName(proto);
            if (stateAndServerName.getFirst() != RegionState.State.OPEN) {
              LOG.warn("Meta region for replica " + replicaId + " is in state " +
                stateAndServerName.getFirst());
              locs[replicaId] = null;
            } else {
              locs[replicaId] =
                new HRegionLocation(RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME)
                  .setRegionId(1).setReplicaId(replicaId).build(), stateAndServerName.getSecond());
            }
          }
          tryComplete(remaining, locs, future);
        });
      }
    }
  }

  // keep the method here just for testing compatibility
  public CompletableFuture<RegionLocations> getMetaRegionLocations() {
    CompletableFuture<RegionLocations> future = new CompletableFuture<>();
    addListener(
      zk.list(znodePaths.baseZNode).thenApply(children -> children.stream()
        .filter(c -> this.znodePaths.isMetaZNodePrefix(c)).collect(Collectors.toList())),
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
  public CompletableFuture<ServerName> getActiveMaster() {
    return getAndConvert(znodePaths.masterAddressZNode, ZKConnectionRegistry::getMasterProto)
      .thenApply(proto -> {
        if (proto == null) {
          return null;
        }
        HBaseProtos.ServerName snProto = proto.getMaster();
        return ServerName.valueOf(snProto.getHostName(), snProto.getPort(), snProto.getStartCode());
      });
  }

  private CompletableFuture<ClientMetaService.Interface> getStub() {
    return ConnectionUtils.getMasterStub(this, cachedStub, stubMakeFuture, rpcClient, user,
      readRpcTimeoutNs, TimeUnit.NANOSECONDS, ClientMetaService::newStub, "ClientMetaService");
  }

  @Override
  public CompletableFuture<RegionLocations> locateMeta(byte[] row, RegionLocateType locateType) {
    CompletableFuture<RegionLocations> future = new CompletableFuture<>();
    addListener(getStub(), (stub, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
        return;
      }
      HBaseRpcController controller = rpcControllerFactory.newController();
      stub.locateMetaRegion(controller,
        LocateMetaRegionRequest.newBuilder().setRow(ByteString.copyFrom(row))
          .setLocateType(ProtobufUtil.toProtoRegionLocateType(locateType)).build(),
        resp -> {
          if (controller.failed()) {
            IOException ex = controller.getFailed();
            ConnectionUtils.tryClearMasterStubCache(ex, stub, ZKConnectionRegistry.this.cachedStub);
            future.completeExceptionally(ex);
            return;
          }
          RegionLocations locs = new RegionLocations(resp.getMetaLocationsList().stream()
            .map(ProtobufUtil::toRegionLocation).collect(Collectors.toList()));
          future.complete(locs);
        });
    });
    return future;
  }

  @Override
  public CompletableFuture<List<HRegionLocation>>
    getAllMetaRegionLocations(boolean excludeOfflinedSplitParents) {
    return ConnectionUtils.getAllMetaRegionLocations(excludeOfflinedSplitParents, getStub(),
      cachedStub, rpcControllerFactory, -1);
  }

  @Override
  public void close() {
    rpcClient.close();
    zk.close();
  }
}
