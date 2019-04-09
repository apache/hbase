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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.client.RegionInfo.DEFAULT_REPLICA_ID;
import static org.apache.hadoop.hbase.client.RegionInfoBuilder.FIRST_META_REGIONINFO;
import static org.apache.hadoop.hbase.client.RegionReplicaUtil.getRegionInfoForDefaultReplica;
import static org.apache.hadoop.hbase.client.RegionReplicaUtil.getRegionInfoForReplica;
import static org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil.lengthOfPBMagic;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;
import static org.apache.hadoop.hbase.zookeeper.ZKMetadata.removeMetaData;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ReadOnlyZKClient;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;

/**
 * Fetch the registry data from zookeeper.
 */
@InterfaceAudience.Private
class ZKAsyncRegistry implements AsyncRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(ZKAsyncRegistry.class);

  private final ReadOnlyZKClient zk;

  private final ZNodePaths znodePaths;

  ZKAsyncRegistry(Configuration conf) {
    this.znodePaths = new ZNodePaths(conf);
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

  @Override
  public CompletableFuture<RegionLocations> getMetaRegionLocation() {
    CompletableFuture<RegionLocations> future = new CompletableFuture<>();
    HRegionLocation[] locs = new HRegionLocation[znodePaths.metaReplicaZNodes.size()];
    MutableInt remaining = new MutableInt(locs.length);
    znodePaths.metaReplicaZNodes.forEach((replicaId, path) -> {
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
            future.completeExceptionally(
              new IOException("Meta region is in state " + stateAndServerName.getFirst()));
            return;
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
                new HRegionLocation(getRegionInfoForReplica(FIRST_META_REGIONINFO, replicaId),
                  stateAndServerName.getSecond());
            }
          }
          tryComplete(remaining, locs, future);
        });
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Integer> getCurrentNrHRS() {
    return zk.exists(znodePaths.rsZNode).thenApply(s -> s != null ? s.getNumChildren() : 0);
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
  public CompletableFuture<Integer> getMasterInfoPort() {
    return getAndConvert(znodePaths.masterAddressZNode, ZKAsyncRegistry::getMasterProto)
        .thenApply(proto -> proto != null ? proto.getInfoPort() : 0);
  }

  @Override
  public void close() {
    zk.close();
  }
}
