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

import static org.apache.hadoop.hbase.HConstants.DEFAULT_ZK_SESSION_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.ZK_SESSION_TIMEOUT;
import static org.apache.hadoop.hbase.HRegionInfo.DEFAULT_REPLICA_ID;
import static org.apache.hadoop.hbase.HRegionInfo.FIRST_META_REGIONINFO;
import static org.apache.hadoop.hbase.client.RegionReplicaUtil.getRegionInfoForDefaultReplica;
import static org.apache.hadoop.hbase.client.RegionReplicaUtil.getRegionInfoForReplica;
import static org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil.lengthOfPBMagic;
import static org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper.removeMetaData;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.zookeeper.data.Stat;

/**
 * Cache the cluster registry data in memory and use zk watcher to update. The only exception is
 * {@link #getClusterId()}, it will fetch the data from zk directly.
 */
@InterfaceAudience.Private
class ZKAsyncRegistry implements AsyncRegistry {

  private static final Log LOG = LogFactory.getLog(ZKAsyncRegistry.class);

  private final CuratorFramework zk;

  private final ZNodePaths znodePaths;

  ZKAsyncRegistry(Configuration conf) {
    this.znodePaths = new ZNodePaths(conf);
    int zkSessionTimeout = conf.getInt(ZK_SESSION_TIMEOUT, DEFAULT_ZK_SESSION_TIMEOUT);
    int zkRetry = conf.getInt("zookeeper.recovery.retry", 3);
    int zkRetryIntervalMs = conf.getInt("zookeeper.recovery.retry.intervalmill", 1000);
    this.zk = CuratorFrameworkFactory.builder()
        .connectString(ZKConfig.getZKQuorumServersString(conf)).sessionTimeoutMs(zkSessionTimeout)
        .retryPolicy(new RetryNTimes(zkRetry, zkRetryIntervalMs))
        .threadFactory(
          Threads.newDaemonThreadFactory(String.format("ZKClusterRegistry-0x%08x", hashCode())))
        .build();
    this.zk.start();
  }

  @Override
  public String getClusterId() {
    try {
      byte[] data = zk.getData().forPath(znodePaths.clusterIdZNode);
      if (data == null || data.length == 0) {
        return null;
      }
      data = removeMetaData(data);
      return ClusterId.parseFrom(data).toString();
    } catch (Exception e) {
      LOG.warn("failed to get cluster id", e);
      return null;
    }
  }

  @Override
  public void close() {
    zk.close();
  }

  private interface CuratorEventProcessor<T> {
    T process(CuratorEvent event) throws Exception;
  }

  private static <T> CompletableFuture<T> exec(BackgroundPathable<?> opBuilder, String path,
      CuratorEventProcessor<T> processor) {
    CompletableFuture<T> future = new CompletableFuture<>();
    try {
      opBuilder.inBackground((client, event) -> {
        try {
          future.complete(processor.process(event));
        } catch (Exception e) {
          future.completeExceptionally(e);
        }
      }).withUnhandledErrorListener((msg, e) -> future.completeExceptionally(e)).forPath(path);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  private static ZooKeeperProtos.MetaRegionServer getMetaProto(CuratorEvent event)
      throws IOException {
    byte[] data = event.getData();
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
        exec(zk.getData(), path, ZKAsyncRegistry::getMetaProto).whenComplete((proto, error) -> {
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
              getRegionInfoForDefaultReplica(FIRST_META_REGIONINFO),
              stateAndServerName.getSecond());
          tryComplete(remaining, locs, future);
        });
      } else {
        exec(zk.getData(), path, ZKAsyncRegistry::getMetaProto).whenComplete((proto, error) -> {
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
              LOG.warn("Meta region for replica " + replicaId + " is in state "
                  + stateAndServerName.getFirst());
              locs[replicaId] = null;
            } else {
              locs[replicaId] = new HRegionLocation(
                  getRegionInfoForReplica(FIRST_META_REGIONINFO, replicaId),
                  stateAndServerName.getSecond());
            }
          }
          tryComplete(remaining, locs, future);
        });
      }
    });
    return future;
  }

  private static int getCurrentNrHRS(CuratorEvent event) {
    Stat stat = event.getStat();
    return stat != null ? stat.getNumChildren() : 0;
  }

  @Override
  public CompletableFuture<Integer> getCurrentNrHRS() {
    return exec(zk.checkExists(), znodePaths.rsZNode, ZKAsyncRegistry::getCurrentNrHRS);
  }

  private static ZooKeeperProtos.Master getMasterProto(CuratorEvent event) throws IOException {
    byte[] data = event.getData();
    if (data == null || data.length == 0) {
      return null;
    }
    data = removeMetaData(data);
    int prefixLen = lengthOfPBMagic();
    return ZooKeeperProtos.Master.parser().parseFrom(data, prefixLen, data.length - prefixLen);
  }

  @Override
  public CompletableFuture<ServerName> getMasterAddress() {
    return exec(zk.getData(), znodePaths.masterAddressZNode, ZKAsyncRegistry::getMasterProto)
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
    return exec(zk.getData(), znodePaths.masterAddressZNode, ZKAsyncRegistry::getMasterProto)
        .thenApply(proto -> proto != null ? proto.getInfoPort() : 0);
  }
}