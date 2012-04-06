/*
 * Copyright 2011 The Apache Software Foundation
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

package org.apache.hadoop.hbase.zookeeper;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.zookeeper.KeeperException;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Publishes and synchronizes a unique identifier specific to a given HBase
 * cluster.  The stored identifier is read from the file system by the active
 * master on startup, and is subsequently available to all watchers (including
 * clients).
 */
@InterfaceAudience.Private
public class ClusterId {
  private ZooKeeperWatcher watcher;
  private Abortable abortable;
  private String id;

  public ClusterId(ZooKeeperWatcher watcher, Abortable abortable) {
    this.watcher = watcher;
    this.abortable = abortable;
  }

  public boolean hasId() {
    return getId() != null;
  }

  public String getId() {
    try {
      if (id == null) {
        id = readClusterIdZNode(watcher);
      }
    } catch (KeeperException ke) {
      abortable.abort("Unexpected exception from ZooKeeper reading cluster ID",
          ke);
    }
    return id;
  }

  public static String readClusterIdZNode(ZooKeeperWatcher watcher)
      throws KeeperException {
    if (ZKUtil.checkExists(watcher, watcher.clusterIdZNode) != -1) {
      byte [] data = ZKUtil.getData(watcher, watcher.clusterIdZNode);
      if (data != null) {
        return getZNodeClusterId(data);
      }
    }
    return null;
  }

  public static void setClusterId(ZooKeeperWatcher watcher, String id)
      throws KeeperException {
    ZKUtil.createSetData(watcher, watcher.clusterIdZNode, getZNodeData(id));
  }

  /**
   * @param clusterid
   * @return Content of the clusterid znode as a serialized pb with the pb
   * magic as prefix.
   */
  static byte [] getZNodeData(final String clusterid) {
    ZooKeeperProtos.ClusterId.Builder builder =
      ZooKeeperProtos.ClusterId.newBuilder();
    builder.setClusterId(clusterid);
    return ProtobufUtil.prependPBMagic(builder.build().toByteArray());
  }

  /**
   * @param data
   * @return The clusterid extracted from the passed znode <code>data</code>
   */
  static String getZNodeClusterId(final byte [] data) {
    if (data == null || data.length <= 0) return null;
    // If no magic, something is seriously wrong.  Fail fast.
    if (!ProtobufUtil.isPBMagicPrefix(data)) throw new RuntimeException("No magic preamble");
    int prefixLen = ProtobufUtil.lengthOfPBMagic();
    try {
      ZooKeeperProtos.ClusterId clusterid =
        ZooKeeperProtos.ClusterId.newBuilder().mergeFrom(data, prefixLen, data.length - prefixLen).build();
      return clusterid.getClusterId();
    } catch (InvalidProtocolBufferException e) {
      // A failed parse of the znode is pretty catastrophic. Fail fast.
      throw new RuntimeException(e);
    }
  }
}