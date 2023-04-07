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
package org.apache.hadoop.hbase.replication;

import java.lang.reflect.Constructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Used to create replication storage(peer, queue) classes.
 */
@InterfaceAudience.Private
public final class ReplicationStorageFactory {

  public static final String REPLICATION_PEER_STORAGE_IMPL = "hbase.replication.peer.storage.impl";

  // must use zookeeper here, otherwise when user upgrading from an old version without changing the
  // config file, they will loss all the replication peer data.
  public static final ReplicationPeerStorageType DEFAULT_REPLICATION_PEER_STORAGE_IMPL =
    ReplicationPeerStorageType.ZOOKEEPER;

  private ReplicationStorageFactory() {
  }

  private static Class<? extends ReplicationPeerStorage>
    getReplicationPeerStorageClass(Configuration conf) {
    try {
      ReplicationPeerStorageType type = ReplicationPeerStorageType.valueOf(
        conf.get(REPLICATION_PEER_STORAGE_IMPL, DEFAULT_REPLICATION_PEER_STORAGE_IMPL.name())
          .toUpperCase());
      return type.getClazz();
    } catch (IllegalArgumentException e) {
      return conf.getClass(REPLICATION_PEER_STORAGE_IMPL,
        DEFAULT_REPLICATION_PEER_STORAGE_IMPL.getClazz(), ReplicationPeerStorage.class);
    }
  }

  /**
   * Create a new {@link ReplicationPeerStorage}.
   */
  public static ReplicationPeerStorage getReplicationPeerStorage(FileSystem fs, ZKWatcher zk,
    Configuration conf) {
    Class<? extends ReplicationPeerStorage> clazz = getReplicationPeerStorageClass(conf);
    for (Constructor<?> c : clazz.getConstructors()) {
      if (c.getParameterCount() != 2) {
        continue;
      }
      if (c.getParameterTypes()[0].isAssignableFrom(FileSystem.class)) {
        return ReflectionUtils.newInstance(clazz, fs, conf);
      } else if (c.getParameterTypes()[0].isAssignableFrom(ZKWatcher.class)) {
        return ReflectionUtils.newInstance(clazz, zk, conf);
      }
    }
    throw new IllegalArgumentException(
      "Can not create replication peer storage with type " + clazz);
  }

  /**
   * Create a new {@link ReplicationQueueStorage}.
   */
  public static ReplicationQueueStorage getReplicationQueueStorage(ZKWatcher zk,
    Configuration conf) {
    return new ZKReplicationQueueStorage(zk, conf);
  }
}
