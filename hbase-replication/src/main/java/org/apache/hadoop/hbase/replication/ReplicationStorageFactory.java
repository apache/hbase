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
package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Used to create replication storage(peer, queue) classes.
 * <p>
 * For now we only have zk based implementation.
 */
@InterfaceAudience.Private
public final class ReplicationStorageFactory {

  public static final String REPLICATION_PEER_STORAGE_IMPL = "hbase.replication.peer.storage.impl";
  public static final String DEFAULT_REPLICATION_PEER_STORAGE_IMPL =
      ZKReplicationPeerStorage.class.getName();

  public static final String REPLICATION_QUEUE_STORAGE_IMPL =
      "hbase.replication.queue.storage.impl";
  public static final String DEFAULT_REPLICATION_QUEUE_STORAGE_IMPL =
      ZKReplicationQueueStorage.class.getName();

  private ReplicationStorageFactory() {
  }

  /**
   * Create a new {@link ReplicationPeerStorage}.
   */
  public static ReplicationPeerStorage getReplicationPeerStorage(ZKWatcher zk, Configuration conf) {
    String peerStorageClass =
        conf.get(REPLICATION_PEER_STORAGE_IMPL, DEFAULT_REPLICATION_PEER_STORAGE_IMPL);
    return ReflectionUtils.instantiateWithCustomCtor(peerStorageClass,
      new Class[] { ZKWatcher.class, Configuration.class }, new Object[] { zk, conf });
  }

  /**
   * Create a new {@link ReplicationQueueStorage}.
   */
  public static ReplicationQueueStorage getReplicationQueueStorage(ZKWatcher zk,
      Configuration conf) {
    String queueStorageClass =
        conf.get(REPLICATION_QUEUE_STORAGE_IMPL, DEFAULT_REPLICATION_QUEUE_STORAGE_IMPL);
    return ReflectionUtils.instantiateWithCustomCtor(queueStorageClass,
      new Class[] { ZKWatcher.class, Configuration.class }, new Object[] { zk, conf });
  }
}
