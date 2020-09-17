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
package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Constructs appropriate {@link ReplicationSourceInterface}.
 * Considers whether Recovery or not, whether hbase:meta Region Read Replicas or not, etc.
 */
@InterfaceAudience.Private
public final class ReplicationSourceFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationSourceFactory.class);

  private ReplicationSourceFactory() {}

  static ReplicationSourceInterface create(Configuration conf, String queueId,
      WALFactory walFactory) throws IOException {
    // Check for the marker name used to enable a replication source for hbase:meta for region read
    // replicas. There is no peer nor use of replication storage (or need for queue recovery) when
    // running hbase:meta region read replicas.
    if (ReplicationSourceManager.META_REGION_REPLICA_REPLICATION_SOURCE.equals(queueId)) {
      return new HBaseMetaNoQueueStoreReplicationSource(walFactory.getMetaProvider());
    }
    ReplicationQueueInfo replicationQueueInfo = new ReplicationQueueInfo(queueId);
    boolean queueRecovered = replicationQueueInfo.isQueueRecovered();
    String defaultReplicationSourceImpl = null;
    Class<?> c = null;
    try {
      defaultReplicationSourceImpl = queueRecovered?
        RecoveredReplicationSource.class.getCanonicalName():
        ReplicationSource.class.getCanonicalName();
      c = Class.forName(conf.get("replication.replicationsource.implementation",
        defaultReplicationSourceImpl));
      return c.asSubclass(ReplicationSourceInterface.class).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      LOG.warn("Configured replication class {} failed construction, defaulting to {}",
        c != null? c.getName(): "null", defaultReplicationSourceImpl, e);
      WALProvider walProvider = walFactory == null? null: walFactory.getWALProvider();
      return queueRecovered? new RecoveredReplicationSource(walProvider): new ReplicationSource(walProvider);
    }
  }
}
