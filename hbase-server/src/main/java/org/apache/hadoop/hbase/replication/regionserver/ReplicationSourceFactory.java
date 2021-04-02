/*
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
package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Constructs a {@link ReplicationSourceInterface}
 * Note, not used to create specialized ReplicationSources
 * @see CatalogReplicationSource
 */
@InterfaceAudience.Private
public final class ReplicationSourceFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationSourceFactory.class);

  private ReplicationSourceFactory() {}

  static ReplicationSourceInterface create(Configuration conf, String queueId) {
    ReplicationQueueInfo replicationQueueInfo = new ReplicationQueueInfo(queueId);
    boolean isQueueRecovered = replicationQueueInfo.isQueueRecovered();
    ReplicationSourceInterface src;
    try {
      String defaultReplicationSourceImpl =
          isQueueRecovered ? RecoveredReplicationSource.class.getCanonicalName()
              : ReplicationSource.class.getCanonicalName();
      Class<?> c = Class.forName(
        conf.get("replication.replicationsource.implementation", defaultReplicationSourceImpl));
      src = c.asSubclass(ReplicationSourceInterface.class).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      LOG.warn("Passed replication source implementation throws errors, "
          + "defaulting to ReplicationSource",
        e);
      src = isQueueRecovered ? new RecoveredReplicationSource() : new ReplicationSource();
    }
    return src;
  }
}
