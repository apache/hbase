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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Class that handles the recovered source of a replication stream, which is transfered from another
 * dead region server. This will be closed when all logs are pushed to peer cluster.
 */
@InterfaceAudience.Private
public class RecoveredReplicationSource extends ReplicationSource {

  @Override
  protected void startShippers() {
    for (String walGroupId : logQueue.getQueues().keySet()) {
      workerThreads.put(walGroupId, createNewShipper(walGroupId));
    }
    // start shippers after initializing the workerThreads, as in the below postFinish logic, if
    // workerThreads is empty, we will mark the RecoveredReplicationSource as finished. So if we
    // start the worker on the fly, it is possible that a shipper has already finished its work and
    // called postFinish, and find out the workerThreads is empty and then mark the
    // RecoveredReplicationSource as finish, while the next shipper has not been added to
    // workerThreads yet. See HBASE-28155 for more details.
    for (ReplicationSourceShipper shipper : workerThreads.values()) {
      startShipper(shipper);
    }
  }

  @Override
  protected RecoveredReplicationSourceShipper createNewShipper(String walGroupId,
    ReplicationSourceWALReader walReader) {
    return new RecoveredReplicationSourceShipper(conf, walGroupId, this, walReader, queueStorage,
      () -> {
        if (workerThreads.isEmpty()) {
          this.getSourceMetrics().clear();
          if (this.getReplicationEndpoint() != null) {
            this.getReplicationEndpoint().stop();
          }
          manager.finishRecoveredSource(this);
        }
      });
  }
}
