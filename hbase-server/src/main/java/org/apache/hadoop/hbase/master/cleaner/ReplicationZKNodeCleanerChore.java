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
package org.apache.hadoop.hbase.master.cleaner;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Chore that will clean the replication queues belonging to the peer which does not exist.
 */
@InterfaceAudience.Private
public class ReplicationZKNodeCleanerChore extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationZKNodeCleanerChore.class);
  private final ReplicationZKNodeCleaner cleaner;

  public ReplicationZKNodeCleanerChore(Stoppable stopper, int period,
      ReplicationZKNodeCleaner cleaner) {
    super("ReplicationZKNodeCleanerChore", stopper, period);
    this.cleaner = cleaner;
  }

  @Override
  protected void chore() {
    try {
      Map<ServerName, List<String>> undeletedQueues = cleaner.getUnDeletedQueues();
      cleaner.removeQueues(undeletedQueues);
    } catch (IOException e) {
      LOG.warn("Failed to clean replication zk node", e);
    }
  }
}
