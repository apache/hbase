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
package org.apache.hadoop.hbase.namequeues;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.regionHistorian.RegionHistorianTableAccessor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionHist;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.EvictingQueue;
import org.apache.hbase.thirdparty.com.google.common.collect.Queues;

/**
 * Persistent service provider for RegionHistorian events
 */
@InterfaceAudience.Private
public class RegionHistorianPersistentService {
  private static final Logger LOG = LoggerFactory.getLogger(RegionHistorianPersistentService.class);

  private static final ReentrantLock LOCK = new ReentrantLock();
  private static final String SYS_TABLE_QUEUE_SIZE =
    "hbase.regionserver.regionHistorian.systable.queue.size";
  private static final int DEFAULT_SYS_TABLE_QUEUE_SIZE = 1000;
  private static final int SYSTABLE_PUT_BATCH_SIZE = 100;

  private final Queue<RegionHist.RegionHistorianPayload> queueForSysTable;

  private Configuration configuration;

  public RegionHistorianPersistentService(final Configuration configuration) {
    this.configuration = configuration;
    int sysTableQueueSize =
      configuration.getInt(SYS_TABLE_QUEUE_SIZE, DEFAULT_SYS_TABLE_QUEUE_SIZE);
    EvictingQueue<RegionHist.RegionHistorianPayload> evictingQueueForTable =
      EvictingQueue.create(sysTableQueueSize);
    queueForSysTable = Queues.synchronizedQueue(evictingQueueForTable);
  }

  public void addToQueueForSysTable(RegionHist.RegionHistorianPayload regionHistorianPayload) {
    queueForSysTable.add(regionHistorianPayload);
  }

  public void addAllLogsToSysTable(Connection connection){
    if (LOCK.isLocked()) {
      return;
    }
    LOCK.lock();
    try {
      List<RegionHist.RegionHistorianPayload> regionHistorianPayloads = new ArrayList<>();
      int i = 0;
      while (!queueForSysTable.isEmpty()) {
        regionHistorianPayloads.add(queueForSysTable.poll());
        i++;
        if (i == SYSTABLE_PUT_BATCH_SIZE) {
          RegionHistorianTableAccessor.addRegionHistorianRecords(regionHistorianPayloads, connection);
          regionHistorianPayloads.clear();
          i = 0;
        }
      }
      if (regionHistorianPayloads.size() > 0) {
        RegionHistorianTableAccessor.addRegionHistorianRecords(regionHistorianPayloads, connection);
      }
    } finally {
      LOCK.unlock();
    }
  }
}
