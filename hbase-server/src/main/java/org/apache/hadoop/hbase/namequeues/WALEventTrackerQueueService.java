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

import static org.apache.hadoop.hbase.master.waleventtracker.WALEventTrackerTableCreator.WAL_EVENT_TRACKER_ENABLED_DEFAULT;
import static org.apache.hadoop.hbase.master.waleventtracker.WALEventTrackerTableCreator.WAL_EVENT_TRACKER_ENABLED_KEY;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.namequeues.request.NamedQueueGetRequest;
import org.apache.hadoop.hbase.namequeues.response.NamedQueueGetResponse;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.EvictingQueue;

/*
  This class provides the queue to save Wal events from backing RingBuffer.
 */
@InterfaceAudience.Private
public class WALEventTrackerQueueService implements NamedQueueService {

  private EvictingQueue<WALEventTrackerPayload> queue;
  private static final String WAL_EVENT_TRACKER_RING_BUFFER_SIZE =
    "hbase.regionserver.wal.event.tracker.ringbuffer.size";
  private final boolean walEventTrackerEnabled;
  private int queueSize;
  private MetricsWALEventTrackerSource source = null;

  private static final Logger LOG = LoggerFactory.getLogger(WALEventTrackerQueueService.class);

  public WALEventTrackerQueueService(Configuration conf) {
    this(conf, null);
  }

  public WALEventTrackerQueueService(Configuration conf, MetricsWALEventTrackerSource source) {
    this.walEventTrackerEnabled =
      conf.getBoolean(WAL_EVENT_TRACKER_ENABLED_KEY, WAL_EVENT_TRACKER_ENABLED_DEFAULT);
    if (!walEventTrackerEnabled) {
      return;
    }

    this.queueSize = conf.getInt(WAL_EVENT_TRACKER_RING_BUFFER_SIZE, 256);
    queue = EvictingQueue.create(queueSize);
    if (source == null) {
      this.source = CompatibilitySingletonFactory.getInstance(MetricsWALEventTrackerSource.class);
    } else {
      this.source = source;
    }
  }

  @Override
  public NamedQueuePayload.NamedQueueEvent getEvent() {
    return NamedQueuePayload.NamedQueueEvent.WAL_EVENT_TRACKER;
  }

  @Override
  public void consumeEventFromDisruptor(NamedQueuePayload namedQueuePayload) {
    if (!walEventTrackerEnabled) {
      return;
    }
    if (!(namedQueuePayload instanceof WALEventTrackerPayload)) {
      LOG.warn("WALEventTrackerQueueService: NamedQueuePayload is not of type"
        + " WALEventTrackerPayload.");
      return;
    }

    WALEventTrackerPayload payload = (WALEventTrackerPayload) namedQueuePayload;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding wal event tracker payload " + payload);
    }
    addToQueue(payload);
  }

  /*
   * Made it default to use it in testing.
   */
  synchronized void addToQueue(WALEventTrackerPayload payload) {
    queue.add(payload);
  }

  @Override
  public boolean clearNamedQueue() {
    if (!walEventTrackerEnabled) {
      return false;
    }
    LOG.debug("Clearing wal event tracker queue");
    queue.clear();
    return true;
  }

  @Override
  public NamedQueueGetResponse getNamedQueueRecords(NamedQueueGetRequest request) {
    return null;
  }

  @Override
  public void persistAll(Connection connection) {
    if (!walEventTrackerEnabled) {
      return;
    }
    if (queue.isEmpty()) {
      LOG.debug("Wal Event tracker queue is empty.");
      return;
    }

    Queue<WALEventTrackerPayload> queue = getWALEventTrackerList();
    try {
      WALEventTrackerTableAccessor.addWalEventTrackerRows(queue, connection);
    } catch (Exception ioe) {
      // If we fail to persist the records with retries then just forget about them.
      // This is a best effort service.
      LOG.error("Failed while persisting wal tracker records", ioe);
      // Increment metrics for failed puts
      source.incrFailedPuts(queue.size());
    }
  }

  private synchronized Queue<WALEventTrackerPayload> getWALEventTrackerList() {
    Queue<WALEventTrackerPayload> retQueue = new ArrayDeque<>();
    Iterator<WALEventTrackerPayload> iterator = queue.iterator();
    while (iterator.hasNext()) {
      retQueue.add(iterator.next());
    }
    queue.clear();
    return retQueue;
  }
}
