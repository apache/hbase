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

import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Chore to insert multiple accumulated slow/large logs to hbase:slowlog system table
 */
@InterfaceAudience.Private
public class NamedQueueServiceChore extends ScheduledChore {

  private static final Logger LOG = LoggerFactory.getLogger(NamedQueueServiceChore.class);
  public static final String NAMED_QUEUE_CHORE_DURATION_KEY =
    "hbase.regionserver.named.queue.chore.duration";
  // 10 mins default.
  public static final int NAMED_QUEUE_CHORE_DURATION_DEFAULT = 10 * 60 * 1000;

  private final NamedQueueRecorder namedQueueRecorder;
  private final Connection connection;

  /**
   * Chore Constructor
   * @param stopper            The stopper - When {@link Stoppable#isStopped()} is true, this chore
   *                           will cancel and cleanup
   * @param period             Period in millis with which this Chore repeats execution when
   *                           scheduled
   * @param namedQueueRecorder {@link NamedQueueRecorder} instance
   */
  public NamedQueueServiceChore(final Stoppable stopper, final int period,
    final NamedQueueRecorder namedQueueRecorder, Connection connection) {
    super("NamedQueueServiceChore", stopper, period);
    this.namedQueueRecorder = namedQueueRecorder;
    this.connection = connection;
  }

  @Override
  protected void chore() {
    for (NamedQueuePayload.NamedQueueEvent event : NamedQueuePayload.NamedQueueEvent.values()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Starting chore for event %s", event.name()));
      }
      namedQueueRecorder.persistAll(event, connection);
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Stopping chore for event %s", event.name()));
      }
    }
  }
}
