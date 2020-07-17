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

package org.apache.hadoop.hbase.namequeues;

import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Chore to insert multiple accumulated slow/large logs to hbase:slowlog system table
 */
@InterfaceAudience.Private
public class SlowLogTableOpsChore extends ScheduledChore {

  private static final Logger LOG = LoggerFactory.getLogger(SlowLogTableOpsChore.class);

  private final NamedQueueRecorder namedQueueRecorder;

  /**
   * Chore Constructor
   *
   * @param stopper The stopper - When {@link Stoppable#isStopped()} is true, this chore will
   *   cancel and cleanup
   * @param period Period in millis with which this Chore repeats execution when scheduled
   * @param namedQueueRecorder {@link NamedQueueRecorder} instance
   */
  public SlowLogTableOpsChore(final Stoppable stopper, final int period,
      final NamedQueueRecorder namedQueueRecorder) {
    super("SlowLogTableOpsChore", stopper, period);
    this.namedQueueRecorder = namedQueueRecorder;
  }

  @Override
  protected void chore() {
    if (LOG.isTraceEnabled()) {
      LOG.trace("SlowLog Table Ops Chore is starting up.");
    }
    namedQueueRecorder.persistAll(NamedQueuePayload.NamedQueueEvent.SLOW_LOG);
    if (LOG.isTraceEnabled()) {
      LOG.trace("SlowLog Table Ops Chore is closing.");
    }
  }

}
