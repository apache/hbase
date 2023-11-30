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
package org.apache.hadoop.hbase.replication.master;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A barrier to guard the execution of {@link ReplicationLogCleaner}.
 * <p/>
 * The reason why we introduce this class is because there could be race between
 * {@link org.apache.hadoop.hbase.master.replication.AddPeerProcedure} and
 * {@link ReplicationLogCleaner}. See HBASE-27214 for more details.
 */
@InterfaceAudience.Private
public class ReplicationLogCleanerBarrier {

  private enum State {
    // the cleaner is not running
    NOT_RUNNING,
    // the cleaner is running
    RUNNING,
    // the cleaner is disabled
    DISABLED
  }

  private State state = State.NOT_RUNNING;

  // we could have multiple AddPeerProcedure running at the same time, so here we need to do
  // reference counting.
  private int numberDisabled = 0;

  public synchronized boolean start() {
    if (state == State.NOT_RUNNING) {
      state = State.RUNNING;
      return true;
    }
    if (state == State.DISABLED) {
      return false;
    }
    throw new IllegalStateException("Unexpected state " + state);
  }

  public synchronized void stop() {
    if (state != State.RUNNING) {
      throw new IllegalStateException("Unexpected state " + state);
    }
    state = State.NOT_RUNNING;
  }

  public synchronized boolean disable() {
    if (state == State.RUNNING) {
      return false;
    }
    if (state == State.NOT_RUNNING) {
      state = State.DISABLED;
    }
    numberDisabled++;
    return true;
  }

  public synchronized void enable() {
    if (state != State.DISABLED) {
      throw new IllegalStateException("Unexpected state " + state);
    }
    numberDisabled--;
    if (numberDisabled == 0) {
      state = State.NOT_RUNNING;
    }
  }
}
