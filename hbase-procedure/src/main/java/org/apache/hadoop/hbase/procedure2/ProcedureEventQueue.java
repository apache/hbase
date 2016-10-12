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

package org.apache.hadoop.hbase.procedure2;

import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayDeque;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Basic queue to store suspended procedures.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProcedureEventQueue {
  private static final Log LOG = LogFactory.getLog(ProcedureEventQueue.class);

  private ArrayDeque<Procedure> waitingProcedures = null;

  public ProcedureEventQueue() {
  }

  @InterfaceAudience.Private
  public synchronized void suspendProcedure(final Procedure proc) {
    if (waitingProcedures == null) {
      waitingProcedures = new ArrayDeque<Procedure>();
    }
    waitingProcedures.addLast(proc);
  }

  @InterfaceAudience.Private
  public synchronized void removeProcedure(final Procedure proc) {
    if (waitingProcedures != null) {
      waitingProcedures.remove(proc);
    }
  }

  @InterfaceAudience.Private
  public synchronized boolean hasWaitingProcedures() {
    return waitingProcedures != null;
  }

  @InterfaceAudience.Private
  public synchronized Procedure popWaitingProcedure(final boolean popFront) {
    // it will be nice to use IterableList on a procedure and avoid allocations...
    Procedure proc = popFront ? waitingProcedures.removeFirst() : waitingProcedures.removeLast();
    if (waitingProcedures.isEmpty()) {
      waitingProcedures = null;
    }
    return proc;
  }

  @VisibleForTesting
  public synchronized void clear() {
    waitingProcedures = null;
  }

  @VisibleForTesting
  public synchronized int size() {
    if (waitingProcedures != null) {
      return waitingProcedures.size();
    }
    return 0;
  }
}
