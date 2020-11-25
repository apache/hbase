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

import java.util.Collections;
import java.util.List;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Simple scheduler for procedures
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SimpleProcedureScheduler extends AbstractProcedureScheduler {
  private final ProcedureDeque runnables = new ProcedureDeque();

  @Override
  protected void enqueue(final Procedure procedure, final boolean addFront) {
    if (addFront) {
      runnables.addFirst(procedure);
    } else {
      runnables.addLast(procedure);
    }
  }

  @Override
  protected Procedure dequeue() {
    return runnables.poll();
  }

  @Override
  public void clear() {
    schedLock();
    try {
      runnables.clear();
    } finally {
      schedUnlock();
    }
  }

  @Override
  public void yield(final Procedure proc) {
    addBack(proc);
  }

  @Override
  public boolean queueHasRunnables() {
    return runnables.size() > 0;
  }

  @Override
  public int queueSize() {
    return runnables.size();
  }

  @Override
  public void completionCleanup(Procedure proc) {
  }

  @Override
  public List<LockedResource> getLocks() {
    return Collections.emptyList();
  }

  @Override
  public LockedResource getLockResource(LockedResourceType resourceType,
      String resourceName) {
    return null;
  }
}
