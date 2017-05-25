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

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Basic ProcedureEvent that contains an "object", which can be a description or a reference to the
 * resource to wait on, and a queue for suspended procedures.
 * Access to suspended procedures queue is 'synchronized' on the event itself.
 */
@InterfaceAudience.Private
public class ProcedureEvent<T> {
  private final T object;
  private boolean ready = false;
  private ProcedureDeque suspendedProcedures = new ProcedureDeque();

  public ProcedureEvent(final T object) {
    this.object = object;
  }

  public synchronized boolean isReady() {
    return ready;
  }

  synchronized void setReady(final boolean isReady) {
    this.ready = isReady;
  }

  public ProcedureDeque getSuspendedProcedures() {
    return suspendedProcedures;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " for " + object + ", ready=" + isReady() +
        ", suspended procedures count=" + getSuspendedProcedures().size();
  }
}