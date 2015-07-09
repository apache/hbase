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

package org.apache.hadoop.hbase.procedure2.store;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for {@link ProcedureStore}s.
 */
public abstract class ProcedureStoreBase implements ProcedureStore {
  private final CopyOnWriteArrayList<ProcedureStoreListener> listeners =
      new CopyOnWriteArrayList<ProcedureStoreListener>();

  private final AtomicBoolean running = new AtomicBoolean(false);

  /**
   * Change the state to 'isRunning',
   * returns true if the store state was changed,
   * false if the store was already in that state.
   * @param isRunning the state to set.
   * @return true if the store state was changed, otherwise false.
   */
  protected boolean setRunning(boolean isRunning) {
    return running.getAndSet(isRunning) != isRunning;
  }

  @Override
  public boolean isRunning() {
    return running.get();
  }

  @Override
  public void registerListener(ProcedureStoreListener listener) {
    listeners.add(listener);
  }

  @Override
  public boolean unregisterListener(ProcedureStoreListener listener) {
    return listeners.remove(listener);
  }

  protected void sendPostSyncSignal() {
    if (!this.listeners.isEmpty()) {
      for (ProcedureStoreListener listener : this.listeners) {
        listener.postSync();
      }
    }
  }

  protected void sendAbortProcessSignal() {
    if (!this.listeners.isEmpty()) {
      for (ProcedureStoreListener listener : this.listeners) {
        listener.abortProcess();
      }
    }
  }
}