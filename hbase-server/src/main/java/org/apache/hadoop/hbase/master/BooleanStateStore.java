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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

/**
 * Store a boolean state.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "UG_SYNC_SET_UNSYNC_GET",
    justification = "the flag is volatile")
@InterfaceAudience.Private
public abstract class BooleanStateStore extends MasterStateStore {

  private volatile boolean on;

  protected BooleanStateStore(MasterRegion masterRegion, String stateName, ZKWatcher watcher,
    String zkPath) throws IOException, KeeperException, DeserializationException {
    super(masterRegion, stateName, watcher, zkPath);
    byte[] state = getState();
    this.on = state == null || parseFrom(state);
  }

  /**
   * Returns true if the flag is on, otherwise false
   */
  public boolean get() {
    return on;
  }

  /**
   * Set the flag on/off.
   * @param on true if the flag should be on, false otherwise
   * @throws IOException if the operation fails
   * @return returns the previous state
   */
  public synchronized boolean set(boolean on) throws IOException {
    byte[] state = toByteArray(on);
    setState(state);
    boolean prevOn = this.on;
    this.on = on;
    return prevOn;
  }

  protected abstract byte[] toByteArray(boolean on);

  protected abstract boolean parseFrom(byte[] bytes) throws DeserializationException;
}
