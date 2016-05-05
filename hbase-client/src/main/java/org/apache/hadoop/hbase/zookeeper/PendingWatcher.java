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

package org.apache.hadoop.hbase.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Placeholder of a watcher which might be triggered before the instance is not yet created.
 * <p>
 * {@code ZooKeeper} starts its event thread within its constructor (and that is an anti-pattern),
 * and the watcher passed to the constructor might be called back by the event thread
 * before you get the instance of {@code ZooKeeper} from the constructor.
 * If your watcher calls methods of {@code ZooKeeper},
 * pass this placeholder to the constructor of the {@code ZooKeeper},
 * create your watcher using the instance of {@code ZooKeeper},
 * and then call the method {@code PendingWatcher.prepare}.
 */
class PendingWatcher implements Watcher {
  private final InstancePending<Watcher> pending = new InstancePending<Watcher>();

  @Override
  public void process(WatchedEvent event) {
    pending.get().process(event);
  }

  /**
   * Associates the substantial watcher of processing events.
   * This method should be called once, and {@code watcher} should be non-null.
   * This method is expected to call as soon as possible
   * because the event processing, being invoked by the ZooKeeper event thread,
   * is uninterruptibly blocked until this method is called.
   */
  void prepare(Watcher watcher) {
    pending.prepare(watcher);
  }
}
