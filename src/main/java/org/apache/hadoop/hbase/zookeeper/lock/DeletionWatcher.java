/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.zookeeper.lock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.util.concurrent.CountDownLatch;

/**
 * A ZooKeeper watcher meant to detect deletions of ZNodes.
 */
public class DeletionWatcher implements Watcher {

  private static final Log LOG = LogFactory.getLog(DeletionWatcher.class);

  private final ZooKeeperWrapper zooKeeperWrapper;
  private final String pathToWatch;
  private final CountDownLatch deletedLatch;

  private volatile Throwable exception;

  /**
   * Create a new instance of the deletion watcher.
   * @param zkWrapper
   * @param pathToWatch (Fully qualified) ZNode path that we are waiting to
   *                    be deleted.
   * @param deletedLatch Count down on this latch when deletion has occured.
   */
  public DeletionWatcher(ZooKeeperWrapper zkWrapper, String pathToWatch,
      CountDownLatch deletedLatch) {
    this.zooKeeperWrapper = zkWrapper;
    this.pathToWatch = pathToWatch;
    this.deletedLatch = deletedLatch;
    exception = null;
  }

  /**
   * Check if an exception has occurred when re-setting the watch.
   * @return True if we were unable to re-set a watch on a ZNode due to
   *         an exception.
   */
  public boolean hasException() {
    return exception != null;
  }

  /**
   * Get the last exception which has occurred when re-setting the watch.
   * Use hasException() to check whether or not an exception has occurred.
   * @return The last exception observed when re-setting the watch.
   */
  public Throwable getException() {
    return exception;
  }

  @Override
  public void process(WatchedEvent watchedEvent) {
    if (watchedEvent.getPath().equals(pathToWatch)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Processing event " + watchedEvent + " on " + pathToWatch);
      }
      try {
        if (watchedEvent.getType() == EventType.NodeDeleted ||
            !(zooKeeperWrapper.setWatchIfNodeExists(pathToWatch))) {
          deletedLatch.countDown();
        }
      } catch (Throwable t) {
        exception = t;
        deletedLatch.countDown();
        LOG.error("Error when re-setting the watch on " + pathToWatch, t);
      }
    }
  }
}
