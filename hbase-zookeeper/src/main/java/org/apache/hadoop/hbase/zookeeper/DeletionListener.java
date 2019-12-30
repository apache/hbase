/*
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
package org.apache.hadoop.hbase.zookeeper;

import java.util.concurrent.CountDownLatch;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ZooKeeper watcher meant to detect deletions of ZNodes.
 */
@InterfaceAudience.Private
public class DeletionListener extends ZKListener {

  private static final Logger LOG = LoggerFactory.getLogger(DeletionListener.class);

  private final String pathToWatch;
  private final CountDownLatch deletedLatch;

  private volatile Throwable exception;

  /**
   * Create a new instance of the deletion watcher.
   * @param zkWatcher ZookeeperWatcher instance
   * @param pathToWatch (Fully qualified) ZNode path that we are waiting to
   *                    be deleted.
   * @param deletedLatch Count down on this latch when deletion has occurred.
   */
  public DeletionListener(ZKWatcher zkWatcher, String pathToWatch,
                          CountDownLatch deletedLatch) {
    super(zkWatcher);
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
  public void nodeDataChanged(String path) {
    if (!path.equals(pathToWatch)) {
      return;
    }
    try {
      if (!(ZKUtil.setWatchIfNodeExists(watcher, pathToWatch))) {
        deletedLatch.countDown();
      }
    } catch (KeeperException ex) {
      exception = ex;
      deletedLatch.countDown();
      LOG.error("Error when re-setting the watch on " + pathToWatch, ex);
    }
  }

  @Override
  public void nodeDeleted(String path) {
    if (!path.equals(pathToWatch)) {
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing delete on {}", pathToWatch);
    }
    deletedLatch.countDown();
  }
}
