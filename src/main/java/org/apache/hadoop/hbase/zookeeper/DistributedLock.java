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

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * ZooKeeper based distributed lock. This lock is <b>not</b> re-entrant.
 */
public class DistributedLock {

  private static final Log LOG = LogFactory.getLog(DistributedLock.class);

  private final ZooKeeperWrapper zkWrapper;

  private final String lockZNode;

  private final byte[] lockMeta;

  private final String fullyQualifiedZNode;

  private final OwnerMetadataHandler metadataHandler;

  private volatile int lockZNodeVersion;

  /**
   * Create a distributed lock instance.
   * @param zkWrapper
   * @param lockZNode (Non-fully qualified) ZNode path for the lock
   * @param lockMeta  Some metadata about the node. Preferably, a human
   *                  readable string. Must not be null.
   * @param metadataHandler If not null, use this to parse lockMeta
   */
  public DistributedLock(ZooKeeperWrapper zkWrapper, String lockZNode,
    byte[] lockMeta, OwnerMetadataHandler metadataHandler) {
    Preconditions.checkNotNull(lockMeta);

    this.zkWrapper = zkWrapper;
    this.lockZNode = lockZNode;
    this.lockMeta = lockMeta;
    this.metadataHandler = metadataHandler;
    this.fullyQualifiedZNode = zkWrapper.getZNode(zkWrapper.getParentZNode(),
      lockZNode);
    this.lockZNodeVersion = -1;
  }


  /**
   * Acquires the lock, waiting indefinitely until the lock is released, or
   * the thread is interrupted.
   * @throws IOException
   * @throws InterruptedException If current thread is interrupted while
   *                              waiting for the lock
   */
  public void acquire()
  throws IOException, InterruptedException {
    if (!tryAcquire(-1)) {
      // Should either throw an exception or acquire lock
      throw new IllegalStateException("tryAcquire() should either wait" +
        " indefinitely, acquire the lock, or throw an exception");
    }
  }

  /**
   * Acquired a lock within a given wait time
   * @param timeoutMs The maximum time (in milliseconds) to wait for the lock,
   *                  -1 to wait indefinitely.
   * @return True if the lock was acquired, false if waiting time elapsed
   *         before the lock was acquired
   * @throws IOException
   * @throws InterruptedException If thread is interrupted while waiting to
   *                              acquire the lock
   */
  public boolean tryAcquire(long timeoutMs)
  throws IOException, InterruptedException {
    if (isAcquired()) {
      throw new IllegalStateException("Lock " + fullyQualifiedZNode +
        "has already been acquired");
    }

    boolean acquiredLock = waitForLock(timeoutMs);
    if (acquiredLock) {
      lockZNodeVersion = getZNodeVersionOrThrow();
      LOG.debug("Acquired lock " + fullyQualifiedZNode + ", version " +
        lockZNodeVersion);
    } else {
      LOG.error("Unable to acquire lock " + fullyQualifiedZNode + " in " +
        timeoutMs + " ms");
    }
    return acquiredLock;
  }

  /**
   * Main loop: if a ZNode already exists for the lock, set a watcher, to
   * await the ZNode's deletion. Once the ZNode is deleted, try creating
   * the ZNode again. Continue this loop until either the current thread
   * is interrupted or maximum waiting time (if set) elapses.
   * @param timeoutMs Maximum waiting time, -1 to wait indefinitely.
   * @return False if maximum waiting time (if set) has elapsed
   */
  private boolean waitForLock(long timeoutMs)
  throws IOException, InterruptedException {
    boolean hasTimeout = timeoutMs != -1;
    long waitUntilMs =
      hasTimeout ? EnvironmentEdgeManager.currentTimeMillis() + timeoutMs : -1;
    boolean acquiredLock = false;
    boolean shouldCreate = true;
    while (!acquiredLock && shouldCreate) {
      CountDownLatch watcherFinished = new CountDownLatch(1);
      DeletionWatcher watcher = new DeletionWatcher(watcherFinished);

      // It's possible for another thread to acquire a znode after our
      // watcher has fired
      try {
        acquiredLock = tryCreateZNode(watcher);
      } catch (KeeperException.NoNodeException e) {
        // If the znode is deleted before the watcher could be set,
        // simulate the watcher firing; this means we will still
        // check if the specified waiting time has elapsed before
        // trying to create the znode again.
        watcherFinished.countDown();
      }
      try {
        if (!acquiredLock) {
          printLockMetadata();
          if (hasTimeout) {
            long remainingMs =
              waitUntilMs - EnvironmentEdgeManager.currentTimeMillis();
            if (remainingMs < 0) {
              shouldCreate = false;
            } else {
              shouldCreate = watcherFinished.await(remainingMs,
                TimeUnit.MILLISECONDS);
            }
          } else {
            // If a timeout is not set, await indefinitely
            watcherFinished.await();
          }
          // Check if the watcher thread encountered an exception
          // when re-setting the watch
          if (watcher.hasException()) {
            Throwable t = watcher.getException();
            LOG.error("Exception in the watcher ", t);
            throw new IOException("Exception in the watcher", t);
          }
        }
      } finally {
        zkWrapper.unregisterListener(watcher);
      }
    }
    return acquiredLock;
  }

  /**
   * Check if the lock is already acquired.
   */
  public boolean isAcquired() {
    return lockZNodeVersion != -1;
  }

  private int getZNodeVersionOrThrow()
  throws IOException {
    Stat stat = new Stat();
    if (zkWrapper.readZNode(fullyQualifiedZNode, stat) == null) {
      throw new IllegalStateException("ZNode " + fullyQualifiedZNode +
        " no longer exists!");
    }
    return stat.getVersion();
  }

  private boolean tryCreateZNode(Watcher watcher)
  throws IOException, InterruptedException, KeeperException.NoNodeException {
    return zkWrapper.checkExistsAndCreate(lockZNode, lockMeta,
      CreateMode.PERSISTENT, watcher);
  }

  private boolean printLockMetadata()
  throws IOException, InterruptedException {
    byte[] ownerMetadata = null;
    Stat stat = new Stat();
    try {
      ownerMetadata = zkWrapper.readZNodeIfExists(lockZNode, stat);
      if (ownerMetadata == null) {
        return false;
      }
      LOG.trace("Lock ZNode statistics " + stat);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted reading ZNode " + fullyQualifiedZNode, e);
      throw e;
    }
    if (metadataHandler != null) {
      metadataHandler.printOwnerMetadata(ownerMetadata);
    }
    return true;
  }

  /**
   * Release the lock.
   * @throws IOException If there is an unrecoverable ZooKeeper error
   * @throws InterruptedException If the current thread is interrupted while
   *                              releasing the lock
   */
  public void release()
  throws IOException, InterruptedException {
    if (!isAcquired()) {
      throw new IllegalStateException("Lock " + fullyQualifiedZNode +
        " is not held by this thread");
    }
    try {
      if (zkWrapper.checkExists(fullyQualifiedZNode) != -1) {
        zkWrapper.deleteZNode(fullyQualifiedZNode, false, lockZNodeVersion);
        lockZNodeVersion = -1;
        LOG.debug("Released lock " + fullyQualifiedZNode);
      } else {
        throw new IllegalStateException("ZNode " + fullyQualifiedZNode +
          " does not exist in ZooKeeper");
      }
    } catch (KeeperException.BadVersionException e) {
      LOG.error("Attempted to delete a version we do not hold", e);
      throw new IllegalStateException("ZNode " + fullyQualifiedZNode +
        " has been modified since acquiring the lock");
    } catch (KeeperException e) {
      LOG.error("Unrecoverable ZooKeeper error releasing lock for " +
                  fullyQualifiedZNode, e);
      throw new IOException("Unrecoverable ZooKeeper error", e);
    }
  }

  /**
   * Callback interface to parse, and print metadata associated with locks
   * (e.g., owner, purpose) stored within ZNodes.
   */
  public static interface OwnerMetadataHandler {
    /**
     * Called after the metadata associated with the lock is read.
     * @param ownerMetadata
     */
    public void printOwnerMetadata(byte[] ownerMetadata);
  }

  private class DeletionWatcher implements Watcher {
    private final CountDownLatch watcherFinished;

    private volatile Throwable exception = null;

    DeletionWatcher(CountDownLatch watcherFinished) {
      this.watcherFinished = watcherFinished;
    }

    public Throwable getException() {
      return exception;
    }

    public boolean hasException() {
      return exception != null;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
      if (watchedEvent.getPath().equals(fullyQualifiedZNode)) {
        if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
          watcherFinished.countDown();
        } else {
          try {
            zkWrapper.watchAndCheckExists(fullyQualifiedZNode);
          } catch (Throwable t) {
            // If an exception occurs when we try to re-set the watcher,
            // we should return control back waitForLock(): otherwise,
            // waitForLock() would continue to wait for countDown on
            // watcherFinished, which would never happen as the exception
            // prevented the watcher from being re-set. When waitForLock()
            // finishes waiting for the latch, it will use hasException()
            // and getException() to check whether the watcher has encounter
            // an exception and (if an exception was encountered), re-throw
            // the exception.
            exception = t;
            watcherFinished.countDown();
            LOG.error("Error when re-setting the watch on " +
              fullyQualifiedZNode, t);
          }
        }
      }
    }
  }
}
