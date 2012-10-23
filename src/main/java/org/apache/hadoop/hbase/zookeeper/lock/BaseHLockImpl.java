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
import org.apache.hadoop.hbase.HLock;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ZooKeeper based HLock implementation. Based on the Shared Locks recipe.
 * (see:
 * <a href="http://zookeeper.apache.org/doc/trunk/recipes.html">
 * ZooKeeper Recipes and Solutions
 * </a>)
 */
public abstract class BaseHLockImpl implements HLock {

  private static final Log LOG = LogFactory.getLog(BaseHLockImpl.class);

  /** ZNode prefix used by processes acquiring reader locks */
  protected static final String READ_LOCK_CHILD_NODE = "read-";

  /** ZNode prefix used by processes acquiring writer locks */
  protected static final String WRITE_LOCK_CHILD_NODE = "write-";

  protected final ZooKeeperWrapper zkWrapper;
  protected final String parentLockNode;
  protected final String fullyQualifiedZNode;
  protected final byte[] metadata;
  protected final MetadataHandler handler;

  // If we acquire a lock, update this field
  protected final AtomicReference<AcquiredLock> acquiredLock =
      new AtomicReference<AcquiredLock>(null);

  /**
   * Called by implementing classes.
   * @param zkWrapper
   * @param name The (non-fully qualified) lock ZNode path
   * @param metadata
   * @param handler
   * @param childNode The prefix for child nodes created under the parent
   */
  protected BaseHLockImpl(ZooKeeperWrapper zkWrapper,
      String name, byte[] metadata, MetadataHandler handler,
      String childNode) {
    this.zkWrapper = zkWrapper;
    parentLockNode = zkWrapper.getZNode(zkWrapper.getParentZNode(), name);
    this.fullyQualifiedZNode = zkWrapper.getZNode(parentLockNode, childNode);
    this.metadata = metadata;
    this.handler = handler;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void acquire() throws IOException, InterruptedException {
    tryAcquire(-1);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean tryAcquire(long timeoutMs)
  throws IOException, InterruptedException {
    boolean hasTimeout = timeoutMs != -1;
    long waitUntilMs =
        hasTimeout ?EnvironmentEdgeManager.currentTimeMillis() + timeoutMs : -1;
    String createdZNode = createLockZNode();
    while (true) {
      List<String> children;
      try {
        children = zkWrapper.listChildrenNoWatch(parentLockNode);
      } catch (KeeperException e) {
        LOG.error("Unexpected ZooKeeper error when listing children", e);
        throw new IOException("Unexpected ZooKeeper exception", e);
      }
      String pathToWatch;
      if ((pathToWatch = getLockPath(createdZNode, children)) == null) {
        break;
      } else {
        CountDownLatch deletedLatch = new CountDownLatch(1);
        String zkPathToWatch =
            zkWrapper.getZNode(parentLockNode, pathToWatch);
        DeletionWatcher deletionWatcher =
            new DeletionWatcher(zkWrapper, zkPathToWatch, deletedLatch);
        zkWrapper.registerListener(deletionWatcher);
        try {
          if (zkWrapper.setWatchIfNodeExists(zkPathToWatch)) {
            // Wait for the watcher to fire
            if (hasTimeout) {
              long remainingMs = waitUntilMs - EnvironmentEdgeManager.currentTimeMillis();
              if (remainingMs < 0 || 
                  !deletedLatch.await(remainingMs, TimeUnit.MILLISECONDS)) {
                LOG.warn("Unable to acquire the lock in " + timeoutMs +
                    " milliseconds.");
                try {
                  zkWrapper.deleteZNode(createdZNode);
                } catch (KeeperException e) {
                  LOG.warn("Unable to remove ZNode " + createdZNode);
                }
                return false;
              }
            } else {
              deletedLatch.await();
            }
            if (deletionWatcher.hasException()) {
              Throwable t = deletionWatcher.getException();
              throw new IOException("Exception in the watcher", t);
            }
          }
        } catch (KeeperException e) {
          throw new IOException("Unexpected ZooKeeper exception", e);
        } finally {
          zkWrapper.unregisterListener(deletionWatcher);
        }
      }
    }
    updateAcquiredLock(createdZNode);
    LOG.debug("Successfully acquired a lock for " + fullyQualifiedZNode);
    return true;
  }

  private String createLockZNode() {
    return zkWrapper.createZNodeIfNotExists(fullyQualifiedZNode,
        metadata, CreateMode.PERSISTENT_SEQUENTIAL, false);
  }

  /**
   * Update state as to indicate that a lock is held
   * @param createdZNode The lock znode
   * @throws IOException If an unrecoverable ZooKeeper error occurs
   */
  protected void updateAcquiredLock(String createdZNode) throws IOException {
    Stat stat = new Stat();
    if (zkWrapper.readZNode(createdZNode, stat) == null) {
      LOG.error("Can't acquire a lock on a non-existent node " + createdZNode);
      throw new IllegalStateException("ZNode " + createdZNode +
          "no longer exists!");
    }
    AcquiredLock newLock = new AcquiredLock(createdZNode, stat.getVersion());
    if (!acquiredLock.compareAndSet(null, newLock)) {
      LOG.error("The lock " + fullyQualifiedZNode +
          " has already been acquired by another process!");
      throw new IllegalStateException(fullyQualifiedZNode +
          " is held by another process");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void release() throws IOException, InterruptedException {
    AcquiredLock lock = acquiredLock.get();
    if (lock == null) {
      LOG.error("Cannot release " + lock +
          ", process does not have a lock for " + fullyQualifiedZNode);
      throw new IllegalStateException("No lock held for " + fullyQualifiedZNode);
    }
    try {
      if (zkWrapper.checkExists(lock.getPath()) != -1) {
        zkWrapper.deleteZNode(lock.getPath(), false, lock.getVersion());
        if (!acquiredLock.compareAndSet(lock, null)) {
          LOG.debug("Current process no longer holds " + lock + " for " +
              fullyQualifiedZNode);
          throw new IllegalStateException("Not holding a lock for " +
              fullyQualifiedZNode +"!");
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Successfully released " + fullyQualifiedZNode);
      }
    } catch (BadVersionException e) {
      throw new IllegalStateException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * Process metadata stored in a ZNode using a callback object passed to
   * this instance.
   * <p>
   * See {@link HWriteLockImpl.MetadataHandler#handleMetadata(byte[])}
   * @param lockZNode The node holding the metadata
   * @return True if metadata was ready and processed
   * @throws IOException If an unexpected ZooKeeper error occurs
   * @throws InterruptedException If interrupted when reading the metadata
   */
  protected boolean handleLockMetadata(String lockZNode)
  throws IOException, InterruptedException {
    Stat stat = new Stat();
    byte[] metadata = zkWrapper.readZNodeIfExists(lockZNode, stat);
    if (metadata == null) {
      return false;
    }
    if (handler != null) {
      handler.handleMetadata(metadata);
    }
    return true;
  }

  /**
   * Determine based on a list of children under a ZNode, whether or not a
   * process which created a specified ZNode has obtained a lock. If a lock is 
   * not obtained, return the path that we should watch awaiting its deletion.
   * Otherwise, return null.
   * This method is abstract as the logic for determining whether or not a 
   * lock is obtained depends on the type of lock being implemented.
   * @param myZNode The ZNode created by the process attempting to acquire
   *                a lock
   * @param children List of all child ZNodes under the lock's parent ZNode
   * @return The path to watch, or null if myZNode can represent a correctly 
   *         acquired lock.
   */
  protected abstract String getLockPath(String myZNode, List<String> children)
  throws IOException, InterruptedException;  
}
