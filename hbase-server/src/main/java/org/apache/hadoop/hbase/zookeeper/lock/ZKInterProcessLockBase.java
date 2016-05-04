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

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.InterProcessLock;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.DeletionListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.data.Stat;

import com.google.common.base.Preconditions;

/**
 * ZooKeeper based HLock implementation. Based on the Shared Locks recipe.
 * (see:
 * <a href="http://zookeeper.apache.org/doc/trunk/recipes.html">
 * ZooKeeper Recipes and Solutions
 * </a>)
 */
@InterfaceAudience.Private
public abstract class ZKInterProcessLockBase implements InterProcessLock {

  private static final Log LOG = LogFactory.getLog(ZKInterProcessLockBase.class);

  /** ZNode prefix used by processes acquiring reader locks */
  protected static final String READ_LOCK_CHILD_NODE_PREFIX = "read-";

  /** ZNode prefix used by processes acquiring writer locks */
  protected static final String WRITE_LOCK_CHILD_NODE_PREFIX = "write-";

  protected final ZooKeeperWatcher zkWatcher;
  protected final String parentLockNode;
  protected final String fullyQualifiedZNode;
  protected final String childZNode;
  protected final byte[] metadata;
  protected final MetadataHandler handler;

  // If we acquire a lock, update this field
  protected final AtomicReference<AcquiredLock> acquiredLock =
      new AtomicReference<AcquiredLock>(null);

  /**
   * Represents information about a lock held by this thread.
   */
  protected static class AcquiredLock {
    private final String path;
    private final int version;

    /**
     * Store information about a lock.
     * @param path The path to a lock's ZNode
     * @param version The current version of the lock's ZNode
     */
    public AcquiredLock(String path, int version) {
      this.path = path;
      this.version = version;
    }

    public String getPath() {
      return path;
    }

    public int getVersion() {
      return version;
    }

    @Override
    public String toString() {
      return "AcquiredLockInfo{" +
          "path='" + path + '\'' +
          ", version=" + version +
          '}';
    }
  }

  protected static class ZNodeComparator implements Comparator<String> {

    public static final ZNodeComparator COMPARATOR = new ZNodeComparator();

    private ZNodeComparator() {
    }

    /** Parses sequenceId from the znode name. ZooKeeper documentation
     * states: The sequence number is always fixed length of 10 digits, 0 padded
     */
    public static long getChildSequenceId(String childZNode) {
      Preconditions.checkNotNull(childZNode);
      assert childZNode.length() >= 10;
      String sequenceIdStr = childZNode.substring(childZNode.length() - 10);
      return Long.parseLong(sequenceIdStr);
    }

    @Override
    public int compare(String zNode1, String zNode2) {
      long seq1 = getChildSequenceId(zNode1);
      long seq2 = getChildSequenceId(zNode2);
      if (seq1 == seq2) {
        return 0;
      } else {
        return seq1 < seq2 ? -1 : 1;
      }
    }
  }

  /**
   * Called by implementing classes.
   * @param zkWatcher
   * @param parentLockNode The lock ZNode path
   * @param metadata
   * @param handler
   * @param childNode The prefix for child nodes created under the parent
   */
  protected ZKInterProcessLockBase(ZooKeeperWatcher zkWatcher,
      String parentLockNode, byte[] metadata, MetadataHandler handler, String childNode) {
    this.zkWatcher = zkWatcher;
    this.parentLockNode = parentLockNode;
    this.fullyQualifiedZNode = ZKUtil.joinZNode(parentLockNode, childNode);
    this.metadata = metadata;
    this.handler = handler;
    this.childZNode = childNode;
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
        hasTimeout ?EnvironmentEdgeManager.currentTime() + timeoutMs : -1;
    String createdZNode;
    try {
      createdZNode = createLockZNode();
    } catch (KeeperException ex) {
      throw new IOException("Failed to create znode: " + fullyQualifiedZNode, ex);
    }
    while (true) {
      List<String> children;
      try {
        children = ZKUtil.listChildrenNoWatch(zkWatcher, parentLockNode);
      } catch (KeeperException e) {
        LOG.error("Unexpected ZooKeeper error when listing children", e);
        throw new IOException("Unexpected ZooKeeper exception", e);
      }
      String pathToWatch;
      if ((pathToWatch = getLockPath(createdZNode, children)) == null) {
        break;
      }
      CountDownLatch deletedLatch = new CountDownLatch(1);
      String zkPathToWatch =
          ZKUtil.joinZNode(parentLockNode, pathToWatch);
      DeletionListener deletionListener =
          new DeletionListener(zkWatcher, zkPathToWatch, deletedLatch);
      zkWatcher.registerListener(deletionListener);
      try {
        if (ZKUtil.setWatchIfNodeExists(zkWatcher, zkPathToWatch)) {
          // Wait for the watcher to fire
          if (hasTimeout) {
            long remainingMs = waitUntilMs - EnvironmentEdgeManager.currentTime();
            if (remainingMs < 0 ||
                !deletedLatch.await(remainingMs, TimeUnit.MILLISECONDS)) {
              LOG.warn("Unable to acquire the lock in " + timeoutMs +
                  " milliseconds.");
              try {
                ZKUtil.deleteNode(zkWatcher, createdZNode);
              } catch (KeeperException e) {
                LOG.warn("Unable to remove ZNode " + createdZNode);
              }
              return false;
            }
          } else {
            deletedLatch.await();
          }
          if (deletionListener.hasException()) {
            Throwable t = deletionListener.getException();
            throw new IOException("Exception in the watcher", t);
          }
        }
      } catch (KeeperException e) {
        throw new IOException("Unexpected ZooKeeper exception", e);
      } finally {
        zkWatcher.unregisterListener(deletionListener);
      }
    }
    updateAcquiredLock(createdZNode);
    LOG.debug("Acquired a lock for " + createdZNode);
    return true;
  }

  private String createLockZNode() throws KeeperException {
    try {
      return ZKUtil.createNodeIfNotExistsNoWatch(zkWatcher, fullyQualifiedZNode,
          metadata, CreateMode.EPHEMERAL_SEQUENTIAL);
    } catch (KeeperException.NoNodeException nne) {
      //create parents, retry
      ZKUtil.createWithParents(zkWatcher, parentLockNode);
      return createLockZNode();
    }
  }

  /**
   * Check if a child znode represents a read lock.
   * @param child The child znode we want to check.
   * @return whether the child znode represents a read lock
   */
  protected static boolean isChildReadLock(String child) {
    int idx = child.lastIndexOf(ZKUtil.ZNODE_PATH_SEPARATOR);
    String suffix = child.substring(idx + 1);
    return suffix.startsWith(READ_LOCK_CHILD_NODE_PREFIX);
  }

  /**
   * Check if a child znode represents a write lock.
   * @param child The child znode we want to check.
   * @return whether the child znode represents a write lock
   */
  protected static boolean isChildWriteLock(String child) {
    int idx = child.lastIndexOf(ZKUtil.ZNODE_PATH_SEPARATOR);
    String suffix = child.substring(idx + 1);
    return suffix.startsWith(WRITE_LOCK_CHILD_NODE_PREFIX);
  }

  /**
   * Check if a child znode represents the same type(read or write) of lock
   * @param child The child znode we want to check.
   * @return whether the child znode represents the same type(read or write) of lock
   */
  protected boolean isChildOfSameType(String child) {
    int idx = child.lastIndexOf(ZKUtil.ZNODE_PATH_SEPARATOR);
    String suffix = child.substring(idx + 1);
    return suffix.startsWith(this.childZNode);
  }

  /**
   * Update state as to indicate that a lock is held
   * @param createdZNode The lock znode
   * @throws IOException If an unrecoverable ZooKeeper error occurs
   */
  protected void updateAcquiredLock(String createdZNode) throws IOException {
    Stat stat = new Stat();
    byte[] data = null;
    Exception ex = null;
    try {
      data = ZKUtil.getDataNoWatch(zkWatcher, createdZNode, stat);
    } catch (KeeperException e) {
      LOG.warn("Cannot getData for znode:" + createdZNode, e);
      ex = e;
    }
    if (data == null) {
      LOG.error("Can't acquire a lock on a non-existent node " + createdZNode);
      throw new IllegalStateException("ZNode " + createdZNode +
          "no longer exists!", ex);
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
      LOG.error("Cannot release lock" +
          ", process does not have a lock for " + fullyQualifiedZNode);
      throw new IllegalStateException("No lock held for " + fullyQualifiedZNode);
    }
    try {
      if (ZKUtil.checkExists(zkWatcher, lock.getPath()) != -1) {
        boolean ret = ZKUtil.deleteNode(zkWatcher, lock.getPath(), lock.getVersion());
        if (!ret && ZKUtil.checkExists(zkWatcher, lock.getPath()) != -1) {
          throw new IllegalStateException("Couldn't delete " + lock.getPath());
        }
        if (!acquiredLock.compareAndSet(lock, null)) {
          LOG.debug("Current process no longer holds " + lock + " for " +
              fullyQualifiedZNode);
          throw new IllegalStateException("Not holding a lock for " +
              fullyQualifiedZNode +"!");
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Released " + lock.getPath());
      }
    } catch (BadVersionException e) {
      throw new IllegalStateException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  /**
   * Process metadata stored in a ZNode using a callback
   * <p>
   * @param lockZNode The node holding the metadata
   * @return True if metadata was ready and processed, false otherwise.
   */
  protected boolean handleLockMetadata(String lockZNode) {
    return handleLockMetadata(lockZNode, handler);
  }

  /**
   * Process metadata stored in a ZNode using a callback object passed to
   * this instance.
   * <p>
   * @param lockZNode The node holding the metadata
   * @param handler the metadata handler
   * @return True if metadata was ready and processed, false on exception.
   */
  protected boolean handleLockMetadata(String lockZNode, MetadataHandler handler) {
    if (handler == null) {
      return false;
    }
    try {
      byte[] metadata = ZKUtil.getData(zkWatcher, lockZNode);
      handler.handleMetadata(metadata);
    } catch (KeeperException ex) {
      LOG.warn("Error processing lock metadata in " + lockZNode);
      return false;
    } catch (InterruptedException e) {
      LOG.warn("InterruptedException processing lock metadata in " + lockZNode);
      Thread.currentThread().interrupt();
      return false;
    }
    return true;
  }

  @Override
  public void reapAllLocks() throws IOException {
    reapExpiredLocks(0);
  }

  /**
   * Will delete all lock znodes of this type (either read or write) which are "expired"
   * according to timeout. Assumption is that the clock skew between zookeeper and this servers
   * is negligible.
   * Referred in zk recipe as "Revocable Shared Locks with Freaking Laser Beams".
   * (http://zookeeper.apache.org/doc/trunk/recipes.html).
   */
  public void reapExpiredLocks(long timeout) throws IOException {
    List<String> children;
    try {
      children = ZKUtil.listChildrenNoWatch(zkWatcher, parentLockNode);
    } catch (KeeperException e) {
      LOG.error("Unexpected ZooKeeper error when listing children", e);
      throw new IOException("Unexpected ZooKeeper exception", e);
    }
    if (children == null) return;

    KeeperException deferred = null;
    Stat stat = new Stat();
    long expireDate = System.currentTimeMillis() - timeout; //we are using cTime in zookeeper
    for (String child : children) {
      if (isChildOfSameType(child)) {
        String znode = ZKUtil.joinZNode(parentLockNode, child);
        try {
          ZKUtil.getDataNoWatch(zkWatcher, znode, stat);
          if (stat.getCtime() < expireDate) {
            LOG.info("Reaping lock for znode:" + znode);
            ZKUtil.deleteNodeFailSilent(zkWatcher, znode);
          }
        } catch (KeeperException ex) {
          LOG.warn("Error reaping the znode for write lock :" + znode);
          deferred = ex;
        }
      }
    }
    if (deferred != null) {
      throw new IOException("ZK exception while reaping locks:", deferred);
    }
  }

  /**
   * Visits the locks (both held and attempted) with the given MetadataHandler.
   * @throws IOException If there is an unrecoverable error
   */
  public void visitLocks(MetadataHandler handler) throws IOException {
    List<String> children;
    try {
      children = ZKUtil.listChildrenNoWatch(zkWatcher, parentLockNode);
    } catch (KeeperException e) {
      LOG.error("Unexpected ZooKeeper error when listing children", e);
      throw new IOException("Unexpected ZooKeeper exception", e);
    }
    if (children != null && children.size() > 0) {
      for (String child : children) {
        if (isChildOfSameType(child)) {
          String znode = ZKUtil.joinZNode(parentLockNode, child);
          String childWatchesZNode = getLockPath(child, children);
          if (childWatchesZNode == null) {
            LOG.info("Lock is held by: " + child);
          }
          handleLockMetadata(znode, handler);
        }
      }
    }
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
  throws IOException;
}
