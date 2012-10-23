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

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * ZooKeeper based read lock: does not exclude other read locks, but excludes
 * and is excluded by write locks.
 */
public class HReadLockImpl extends BaseHLockImpl {

  private static final Log LOG = LogFactory.getLog(HReadLockImpl.class);

  /** Temporary until this is a constant in ZK */
  private static final char ZNODE_PATH_SEPARATOR = '/';

  public HReadLockImpl(ZooKeeperWrapper zooKeeperWrapper,
      String name, byte[] metadata, MetadataHandler handler) {
    super(zooKeeperWrapper, name, metadata, handler, READ_LOCK_CHILD_NODE);
  }

  /**
   * Check if a child znode represents a write lock.
   * @param child The child znode we want to check.
   * @return
   */
  private static boolean isWriteChild(String child) {
    int idx = child.lastIndexOf(ZNODE_PATH_SEPARATOR);
    String suffix = child.substring(idx + 1);
    return suffix.startsWith(WRITE_LOCK_CHILD_NODE);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getLockPath(String createdZNode, List<String> children)
  throws IOException, InterruptedException {
    TreeSet<String> writeChildren =
        new TreeSet<String>(new ZNodeComparator(zkWrapper.getIdentifier()));
    for (String child : children) {
      if (isWriteChild(child)) {
        writeChildren.add(child);
      }
    }
    if (writeChildren.isEmpty()) {
      return null;
    }
    SortedSet<String> lowerChildren = writeChildren.headSet(createdZNode);
    if (lowerChildren.isEmpty()) {
      return null;
    }
    String pathToWatch = lowerChildren.last();
    String nodeHoldingLock = lowerChildren.first();
    try {
      handleLockMetadata(nodeHoldingLock);
    } catch (IOException e) {
      LOG.warn("Error processing lock metadata in " + nodeHoldingLock, e);
    }
    return pathToWatch;
  }
}
