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
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * ZooKeeper based read lock: does not exclude other read locks, but excludes
 * and is excluded by write locks.
 */
@InterfaceAudience.Private
public class ZKInterProcessReadLock extends ZKInterProcessLockBase {

  private static final Log LOG = LogFactory.getLog(ZKInterProcessReadLock.class);

  public ZKInterProcessReadLock(ZooKeeperWatcher zooKeeperWatcher,
      String znode, byte[] metadata, MetadataHandler handler) {
    super(zooKeeperWatcher, znode, metadata, handler, READ_LOCK_CHILD_NODE_PREFIX);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getLockPath(String createdZNode, List<String> children) throws IOException {
    TreeSet<String> writeChildren =
        new TreeSet<String>(ZNodeComparator.COMPARATOR);
    for (String child : children) {
      if (isChildWriteLock(child)) {
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
    String znode = ZKUtil.joinZNode(parentLockNode, nodeHoldingLock);
    handleLockMetadata(znode);

    return pathToWatch;
  }
}
