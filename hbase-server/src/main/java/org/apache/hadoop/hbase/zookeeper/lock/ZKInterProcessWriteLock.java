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
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * ZooKeeper based write lock:
 */
@InterfaceAudience.Private
public class ZKInterProcessWriteLock extends ZKInterProcessLockBase {

  private static final Log LOG = LogFactory.getLog(ZKInterProcessWriteLock.class);

  public ZKInterProcessWriteLock(ZooKeeperWatcher zooKeeperWatcher,
      String znode, byte[] metadata, MetadataHandler handler) {
    super(zooKeeperWatcher, znode, metadata, handler, WRITE_LOCK_CHILD_NODE_PREFIX);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getLockPath(String createdZNode, List<String> children) throws IOException {
    TreeSet<String> sortedChildren =
        new TreeSet<String>(ZNodeComparator.COMPARATOR);
    sortedChildren.addAll(children);
    String pathToWatch = sortedChildren.lower(createdZNode);
    if (pathToWatch != null) {
      String nodeHoldingLock = sortedChildren.first();
      String znode = ZKUtil.joinZNode(parentLockNode, nodeHoldingLock);
      handleLockMetadata(znode);
    }
    return pathToWatch;
  }
}
