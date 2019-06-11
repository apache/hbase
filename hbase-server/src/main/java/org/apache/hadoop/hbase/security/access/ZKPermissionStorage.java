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

package org.apache.hadoop.hbase.security.access;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;

/**
 * Store user permissions in zk. Used when cluster startup, because the acl table is not online.
 * There are three types znodes under '/hbase/acl' znode:
 * '/hbase/acl/hbase:acl' znode: contains a serialized list of global user permissions;
 * '/hbase/acl/@namespace' znode: contains a serialized list of namespace user permissions;
 * '/hbase/acl/tableName' znode: contains a serialized list of table user permissions.
 */
@InterfaceAudience.Private
public class ZKPermissionStorage {
  private static final Logger LOG = LoggerFactory.getLogger(ZKPermissionStorage.class);
  // parent node for permissions lists
  static final String ACL_NODE = "acl";

  private ZKWatcher watcher;
  private final String aclZNode;

  public ZKPermissionStorage(ZKWatcher watcher, Configuration conf) {
    this.watcher = watcher;
    this.aclZNode = ZNodePaths.joinZNode(watcher.getZNodePaths().baseZNode,
      conf.get("zookeeper.znode.acl.parent", ACL_NODE));
  }

  public ZKWatcher getWatcher() {
    return watcher;
  }

  /**
   * Reload permission data to acl znode
   * @param permissions the permission map
   */
  public void reloadPermissions(Map<byte[], ListMultimap<String, UserPermission>> permissions) {
    for (Map.Entry<byte[], ListMultimap<String, UserPermission>> permission : permissions
        .entrySet()) {
      byte[] entry = permission.getKey();
      ListMultimap<String, UserPermission> perms = permission.getValue();
      byte[] serialized = PermissionStorage.writePermissionsAsBytes(perms);
      writePermission(entry, serialized);
    }
  }

  /**
   * Reload permission data from acl znode to auth manager cache
   * @param authManager the auth manager instance
   */
  public void reloadPermissionsToAuthManager(AuthManager authManager) {
    try {
      if (ZKUtil.checkExists(watcher, aclZNode) != -1) {
        List<String> nodes = ZKUtil.listChildrenNoWatch(watcher, aclZNode);
        if (nodes != null) {
          for (String node : nodes) {
            byte[] data = ZKUtil.getData(watcher, ZNodePaths.joinZNode(aclZNode, node));
            if (data != null) {
              try {
                authManager.refresh(node, data);
              } catch (IOException e) {
                LOG.error("Failed deserialize permission data for {}", node, e);
              }
            }
          }
        }
      }
    } catch (KeeperException | InterruptedException e) {
      LOG.error("Failed loading permission cache from {}", aclZNode, e);
      watcher.abort("Failed loading permission cache from " + aclZNode, e);
    }
  }

  /***
   * Write a table's access controls to the permissions mirror in zookeeper
   * @param entry
   * @param permsData
   */
  public void writePermission(byte[] entry, byte[] permsData) {
    String entryName = Bytes.toString(entry);
    String zkNode = ZNodePaths.joinZNode(aclZNode, entryName);
    try {
      ZKUtil.createSetData(watcher, zkNode, permsData);
    } catch (KeeperException e) {
      LOG.error("Failed updating permissions for entry '{}'", entryName, e);
      watcher.abort("Failed writing node " + zkNode + " to zookeeper", e);
    }
  }

  /**
   * Get the permissions for the entry
   * @param entry the given entry, it's '@namespace', 'hbase:acl' or 'tablename'.
   * @return the user permissions in bytes or null if the entry node does not exist in zk
   */
  public byte[] getPermission(byte[] entry) {
    String entryName = Bytes.toString(entry);
    String zkNode = ZNodePaths.joinZNode(aclZNode, entryName);
    try {
      if (ZKUtil.checkExists(watcher, zkNode) != -1) {
        byte[] data = ZKUtil.getData(watcher, zkNode);
        return data;
      } else {
        return null;
      }
    } catch (KeeperException | InterruptedException e) {
      LOG.error("Failed getting permissions for entry '{}'", entryName, e);
      watcher.abort("Failed getting node " + zkNode + " from zookeeper", e);
      return null;
    }
  }

  /**
   * Delete the acl node of table
   * @param tableName the table name
   */
  public void deleteTablePermission(final TableName tableName) {
    String zkNode = ZNodePaths.joinZNode(aclZNode, tableName.getNameAsString());
    deletePermission(zkNode);
  }

  /**
   * Delete the acl node of namespace
   * @param namespace the namespace
   */
  public void deleteNamespacePermission(final String namespace) {
    String zkNode = ZNodePaths.joinZNode(aclZNode, PermissionStorage.NAMESPACE_PREFIX + namespace);
    deletePermission(zkNode);
  }

  private void deletePermission(final String zkNode) {
    try {
      ZKUtil.deleteNode(watcher, zkNode);
    } catch (KeeperException.NoNodeException e) {
      LOG.warn("No acl node '{}'", zkNode);
    } catch (KeeperException e) {
      LOG.error("Failed deleting acl node '{}'", zkNode, e);
      watcher.abort("Failed deleting node " + zkNode, e);
    }
  }
}
