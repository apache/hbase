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

package org.apache.hadoop.hbase.security.token;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Synchronizes token encryption keys across cluster nodes.
 */
@InterfaceAudience.Private
public class ZKSecretWatcher extends ZKListener {
  private static final String DEFAULT_ROOT_NODE = "tokenauth";
  private static final String DEFAULT_KEYS_PARENT = "keys";
  private static final Logger LOG = LoggerFactory.getLogger(ZKSecretWatcher.class);

  private AuthenticationTokenSecretManager secretManager;
  private String baseKeyZNode;
  private String keysParentZNode;

  public ZKSecretWatcher(Configuration conf,
      ZKWatcher watcher,
      AuthenticationTokenSecretManager secretManager) {
    super(watcher);
    this.secretManager = secretManager;
    String keyZNodeParent = conf.get("zookeeper.znode.tokenauth.parent", DEFAULT_ROOT_NODE);
    this.baseKeyZNode = ZNodePaths.joinZNode(watcher.getZNodePaths().baseZNode, keyZNodeParent);
    this.keysParentZNode = ZNodePaths.joinZNode(baseKeyZNode, DEFAULT_KEYS_PARENT);
  }

  public void start() throws KeeperException {
    watcher.registerListener(this);
    // make sure the base node exists
    ZKUtil.createWithParents(watcher, keysParentZNode);

    if (ZKUtil.watchAndCheckExists(watcher, keysParentZNode)) {
      List<ZKUtil.NodeAndData> nodes =
          ZKUtil.getChildDataAndWatchForNewChildren(watcher, keysParentZNode);
      refreshNodes(nodes);
    }
  }

  @Override
  public void nodeCreated(String path) {
    if (path.equals(keysParentZNode)) {
      try {
        List<ZKUtil.NodeAndData> nodes =
            ZKUtil.getChildDataAndWatchForNewChildren(watcher, keysParentZNode);
        refreshNodes(nodes);
      } catch (KeeperException ke) {
        LOG.error(HBaseMarkers.FATAL, "Error reading data from zookeeper", ke);
        watcher.abort("Error reading new key znode "+path, ke);
      }
    }
  }

  @Override
  public void nodeDeleted(String path) {
    if (keysParentZNode.equals(ZKUtil.getParent(path))) {
      String keyId = ZKUtil.getNodeName(path);
      try {
        Integer id = Integer.valueOf(keyId);
        secretManager.removeKey(id);
        LOG.info("Node deleted id={}", id);
      } catch (NumberFormatException nfe) {
        LOG.error("Invalid znode name for key ID '"+keyId+"'", nfe);
      }
    }
  }

  @Override
  public void nodeDataChanged(String path) {
    if (keysParentZNode.equals(ZKUtil.getParent(path))) {
      try {
        byte[] data = ZKUtil.getDataAndWatch(watcher, path);
        if (data == null || data.length == 0) {
          LOG.debug("Ignoring empty node "+path);
          return;
        }

        AuthenticationKey key = (AuthenticationKey)Writables.getWritable(data,
            new AuthenticationKey());
        secretManager.addKey(key);
      } catch (KeeperException ke) {
        LOG.error(HBaseMarkers.FATAL, "Error reading data from zookeeper", ke);
        watcher.abort("Error reading updated key znode "+path, ke);
      } catch (IOException ioe) {
        LOG.error(HBaseMarkers.FATAL, "Error reading key writables", ioe);
        watcher.abort("Error reading key writables from znode "+path, ioe);
      }
    }
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (path.equals(keysParentZNode)) {
      // keys changed
      try {
        List<ZKUtil.NodeAndData> nodes =
            ZKUtil.getChildDataAndWatchForNewChildren(watcher, keysParentZNode);
        refreshNodes(nodes);
      } catch (KeeperException ke) {
        LOG.error(HBaseMarkers.FATAL, "Error reading data from zookeeper", ke);
        watcher.abort("Error reading changed keys from zookeeper", ke);
      }
    }
  }

  public String getRootKeyZNode() {
    return baseKeyZNode;
  }

  private void refreshNodes(List<ZKUtil.NodeAndData> nodes) {
    for (ZKUtil.NodeAndData n : nodes) {
      String path = n.getNode();
      String keyId = ZKUtil.getNodeName(path);
      try {
        byte[] data = n.getData();
        if (data == null || data.length == 0) {
          LOG.debug("Ignoring empty node "+path);
          continue;
        }
        AuthenticationKey key = (AuthenticationKey)Writables.getWritable(
            data, new AuthenticationKey());
        secretManager.addKey(key);
      } catch (IOException ioe) {
        LOG.error(HBaseMarkers.FATAL, "Failed reading new secret key for id '" +
            keyId + "' from zk", ioe);
        watcher.abort("Error deserializing key from znode "+path, ioe);
      }
    }
  }

  private String getKeyNode(int keyId) {
    return ZNodePaths.joinZNode(keysParentZNode, Integer.toString(keyId));
  }

  public void removeKeyFromZK(AuthenticationKey key) {
    String keyZNode = getKeyNode(key.getKeyId());
    try {
      ZKUtil.deleteNode(watcher, keyZNode);
    } catch (KeeperException.NoNodeException nne) {
      LOG.error("Non-existent znode "+keyZNode+" for key "+key.getKeyId(), nne);
    } catch (KeeperException ke) {
      LOG.error(HBaseMarkers.FATAL, "Failed removing znode "+keyZNode+" for key "+
          key.getKeyId(), ke);
      watcher.abort("Unhandled zookeeper error removing znode "+keyZNode+
          " for key "+key.getKeyId(), ke);
    }
  }

  public void addKeyToZK(AuthenticationKey key) {
    String keyZNode = getKeyNode(key.getKeyId());
    try {
      byte[] keyData = Writables.getBytes(key);
      // TODO: is there any point in retrying beyond what ZK client does?
      ZKUtil.createSetData(watcher, keyZNode, keyData);
    } catch (KeeperException ke) {
      LOG.error(HBaseMarkers.FATAL, "Unable to synchronize master key "+key.getKeyId()+
          " to znode "+keyZNode, ke);
      watcher.abort("Unable to synchronize secret key "+
          key.getKeyId()+" in zookeeper", ke);
    } catch (IOException ioe) {
      // this can only happen from an error serializing the key
      watcher.abort("Failed serializing key "+key.getKeyId(), ioe);
    }
  }

  public void updateKeyInZK(AuthenticationKey key) {
    String keyZNode = getKeyNode(key.getKeyId());
    try {
      byte[] keyData = Writables.getBytes(key);
      try {
        ZKUtil.updateExistingNodeData(watcher, keyZNode, keyData, -1);
      } catch (KeeperException.NoNodeException ne) {
        // node was somehow removed, try adding it back
        ZKUtil.createSetData(watcher, keyZNode, keyData);
      }
    } catch (KeeperException ke) {
      LOG.error(HBaseMarkers.FATAL, "Unable to update master key "+key.getKeyId()+
          " in znode "+keyZNode);
      watcher.abort("Unable to synchronize secret key "+
          key.getKeyId()+" in zookeeper", ke);
    } catch (IOException ioe) {
      // this can only happen from an error serializing the key
      watcher.abort("Failed serializing key "+key.getKeyId(), ioe);
    }
  }

  /**
   * refresh keys
   */
  synchronized void refreshKeys() {
    try {
      List<ZKUtil.NodeAndData> nodes =
          ZKUtil.getChildDataAndWatchForNewChildren(watcher, keysParentZNode);
      refreshNodes(nodes);
    } catch (KeeperException ke) {
      LOG.error(HBaseMarkers.FATAL, "Error reading data from zookeeper", ke);
      watcher.abort("Error reading changed keys from zookeeper", ke);
    }
  }

  /**
   * get token keys parent node
   * @return token keys parent node
   */
  String getKeysParentZNode() {
    return keysParentZNode;
  }
}
