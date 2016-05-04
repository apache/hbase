/**
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

package org.apache.hadoop.hbase;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListMap;


/**
 * Class servers two purposes:
 *
 * 1. Broadcast NamespaceDescriptor information via ZK
 * (Done by the Master)
 * 2. Consume broadcasted NamespaceDescriptor changes
 * (Done by the RegionServers)
 *
 */
@InterfaceAudience.Private
public class ZKNamespaceManager extends ZooKeeperListener {
  private static final Log LOG = LogFactory.getLog(ZKNamespaceManager.class);
  private final String nsZNode;
  private volatile NavigableMap<String,NamespaceDescriptor> cache;

  public ZKNamespaceManager(ZooKeeperWatcher zkw) throws IOException {
    super(zkw);
    nsZNode = ZooKeeperWatcher.namespaceZNode;
    cache = new ConcurrentSkipListMap<String, NamespaceDescriptor>();
  }

  public void start() throws IOException {
    watcher.registerListener(this);
    try {
      if (ZKUtil.watchAndCheckExists(watcher, nsZNode)) {
        List<ZKUtil.NodeAndData> existing =
            ZKUtil.getChildDataAndWatchForNewChildren(watcher, nsZNode);
        if (existing != null) {
          refreshNodes(existing);
        }
      } else {
        ZKUtil.createWithParents(watcher, nsZNode);
      }
    } catch (KeeperException e) {
      throw new IOException("Failed to initialize ZKNamespaceManager", e);
    }
  }

  public NamespaceDescriptor get(String name) {
    return cache.get(name);
  }

  public void update(NamespaceDescriptor ns) throws IOException {
    writeNamespace(ns);
    cache.put(ns.getName(), ns);
  }

  public void remove(String name) throws IOException {
    deleteNamespace(name);
    cache.remove(name);
  }

  public NavigableSet<NamespaceDescriptor> list() throws IOException {
    NavigableSet<NamespaceDescriptor> ret =
        Sets.newTreeSet(NamespaceDescriptor.NAMESPACE_DESCRIPTOR_COMPARATOR);
    for(NamespaceDescriptor ns: cache.values()) {
      ret.add(ns);
    }
    return ret;
  }

  @Override
  public void nodeCreated(String path) {
    if (nsZNode.equals(path)) {
      try {
        List<ZKUtil.NodeAndData> nodes =
            ZKUtil.getChildDataAndWatchForNewChildren(watcher, nsZNode);
        refreshNodes(nodes);
      } catch (KeeperException ke) {
        String msg = "Error reading data from zookeeper";
        LOG.error(msg, ke);
        watcher.abort(msg, ke);
      } catch (IOException e) {
        String msg = "Error parsing data from zookeeper";
        LOG.error(msg, e);
        watcher.abort(msg, e);
      }
    }
  }

  @Override
  public void nodeDeleted(String path) {
    if (nsZNode.equals(ZKUtil.getParent(path))) {
      String nsName = ZKUtil.getNodeName(path);
      cache.remove(nsName);
    }
  }

  @Override
  public void nodeDataChanged(String path) {
    if (nsZNode.equals(ZKUtil.getParent(path))) {
      try {
        byte[] data = ZKUtil.getDataAndWatch(watcher, path);
        NamespaceDescriptor ns =
            ProtobufUtil.toNamespaceDescriptor(
                HBaseProtos.NamespaceDescriptor.parseFrom(data));
        cache.put(ns.getName(), ns);
      } catch (KeeperException ke) {
        String msg = "Error reading data from zookeeper for node "+path;
        LOG.error(msg, ke);
        // only option is to abort
        watcher.abort(msg, ke);
      } catch (IOException ioe) {
        String msg = "Error deserializing namespace: "+path;
        LOG.error(msg, ioe);
        watcher.abort(msg, ioe);
      }
    }
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (nsZNode.equals(path)) {
      try {
        List<ZKUtil.NodeAndData> nodes =
            ZKUtil.getChildDataAndWatchForNewChildren(watcher, nsZNode);
        refreshNodes(nodes);
      } catch (KeeperException ke) {
        LOG.error("Error reading data from zookeeper for path "+path, ke);
        watcher.abort("ZooKeeper error get node children for path "+path, ke);
      } catch (IOException e) {
        LOG.error("Error deserializing namespace child from: "+path, e);
        watcher.abort("Error deserializing namespace child from: " + path, e);
      }
    }
  }

  private void deleteNamespace(String name) throws IOException {
    String zNode = ZKUtil.joinZNode(nsZNode, name);
    try {
      ZKUtil.deleteNode(watcher, zNode);
    } catch (KeeperException e) {
      if (e instanceof KeeperException.NoNodeException) {
        // If the node does not exist, it could be already deleted. Continue without fail.
        LOG.warn("The ZNode " + zNode + " for namespace " + name + " does not exist.");
      } else {
        LOG.error("Failed updating permissions for namespace " + name, e);
        throw new IOException("Failed updating permissions for namespace " + name, e);
      }
    }
  }

  private void writeNamespace(NamespaceDescriptor ns) throws IOException {
    String zNode = ZKUtil.joinZNode(nsZNode, ns.getName());
    try {
      ZKUtil.createWithParents(watcher, zNode);
      ZKUtil.updateExistingNodeData(watcher, zNode,
          ProtobufUtil.toProtoNamespaceDescriptor(ns).toByteArray(), -1);
    } catch (KeeperException e) {
      LOG.error("Failed updating permissions for namespace "+ns.getName(), e);
      throw new IOException("Failed updating permissions for namespace "+ns.getName(), e);
    }
  }

  private void refreshNodes(List<ZKUtil.NodeAndData> nodes) throws IOException {
    for (ZKUtil.NodeAndData n : nodes) {
      if (n.isEmpty()) continue;
      String path = n.getNode();
      String namespace = ZKUtil.getNodeName(path);
      byte[] nodeData = n.getData();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Updating namespace cache from node "+namespace+" with data: "+
            Bytes.toStringBinary(nodeData));
      }
      NamespaceDescriptor ns =
          ProtobufUtil.toNamespaceDescriptor(
              HBaseProtos.NamespaceDescriptor.parseFrom(nodeData));
      cache.put(ns.getName(), ns);
    }
  }
}
