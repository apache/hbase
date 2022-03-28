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
package org.apache.hadoop.hbase.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.ZKUtilOp.CreateAndFailSilent;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.ZKUtilOp.DeleteNodeFailSilent;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.ZKUtilOp.SetData;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos;

/**
 * Internal HBase utility class for ZooKeeper.
 *
 * <p>Contains only static methods and constants.
 *
 * <p>Methods all throw {@link KeeperException} if there is an unexpected
 * zookeeper exception, so callers of these methods must handle appropriately.
 * If ZK is required for the operation, the server will need to be aborted.
 */
@InterfaceAudience.Private
public final class ZKUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ZKUtil.class);

  private ZKUtil() {
  }

  //
  // Helper methods
  //
  /**
   * Returns the full path of the immediate parent of the specified node.
   * @param node path to get parent of
   * @return parent of path, null if passed the root node or an invalid node
   */
  public static String getParent(String node) {
    int idx = node.lastIndexOf(ZNodePaths.ZNODE_PATH_SEPARATOR);
    return idx <= 0 ? null : node.substring(0, idx);
  }

  /**
   * Get the name of the current node from the specified fully-qualified path.
   * @param path fully-qualified path
   * @return name of the current node
   */
  public static String getNodeName(String path) {
    return path.substring(path.lastIndexOf("/")+1);
  }

  //
  // Existence checks and watches
  //

  /**
   * Watch the specified znode for delete/create/change events.  The watcher is
   * set whether or not the node exists.  If the node already exists, the method
   * returns true.  If the node does not exist, the method returns false.
   *
   * @param zkw zk reference
   * @param znode path of node to watch
   * @return true if znode exists, false if does not exist or error
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean watchAndCheckExists(ZKWatcher zkw, String znode)
    throws KeeperException {
    try {
      Stat s = zkw.getRecoverableZooKeeper().exists(znode, zkw);
      boolean exists = s != null;
      if (exists) {
        LOG.debug(zkw.prefix("Set watcher on existing znode=" + znode));
      } else {
        LOG.debug(zkw.prefix("Set watcher on znode that does not yet exist, " + znode));
      }
      return exists;
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to set watcher on znode " + znode), e);
      zkw.keeperException(e);
      return false;
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to set watcher on znode " + znode), e);
      zkw.interruptedException(e);
      return false;
    }
  }

  /**
   * Watch the specified znode, but only if exists. Useful when watching
   * for deletions. Uses .getData() (and handles NoNodeException) instead
   * of .exists() to accomplish this, as .getData() will only set a watch if
   * the znode exists.
   * @param zkw zk reference
   * @param znode path of node to watch
   * @return true if the watch is set, false if node does not exists
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean setWatchIfNodeExists(ZKWatcher zkw, String znode)
      throws KeeperException {
    try {
      zkw.getRecoverableZooKeeper().getData(znode, true, null);
      return true;
    } catch (NoNodeException e) {
      return false;
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to set watcher on znode " + znode), e);
      zkw.interruptedException(e);
      return false;
    }
  }

  /**
   * Check if the specified node exists.  Sets no watches.
   *
   * @param zkw zk reference
   * @param znode path of node to watch
   * @return version of the node if it exists, -1 if does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static int checkExists(ZKWatcher zkw, String znode)
    throws KeeperException {
    try {
      Stat s = zkw.getRecoverableZooKeeper().exists(znode, null);
      return s != null ? s.getVersion() : -1;
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to set watcher on znode (" + znode + ")"), e);
      zkw.keeperException(e);
      return -1;
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to set watcher on znode (" + znode + ")"), e);
      zkw.interruptedException(e);
      return -1;
    }
  }

  //
  // Znode listings
  //

  /**
   * Lists the children znodes of the specified znode.  Also sets a watch on
   * the specified znode which will capture a NodeDeleted event on the specified
   * znode as well as NodeChildrenChanged if any children of the specified znode
   * are created or deleted.
   *
   * Returns null if the specified node does not exist.  Otherwise returns a
   * list of children of the specified node.  If the node exists but it has no
   * children, an empty list will be returned.
   *
   * @param zkw zk reference
   * @param znode path of node to list and watch children of
   * @return list of children of the specified node, an empty list if the node
   *          exists but has no children, and null if the node does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static List<String> listChildrenAndWatchForNewChildren(
          ZKWatcher zkw, String znode)
    throws KeeperException {
    try {
      return zkw.getRecoverableZooKeeper().getChildren(znode, zkw);
    } catch(KeeperException.NoNodeException ke) {
      LOG.debug(zkw.prefix("Unable to list children of znode " + znode + " " +
          "because node does not exist (not an error)"));
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to list children of znode " + znode + " "), e);
      zkw.keeperException(e);
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to list children of znode " + znode + " "), e);
      zkw.interruptedException(e);
    }

    return null;
  }

  /**
   * List all the children of the specified znode, setting a watch for children
   * changes and also setting a watch on every individual child in order to get
   * the NodeCreated and NodeDeleted events.
   * @param zkw zookeeper reference
   * @param znode node to get children of and watch
   * @return list of znode names, null if the node doesn't exist
   * @throws KeeperException if a ZooKeeper operation fails
   */
  public static List<String> listChildrenAndWatchThem(ZKWatcher zkw,
      String znode) throws KeeperException {
    List<String> children = listChildrenAndWatchForNewChildren(zkw, znode);
    if (children == null) {
      return null;
    }
    for (String child : children) {
      watchAndCheckExists(zkw, ZNodePaths.joinZNode(znode, child));
    }
    return children;
  }

  /**
   * Lists the children of the specified znode without setting any watches.
   *
   * Sets no watches at all, this method is best effort.
   *
   * Returns an empty list if the node has no children.  Returns null if the
   * parent node itself does not exist.
   *
   * @param zkw zookeeper reference
   * @param znode node to get children
   * @return list of data of children of specified znode, empty if no children,
   *         null if parent does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static List<String> listChildrenNoWatch(ZKWatcher zkw, String znode)
    throws KeeperException {
    List<String> children = null;
    try {
      // List the children without watching
      children = zkw.getRecoverableZooKeeper().getChildren(znode, null);
    } catch(KeeperException.NoNodeException nne) {
      return null;
    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
    }
    return children;
  }

  /**
   * Simple class to hold a node path and node data.
   * @deprecated Unused
   */
  @Deprecated
  public static class NodeAndData {
    private String node;
    private byte [] data;
    public NodeAndData(String node, byte [] data) {
      this.node = node;
      this.data = data;
    }
    public String getNode() {
      return node;
    }
    public byte [] getData() {
      return data;
    }
    @Override
    public String toString() {
      return node;
    }
    public boolean isEmpty() {
      return (data == null || data.length == 0);
    }
  }

  /**
   * Checks if the specified znode has any children.  Sets no watches.
   *
   * Returns true if the node exists and has children.  Returns false if the
   * node does not exist or if the node does not have any children.
   *
   * Used during master initialization to determine if the master is a
   * failed-over-to master or the first master during initial cluster startup.
   * If the directory for regionserver ephemeral nodes is empty then this is
   * a cluster startup, if not then it is not cluster startup.
   *
   * @param zkw zk reference
   * @param znode path of node to check for children of
   * @return true if node has children, false if not or node does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean nodeHasChildren(ZKWatcher zkw, String znode)
    throws KeeperException {
    try {
      return !zkw.getRecoverableZooKeeper().getChildren(znode, null).isEmpty();
    } catch(KeeperException.NoNodeException ke) {
      LOG.debug(zkw.prefix("Unable to list children of znode " + znode +
              " because node does not exist (not an error)"));
      return false;
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to list children of znode " + znode), e);
      zkw.keeperException(e);
      return false;
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to list children of znode " + znode), e);
      zkw.interruptedException(e);
      return false;
    }
  }

  /**
   * Get the number of children of the specified node.
   *
   * If the node does not exist or has no children, returns 0.
   *
   * Sets no watches at all.
   *
   * @param zkw zk reference
   * @param znode path of node to count children of
   * @return number of children of specified node, 0 if none or parent does not
   *         exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static int getNumberOfChildren(ZKWatcher zkw, String znode)
    throws KeeperException {
    try {
      Stat stat = zkw.getRecoverableZooKeeper().exists(znode, null);
      return stat == null ? 0 : stat.getNumChildren();
    } catch(KeeperException e) {
      LOG.warn(zkw.prefix("Unable to get children of node " + znode));
      zkw.keeperException(e);
    } catch(InterruptedException e) {
      zkw.interruptedException(e);
    }
    return 0;
  }

  //
  // Data retrieval
  //

  /**
   * Get znode data. Does not set a watcher.
   *
   * @return ZNode data, null if the node does not exist or if there is an error.
   */
  public static byte [] getData(ZKWatcher zkw, String znode)
      throws KeeperException, InterruptedException {
    try {
      byte [] data = zkw.getRecoverableZooKeeper().getData(znode, null, null);
      logRetrievedMsg(zkw, znode, data, false);
      return data;
    } catch (KeeperException.NoNodeException e) {
      LOG.debug(zkw.prefix("Unable to get data of znode " + znode + " " +
          "because node does not exist (not an error)"));
      return null;
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
      zkw.keeperException(e);
      return null;
    }
  }

  /**
   * Get the data at the specified znode and set a watch.
   *
   * Returns the data and sets a watch if the node exists.  Returns null and no
   * watch is set if the node does not exist or there is an exception.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @return data of the specified znode, or null
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static byte[] getDataAndWatch(ZKWatcher zkw, String znode) throws KeeperException {
    return getDataInternal(zkw, znode, null, true, true);
  }

  /**
   * Get the data at the specified znode and set a watch.
   * Returns the data and sets a watch if the node exists.  Returns null and no
   * watch is set if the node does not exist or there is an exception.
   *
   * @param zkw              zk reference
   * @param znode            path of node
   * @param throwOnInterrupt if false then just interrupt the thread, do not throw exception
   * @return data of the specified znode, or null
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static byte[] getDataAndWatch(ZKWatcher zkw, String znode, boolean throwOnInterrupt)
    throws KeeperException {
    return getDataInternal(zkw, znode, null, true, throwOnInterrupt);
  }

  /**
   * Get the data at the specified znode and set a watch.
   *
   * Returns the data and sets a watch if the node exists.  Returns null and no
   * watch is set if the node does not exist or there is an exception.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param stat object to populate the version of the znode
   * @return data of the specified znode, or null
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static byte[] getDataAndWatch(ZKWatcher zkw, String znode,
                                       Stat stat) throws KeeperException {
    return getDataInternal(zkw, znode, stat, true, true);
  }

  private static byte[] getDataInternal(ZKWatcher zkw, String znode, Stat stat, boolean watcherSet,
    boolean throwOnInterrupt)
      throws KeeperException {
    try {
      byte [] data = zkw.getRecoverableZooKeeper().getData(znode, zkw, stat);
      logRetrievedMsg(zkw, znode, data, watcherSet);
      return data;
    } catch (KeeperException.NoNodeException e) {
      // This log can get pretty annoying when we cycle on 100ms waits.
      // Enable trace if you really want to see it.
      LOG.trace(zkw.prefix("Unable to get data of znode " + znode + " " +
        "because node does not exist (not an error)"));
      return null;
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
      zkw.keeperException(e);
      return null;
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
      if (throwOnInterrupt) {
        zkw.interruptedException(e);
      } else {
        zkw.interruptedExceptionNoThrow(e, true);
      }
      return null;
    }
  }

  /**
   * Get the data at the specified znode without setting a watch.
   *
   * Returns the data if the node exists.  Returns null if the node does not
   * exist.
   *
   * Sets the stats of the node in the passed Stat object.  Pass a null stat if
   * not interested.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param stat node status to get if node exists
   * @return data of the specified znode, or null if node does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static byte [] getDataNoWatch(ZKWatcher zkw, String znode,
                                       Stat stat)
    throws KeeperException {
    try {
      byte [] data = zkw.getRecoverableZooKeeper().getData(znode, null, stat);
      logRetrievedMsg(zkw, znode, data, false);
      return data;
    } catch (KeeperException.NoNodeException e) {
      LOG.debug(zkw.prefix("Unable to get data of znode " + znode + " " +
          "because node does not exist (not necessarily an error)"));
      return null;
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
      zkw.keeperException(e);
      return null;
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
      zkw.interruptedException(e);
      return null;
    }
  }

  /**
   * Returns the date of child znodes of the specified znode.  Also sets a watch on
   * the specified znode which will capture a NodeDeleted event on the specified
   * znode as well as NodeChildrenChanged if any children of the specified znode
   * are created or deleted.
   *
   * Returns null if the specified node does not exist.  Otherwise returns a
   * list of children of the specified node.  If the node exists but it has no
   * children, an empty list will be returned.
   *
   * @param zkw zk reference
   * @param baseNode path of node to list and watch children of
   * @return list of data of children of the specified node, an empty list if the node
   *          exists but has no children, and null if the node does not exist
   * @throws KeeperException if unexpected zookeeper exception
   * @deprecated Unused
   */
  @Deprecated
  public static List<NodeAndData> getChildDataAndWatchForNewChildren(ZKWatcher zkw, String baseNode)
    throws KeeperException {
    return getChildDataAndWatchForNewChildren(zkw, baseNode, true);
  }

  /**
   * Returns the date of child znodes of the specified znode.  Also sets a watch on
   * the specified znode which will capture a NodeDeleted event on the specified
   * znode as well as NodeChildrenChanged if any children of the specified znode
   * are created or deleted.
   *
   * Returns null if the specified node does not exist.  Otherwise returns a
   * list of children of the specified node.  If the node exists but it has no
   * children, an empty list will be returned.
   *
   * @param zkw zk reference
   * @param baseNode path of node to list and watch children of
   * @param throwOnInterrupt if true then just interrupt the thread, do not throw exception
   * @return list of data of children of the specified node, an empty list if the node
   *          exists but has no children, and null if the node does not exist
   * @throws KeeperException if unexpected zookeeper exception
   * @deprecated Unused
   */
  @Deprecated
  public static List<NodeAndData> getChildDataAndWatchForNewChildren(
          ZKWatcher zkw, String baseNode, boolean throwOnInterrupt) throws KeeperException {
    List<String> nodes =
      ZKUtil.listChildrenAndWatchForNewChildren(zkw, baseNode);
    if (nodes != null) {
      List<NodeAndData> newNodes = new ArrayList<>();
      for (String node : nodes) {
        if (Thread.interrupted()) {
          // Partial data should not be processed. Cancel processing by sending empty list.
          return Collections.emptyList();
        }
        String nodePath = ZNodePaths.joinZNode(baseNode, node);
        byte[] data = ZKUtil.getDataAndWatch(zkw, nodePath, throwOnInterrupt);
        newNodes.add(new NodeAndData(nodePath, data));
      }
      return newNodes;
    }
    return null;
  }

  /**
   * Update the data of an existing node with the expected version to have the
   * specified data.
   *
   * Throws an exception if there is a version mismatch or some other problem.
   *
   * Sets no watches under any conditions.
   *
   * @param zkw zk reference
   * @param znode the path to the ZNode
   * @param data the data to store in ZooKeeper
   * @param expectedVersion the expected version
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.BadVersionException if version mismatch
   * @deprecated Unused
   */
  @Deprecated
  public static void updateExistingNodeData(ZKWatcher zkw, String znode, byte[] data,
      int expectedVersion) throws KeeperException {
    try {
      zkw.getRecoverableZooKeeper().setData(znode, data, expectedVersion);
    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
    }
  }

  //
  // Data setting
  //

  /**
   * Sets the data of the existing znode to be the specified data.  Ensures that
   * the current data has the specified expected version.
   *
   * <p>If the node does not exist, a {@link NoNodeException} will be thrown.
   *
   * <p>If their is a version mismatch, method returns null.
   *
   * <p>No watches are set but setting data will trigger other watchers of this
   * node.
   *
   * <p>If there is another problem, a KeeperException will be thrown.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param data data to set for node
   * @param expectedVersion version expected when setting data
   * @return true if data set, false if version mismatch
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean setData(ZKWatcher zkw, String znode,
                                byte [] data, int expectedVersion)
    throws KeeperException, KeeperException.NoNodeException {
    try {
      return zkw.getRecoverableZooKeeper().setData(znode, data, expectedVersion) != null;
    } catch (InterruptedException e) {
      zkw.interruptedException(e);
      return false;
    }
  }

  /**
   * Set data into node creating node if it doesn't yet exist.
   * Does not set watch.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param data data to set for node
   * @throws KeeperException if a ZooKeeper operation fails
   */
  public static void createSetData(final ZKWatcher zkw, final String znode, final byte [] data)
          throws KeeperException {
    if (checkExists(zkw, znode) == -1) {
      ZKUtil.createWithParents(zkw, znode, data);
    } else {
      ZKUtil.setData(zkw, znode, data);
    }
  }

  /**
   * Sets the data of the existing znode to be the specified data.  The node
   * must exist but no checks are done on the existing data or version.
   *
   * <p>If the node does not exist, a {@link NoNodeException} will be thrown.
   *
   * <p>No watches are set but setting data will trigger other watchers of this
   * node.
   *
   * <p>If there is another problem, a KeeperException will be thrown.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param data data to set for node
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static void setData(ZKWatcher zkw, String znode, byte [] data)
    throws KeeperException, KeeperException.NoNodeException {
    setData(zkw, (SetData)ZKUtilOp.setData(znode, data));
  }

  private static void setData(ZKWatcher zkw, SetData setData)
    throws KeeperException, KeeperException.NoNodeException {
    SetDataRequest sd = (SetDataRequest)toZooKeeperOp(zkw, setData).toRequestRecord();
    setData(zkw, sd.getPath(), sd.getData(), sd.getVersion());
  }

  //
  // Node creation
  //

  /**
   *
   * Set the specified znode to be an ephemeral node carrying the specified
   * data.
   *
   * If the node is created successfully, a watcher is also set on the node.
   *
   * If the node is not created successfully because it already exists, this
   * method will also set a watcher on the node.
   *
   * If there is another problem, a KeeperException will be thrown.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param data data of node
   * @return true if node created, false if not, watch set in both cases
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean createEphemeralNodeAndWatch(ZKWatcher zkw, String znode, byte [] data)
    throws KeeperException {
    boolean ret = true;
    try {
      zkw.getRecoverableZooKeeper().create(znode, data, zkw.createACL(znode),
          CreateMode.EPHEMERAL);
    } catch (KeeperException.NodeExistsException nee) {
      ret = false;
    } catch (InterruptedException e) {
      LOG.info("Interrupted", e);
      Thread.currentThread().interrupt();
    }
    if(!watchAndCheckExists(zkw, znode)) {
      // It did exist but now it doesn't, try again
      return createEphemeralNodeAndWatch(zkw, znode, data);
    }
    return ret;
  }

  /**
   * Creates the specified znode to be a persistent node carrying the specified
   * data.
   *
   * Returns true if the node was successfully created, false if the node
   * already existed.
   *
   * If the node is created successfully, a watcher is also set on the node.
   *
   * If the node is not created successfully because it already exists, this
   * method will also set a watcher on the node but return false.
   *
   * If there is another problem, a KeeperException will be thrown.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param data data of node
   * @return true if node created, false if not, watch set in both cases
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean createNodeIfNotExistsAndWatch(
          ZKWatcher zkw, String znode, byte [] data)
    throws KeeperException {
    boolean ret = true;
    try {
      zkw.getRecoverableZooKeeper().create(znode, data, zkw.createACL(znode),
          CreateMode.PERSISTENT);
    } catch (KeeperException.NodeExistsException nee) {
      ret = false;
    } catch (InterruptedException e) {
      zkw.interruptedException(e);
      return false;
    }
    try {
      zkw.getRecoverableZooKeeper().exists(znode, zkw);
    } catch (InterruptedException e) {
      zkw.interruptedException(e);
      return false;
    }
    return ret;
  }

  /**
   * Creates the specified znode with the specified data but does not watch it.
   *
   * Returns the znode of the newly created node
   *
   * If there is another problem, a KeeperException will be thrown.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param data data of node
   * @param createMode specifying whether the node to be created is ephemeral and/or sequential
   * @return true name of the newly created znode or null
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static String createNodeIfNotExistsNoWatch(ZKWatcher zkw, String znode, byte[] data,
      CreateMode createMode) throws KeeperException {
    try {
      return zkw.getRecoverableZooKeeper().create(znode, data, zkw.createACL(znode), createMode);
    } catch (KeeperException.NodeExistsException nee) {
      return znode;
    } catch (InterruptedException e) {
      zkw.interruptedException(e);
      return null;
    }
  }

  /**
   * Creates the specified node with the specified data and watches it.
   *
   * <p>Throws an exception if the node already exists.
   *
   * <p>The node created is persistent and open access.
   *
   * <p>Returns the version number of the created node if successful.
   *
   * @param zkw zk reference
   * @param znode path of node to create
   * @param data data of node to create
   * @return version of node created
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.NodeExistsException if node already exists
   */
  public static int createAndWatch(ZKWatcher zkw,
      String znode, byte [] data)
    throws KeeperException, KeeperException.NodeExistsException {
    try {
      zkw.getRecoverableZooKeeper().create(znode, data, zkw.createACL(znode),
        CreateMode.PERSISTENT);
      Stat stat = zkw.getRecoverableZooKeeper().exists(znode, zkw);
      if (stat == null){
        // Likely a race condition. Someone deleted the znode.
        throw KeeperException.create(KeeperException.Code.SYSTEMERROR,
            "ZK.exists returned null (i.e.: znode does not exist) for znode=" + znode);
      }

      return stat.getVersion();
    } catch (InterruptedException e) {
      zkw.interruptedException(e);
      return -1;
    }
  }

  /**
   * Async creates the specified node with the specified data.
   *
   * <p>Throws an exception if the node already exists.
   *
   * <p>The node created is persistent and open access.
   *
   * @param zkw zk reference
   * @param znode path of node to create
   * @param data data of node to create
   * @param cb the callback to use for the creation
   * @param ctx the context to use for the creation
   */
  public static void asyncCreate(ZKWatcher zkw,
      String znode, byte [] data, final AsyncCallback.StringCallback cb,
      final Object ctx) {
    zkw.getRecoverableZooKeeper().getZooKeeper().create(znode, data,
        zkw.createACL(znode), CreateMode.PERSISTENT, cb, ctx);
  }

  /**
   * Creates the specified node, iff the node does not exist.  Does not set a
   * watch and fails silently if the node already exists.
   *
   * The node created is persistent and open access.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static void createAndFailSilent(ZKWatcher zkw,
      String znode) throws KeeperException {
    createAndFailSilent(zkw, znode, new byte[0]);
  }

  /**
   * Creates the specified node containing specified data, iff the node does not exist.  Does
   * not set a watch and fails silently if the node already exists.
   *
   * The node created is persistent and open access.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param data a byte array data to store in the znode
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static void createAndFailSilent(ZKWatcher zkw,
      String znode, byte[] data)
    throws KeeperException {
    createAndFailSilent(zkw,
        (CreateAndFailSilent)ZKUtilOp.createAndFailSilent(znode, data));
  }

  private static void createAndFailSilent(ZKWatcher zkw, CreateAndFailSilent cafs)
    throws KeeperException {
    CreateRequest create = (CreateRequest)toZooKeeperOp(zkw, cafs).toRequestRecord();
    String znode = create.getPath();
    try {
      RecoverableZooKeeper zk = zkw.getRecoverableZooKeeper();
      if (zk.exists(znode, false) == null) {
        zk.create(znode, create.getData(), create.getAcl(), CreateMode.fromFlag(create.getFlags()));
      }
    } catch (KeeperException.NodeExistsException nee) {
      // pass
    } catch (KeeperException.NoAuthException nee) {
      try {
        if (null == zkw.getRecoverableZooKeeper().exists(znode, false)) {
          // If we failed to create the file and it does not already exist.
          throw(nee);
        }
      } catch (InterruptedException ie) {
        zkw.interruptedException(ie);
      }
    } catch (InterruptedException ie) {
      zkw.interruptedException(ie);
    }
  }

  /**
   * Creates the specified node and all parent nodes required for it to exist.
   *
   * No watches are set and no errors are thrown if the node already exists.
   *
   * The nodes created are persistent and open access.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static void createWithParents(ZKWatcher zkw, String znode)
    throws KeeperException {
    createWithParents(zkw, znode, new byte[0]);
  }

  /**
   * Creates the specified node and all parent nodes required for it to exist.  The creation of
   * parent znodes is not atomic with the leafe znode creation but the data is written atomically
   * when the leaf node is created.
   *
   * No watches are set and no errors are thrown if the node already exists.
   *
   * The nodes created are persistent and open access.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static void createWithParents(ZKWatcher zkw, String znode, byte[] data)
    throws KeeperException {
    try {
      if(znode == null) {
        return;
      }
      zkw.getRecoverableZooKeeper().create(znode, data, zkw.createACL(znode),
          CreateMode.PERSISTENT);
    } catch(KeeperException.NodeExistsException nee) {
      return;
    } catch(KeeperException.NoNodeException nne) {
      createWithParents(zkw, getParent(znode));
      createWithParents(zkw, znode, data);
    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
    }
  }

  //
  // Deletes
  //

  /**
   * Delete the specified node.  Sets no watches.  Throws all exceptions.
   */
  public static void deleteNode(ZKWatcher zkw, String node)
    throws KeeperException {
    deleteNode(zkw, node, -1);
  }

  /**
   * Delete the specified node with the specified version.  Sets no watches.
   * Throws all exceptions.
   */
  public static boolean deleteNode(ZKWatcher zkw, String node,
                                   int version)
    throws KeeperException {
    try {
      zkw.getRecoverableZooKeeper().delete(node, version);
      return true;
    } catch(KeeperException.BadVersionException bve) {
      return false;
    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
      return false;
    }
  }

  /**
   * Deletes the specified node.  Fails silent if the node does not exist.
   *
   * @param zkw reference to the {@link ZKWatcher} which also contains configuration and operation
   * @param node the node to delete
   * @throws KeeperException if a ZooKeeper operation fails
   */
  public static void deleteNodeFailSilent(ZKWatcher zkw, String node)
    throws KeeperException {
    deleteNodeFailSilent(zkw,
      (DeleteNodeFailSilent)ZKUtilOp.deleteNodeFailSilent(node));
  }

  private static void deleteNodeFailSilent(ZKWatcher zkw,
      DeleteNodeFailSilent dnfs) throws KeeperException {
    DeleteRequest delete = (DeleteRequest)toZooKeeperOp(zkw, dnfs).toRequestRecord();
    try {
      zkw.getRecoverableZooKeeper().delete(delete.getPath(), delete.getVersion());
    } catch(KeeperException.NoNodeException nne) {
    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
    }
  }


  /**
   * Delete the specified node and all of it's children.
   * <p>
   * If the node does not exist, just returns.
   * <p>
   * Sets no watches. Throws all exceptions besides dealing with deletion of
   * children.
   */
  public static void deleteNodeRecursively(ZKWatcher zkw, String node)
    throws KeeperException {
    deleteNodeRecursivelyMultiOrSequential(zkw, true, node);
  }

  /**
   * Delete all the children of the specified node but not the node itself.
   *
   * Sets no watches.  Throws all exceptions besides dealing with deletion of
   * children.
   *
   * @throws KeeperException if a ZooKeeper operation fails
   */
  public static void deleteChildrenRecursively(ZKWatcher zkw, String node)
      throws KeeperException {
    deleteChildrenRecursivelyMultiOrSequential(zkw, true, node);
  }

  /**
   * Delete all the children of the specified node but not the node itself. This
   * will first traverse the znode tree for listing the children and then delete
   * these znodes using multi-update api or sequential based on the specified
   * configurations.
   * <p>
   * Sets no watches. Throws all exceptions besides dealing with deletion of
   * children.
   * <p>
   * If the following is true:
   * <ul>
   * <li>runSequentialOnMultiFailure is true
   * </ul>
   * on calling multi, we get a ZooKeeper exception that can be handled by a
   * sequential call(*), we retry the operations one-by-one (sequentially).
   *
   * @param zkw
   *          - zk reference
   * @param runSequentialOnMultiFailure
   *          - if true when we get a ZooKeeper exception that could retry the
   *          operations one-by-one (sequentially)
   * @param pathRoots
   *          - path of the parent node(s)
   * @throws KeeperException.NotEmptyException
   *           if node has children while deleting
   * @throws KeeperException
   *           if unexpected ZooKeeper exception
   * @throws IllegalArgumentException
   *           if an invalid path is specified
   */
  public static void deleteChildrenRecursivelyMultiOrSequential(
          ZKWatcher zkw, boolean runSequentialOnMultiFailure,
          String... pathRoots) throws KeeperException {
    if (pathRoots == null || pathRoots.length <= 0) {
      LOG.warn("Given path is not valid!");
      return;
    }
    List<ZKUtilOp> ops = new ArrayList<>();
    for (String eachRoot : pathRoots) {
      List<String> children = listChildrenBFSNoWatch(zkw, eachRoot);
      // Delete the leaves first and eventually get rid of the root
      for (int i = children.size() - 1; i >= 0; --i) {
        ops.add(ZKUtilOp.deleteNodeFailSilent(children.get(i)));
      }
    }
    submitBatchedMultiOrSequential(zkw, runSequentialOnMultiFailure, ops);
  }

  /**
   * Delete the specified node and its children. This traverse the
   * znode tree for listing the children and then delete
   * these znodes including the parent using multi-update api or
   * sequential based on the specified configurations.
   * <p>
   * Sets no watches. Throws all exceptions besides dealing with deletion of
   * children.
   * <p>
   * If the following is true:
   * <ul>
   * <li>runSequentialOnMultiFailure is true
   * </ul>
   * on calling multi, we get a ZooKeeper exception that can be handled by a
   * sequential call(*), we retry the operations one-by-one (sequentially).
   *
   * @param zkw
   *          - zk reference
   * @param runSequentialOnMultiFailure
   *          - if true when we get a ZooKeeper exception that could retry the
   *          operations one-by-one (sequentially)
   * @param pathRoots
   *          - path of the parent node(s)
   * @throws KeeperException.NotEmptyException
   *           if node has children while deleting
   * @throws KeeperException
   *           if unexpected ZooKeeper exception
   * @throws IllegalArgumentException
   *           if an invalid path is specified
   */
  public static void deleteNodeRecursivelyMultiOrSequential(ZKWatcher zkw,
      boolean runSequentialOnMultiFailure, String... pathRoots) throws KeeperException {
    if (pathRoots == null || pathRoots.length <= 0) {
      LOG.warn("Given path is not valid!");
      return;
    }
    List<ZKUtilOp> ops = new ArrayList<>();
    for (String eachRoot : pathRoots) {
      // ZooKeeper Watches are one time triggers; When children of parent nodes are deleted
      // recursively, must set another watch, get notified of delete node
      List<String> children = listChildrenBFSAndWatchThem(zkw, eachRoot);
      // Delete the leaves first and eventually get rid of the root
      for (int i = children.size() - 1; i >= 0; --i) {
        ops.add(ZKUtilOp.deleteNodeFailSilent(children.get(i)));
      }
      try {
        if (zkw.getRecoverableZooKeeper().exists(eachRoot, zkw) != null) {
          ops.add(ZKUtilOp.deleteNodeFailSilent(eachRoot));
        }
      } catch (InterruptedException e) {
        zkw.interruptedException(e);
      }
    }
    submitBatchedMultiOrSequential(zkw, runSequentialOnMultiFailure, ops);
  }

  /**
   * Chunks the provided {@code ops} when their approximate size exceeds the the configured limit.
   * Take caution that this can ONLY be used for operations where atomicity is not important,
   * e.g. deletions. It must not be used when atomicity of the operations is critical.
   *
   * @param zkw reference to the {@link ZKWatcher} which contains configuration and constants
   * @param runSequentialOnMultiFailure if true when we get a ZooKeeper exception that could
   *        retry the operations one-by-one (sequentially)
   * @param ops list of ZKUtilOp {@link ZKUtilOp} to partition while submitting batched multi
   *        or sequential
   * @throws KeeperException unexpected ZooKeeper Exception / Zookeeper unreachable
   */
  private static void submitBatchedMultiOrSequential(ZKWatcher zkw,
      boolean runSequentialOnMultiFailure, List<ZKUtilOp> ops) throws KeeperException {
    // at least one element should exist
    if (ops.isEmpty()) {
      return;
    }
    final int maxMultiSize = zkw.getRecoverableZooKeeper().getMaxMultiSizeLimit();
    // Batch up the items to over smashing through jute.maxbuffer with too many Ops.
    final List<List<ZKUtilOp>> batchedOps = partitionOps(ops, maxMultiSize);
    // Would use forEach() but have to handle KeeperException
    for (List<ZKUtilOp> batch : batchedOps) {
      multiOrSequential(zkw, batch, runSequentialOnMultiFailure);
    }
  }

  /**
   * Partition the list of {@code ops} by size (using {@link #estimateSize(ZKUtilOp)}).
   */
  static List<List<ZKUtilOp>> partitionOps(List<ZKUtilOp> ops, int maxPartitionSize) {
    List<List<ZKUtilOp>> partitionedOps = new ArrayList<>();
    List<ZKUtilOp> currentPartition = new ArrayList<>();
    int currentPartitionSize = 0;
    partitionedOps.add(currentPartition);
    Iterator<ZKUtilOp> iter = ops.iterator();
    while (iter.hasNext()) {
      ZKUtilOp currentOp = iter.next();
      int currentOpSize = estimateSize(currentOp);

      // Roll a new partition if necessary
      // If the current partition is empty, put the element in there anyways.
      // We can roll a new partition if we get another element
      if (!currentPartition.isEmpty() && currentOpSize + currentPartitionSize > maxPartitionSize) {
        currentPartition = new ArrayList<>();
        partitionedOps.add(currentPartition);
        currentPartitionSize = 0;
      }

      // Add the current op to the partition
      currentPartition.add(currentOp);
      // And record its size
      currentPartitionSize += currentOpSize;
    }
    return partitionedOps;
  }

  static int estimateSize(ZKUtilOp op) {
    return Bytes.toBytes(op.getPath()).length;
  }

  /**
   * BFS Traversal of all the children under path, with the entries in the list,
   * in the same order as that of the traversal. Lists all the children without
   * setting any watches.
   *
   * @param zkw
   *          - zk reference
   * @param znode
   *          - path of node
   * @return list of children znodes under the path
   * @throws KeeperException
   *           if unexpected ZooKeeper exception
   */
  private static List<String> listChildrenBFSNoWatch(ZKWatcher zkw,
      final String znode) throws KeeperException {
    Deque<String> queue = new LinkedList<>();
    List<String> tree = new ArrayList<>();
    queue.add(znode);
    while (true) {
      String node = queue.pollFirst();
      if (node == null) {
        break;
      }
      List<String> children = listChildrenNoWatch(zkw, node);
      if (children == null) {
        continue;
      }
      for (final String child : children) {
        final String childPath = node + "/" + child;
        queue.add(childPath);
        tree.add(childPath);
      }
    }
    return tree;
  }

  /**
   * BFS Traversal of all the children under path, with the entries in the list,
   * in the same order as that of the traversal.
   * Lists all the children and set watches on to them.
   *
   * @param zkw
   *          - zk reference
   * @param znode
   *          - path of node
   * @return list of children znodes under the path
   * @throws KeeperException
   *           if unexpected ZooKeeper exception
   */
  private static List<String> listChildrenBFSAndWatchThem(ZKWatcher zkw, final String znode)
      throws KeeperException {
    Deque<String> queue = new LinkedList<>();
    List<String> tree = new ArrayList<>();
    queue.add(znode);
    while (true) {
      String node = queue.pollFirst();
      if (node == null) {
        break;
      }
      List<String> children = listChildrenAndWatchThem(zkw, node);
      if (children == null) {
        continue;
      }
      for (final String child : children) {
        final String childPath = node + "/" + child;
        queue.add(childPath);
        tree.add(childPath);
      }
    }
    return tree;
  }

  /**
   * Represents an action taken by ZKUtil, e.g. createAndFailSilent.
   * These actions are higher-level than ZKOp actions, which represent
   * individual actions in the ZooKeeper API, like create.
   */
  public abstract static class ZKUtilOp {
    private String path;

    @Override public String toString() {
      return this.getClass().getSimpleName() + ", path=" + this.path;
    }

    private ZKUtilOp(String path) {
      this.path = path;
    }

    /**
     * @return a createAndFailSilent ZKUtilOp
     */
    public static ZKUtilOp createAndFailSilent(String path, byte[] data) {
      return new CreateAndFailSilent(path, data);
    }

    /**
     * @return a deleteNodeFailSilent ZKUtilOP
     */
    public static ZKUtilOp deleteNodeFailSilent(String path) {
      return new DeleteNodeFailSilent(path);
    }

    /**
     * @return a setData ZKUtilOp
     */
    public static ZKUtilOp setData(String path, byte[] data) {
      return new SetData(path, data);
    }

    /**
     * @return a setData ZKUtilOp
     */
    public static ZKUtilOp setData(String path, byte[] data, int version) {
      return new SetData(path, data, version);
    }

    /**
     * @return path to znode where the ZKOp will occur
     */
    public String getPath() {
      return path;
    }

    /**
     * ZKUtilOp representing createAndFailSilent in ZooKeeper
     * (attempt to create node, ignore error if already exists)
     */
    public static final class CreateAndFailSilent extends ZKUtilOp {
      private byte [] data;

      private CreateAndFailSilent(String path, byte [] data) {
        super(path);
        this.data = data;
      }

      public byte[] getData() {
        return data;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (!(o instanceof CreateAndFailSilent)) {
          return false;
        }

        CreateAndFailSilent op = (CreateAndFailSilent) o;
        return getPath().equals(op.getPath()) && Arrays.equals(data, op.data);
      }

      @Override
      public int hashCode() {
        int ret = 17 + getPath().hashCode() * 31;
        return ret * 31 + Bytes.hashCode(data);
      }
    }

    /**
     * ZKUtilOp representing deleteNodeFailSilent in ZooKeeper
     * (attempt to delete node, ignore error if node doesn't exist)
     */
    public static final class DeleteNodeFailSilent extends ZKUtilOp {
      private DeleteNodeFailSilent(String path) {
        super(path);
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (!(o instanceof DeleteNodeFailSilent)) {
          return false;
        }

        return super.equals(o);
      }

      @Override
      public int hashCode() {
        return getPath().hashCode();
      }
    }

    /**
     * ZKUtilOp representing setData in ZooKeeper
     */
    public static final class SetData extends ZKUtilOp {
      private byte[] data;
      private int version = -1;

      private SetData(String path, byte[] data) {
        super(path);
        this.data = data;
      }

      private SetData(String path, byte[] data, int version) {
        super(path);
        this.data = data;
        this.version = version;
      }

      public byte[] getData() {
        return data;
      }

      public int getVersion() {
        return version;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (!(o instanceof SetData)) {
          return false;
        }

        SetData op = (SetData) o;
        return getPath().equals(op.getPath()) && Arrays.equals(data, op.data)
            && getVersion() == op.getVersion();
      }

      @Override
      public int hashCode() {
        int ret = getPath().hashCode();
        ret = ret * 31 + Bytes.hashCode(data);
        return ret * 31 + Integer.hashCode(version);
      }
    }
  }

  /**
   * Convert from ZKUtilOp to ZKOp
   */
  private static Op toZooKeeperOp(ZKWatcher zkw, ZKUtilOp op) throws UnsupportedOperationException {
    if(op == null) {
      return null;
    }

    if (op instanceof CreateAndFailSilent) {
      CreateAndFailSilent cafs = (CreateAndFailSilent)op;
      return Op.create(cafs.getPath(), cafs.getData(), zkw.createACL(cafs.getPath()),
        CreateMode.PERSISTENT);
    } else if (op instanceof DeleteNodeFailSilent) {
      DeleteNodeFailSilent dnfs = (DeleteNodeFailSilent)op;
      return Op.delete(dnfs.getPath(), -1);
    } else if (op instanceof SetData) {
      SetData sd = (SetData) op;
      return Op.setData(sd.getPath(), sd.getData(), sd.getVersion());
    } else {
      throw new UnsupportedOperationException("Unexpected ZKUtilOp type: "
        + op.getClass().getName());
    }
  }

  // Static boolean for warning about useMulti because ideally there will be one warning per
  // process instance. It is fine if two threads end up racing on this for a bit. We do not
  // need to use an atomic type for this, that is overkill. The goal of reducing the number of
  // warnings from many to one or a few at startup is still achieved.
  private static boolean useMultiWarn = true;

  /**
   * Use ZooKeeper's multi-update functionality.
   *
   * If all of the following are true:
   * - runSequentialOnMultiFailure is true
   * - on calling multi, we get a ZooKeeper exception that can be handled by a sequential call(*)
   * Then:
   * - we retry the operations one-by-one (sequentially)
   *
   * Note *: an example is receiving a NodeExistsException from a "create" call.  Without multi,
   * a user could call "createAndFailSilent" to ensure that a node exists if they don't care who
   * actually created the node (i.e. the NodeExistsException from ZooKeeper is caught).
   * This will cause all operations in the multi to fail, however, because
   * the NodeExistsException that zk.create throws will fail the multi transaction.
   * In this case, if the previous conditions hold, the commands are run sequentially, which should
   * result in the correct final state, but means that the operations will not run atomically.
   *
   * @throws KeeperException if a ZooKeeper operation fails
   */
  public static void multiOrSequential(ZKWatcher zkw, List<ZKUtilOp> ops,
      boolean runSequentialOnMultiFailure) throws KeeperException {
    if (ops == null) {
      return;
    }
    if (useMultiWarn) { // Only check and warn at first use
      if (zkw.getConfiguration().get("hbase.zookeeper.useMulti") != null) {
        LOG.warn("hbase.zookeeper.useMulti is deprecated. Default to true always.");
      }
      useMultiWarn = false;
    }
    List<Op> zkOps = new LinkedList<>();
    for (ZKUtilOp op : ops) {
      zkOps.add(toZooKeeperOp(zkw, op));
    }
    try {
      zkw.getRecoverableZooKeeper().multi(zkOps);
    } catch (KeeperException ke) {
      switch (ke.code()) {
        case NODEEXISTS:
        case NONODE:
        case BADVERSION:
        case NOAUTH:
        case NOTEMPTY:
          // if we get an exception that could be solved by running sequentially
          // (and the client asked us to), then break out and run sequentially
          if (runSequentialOnMultiFailure) {
            LOG.info("multi exception: {}; running operations sequentially " +
              "(runSequentialOnMultiFailure=true); {}", ke.toString(),
              ops.stream().map(o -> o.toString()).collect(Collectors.joining(",")));
            processSequentially(zkw, ops);
            break;
          }
        default:
          throw ke;
      }
    } catch (InterruptedException ie) {
      zkw.interruptedException(ie);
    }
  }

  private static void processSequentially(ZKWatcher zkw, List<ZKUtilOp> ops)
      throws KeeperException, NoNodeException {
    for (ZKUtilOp op : ops) {
      if (op instanceof CreateAndFailSilent) {
        createAndFailSilent(zkw, (CreateAndFailSilent) op);
      } else if (op instanceof DeleteNodeFailSilent) {
        deleteNodeFailSilent(zkw, (DeleteNodeFailSilent) op);
      } else if (op instanceof SetData) {
        setData(zkw, (SetData) op);
      } else {
        throw new UnsupportedOperationException("Unexpected ZKUtilOp type: "
            + op.getClass().getName());
      }
    }
  }

  //
  // ZooKeeper cluster information
  //

  private static void logRetrievedMsg(final ZKWatcher zkw,
      final String znode, final byte [] data, final boolean watcherSet) {
    if (!LOG.isTraceEnabled()) {
      return;
    }

    LOG.trace(zkw.prefix("Retrieved " + ((data == null)? 0: data.length) +
      " byte(s) of data from znode " + znode +
      (watcherSet? " and set watcher; ": "; data=") +
      (data == null? "null": data.length == 0? "empty": (
          zkw.getZNodePaths().isMetaZNodePath(znode)?
            getServerNameOrEmptyString(data):
          znode.startsWith(zkw.getZNodePaths().backupMasterAddressesZNode)?
            getServerNameOrEmptyString(data):
          StringUtils.abbreviate(Bytes.toStringBinary(data), 32)))));
  }

  private static String getServerNameOrEmptyString(final byte [] data) {
    try {
      return ProtobufUtil.parseServerNameFrom(data).toString();
    } catch (DeserializationException e) {
      return "";
    }
  }

  /**
   * Waits for HBase installation's base (parent) znode to become available.
   * @throws IOException on ZK errors
   */
  public static void waitForBaseZNode(Configuration conf) throws IOException {
    LOG.info("Waiting until the base znode is available");
    String parentZNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    ZooKeeper zk = new ZooKeeper(ZKConfig.getZKQuorumServersString(conf),
        conf.getInt(HConstants.ZK_SESSION_TIMEOUT,
        HConstants.DEFAULT_ZK_SESSION_TIMEOUT), EmptyWatcher.instance);

    final int maxTimeMs = 10000;
    final int maxNumAttempts = maxTimeMs / HConstants.SOCKET_RETRY_WAIT_MS;

    KeeperException keeperEx = null;
    try {
      try {
        for (int attempt = 0; attempt < maxNumAttempts; ++attempt) {
          try {
            if (zk.exists(parentZNode, false) != null) {
              LOG.info("Parent znode exists: {}", parentZNode);
              keeperEx = null;
              break;
            }
          } catch (KeeperException e) {
            keeperEx = e;
          }
          Threads.sleepWithoutInterrupt(HConstants.SOCKET_RETRY_WAIT_MS);
        }
      } finally {
        zk.close();
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    if (keeperEx != null) {
      throw new IOException(keeperEx);
    }
  }

  /**
   * Convert a {@link DeserializationException} to a more palatable {@link KeeperException}.
   * Used when can't let a {@link DeserializationException} out w/o changing public API.
   * @param e Exception to convert
   * @return Converted exception
   */
  public static KeeperException convert(final DeserializationException e) {
    KeeperException ke = new KeeperException.DataInconsistencyException();
    ke.initCause(e);
    return ke;
  }

  /**
   * Recursively print the current state of ZK (non-transactional)
   * @param root name of the root directory in zk to print
   */
  public static void logZKTree(ZKWatcher zkw, String root) {
    if (!LOG.isDebugEnabled()) {
      return;
    }

    LOG.debug("Current zk system:");
    String prefix = "|-";
    LOG.debug(prefix + root);
    try {
      logZKTree(zkw, root, prefix);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Helper method to print the current state of the ZK tree.
   * @see #logZKTree(ZKWatcher, String)
   * @throws KeeperException if an unexpected exception occurs
   */
  private static void logZKTree(ZKWatcher zkw, String root, String prefix)
      throws KeeperException {
    List<String> children = ZKUtil.listChildrenNoWatch(zkw, root);

    if (children == null) {
      return;
    }

    for (String child : children) {
      LOG.debug(prefix + child);
      String node = ZNodePaths.joinZNode(root.equals("/") ? "" : root, child);
      logZKTree(zkw, node, prefix + "---");
    }
  }

  /**
   * @param position the position to serialize
   * @return Serialized protobuf of <code>position</code> with pb magic prefix prepended suitable
   *         for use as content of an wal position in a replication queue.
   */
  public static byte[] positionToByteArray(final long position) {
    byte[] bytes = ReplicationProtos.ReplicationHLogPosition.newBuilder().setPosition(position)
        .build().toByteArray();
    return ProtobufUtil.prependPBMagic(bytes);
  }

  /**
   * @param bytes - Content of a WAL position znode.
   * @return long - The current WAL position.
   * @throws DeserializationException if the WAL position cannot be parsed
   */
  public static long parseWALPositionFrom(final byte[] bytes) throws DeserializationException {
    if (bytes == null) {
      throw new DeserializationException("Unable to parse null WAL position.");
    }
    if (ProtobufUtil.isPBMagicPrefix(bytes)) {
      int pblen = ProtobufUtil.lengthOfPBMagic();
      ReplicationProtos.ReplicationHLogPosition.Builder builder =
          ReplicationProtos.ReplicationHLogPosition.newBuilder();
      ReplicationProtos.ReplicationHLogPosition position;
      try {
        ProtobufUtil.mergeFrom(builder, bytes, pblen, bytes.length - pblen);
        position = builder.build();
      } catch (IOException e) {
        throw new DeserializationException(e);
      }
      return position.getPosition();
    } else {
      if (bytes.length > 0) {
        return Bytes.toLong(bytes);
      }
      return 0;
    }
  }
}
