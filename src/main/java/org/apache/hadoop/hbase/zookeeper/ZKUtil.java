/**
 * Copyright 2010 The Apache Software Foundation
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * Internal HBase utility class for ZooKeeper.
 *
 * <p>Contains only static methods and constants.
 *
 * <p>Methods all throw {@link KeeperException} if there is an unexpected
 * zookeeper exception, so callers of these methods must handle appropriately.
 * If ZK is required for the operation, the server will need to be aborted.
 */
public class ZKUtil {
  private static final Log LOG = LogFactory.getLog(ZKUtil.class);

  // TODO: Replace this with ZooKeeper constant when ZOOKEEPER-277 is resolved.
  private static final char ZNODE_PATH_SEPARATOR = '/';

  /**
   * Creates a new connection to ZooKeeper, pulling settings and quorum config
   * from the specified configuration object using methods from {@link ZKConfig}.
   *
   * Sets the connection status monitoring watcher to the specified watcher.
   *
   * @param conf configuration to pull quorum and other settings from
   * @param watcher watcher to monitor connection changes
   * @return connection to zookeeper
   * @throws IOException if unable to connect to zk or config problem
   */
  public static ZooKeeper connect(Configuration conf, Watcher watcher)
  throws IOException {
    Properties properties = ZKConfig.makeZKProps(conf);
    String quorum = ZKConfig.getZKQuorumServersString(properties);
    return connect(conf, quorum, watcher);
  }

  public static ZooKeeper connect(Configuration conf, String quorum,
      Watcher watcher)
  throws IOException {
    if(quorum == null) {
      throw new IOException("Unable to determine ZooKeeper quorum");
    }
    int timeout = conf.getInt("zookeeper.session.timeout", 60 * 1000);
    LOG.debug("Opening connection to ZooKeeper with quorum (" + quorum + ")");
    return new ZooKeeper(quorum, timeout, watcher);
  }

  //
  // Helper methods
  //

  /**
   * Join the prefix znode name with the suffix znode name to generate a proper
   * full znode name.
   *
   * Assumes prefix does not end with slash and suffix does not begin with it.
   *
   * @param prefix beginning of znode name
   * @param suffix ending of znode name
   * @return result of properly joining prefix with suffix
   */
  public static String joinZNode(String prefix, String suffix) {
    return prefix + ZNODE_PATH_SEPARATOR + suffix;
  }

  /**
   * Returns the full path of the immediate parent of the specified node.
   * @param node path to get parent of
   * @return parent of path, null if passed the root node or an invalid node
   */
  public static String getParent(String node) {
    int idx = node.lastIndexOf(ZNODE_PATH_SEPARATOR);
    return idx <= 0 ? null : node.substring(0, idx);
  }

  /**
   * Get the unique node-name for the specified regionserver.
   *
   * Used when a server puts up an ephemeral node for itself and needs to use
   * a unique name.
   *
   * Returns the fully-qualified znode path.
   *
   * @param serverInfo server information
   * @return unique, zookeeper-safe znode path for the server instance
   */
  public static String getNodeName(HServerInfo serverInfo) {
    return serverInfo.getServerName();
  }

  /**
   * Get the key to the ZK ensemble for this configuration without
   * adding a name at the end
   * @param conf Configuration to use to build the key
   * @return ensemble key without a name
   */
  public static String getZooKeeperClusterKey(Configuration conf) {
    return getZooKeeperClusterKey(conf, null);
  }

  /**
   * Get the key to the ZK ensemble for this configuration and append
   * a name at the end
   * @param conf Configuration to use to build the key
   * @param name Name that should be appended at the end if not empty or null
   * @return ensemble key with a name (if any)
   */
  public static String getZooKeeperClusterKey(Configuration conf, String name) {
    String quorum = conf.get(HConstants.ZOOKEEPER_QUORUM.replaceAll(
        "[\\t\\n\\x0B\\f\\r]", ""));
    StringBuilder builder = new StringBuilder(quorum);
    builder.append(":");
    builder.append(conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
    if (name != null && !name.isEmpty()) {
      builder.append(",");
      builder.append(name);
    }
    return builder.toString();
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
  public static boolean watchAndCheckExists(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    try {
      Stat s = zkw.getZooKeeper().exists(znode, zkw);
      zkw.debug("Set watcher on existing znode (" + znode + ")");
      return s != null ? true : false;
    } catch (KeeperException e) {
      zkw.warn("Unable to set watcher on znode (" + znode + ")", e);
      zkw.keeperException(e);
      return false;
    } catch (InterruptedException e) {
      zkw.warn("Unable to set watcher on znode (" + znode + ")", e);
      zkw.interruptedException(e);
      return false;
    }
  }

  /**
   * Check if the specified node exists.  Sets no watches.
   *
   * Returns true if node exists, false if not.  Returns an exception if there
   * is an unexpected zookeeper exception.
   *
   * @param zkw zk reference
   * @param znode path of node to watch
   * @return true if znode exists, false if does not exist or error
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean checkExists(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    try {
      Stat s = zkw.getZooKeeper().exists(znode, null);
      return s != null ? true : false;
    } catch (KeeperException e) {
      zkw.warn("Unable to set watcher on znode (" + znode + ")", e);
      zkw.keeperException(e);
      return false;
    } catch (InterruptedException e) {
      zkw.warn("Unable to set watcher on znode (" + znode + ")", e);
      zkw.interruptedException(e);
      return false;
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
   * @returns list of children of the specified node, an empty list if the node
   *          exists but has no children, and null if the node does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static List<String> listChildrenAndWatchForNewChildren(
      ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    try {
      List<String> children = zkw.getZooKeeper().getChildren(znode, zkw);
      return children;
    } catch(KeeperException.NoNodeException ke) {
      zkw.debug("Unable to list children of znode (" + znode + ") " +
          "because node does not exist (not an error)");
      return null;
    } catch (KeeperException e) {
      zkw.warn("Unable to list children of znode (" + znode + ")", e);
      zkw.keeperException(e);
      return null;
    } catch (InterruptedException e) {
      zkw.warn("Unable to list children of znode (" + znode + ")", e);
      zkw.interruptedException(e);
      return null;
    }
  }

  /**
   * Lists the children of the specified znode, retrieving the data of each
   * child as a server address.
   *
   * Used to list the currently online regionservers and their addresses.
   *
   * Sets no watches at all, this method is best effort.
   *
   * Returns an empty list if the node has no children.  Returns null if the
   * parent node itself does not exist.
   *
   * @param zkw zookeeper reference
   * @param znode node to get children of as addresses
   * @return list of data of children of specified znode, empty if no children,
   *         null if parent does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static List<HServerAddress> listChildrenAndGetAsAddresses(
      ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    List<String> children = listChildrenNoWatch(zkw, znode);
    if(children == null) {
      return null;
    }
    List<HServerAddress> addresses =
      new ArrayList<HServerAddress>(children.size());
    for(String child : children) {
      addresses.add(getDataAsAddress(zkw, joinZNode(znode, child)));
    }
    return addresses;
  }

  /**
   * Lists the children of the specified znode without setting any watches.
   *
   * Used to list the currently online regionservers and their addresses.
   *
   * Sets no watches at all, this method is best effort.
   *
   * Returns an empty list if the node has no children.  Returns null if the
   * parent node itself does not exist.
   *
   * @param zkw zookeeper reference
   * @param znode node to get children of as addresses
   * @return list of data of children of specified znode, empty if no children,
   *         null if parent does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static List<String> listChildrenNoWatch(
      ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    List<String> children = null;
    try {
      // List the children without watching
      children = zkw.getZooKeeper().getChildren(znode, null);
    } catch(KeeperException.NoNodeException nne) {
      return null;
    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
    }
    return children;
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
  public static boolean nodeHasChildren(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    try {
      return !zkw.getZooKeeper().getChildren(znode, null).isEmpty();
    } catch(KeeperException.NoNodeException ke) {
      zkw.debug("Unable to list children of znode (" + znode + ") " +
      "because node does not exist (not an error)");
      return false;
    } catch (KeeperException e) {
      zkw.warn("Unable to list children of znode (" + znode + ")", e);
      zkw.keeperException(e);
      return false;
    } catch (InterruptedException e) {
      zkw.warn("Unable to list children of znode (" + znode + ")", e);
      zkw.interruptedException(e);
      return false;
    }
  }

  //
  // Data retrieval
  //

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
  public static byte [] getDataAndWatch(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    try {
      byte [] data = zkw.getZooKeeper().getData(znode, zkw, null);
      zkw.debug("Retrieved " + data.length + " bytes of data from znode (" +
          znode + ") and set a watcher");
      return data;
    } catch (KeeperException.NoNodeException e) {
      zkw.debug("Unable to get data of znode (" + znode + ") " +
          "because node does not exist (not an error)");
      return null;
    } catch (KeeperException e) {
      zkw.warn("Unable to get data of znode (" + znode + ")", e);
      zkw.keeperException(e);
      return null;
    } catch (InterruptedException e) {
      zkw.warn("Unable to get data of znode (" + znode + ")", e);
      zkw.interruptedException(e);
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
   * @param stat node status to set if node exists
   * @return data of the specified znode, or null if does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static byte [] getDataNoWatch(ZooKeeperWatcher zkw, String znode,
      Stat stat)
  throws KeeperException {
    try {
      byte [] data = zkw.getZooKeeper().getData(znode, zkw, stat);
      zkw.debug("Retrieved " + data.length + " bytes of data from znode (" +
          znode + ") and set a watcher");
      return data;
    } catch (KeeperException.NoNodeException e) {
      zkw.debug("Unable to get data of znode (" + znode + ") " +
          "because node does not exist (not necessarily an error)");
      return null;
    } catch (KeeperException e) {
      zkw.warn("Unable to get data of znode (" + znode + ")", e);
      zkw.keeperException(e);
      return null;
    } catch (InterruptedException e) {
      zkw.warn("Unable to get data of znode (" + znode + ")", e);
      zkw.interruptedException(e);
      return null;
    }
  }

  /**
   * Get the data at the specified znode, deserialize it as an HServerAddress,
   * and set a watch.
   *
   * Returns the data as a server address and sets a watch if the node exists.
   * Returns null and no watch is set if the node does not exist or there is an
   * exception.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @return data of the specified node as a server address, or null
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static HServerAddress getDataAsAddress(ZooKeeperWatcher zkw,
      String znode)
  throws KeeperException {
    byte [] data = getDataAndWatch(zkw, znode);
    if(data == null) {
      return null;
    }
    String addrString = Bytes.toString(data);
    zkw.debug("Read server address from znode (" + znode + "): " + addrString);
    return new HServerAddress(addrString);
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
   * @param znode
   * @param data
   * @param expectedVersion
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.BadVersionException if version mismatch
   */
  public static void updateExistingNodeData(ZooKeeperWatcher zkw, String znode,
      byte [] data, int expectedVersion)
  throws KeeperException {
    try {
      zkw.getZooKeeper().setData(znode, data, expectedVersion);
    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
    }
  }

  //
  // Data setting
  //

  /**
   * Set the specified znode to be an ephemeral node carrying the specified
   * server address.  Used by masters for their ephemeral node and regionservers
   * for their ephemeral node.
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
   * @param address server address
   * @return true if address set, false if not, watch set in both cases
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean setAddressAndWatch(ZooKeeperWatcher zkw,
      String znode, HServerAddress address)
  throws KeeperException {
    return createEphemeralNodeAndWatch(zkw, znode,
        Bytes.toBytes(address.toString()));
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
  public static boolean createEphemeralNodeAndWatch(ZooKeeperWatcher zkw,
      String znode, byte [] data)
  throws KeeperException {
    try {
      zkw.getZooKeeper().create(znode, data, Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL);
    } catch (KeeperException.NodeExistsException nee) {
      if(!watchAndCheckExists(zkw, znode)) {
        // It did exist but now it doesn't, try again
        return createEphemeralNodeAndWatch(zkw, znode, data);
      }
      return false;
    } catch (InterruptedException e) {
      LOG.info("Interrupted", e);
    }
    return true;
  }

  /**
   *
   * Set the specified znode to be a persistent node carrying the specified
   * data.
   *
   * Returns true if the node was successfully created, false if the node
   * already existed.
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
  public static boolean createPersistentNodeIfNotExists(
      ZooKeeperWatcher zkw, String znode, byte [] data)
  throws KeeperException {
    try {
      zkw.getZooKeeper().create(znode, data, Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL);
    } catch (KeeperException.NodeExistsException nee) {
      return false;
    } catch (InterruptedException e) {
      zkw.interruptedException(e);
      return false;
    }
    return true;
  }

  /**
   * Creates the specified node, if the node does not exist.  Does not set a
   * watch and fails silently if the node already exists.
   *
   * The node created is persistent and open access.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static void createIfNotExists(ZooKeeperWatcher zkw,
      String znode)
  throws KeeperException {
    try {
      zkw.getZooKeeper().create(znode, new byte[0], Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
    } catch(KeeperException.NodeExistsException nee) {
    } catch(InterruptedException ie) {
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
  public static void createWithParents(ZooKeeperWatcher zkw,
      String znode)
  throws KeeperException {
    try {
      if(znode == null) {
        return;
      }
      zkw.getZooKeeper().create(znode, new byte[0], Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
    } catch(KeeperException.NodeExistsException nee) {
      return;
    } catch(KeeperException.NoNodeException nne) {
      createWithParents(zkw, getParent(znode));
      createWithParents(zkw, znode);
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
  public static void deleteNode(ZooKeeperWatcher zkw, String node)
  throws KeeperException {
    try {
      zkw.getZooKeeper().delete(node, -1);
    } catch(InterruptedException ie) {
    }
  }

  /**
   * Delete the specified node and all of it's children.
   * 
   * Sets no watches.  Throws all exceptions besides dealing with deletion of
   * children.
   */
  public static void deleteNodeRecursively(ZooKeeperWatcher zkw, String node)
  throws KeeperException {
    try {
      List<String> children = ZKUtil.listChildrenNoWatch(zkw, node);
      if(!children.isEmpty()) {
        for(String child : children) {
          deleteNodeRecursively(zkw, joinZNode(node, child));
        }
      }
      zkw.getZooKeeper().delete(node, -1);
    } catch(InterruptedException ie) {
    }
  }
  //
  // ZooKeeper cluster information
  //

  /** @return String dump of everything in ZooKeeper. */
  public static String dump(ZooKeeperWatcher zkw) {
    StringBuilder sb = new StringBuilder();
    try {
      sb.append("\nHBase tree in ZooKeeper is rooted at ").append(zkw.baseZNode);
      sb.append("\n  Cluster up? ").append(checkExists(zkw, zkw.clusterStateZNode));
      sb.append("\n  Master address: ").append(
          getDataAsAddress(zkw, zkw.masterAddressZNode));
      sb.append("\n  Region server holding ROOT: ").append(
          getDataAsAddress(zkw, zkw.rootServerZNode));
      sb.append("\n  Region servers:");
      for (HServerAddress address : listChildrenAndGetAsAddresses(zkw,
          zkw.rsZNode)) {
        sb.append("\n    - ").append(address);
      }
      sb.append("\n  Quorum Server Statistics:");
      String[] servers = zkw.getQuorum().split(",");
      for (String server : servers) {
        sb.append("\n    - ").append(server);
        try {
          String[] stat = getServerStats(server);
          for (String s : stat) {
            sb.append("\n        ").append(s);
          }
        } catch (Exception e) {
          sb.append("\n        ERROR: ").append(e.getMessage());
        }
      }
    } catch(KeeperException ke) {
      sb.append("\n  FATAL ZooKeeper Exception!\n");
      sb.append("\n  " + ke.getMessage());
    }
    return sb.toString();
  }

  /**
   * Gets the statistics from the given server. Uses a 1 minute timeout.
   *
   * @param server  The server to get the statistics from.
   * @return The array of response strings.
   * @throws IOException When the socket communication fails.
   */
  public static String[] getServerStats(String server)
  throws IOException {
    return getServerStats(server, 60 * 1000);
  }

  /**
   * Gets the statistics from the given server.
   *
   * @param server  The server to get the statistics from.
   * @param timeout  The socket timeout to use.
   * @return The array of response strings.
   * @throws IOException When the socket communication fails.
   */
  public static String[] getServerStats(String server, int timeout)
  throws IOException {
    String[] sp = server.split(":");
    Socket socket = new Socket(sp[0],
      sp.length > 1 ? Integer.parseInt(sp[1]) : 2181);
    socket.setSoTimeout(timeout);
    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
    BufferedReader in = new BufferedReader(new InputStreamReader(
      socket.getInputStream()));
    out.println("stat");
    out.flush();
    ArrayList<String> res = new ArrayList<String>();
    while (true) {
      String line = in.readLine();
      if (line != null) {
        res.add(line);
      } else {
        break;
      }
    }
    socket.close();
    return res.toArray(new String[res.size()]);
  }

}