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
package org.apache.hadoop.hbase.zookeeper;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.DeserializationException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.zookeeper.KeeperException;

/**
 * Tracks the root region server location node in zookeeper.
 * Root region location is set by {@link RootLocationEditor} usually called
 * out of <code>RegionServerServices</code>.
 * This class has a watcher on the root location and notices changes.
 */
@InterfaceAudience.Private
public class RootRegionTracker extends ZooKeeperNodeTracker {
  /**
   * Creates a root region location tracker.
   *
   * <p>After construction, use {@link #start} to kick off tracking.
   *
   * @param watcher
   * @param abortable
   */
  public RootRegionTracker(ZooKeeperWatcher watcher, Abortable abortable) {
    super(watcher, watcher.rootServerZNode, abortable);
  }

  /**
   * Checks if the root region location is available.
   * @return true if root region location is available, false if not
   */
  public boolean isLocationAvailable() {
    return super.getData(true) != null;
  }

  /**
   * Gets the root region location, if available.  Does not block.  Sets a watcher.
   * @return server name or null if we failed to get the data.
   * @throws InterruptedException
   */
  public ServerName getRootRegionLocation() throws InterruptedException {
    try {
      return ServerName.parseFrom(super.getData(true));
    } catch (DeserializationException e) {
      LOG.warn("Failed parse", e);
      return null;
    }
  }

  /**
   * Gets the root region location, if available.  Does not block.  Does not set
   * a watcher (In this regard it differs from {@link #getRootRegionLocation()}.
   * @param zkw
   * @return server name or null if we failed to get the data.
   * @throws KeeperException
   */
  public static ServerName getRootRegionLocation(final ZooKeeperWatcher zkw)
  throws KeeperException {
    try {
      return ServerName.parseFrom(ZKUtil.getData(zkw, zkw.rootServerZNode));
    } catch (DeserializationException e) {
      throw ZKUtil.convert(e);
    }
  }

  /**
   * Gets the root region location, if available, and waits for up to the
   * specified timeout if not immediately available.
   * Given the zookeeper notification could be delayed, we will try to
   * get the latest data.
   * @param timeout maximum time to wait, in millis
   * @return server name for server hosting root region formatted as per
   * {@link ServerName}, or null if none available
   * @throws InterruptedException if interrupted while waiting
   */
  public ServerName waitRootRegionLocation(long timeout)
  throws InterruptedException {
    if (false == checkIfBaseNodeAvailable()) {
      String errorMsg = "Check the value configured in 'zookeeper.znode.parent'. "
          + "There could be a mismatch with the one configured in the master.";
      LOG.error(errorMsg);
      throw new IllegalArgumentException(errorMsg);
    }
    try {
      return ServerName.parseFrom(super.blockUntilAvailable(timeout, true));
    } catch (DeserializationException e) {
      LOG.warn("Failed parse", e);
      return null;
    }
  }

  /**
   * Sets the location of <code>-ROOT-</code> in ZooKeeper to the
   * specified server address.
   * @param zookeeper zookeeper reference
   * @param location The server hosting <code>-ROOT-</code>
   * @throws KeeperException unexpected zookeeper exception
   */
  public static void setRootLocation(ZooKeeperWatcher zookeeper,
      final ServerName location)
  throws KeeperException {
    LOG.info("Setting ROOT region location in ZooKeeper as " + location);
    // Make the RootRegionServer pb and then get its bytes and save this as
    // the znode content.
    byte [] data = toByteArray(location);
    try {
      ZKUtil.createAndWatch(zookeeper, zookeeper.rootServerZNode, data);
    } catch(KeeperException.NodeExistsException nee) {
      LOG.debug("ROOT region location already existed, updated location");
      ZKUtil.setData(zookeeper, zookeeper.rootServerZNode, data);
    }
  }

  /**
   * Build up the znode content.
   * @param sn What to put into the znode.
   * @return The content of the root-region-server znode
   */
  static byte [] toByteArray(final ServerName sn) {
    // ZNode content is a pb message preceeded by some pb magic.
    HBaseProtos.ServerName pbsn =
      HBaseProtos.ServerName.newBuilder().setHostName(sn.getHostname()).
      setPort(sn.getPort()).setStartCode(sn.getStartcode()).build();
    ZooKeeperProtos.RootRegionServer pbrsr =
      ZooKeeperProtos.RootRegionServer.newBuilder().setServer(pbsn).build();
    return ProtobufUtil.prependPBMagic(pbrsr.toByteArray());
  }

  /**
   * Deletes the location of <code>-ROOT-</code> in ZooKeeper.
   * @param zookeeper zookeeper reference
   * @throws KeeperException unexpected zookeeper exception
   */
  public static void deleteRootLocation(ZooKeeperWatcher zookeeper)
  throws KeeperException {
    LOG.info("Unsetting ROOT region location in ZooKeeper");
    try {
      // Just delete the node.  Don't need any watches.
      ZKUtil.deleteNode(zookeeper, zookeeper.rootServerZNode);
    } catch(KeeperException.NoNodeException nne) {
      // Has already been deleted
    }
  }

  /**
   * Wait until the root region is available.
   * @param zkw
   * @param timeout
   * @return ServerName or null if we timed out.
   * @throws InterruptedException
   */
  public static ServerName blockUntilAvailable(final ZooKeeperWatcher zkw,
      final long timeout)
  throws InterruptedException {
    byte [] data = ZKUtil.blockUntilAvailable(zkw, zkw.rootServerZNode, timeout);
    if (data == null) return null;
    try {
      return ServerName.parseFrom(data);
    } catch (DeserializationException e) {
      LOG.warn("Failed parse", e);
      return null;
    }
  }
}