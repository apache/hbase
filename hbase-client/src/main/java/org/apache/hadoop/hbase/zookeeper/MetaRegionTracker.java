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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Stopwatch;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Tracks the meta region server location node in zookeeper.
 * Meta region location is set by <code>RegionServerServices</code>.
 * This class has a watcher on the meta location and notices changes.
 */
@InterfaceAudience.Private
public class MetaRegionTracker extends ZooKeeperNodeTracker {
  /**
   * Creates a meta region location tracker.
   *
   * <p>After construction, use {@link #start} to kick off tracking.
   *
   * @param watcher
   * @param abortable
   */
  public MetaRegionTracker(ZooKeeperWatcher watcher, Abortable abortable) {
    super(watcher, watcher.metaServerZNode, abortable);
  }

  /**
   * Checks if the meta region location is available.
   * @return true if meta region location is available, false if not
   */
  public boolean isLocationAvailable() {
    return getMetaRegionLocation() != null;
  }

  /**
   * Gets the meta region location, if available.  Does not block.  Sets a watcher.
   * @return server name or null if we failed to get the data.
   * @throws InterruptedException
   */
  public ServerName getMetaRegionLocation() {
    RegionState regionState = getMetaRegionState();
    return regionState.isOpened() ? regionState.getServerName() : null;
  }

  /**
   * Gets the meta region location, if available.  Does not block.  Does not set
   * a watcher (In this regard it differs from {@link #getMetaRegionLocation}.
   * @param zkw
   * @return server name or null if we failed to get the data.
   * @throws KeeperException
   */
  public static ServerName getMetaRegionLocation(final ZooKeeperWatcher zkw) throws KeeperException {
    byte[] data = ZKUtil.getData(zkw, zkw.metaServerZNode);
    RegionState regionState = getMetaRegionState(data);
    return regionState.isOpened() ? regionState.getServerName() : null;
  }

  /**
   * Gets the meta region location, if available, and waits for up to the
   * specified timeout if not immediately available.
   * Given the zookeeper notification could be delayed, we will try to
   * get the latest data.
   * @param timeout maximum time to wait, in millis
   * @return server name for server hosting meta region formatted as per
   * {@link ServerName}, or null if none available
   * @throws InterruptedException if interrupted while waiting
   */
  public ServerName waitMetaRegionLocation(long timeout)
  throws InterruptedException {
    if (false == checkIfBaseNodeAvailable()) {
      String errorMsg = "Check the value configured in 'zookeeper.znode.parent'. "
          + "There could be a mismatch with the one configured in the master.";
      LOG.error(errorMsg);
      throw new IllegalArgumentException(errorMsg);
    }
    return blockUntilAvailable(timeout);
   
  }
  
  /**
   * Wait until the meta region is available and is not in transition.
   * @param zkw zookeeper connection to use
   * @param timeout maximum time to wait, in millis
   * @return ServerName or null if we timed out.
   * @throws InterruptedException
   */
  private ServerName blockUntilAvailable(final long timeout) throws InterruptedException {
    if (timeout < 0) throw new IllegalArgumentException("Timeout shouldn't be less than zero");
    Stopwatch sw = new Stopwatch().start();
    ServerName sn = null;
    try {
      while (true) {
        sn = getMetaRegionLocation();
        if (sn != null || sw.elapsedMillis() > timeout - HConstants.SOCKET_RETRY_WAIT_MS) {
          break;
        }
        Thread.sleep(HConstants.SOCKET_RETRY_WAIT_MS);
      }
    } finally {
      sw.stop();
    }
    return sn;
  }

  /**
   * Sets the location of <code>hbase:meta</code> in ZooKeeper to the
   * specified server address.
   * @param zookeeper zookeeper reference
   * @param location The server hosting <code>hbase:meta</code>
   * @throws KeeperException unexpected zookeeper exception
   */
  public static void setMetaLocation(ZooKeeperWatcher zookeeper,
                                     final ServerName location, final RegionState.State regionState)
  throws KeeperException {
    LOG.info("Setting hbase:meta region location in ZooKeeper as " + location);
    // Make the MetaRegionServer pb and then get its bytes and save this as
    // the znode content.
    byte [] data = toByteArray(location, regionState);
    try {
      ZKUtil.createAndWatch(zookeeper, zookeeper.metaServerZNode, data);
    } catch(KeeperException.NodeExistsException nee) {
      LOG.debug("META region location already existed, updated location");
      ZKUtil.setData(zookeeper, zookeeper.metaServerZNode, data);
    }
  }

  /**
   * Build up the znode content.
   * @param sn What to put into the znode.
   * @return The content of the meta-region-server znode
   */
  static byte [] toByteArray(final ServerName sn, final RegionState.State regionState) {
    // ZNode content is a pb message preceded by some pb magic.
    HBaseProtos.ServerName pbsn =
      HBaseProtos.ServerName.newBuilder()
                            .setHostName(sn.getHostname())
                            .setPort(sn.getPort())
                            .setStartCode(sn.getStartcode())
                            .build();

    ZooKeeperProtos.MetaRegionServer pbrsr =
      ZooKeeperProtos.MetaRegionServer.newBuilder()
                                      .setServer(pbsn)
                                      .setRpcVersion(HConstants.RPC_CURRENT_VERSION)
                                      .setState(regionState.convert())
                                      .build();
    return ProtobufUtil.prependPBMagic(pbrsr.toByteArray());
  }

  /**
   * Deletes the location of <code>hbase:meta</code> in ZooKeeper.
   * @param zookeeper zookeeper reference
   * @throws KeeperException unexpected zookeeper exception
   */
  public static void deleteMetaLocation(ZooKeeperWatcher zookeeper)
  throws KeeperException {
    LOG.info("Unsetting hbase:meta region location in ZooKeeper");
    try {
      // Just delete the node.  Don't need any watches.
      ZKUtil.deleteNode(zookeeper, zookeeper.metaServerZNode);
    } catch(KeeperException.NoNodeException nne) {
      // Has already been deleted
    }
  }

  /**
   * Wait until the meta region is available.
   * @param zkw
   * @param timeout
   * @return ServerName or null if we timed out.
   * @throws InterruptedException
   */
  public static ServerName blockUntilAvailable(final ZooKeeperWatcher zkw,
      final long timeout)
  throws InterruptedException {
    byte [] data = ZKUtil.blockUntilAvailable(zkw, zkw.metaServerZNode, timeout);
    RegionState regionState =  getMetaRegionState(data);
    return regionState.isOpened() ? regionState.getServerName() : null;
  }
  
  
  private static RegionState getMetaRegionState(byte[] data) {
    RegionState.State state = RegionState.State.OPEN;
    ServerName serverName = null;
    try {
      if (data != null && data.length > 0 && ProtobufUtil.isPBMagicPrefix(data)) {
        try {
          int prefixLen = ProtobufUtil.lengthOfPBMagic();
          ZooKeeperProtos.MetaRegionServer rl =
              ZooKeeperProtos.MetaRegionServer.PARSER.parseFrom(data, prefixLen, data.length
                  - prefixLen);
          if (rl.hasState()) {
            state = RegionState.State.convert(rl.getState());
          }
          HBaseProtos.ServerName sn = rl.getServer();
          serverName = ServerName.valueOf(sn.getHostName(), sn.getPort(), sn.getStartCode());
        } catch (InvalidProtocolBufferException e) {
          throw new DeserializationException("Unable to parse root region location");
        }
      } else {
        // old style of meta region location?
        serverName = ServerName.parseFrom(data);
      }
    } catch (DeserializationException e) {
      LOG.warn("Failed parse", e);
      return null;
    }
    if (serverName == null) {
      state = RegionState.State.OFFLINE;
    }
    return new RegionState(HRegionInfo.FIRST_META_REGIONINFO, state, serverName);
  }
  
  /**
   * Get meta region state
   * @return RegionState
   */
  public  RegionState getMetaRegionState() {
    byte[] data = super.getData(true);
    return getMetaRegionState(data);
  }
}