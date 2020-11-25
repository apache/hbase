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
import java.io.InterruptedIOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ZooKeeperProtos;

/**
 * Manages the location of the current active Master for the RegionServer.
 * <p>
 * Listens for ZooKeeper events related to the master address. The node
 * <code>/master</code> will contain the address of the current master.
 * This listener is interested in
 * <code>NodeDeleted</code> and <code>NodeCreated</code> events on
 * <code>/master</code>.
 * <p>
 * Utilizes {@link ZKNodeTracker} for zk interactions.
 * <p>
 * You can get the current master via {@link #getMasterAddress()} or via
 * {@link #getMasterAddress(ZKWatcher)} if you do not have a running
 * instance of this Tracker in your context.
 * <p>
 * This class also includes utility for interacting with the master znode, for
 * writing and reading the znode content.
 */
@InterfaceAudience.Private
public class MasterAddressTracker extends ZKNodeTracker {
  /**
   * Construct a master address listener with the specified
   * <code>zookeeper</code> reference.
   * <p>
   * This constructor does not trigger any actions, you must call methods
   * explicitly.  Normally you will just want to execute {@link #start()} to
   * begin tracking of the master address.
   *
   * @param watcher zk reference and watcher
   * @param abortable abortable in case of fatal error
   */
  public MasterAddressTracker(ZKWatcher watcher, Abortable abortable) {
    super(watcher, watcher.getZNodePaths().masterAddressZNode, abortable);
  }

  /**
   * Get the address of the current master if one is available.  Returns null
   * if no current master.
   * @return Server name or null if timed out.
   */
  public ServerName getMasterAddress() {
    return getMasterAddress(false);
  }

  /**
   * Get the info port of the current master of one is available.
   * Return 0 if no current master or zookeeper is unavailable
   * @return info port or 0 if timed out
   */
  public int getMasterInfoPort() {
    try {
      final ZooKeeperProtos.Master master = parse(this.getData(false));
      if (master == null) {
        return 0;
      }
      return master.getInfoPort();
    } catch (DeserializationException e) {
      LOG.warn("Failed parse master zk node data", e);
      return 0;
    }
  }
  /**
   * Get the info port of the backup master if it is available.
   * Return 0 if no backup master or zookeeper is unavailable
   * @param sn server name of backup master
   * @return info port or 0 if timed out or exceptions
   */
  public int getBackupMasterInfoPort(final ServerName sn) {
    String backupZNode = ZNodePaths.joinZNode(watcher.getZNodePaths().backupMasterAddressesZNode,
      sn.toString());
    try {
      byte[] data = ZKUtil.getData(watcher, backupZNode);
      final ZooKeeperProtos.Master backup = parse(data);
      if (backup == null) {
        return 0;
      }
      return backup.getInfoPort();
    } catch (Exception e) {
      LOG.warn("Failed to get backup master: " + sn + "'s info port.", e);
      return 0;
    }
  }

  /**
   * Get the address of the current master if one is available.  Returns null
   * if no current master. If refresh is set, try to load the data from ZK again,
   * otherwise, cached data will be used.
   *
   * @param refresh whether to refresh the data by calling ZK directly.
   * @return Server name or null if timed out.
   */
  public ServerName getMasterAddress(final boolean refresh) {
    try {
      return ProtobufUtil.parseServerNameFrom(super.getData(refresh));
    } catch (DeserializationException e) {
      LOG.warn("Failed parse", e);
      return null;
    }
  }

  /**
   * Get master address.
   * Use this instead of {@link #getMasterAddress()} if you do not have an
   * instance of this tracker in your context.
   * @param zkw ZKWatcher to use
   * @return ServerName stored in the the master address znode or null if no
   *         znode present.
   * @throws KeeperException if a ZooKeeper operation fails
   * @throws IOException if the address of the ZooKeeper master cannot be retrieved
   */
  public static ServerName getMasterAddress(final ZKWatcher zkw)
          throws KeeperException, IOException {
    byte [] data;
    try {
      data = ZKUtil.getData(zkw, zkw.getZNodePaths().masterAddressZNode);
    } catch (InterruptedException e) {
      throw new InterruptedIOException();
    }
    // TODO javadoc claims we return null in this case. :/
    if (data == null){
      throw new IOException("Can't get master address from ZooKeeper; znode data == null");
    }
    try {
      return ProtobufUtil.parseServerNameFrom(data);
    } catch (DeserializationException e) {
      KeeperException ke = new KeeperException.DataInconsistencyException();
      ke.initCause(e);
      throw ke;
    }
  }

  /**
   * Get master info port.
   * Use this instead of {@link #getMasterInfoPort()} if you do not have an
   * instance of this tracker in your context.
   * @param zkw ZKWatcher to use
   * @return master info port in the the master address znode or null if no
   *         znode present.
   *         // TODO can't return null for 'int' return type. non-static verison returns 0
   * @throws KeeperException if a ZooKeeper operation fails
   * @throws IOException if the address of the ZooKeeper master cannot be retrieved
   */
  public static int getMasterInfoPort(final ZKWatcher zkw) throws KeeperException, IOException {
    byte[] data;
    try {
      data = ZKUtil.getData(zkw, zkw.getZNodePaths().masterAddressZNode);
    } catch (InterruptedException e) {
      throw new InterruptedIOException();
    }
    // TODO javadoc claims we return null in this case. :/
    if (data == null) {
      throw new IOException("Can't get master address from ZooKeeper; znode data == null");
    }
    try {
      return parse(data).getInfoPort();
    } catch (DeserializationException e) {
      KeeperException ke = new KeeperException.DataInconsistencyException();
      ke.initCause(e);
      throw ke;
    }
  }

  /**
   * Set master address into the <code>master</code> znode or into the backup
   * subdirectory of backup masters; switch off the passed in <code>znode</code>
   * path.
   * @param zkw The ZKWatcher to use.
   * @param znode Where to create the znode; could be at the top level or it
   *              could be under backup masters
   * @param master ServerName of the current master must not be null.
   * @return true if node created, false if not; a watch is set in both cases
   * @throws KeeperException if a ZooKeeper operation fails
   */
  public static boolean setMasterAddress(final ZKWatcher zkw,
      final String znode, final ServerName master, int infoPort)
    throws KeeperException {
    return ZKUtil.createEphemeralNodeAndWatch(zkw, znode, toByteArray(master, infoPort));
  }

  /**
   * Check if there is a master available.
   * @return true if there is a master set, false if not.
   */
  public boolean hasMaster() {
    return super.getData(false) != null;
  }

  /**
   * @param sn must not be null
   * @return Content of the master znode as a serialized pb with the pb
   *         magic as prefix.
   */
  static byte[] toByteArray(final ServerName sn, int infoPort) {
    ZooKeeperProtos.Master.Builder mbuilder = ZooKeeperProtos.Master.newBuilder();
    HBaseProtos.ServerName.Builder snbuilder = HBaseProtos.ServerName.newBuilder();
    snbuilder.setHostName(sn.getHostname());
    snbuilder.setPort(sn.getPort());
    snbuilder.setStartCode(sn.getStartcode());
    mbuilder.setMaster(snbuilder.build());
    mbuilder.setRpcVersion(HConstants.RPC_CURRENT_VERSION);
    mbuilder.setInfoPort(infoPort);
    return ProtobufUtil.prependPBMagic(mbuilder.build().toByteArray());
  }

  /**
   * @param data zookeeper data. may be null
   * @return pb object of master, null if no active master
   * @throws DeserializationException if the parsing fails
   */
  public static ZooKeeperProtos.Master parse(byte[] data) throws DeserializationException {
    if (data == null) {
      return null;
    }
    int prefixLen = ProtobufUtil.lengthOfPBMagic();
    try {
      return ZooKeeperProtos.Master.PARSER.parseFrom(data, prefixLen, data.length - prefixLen);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
  }
  /**
   * delete the master znode if its content is same as the parameter
   * @param zkw must not be null
   * @param content must not be null
   */
  public static boolean deleteIfEquals(ZKWatcher zkw, final String content) {
    if (content == null){
      throw new IllegalArgumentException("Content must not be null");
    }

    try {
      Stat stat = new Stat();
      byte[] data = ZKUtil.getDataNoWatch(zkw, zkw.getZNodePaths().masterAddressZNode, stat);
      ServerName sn = ProtobufUtil.parseServerNameFrom(data);
      if (sn != null && content.equals(sn.toString())) {
        return (ZKUtil.deleteNode(zkw, zkw.getZNodePaths().masterAddressZNode, stat.getVersion()));
      }
    } catch (KeeperException e) {
      LOG.warn("Can't get or delete the master znode", e);
    } catch (DeserializationException e) {
      LOG.warn("Can't get or delete the master znode", e);
    }

    return false;
  }

  public List<ServerName> getBackupMasters() throws InterruptedIOException {
    return getBackupMastersAndRenewWatch(watcher);
  }

  /**
   * Retrieves the list of registered backup masters and renews a watch on the znode for children
   * updates.
   * @param zkw Zookeeper watcher to use
   * @return List of backup masters.
   * @throws InterruptedIOException if there is any issue fetching the required data from Zookeeper.
   */
  public static List<ServerName> getBackupMastersAndRenewWatch(
      ZKWatcher zkw) throws InterruptedIOException {
    // Build Set of backup masters from ZK nodes
    List<String> backupMasterStrings = null;
    try {
      backupMasterStrings = ZKUtil.listChildrenAndWatchForNewChildren(zkw,
          zkw.getZNodePaths().backupMasterAddressesZNode);
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to list backup servers"), e);
    }

    List<ServerName> backupMasters = Collections.emptyList();
    if (backupMasterStrings != null && !backupMasterStrings.isEmpty()) {
      backupMasters = new ArrayList<>(backupMasterStrings.size());
      for (String s: backupMasterStrings) {
        try {
          byte [] bytes;
          try {
            bytes = ZKUtil.getData(zkw, ZNodePaths.joinZNode(
                zkw.getZNodePaths().backupMasterAddressesZNode, s));
          } catch (InterruptedException e) {
            throw new InterruptedIOException();
          }
          if (bytes != null) {
            ServerName sn;
            try {
              sn = ProtobufUtil.parseServerNameFrom(bytes);
            } catch (DeserializationException e) {
              LOG.warn("Failed parse, skipping registering backup server", e);
              continue;
            }
            backupMasters.add(sn);
          }
        } catch (KeeperException e) {
          LOG.warn(zkw.prefix("Unable to get information about " +
              "backup servers"), e);
        }
      }
      backupMasters.sort(Comparator.comparing(ServerName::getServerName));
    }
    return backupMasters;
  }
}
