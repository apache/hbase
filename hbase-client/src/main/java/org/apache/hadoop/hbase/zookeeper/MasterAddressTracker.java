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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.InterruptedIOException;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Manages the location of the current active Master for the RegionServer.
 * <p>
 * Listens for ZooKeeper events related to the master address. The node
 * <code>/master</code> will contain the address of the current master.
 * This listener is interested in
 * <code>NodeDeleted</code> and <code>NodeCreated</code> events on
 * <code>/master</code>.
 * <p>
 * Utilizes {@link ZooKeeperNodeTracker} for zk interactions.
 * <p>
 * You can get the current master via {@link #getMasterAddress()} or via
 * {@link #getMasterAddress(ZooKeeperWatcher)} if you do not have a running
 * instance of this Tracker in your context.
 * <p>
 * This class also includes utility for interacting with the master znode, for
 * writing and reading the znode content.
 */
@InterfaceAudience.Private
public class MasterAddressTracker extends ZooKeeperNodeTracker {
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
  public MasterAddressTracker(ZooKeeperWatcher watcher, Abortable abortable) {
    super(watcher, watcher.getMasterAddressZNode(), abortable);
  }

  /**
   * @param watcher ZooKeeperWatcher instance to use for querying ZK.
   * @return current list of backup masters.
   */
  public static List<ServerName> getBackupMastersAndRenewWatch(
      ZooKeeperWatcher watcher) {
    // Build Set of backup masters from ZK nodes
    List<String> backupMasterStrings;
    try {
      backupMasterStrings = ZKUtil.listChildrenAndWatchForNewChildren(
          watcher, watcher.backupMasterAddressesZNode);
    } catch (KeeperException e) {
      LOG.warn(watcher.prefix("Unable to list backup servers"), e);
      backupMasterStrings = null;
    }

    List<ServerName> backupMasters = new ArrayList<>();
    if (backupMasterStrings != null && !backupMasterStrings.isEmpty()) {
      for (String s: backupMasterStrings) {
        try {
          byte [] bytes;
          try {
            bytes = ZKUtil.getData(watcher, ZKUtil.joinZNode(
               watcher.backupMasterAddressesZNode, s));
          } catch (InterruptedException e) {
            throw new InterruptedIOException("Thread interrupted.");
          }
          if (bytes != null) {
            ServerName sn;
            try {
              sn = ServerName.parseFrom(bytes);
            } catch (DeserializationException e) {
              LOG.warn("Failed parse, skipping registering backup server", e);
              continue;
            }
            backupMasters.add(sn);
          }
        } catch (KeeperException | InterruptedIOException e) {
          LOG.warn(watcher.prefix("Unable to get information about " +
              "backup servers"), e);
        }
      }
      Collections.sort(backupMasters, new Comparator<ServerName>() {
        @Override
        public int compare(ServerName s1, ServerName s2) {
          return s1.getServerName().compareTo(s2.getServerName());
        }});
    }
    return backupMasters;
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
    String backupZNode = ZKUtil.joinZNode(watcher.backupMasterAddressesZNode, sn.toString());
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
      return ServerName.parseFrom(super.getData(refresh));
    } catch (DeserializationException e) {
      LOG.warn("Failed parse", e);
      return null;
    }
  }

  /**
   * Get master address.
   * Use this instead of {@link #getMasterAddress()} if you do not have an
   * instance of this tracker in your context.
   * @param zkw ZooKeeperWatcher to use
   * @return ServerName stored in the the master address znode or null if no
   * znode present.
   * @throws KeeperException
   * @throws IOException
   */
  public static ServerName getMasterAddress(final ZooKeeperWatcher zkw)
  throws KeeperException, IOException {
    byte [] data;
    try {
      data = ZKUtil.getData(zkw, zkw.getMasterAddressZNode());
    } catch (InterruptedException e) {
      throw new InterruptedIOException();
    }
    // TODO javadoc claims we return null in this case. :/
    if (data == null){
      throw new IOException("Can't get master address from ZooKeeper; znode data == null");
    }
    try {
      return ServerName.parseFrom(data);
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
   * @param zkw ZooKeeperWatcher to use
   * @return master info port in the the master address znode or null if no
   * znode present.
   * // TODO can't return null for 'int' return type. non-static verison returns 0
   * @throws KeeperException
   * @throws IOException
   */
  public static int getMasterInfoPort(final ZooKeeperWatcher zkw) throws KeeperException,
      IOException {
    byte[] data;
    try {
      data = ZKUtil.getData(zkw, zkw.getMasterAddressZNode());
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
   * @param zkw The ZooKeeperWatcher to use.
   * @param znode Where to create the znode; could be at the top level or it
   * could be under backup masters
   * @param master ServerName of the current master must not be null.
   * @return true if node created, false if not; a watch is set in both cases
   * @throws KeeperException
   */
  public static boolean setMasterAddress(final ZooKeeperWatcher zkw,
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
   * magic as prefix.
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
   * @throws DeserializationException
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
  public static boolean deleteIfEquals(ZooKeeperWatcher zkw, final String content) {
    if (content == null){
      throw new IllegalArgumentException("Content must not be null");
    }

    try {
      Stat stat = new Stat();
      byte[] data = ZKUtil.getDataNoWatch(zkw, zkw.getMasterAddressZNode(), stat);
      ServerName sn = ServerName.parseFrom(data);
      if (sn != null && content.equals(sn.toString())) {
        return (ZKUtil.deleteNode(zkw, zkw.getMasterAddressZNode(), stat.getVersion()));
      }
    } catch (KeeperException e) {
      LOG.warn("Can't get or delete the master znode", e);
    } catch (DeserializationException e) {
      LOG.warn("Can't get or delete the master znode", e);
    }

    return false;
  }
}
