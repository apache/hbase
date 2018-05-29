/**
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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionServerInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks the online region servers via ZK.
 *
 * <p>Handling of new RSs checking in is done via RPC.  This class
 * is only responsible for watching for expired nodes.  It handles
 * listening for changes in the RS node list and watching each node.
 *
 * <p>If an RS node gets deleted, this automatically handles calling of
 * {@link ServerManager#expireServer(ServerName)}
 */
@InterfaceAudience.Private
public class RegionServerTracker extends ZKListener {
  private static final Logger LOG = LoggerFactory.getLogger(RegionServerTracker.class);
  private final NavigableMap<ServerName, RegionServerInfo> regionServers = new TreeMap<>();
  private ServerManager serverManager;
  private MasterServices server;

  public RegionServerTracker(ZKWatcher watcher,
      MasterServices server, ServerManager serverManager) {
    super(watcher);
    this.server = server;
    this.serverManager = serverManager;
  }

  /**
   * Starts the tracking of online RegionServers.
   *
   * <p>All RSs will be tracked after this method is called.
   *
   * @throws KeeperException
   * @throws IOException
   */
  public void start() throws KeeperException, IOException {
    watcher.registerListener(this);
    List<String> servers =
      ZKUtil.listChildrenAndWatchThem(watcher, watcher.getZNodePaths().rsZNode);
    refresh(servers);
  }

  private void refresh(final List<String> servers) throws IOException {
    synchronized(this.regionServers) {
      this.regionServers.clear();
      for (String n: servers) {
        ServerName sn = ServerName.parseServerName(ZKUtil.getNodeName(n));
        if (regionServers.get(sn) == null) {
          RegionServerInfo.Builder rsInfoBuilder = RegionServerInfo.newBuilder();
          try {
            String nodePath = ZNodePaths.joinZNode(watcher.getZNodePaths().rsZNode, n);
            byte[] data = ZKUtil.getData(watcher, nodePath);
            if (data != null && data.length > 0 && ProtobufUtil.isPBMagicPrefix(data)) {
              int magicLen = ProtobufUtil.lengthOfPBMagic();
              ProtobufUtil.mergeFrom(rsInfoBuilder, data, magicLen, data.length - magicLen);
            }
            if (LOG.isTraceEnabled()) {
              LOG.trace("Added tracking of RS " + nodePath);
            }
          } catch (KeeperException e) {
            LOG.warn("Get Rs info port from ephemeral node", e);
          } catch (IOException e) {
            LOG.warn("Illegal data from ephemeral node", e);
          } catch (InterruptedException e) {
            throw new InterruptedIOException();
          }
          this.regionServers.put(sn, rsInfoBuilder.build());
        }
      }
    }
  }

  private void remove(final ServerName sn) {
    synchronized(this.regionServers) {
      this.regionServers.remove(sn);
    }
  }

  @Override
  public void nodeCreated(String path) {
    if (path.startsWith(watcher.getZNodePaths().rsZNode)) {
      String serverName = ZKUtil.getNodeName(path);
      LOG.info("RegionServer ephemeral node created, adding [" + serverName + "]");
      if (server.isInitialized()) {
        // Only call the check to move servers if a RegionServer was added to the cluster; in this
        // case it could be a server with a new version so it makes sense to run the check.
        server.checkIfShouldMoveSystemRegionAsync();
      }
    }
  }

  @Override
  public void nodeDeleted(String path) {
    if (path.startsWith(watcher.getZNodePaths().rsZNode)) {
      String serverName = ZKUtil.getNodeName(path);
      LOG.info("RegionServer ephemeral node deleted, processing expiration [" +
        serverName + "]");
      ServerName sn = ServerName.parseServerName(serverName);
      if (!serverManager.isServerOnline(sn)) {
        LOG.warn(serverName.toString() + " is not online or isn't known to the master."+
         "The latter could be caused by a DNS misconfiguration.");
        return;
      }
      remove(sn);
      this.serverManager.expireServer(sn);
    }
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (path.equals(watcher.getZNodePaths().rsZNode)
        && !server.isAborted() && !server.isStopped()) {
      try {
        List<String> servers =
          ZKUtil.listChildrenAndWatchThem(watcher, watcher.getZNodePaths().rsZNode);
        refresh(servers);
      } catch (IOException e) {
        server.abort("Unexpected zk exception getting RS nodes", e);
      } catch (KeeperException e) {
        server.abort("Unexpected zk exception getting RS nodes", e);
      }
    }
  }

  public RegionServerInfo getRegionServerInfo(final ServerName sn) {
    return regionServers.get(sn);
  }

  /**
   * Gets the online servers.
   * @return list of online servers
   */
  public List<ServerName> getOnlineServers() {
    synchronized (this.regionServers) {
      return new ArrayList<>(this.regionServers.keySet());
    }
  }
}
