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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos;

/**
 * Builds a string containing everything in ZooKeeper. This is inherently invasive into the
 * structures of other components' logical responsibilities.
 */
@InterfaceAudience.Private
public final class ZKDump {
  private static final Logger LOG = LoggerFactory.getLogger(ZKDump.class);

  private ZKDump() {}

  public static String dump(final ZKWatcher zkWatcher) {
    final int zkDumpConnectionTimeOut = zkWatcher.getConfiguration()
      .getInt("zookeeper.dump.connection.timeout", 1000);
    StringBuilder sb = new StringBuilder();
    try {
      sb.append("HBase is rooted at ").append(zkWatcher.getZNodePaths().baseZNode);
      sb.append("\nActive master address: ");
      try {
        sb.append("\n ").append(MasterAddressTracker.getMasterAddress(zkWatcher));
      } catch (IOException e) {
        sb.append("<<FAILED LOOKUP: ").append(e.getMessage()).append(">>");
      }
      sb.append("\nBackup master addresses:");
      final List<String> backupMasterChildrenNoWatchList = ZKUtil.listChildrenNoWatch(zkWatcher,
        zkWatcher.getZNodePaths().backupMasterAddressesZNode);
      if (backupMasterChildrenNoWatchList != null) {
        for (String child : backupMasterChildrenNoWatchList) {
          sb.append("\n ").append(child);
        }
      }
      sb.append("\nRegion server holding hbase:meta:");
      sb.append("\n ").append(MetaTableLocator.getMetaRegionLocation(zkWatcher));
      int numMetaReplicas = zkWatcher.getMetaReplicaNodes().size();
      for (int i = 1; i < numMetaReplicas; i++) {
        sb.append("\n")
          .append(" replica").append(i).append(": ")
          .append(MetaTableLocator.getMetaRegionLocation(zkWatcher, i));
      }
      sb.append("\nRegion servers:");
      final List<String> rsChildrenNoWatchList =
        ZKUtil.listChildrenNoWatch(zkWatcher, zkWatcher.getZNodePaths().rsZNode);
      if (rsChildrenNoWatchList != null) {
        for (String child : rsChildrenNoWatchList) {
          sb.append("\n ").append(child);
        }
      }
      try {
        getReplicationZnodesDump(zkWatcher, sb);
      } catch (KeeperException ke) {
        LOG.warn("Couldn't get the replication znode dump", ke);
      }
      sb.append("\nQuorum Server Statistics:");
      String[] servers = zkWatcher.getQuorum().split(",");
      for (String server : servers) {
        sb.append("\n ").append(server);
        try {
          String[] stat = getServerStats(server, zkDumpConnectionTimeOut);

          if (stat == null) {
            sb.append("[Error] invalid quorum server: ").append(server);
            break;
          }

          for (String s : stat) {
            sb.append("\n  ").append(s);
          }
        } catch (Exception e) {
          sb.append("\n  ERROR: ").append(e.getMessage());
        }
      }
    } catch (KeeperException ke) {
      sb.append("\nFATAL ZooKeeper Exception!\n");
      sb.append("\n").append(ke.getMessage());
    }
    return sb.toString();
  }

  /**
   * Appends replication znodes to the passed StringBuilder.
   *
   * @param zkw reference to the {@link ZKWatcher} which also contains configuration and operation
   * @param sb the {@link StringBuilder} to append to
   * @throws KeeperException if a ZooKeeper operation fails
   */
  private static void getReplicationZnodesDump(ZKWatcher zkw, StringBuilder sb)
    throws KeeperException {
    String replicationZnode = zkw.getZNodePaths().replicationZNode;

    if (ZKUtil.checkExists(zkw, replicationZnode) == -1) {
      return;
    }

    // do a ls -r on this znode
    sb.append("\n").append(replicationZnode).append(": ");
    List<String> children = ZKUtil.listChildrenNoWatch(zkw, replicationZnode);
    if (children != null) {
      Collections.sort(children);
      for (String child : children) {
        String zNode = ZNodePaths.joinZNode(replicationZnode, child);
        if (zNode.equals(zkw.getZNodePaths().peersZNode)) {
          appendPeersZnodes(zkw, zNode, sb);
        } else if (zNode.equals(zkw.getZNodePaths().queuesZNode)) {
          appendRSZnodes(zkw, zNode, sb);
        } else if (zNode.equals(zkw.getZNodePaths().hfileRefsZNode)) {
          appendHFileRefsZNodes(zkw, zNode, sb);
        }
      }
    }
  }

  private static void appendHFileRefsZNodes(ZKWatcher zkw, String hFileRefsZNode,
    StringBuilder sb) throws KeeperException {
    sb.append("\n").append(hFileRefsZNode).append(": ");
    final List<String> hFileRefChildrenNoWatchList =
      ZKUtil.listChildrenNoWatch(zkw, hFileRefsZNode);
    if (hFileRefChildrenNoWatchList != null) {
      for (String peerIdZNode : hFileRefChildrenNoWatchList) {
        String zNodeToProcess = ZNodePaths.joinZNode(hFileRefsZNode, peerIdZNode);
        sb.append("\n").append(zNodeToProcess).append(": ");
        List<String> peerHFileRefsZNodes = ZKUtil.listChildrenNoWatch(zkw, zNodeToProcess);
        if (peerHFileRefsZNodes != null) {
          sb.append(String.join(", ", peerHFileRefsZNodes));
        }
      }
    }
  }

  /**
   * Returns a string with replication znodes and position of the replication log
   * @param zkw reference to the {@link ZKWatcher} which also contains configuration and operation
   * @return aq string of replication znodes and log positions
   */
  public static String getReplicationZnodesDump(ZKWatcher zkw) throws KeeperException {
    StringBuilder sb = new StringBuilder();
    getReplicationZnodesDump(zkw, sb);
    return sb.toString();
  }

  private static void appendRSZnodes(ZKWatcher zkw, String znode, StringBuilder sb)
    throws KeeperException {
    List<String> stack = new LinkedList<>();
    stack.add(znode);
    do {
      String znodeToProcess = stack.remove(stack.size() - 1);
      sb.append("\n").append(znodeToProcess).append(": ");
      byte[] data;
      try {
        data = ZKUtil.getData(zkw, znodeToProcess);
      } catch (InterruptedException e) {
        zkw.interruptedException(e);
        return;
      }
      if (data != null && data.length > 0) { // log position
        long position = 0;
        try {
          position = ZKUtil.parseWALPositionFrom(ZKUtil.getData(zkw, znodeToProcess));
          sb.append(position);
        } catch (DeserializationException ignored) {
        } catch (InterruptedException e) {
          zkw.interruptedException(e);
          return;
        }
      }
      for (String zNodeChild : ZKUtil.listChildrenNoWatch(zkw, znodeToProcess)) {
        stack.add(ZNodePaths.joinZNode(znodeToProcess, zNodeChild));
      }
    } while (stack.size() > 0);
  }

  private static void appendPeersZnodes(ZKWatcher zkw, String peersZnode,
    StringBuilder sb) throws KeeperException {
    int pblen = ProtobufUtil.lengthOfPBMagic();
    sb.append("\n").append(peersZnode).append(": ");
    for (String peerIdZnode : ZKUtil.listChildrenNoWatch(zkw, peersZnode)) {
      String znodeToProcess = ZNodePaths.joinZNode(peersZnode, peerIdZnode);
      byte[] data;
      try {
        data = ZKUtil.getData(zkw, znodeToProcess);
      } catch (InterruptedException e) {
        zkw.interruptedException(e);
        return;
      }
      // parse the data of the above peer znode.
      try {
        ReplicationProtos.ReplicationPeer.Builder builder =
          ReplicationProtos.ReplicationPeer.newBuilder();
        ProtobufUtil.mergeFrom(builder, data, pblen, data.length - pblen);
        String clusterKey = builder.getClusterkey();
        sb.append("\n").append(znodeToProcess).append(": ").append(clusterKey);
        // add the peer-state.
        appendPeerState(zkw, znodeToProcess, sb);
      } catch (IOException ipbe) {
        LOG.warn("Got Exception while parsing peer: " + znodeToProcess, ipbe);
      }
    }
  }

  private static void appendPeerState(ZKWatcher zkw, String znodeToProcess, StringBuilder sb)
    throws KeeperException, InvalidProtocolBufferException {
    String peerState = zkw.getConfiguration().get("zookeeper.znode.replication.peers.state",
      "peer-state");
    int pblen = ProtobufUtil.lengthOfPBMagic();
    for (String child : ZKUtil.listChildrenNoWatch(zkw, znodeToProcess)) {
      if (!child.equals(peerState)) {
        continue;
      }

      String peerStateZnode = ZNodePaths.joinZNode(znodeToProcess, child);
      sb.append("\n").append(peerStateZnode).append(": ");
      byte[] peerStateData;
      try {
        peerStateData = ZKUtil.getData(zkw, peerStateZnode);
        ReplicationProtos.ReplicationState.Builder builder =
          ReplicationProtos.ReplicationState.newBuilder();
        ProtobufUtil.mergeFrom(builder, peerStateData, pblen, peerStateData.length - pblen);
        sb.append(builder.getState().name());
      } catch (IOException ipbe) {
        LOG.warn("Got Exception while parsing peer: " + znodeToProcess, ipbe);
      } catch (InterruptedException e) {
        zkw.interruptedException(e);
        return;
      }
    }
  }

  /**
   * Gets the statistics from the given server.
   *
   * @param server  The server to get the statistics from.
   * @param timeout  The socket timeout to use.
   * @return The array of response strings.
   * @throws IOException When the socket communication fails.
   */
  private static String[] getServerStats(String server, int timeout)
    throws IOException {
    String[] sp = server.split(":");
    if (sp.length == 0) {
      return null;
    }

    String host = sp[0];
    int port = sp.length > 1 ? Integer.parseInt(sp[1])
      : HConstants.DEFAULT_ZOOKEEPER_CLIENT_PORT;

    try (Socket socket = new Socket()) {
      InetSocketAddress sockAddr = new InetSocketAddress(host, port);
      if (sockAddr.isUnresolved()) {
        throw new UnknownHostException(host + " cannot be resolved");
      }
      socket.connect(sockAddr, timeout);
      socket.setSoTimeout(timeout);
      try (PrintWriter out = new PrintWriter(new BufferedWriter(
        new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8)), true);
        BufferedReader in = new BufferedReader(
          new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
        out.println("stat");
        out.flush();
        ArrayList<String> res = new ArrayList<>();
        while (true) {
          String line = in.readLine();
          if (line != null) {
            res.add(line);
          } else {
            break;
          }
        }
        return res.toArray(new String[res.size()]);
      }
    }
  }
}
