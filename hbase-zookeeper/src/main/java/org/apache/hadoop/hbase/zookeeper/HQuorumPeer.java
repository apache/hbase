/*
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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HBase's version of ZooKeeper's QuorumPeer. When HBase is set to manage
 * ZooKeeper, this class is used to start up QuorumPeer instances. By doing
 * things in here rather than directly calling to ZooKeeper, we have more
 * control over the process. This class uses {@link ZKConfig} to get settings
 * from the hbase-site.xml file.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@InterfaceStability.Evolving
public final class HQuorumPeer {
  private static final Logger LOG = LoggerFactory.getLogger(HQuorumPeer.class);

  private HQuorumPeer() {
  }

  /**
   * Parse ZooKeeper configuration from HBase XML config and run a QuorumPeer.
   * @param args String[] of command line arguments. Not used.
   */
  public static void main(String[] args) {
    Configuration conf = HBaseConfiguration.create();
    try {
      Properties zkProperties = ZKConfig.makeZKProps(conf);
      writeMyID(zkProperties);
      QuorumPeerConfig zkConfig = new QuorumPeerConfig();
      zkConfig.parseProperties(zkProperties);

      // login the zookeeper server principal (if using security)
      ZKUtil.loginServer(conf, HConstants.ZK_SERVER_KEYTAB_FILE,
        HConstants.ZK_SERVER_KERBEROS_PRINCIPAL,
        zkConfig.getClientPortAddress().getHostName());

      runZKServer(zkConfig);
    } catch (Exception e) {
      LOG.error("Failed to start ZKServer", e);
      System.exit(-1);
    }
  }

  private static void runZKServer(QuorumPeerConfig zkConfig)
          throws UnknownHostException, IOException {
    if (zkConfig.isDistributed()) {
      QuorumPeerMain qp = new QuorumPeerMain();
      qp.runFromConfig(zkConfig);
    } else {
      ZooKeeperServerMain zk = new ZooKeeperServerMain();
      ServerConfig serverConfig = new ServerConfig();
      serverConfig.readFrom(zkConfig);
      zk.runFromConfig(serverConfig);
    }
  }

  private static boolean addressIsLocalHost(String address) {
    return address.equals("localhost") || address.equals("127.0.0.1");
  }

  static void writeMyID(Properties properties) throws IOException {
    long myId = -1;

    Configuration conf = HBaseConfiguration.create();
    String myAddress = Strings.domainNamePointerToHostName(DNS.getDefaultHost(
        conf.get("hbase.zookeeper.dns.interface","default"),
        conf.get("hbase.zookeeper.dns.nameserver","default")));

    List<String> ips = new ArrayList<>();

    // Add what could be the best (configured) match
    ips.add(myAddress.contains(".") ?
        myAddress :
        StringUtils.simpleHostname(myAddress));

    // For all nics get all hostnames and IPs
    Enumeration<?> nics = NetworkInterface.getNetworkInterfaces();
    while(nics.hasMoreElements()) {
      Enumeration<?> rawAdrs =
          ((NetworkInterface)nics.nextElement()).getInetAddresses();
      while(rawAdrs.hasMoreElements()) {
        InetAddress inet = (InetAddress) rawAdrs.nextElement();
        ips.add(StringUtils.simpleHostname(inet.getHostName()));
        ips.add(inet.getHostAddress());
      }
    }

    for (Entry<Object, Object> entry : properties.entrySet()) {
      String key = entry.getKey().toString().trim();
      String value = entry.getValue().toString().trim();
      if (key.startsWith("server.")) {
        int dot = key.indexOf('.');
        long id = Long.parseLong(key.substring(dot + 1));
        String[] parts = value.split(":");
        String address = parts[0];
        if (addressIsLocalHost(address) || ips.contains(address)) {
          myId = id;
          break;
        }
      }
    }

    // Set the max session timeout from the provided client-side timeout
    properties.setProperty("maxSessionTimeout", conf.get(HConstants.ZK_SESSION_TIMEOUT,
            Integer.toString(HConstants.DEFAULT_ZK_SESSION_TIMEOUT)));

    if (myId == -1) {
      throw new IOException("Could not find my address: " + myAddress +
                            " in list of ZooKeeper quorum servers");
    }

    String dataDirStr = properties.get("dataDir").toString().trim();
    File dataDir = new File(dataDirStr);
    if (!dataDir.isDirectory()) {
      if (!dataDir.mkdirs()) {
        throw new IOException("Unable to create data dir " + dataDir);
      }
    }

    File myIdFile = new File(dataDir, "myid");
    PrintWriter w = new PrintWriter(myIdFile, StandardCharsets.UTF_8.name());
    w.println(myId);
    w.close();
  }
}
