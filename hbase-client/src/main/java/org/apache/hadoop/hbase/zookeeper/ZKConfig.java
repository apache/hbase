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
package org.apache.hadoop.hbase.zookeeper;

import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Utility methods for reading, and building the ZooKeeper configuration.
 *
 * The order and priority for reading the config are as follows:
 * (1). Property with "hbase.zookeeper.property." prefix from HBase XML
 * (2). other zookeeper related properties in HBASE XML
 */
@InterfaceAudience.Private
public class ZKConfig {

  private static final String VARIABLE_START = "${";

  /**
   * Make a Properties object holding ZooKeeper config.
   * Parses the corresponding config options from the HBase XML configs
   * and generates the appropriate ZooKeeper properties.
   * @param conf Configuration to read from.
   * @return Properties holding mappings representing ZooKeeper config file.
   */
  public static Properties makeZKProps(Configuration conf) {
    return makeZKPropsFromHbaseConfig(conf);
  }

  /**
   * Make a Properties object holding ZooKeeper config.
   * Parses the corresponding config options from the HBase XML configs
   * and generates the appropriate ZooKeeper properties.
   *
   * @param conf Configuration to read from.
   * @return Properties holding mappings representing ZooKeeper config file.
   */
  private static Properties makeZKPropsFromHbaseConfig(Configuration conf) {
    Properties zkProperties = new Properties();

    // Directly map all of the hbase.zookeeper.property.KEY properties.
    // Synchronize on conf so no loading of configs while we iterate
    synchronized (conf) {
      for (Entry<String, String> entry : conf) {
        String key = entry.getKey();
        if (key.startsWith(HConstants.ZK_CFG_PROPERTY_PREFIX)) {
          String zkKey = key.substring(HConstants.ZK_CFG_PROPERTY_PREFIX_LEN);
          String value = entry.getValue();
          // If the value has variables substitutions, need to do a get.
          if (value.contains(VARIABLE_START)) {
            value = conf.get(key);
          }
          zkProperties.setProperty(zkKey, value);
        }
      }
    }

    // If clientPort is not set, assign the default.
    if (zkProperties.getProperty(HConstants.CLIENT_PORT_STR) == null) {
      zkProperties.put(HConstants.CLIENT_PORT_STR,
          HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT);
    }

    // Create the server.X properties.
    int peerPort = conf.getInt("hbase.zookeeper.peerport", 2888);
    int leaderPort = conf.getInt("hbase.zookeeper.leaderport", 3888);

    final String[] serverHosts = conf.getStrings(HConstants.ZOOKEEPER_QUORUM,
                                                 HConstants.LOCALHOST);
    String serverHost;
    String address;
    String key;
    for (int i = 0; i < serverHosts.length; ++i) {
      if (serverHosts[i].contains(":")) {
        serverHost = serverHosts[i].substring(0, serverHosts[i].indexOf(':'));
      } else {
        serverHost = serverHosts[i];
      }
      address = serverHost + ":" + peerPort + ":" + leaderPort;
      key = "server." + i;
      zkProperties.put(key, address);
    }

    return zkProperties;
  }

  /**
   * Return the ZK Quorum servers string given the specified configuration
   *
   * @param conf
   * @return Quorum servers String
   */
  private static String getZKQuorumServersStringFromHbaseConfig(Configuration conf) {
    String defaultClientPort = Integer.toString(
        conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT));

    // Build the ZK quorum server string with "server:clientport" list, separated by ','
    final String[] serverHosts =
        conf.getStrings(HConstants.ZOOKEEPER_QUORUM, HConstants.LOCALHOST);
    return buildQuorumServerString(serverHosts, defaultClientPort);
  }

  /**
   * Build the ZK quorum server string with "server:clientport" list, separated by ','
   *
   * @param serverHosts a list of servers for ZK quorum
   * @param clientPort the default client port
   * @return the string for a list of "server:port" separated by ","
   */
  public static String buildQuorumServerString(String[] serverHosts, String clientPort) {
    StringBuilder quorumStringBuilder = new StringBuilder();
    String serverHost;
    for (int i = 0; i < serverHosts.length; ++i) {
      if (serverHosts[i].contains(":")) {
        serverHost = serverHosts[i]; // just use the port specified from the input
      } else {
        serverHost = serverHosts[i] + ":" + clientPort;
      }
      if (i > 0) {
        quorumStringBuilder.append(',');
      }
      quorumStringBuilder.append(serverHost);
    }
    return quorumStringBuilder.toString();
  }

  /**
   * Return the ZK Quorum servers string given the specified configuration.
   * @return Quorum servers
   */
  public static String getZKQuorumServersString(Configuration conf) {
    return getZKQuorumServersStringFromHbaseConfig(conf);
  }
}
