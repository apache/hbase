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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Utility methods for reading, and building the ZooKeeper configuration.
 *
 * The order and priority for reading the config are as follows:
 * (1). zoo.cfg if ""hbase.config.read.zookeeper.config" is true
 * (2). Property with "hbase.zookeeper.property." prefix from HBase XML
 * (3). other zookeeper related properties in HBASE XML
 */
@InterfaceAudience.Private
public final class ZKConfig {
  private static final Log LOG = LogFactory.getLog(ZKConfig.class);

  private static final String VARIABLE_START = "${";
  private static final int VARIABLE_START_LENGTH = VARIABLE_START.length();
  private static final String VARIABLE_END = "}";
  private static final int VARIABLE_END_LENGTH = VARIABLE_END.length();

  private ZKConfig() {
  }

  /**
   * Make a Properties object holding ZooKeeper config.
   * Parses the corresponding config options from the HBase XML configs
   * and generates the appropriate ZooKeeper properties.
   * @param conf Configuration to read from.
   * @return Properties holding mappings representing ZooKeeper config file.
   */
  public static Properties makeZKProps(Configuration conf) {
    Properties zkProperties = makeZKPropsFromZooCfg(conf);

    if (zkProperties == null) {
      // Otherwise, use the configuration options from HBase's XML files.
      zkProperties = makeZKPropsFromHbaseConfig(conf);
    }
    return zkProperties;
  }

  /**
   * Parses the corresponding config options from the zoo.cfg file
   * and make a Properties object holding the Zookeeper config.
   *
   * @param conf Configuration to read from.
   * @return Properties holding mappings representing the ZooKeeper config file or null if
   * the HBASE_CONFIG_READ_ZOOKEEPER_CONFIG is false or the file does not exist.
   */
  private static Properties makeZKPropsFromZooCfg(Configuration conf) {
    if (conf.getBoolean(HConstants.HBASE_CONFIG_READ_ZOOKEEPER_CONFIG, false)) {
      LOG.warn(
          "Parsing ZooKeeper's " + HConstants.ZOOKEEPER_CONFIG_NAME +
          " file for ZK properties " +
          "has been deprecated. Please instead place all ZK related HBase " +
          "configuration under the hbase-site.xml, using prefixes " +
          "of the form '" + HConstants.ZK_CFG_PROPERTY_PREFIX + "', and " +
          "set property '" + HConstants.HBASE_CONFIG_READ_ZOOKEEPER_CONFIG +
          "' to false");
      // First check if there is a zoo.cfg in the CLASSPATH. If so, simply read
      // it and grab its configuration properties.
      ClassLoader cl = ZKConfig.class.getClassLoader();
      final InputStream inputStream =
        cl.getResourceAsStream(HConstants.ZOOKEEPER_CONFIG_NAME);
      if (inputStream != null) {
        try {
          return parseZooCfg(conf, inputStream);
        } catch (IOException e) {
          LOG.warn("Cannot read " + HConstants.ZOOKEEPER_CONFIG_NAME +
                   ", loading from XML files", e);
        }
      }
    } else {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Skipped reading ZK properties file '" + HConstants.ZOOKEEPER_CONFIG_NAME +
          "' since '" + HConstants.HBASE_CONFIG_READ_ZOOKEEPER_CONFIG + "' was not set to true");
      }
    }

    return null;
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
          zkProperties.put(zkKey, value);
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
   * Parse ZooKeeper's zoo.cfg, injecting HBase Configuration variables in.
   * This method is used for testing so we can pass our own InputStream.
   * @param conf HBaseConfiguration to use for injecting variables.
   * @param inputStream InputStream to read from.
   * @return Properties parsed from config stream with variables substituted.
   * @throws IOException if anything goes wrong parsing config
   * @deprecated in 0.96 onwards. HBase will no longer rely on zoo.cfg
   * availability.
   */
  @Deprecated
  public static Properties parseZooCfg(Configuration conf,
      InputStream inputStream) throws IOException {
    Properties properties = new Properties();
    try {
      properties.load(inputStream);
    } catch (IOException e) {
      final String msg = "fail to read properties from "
        + HConstants.ZOOKEEPER_CONFIG_NAME;
      LOG.fatal(msg);
      throw new IOException(msg, e);
    }
    for (Entry<Object, Object> entry : properties.entrySet()) {
      String value = entry.getValue().toString().trim();
      String key = entry.getKey().toString().trim();
      StringBuilder newValue = new StringBuilder();
      int varStart = value.indexOf(VARIABLE_START);
      int varEnd = 0;
      while (varStart != -1) {
        varEnd = value.indexOf(VARIABLE_END, varStart);
        if (varEnd == -1) {
          String msg = "variable at " + varStart + " has no end marker";
          LOG.fatal(msg);
          throw new IOException(msg);
        }
        String variable = value.substring(varStart + VARIABLE_START_LENGTH, varEnd);

        String substituteValue = System.getProperty(variable);
        if (substituteValue == null) {
          substituteValue = conf.get(variable);
        }
        if (substituteValue == null) {
          String msg = "variable " + variable + " not set in system property "
                     + "or hbase configs";
          LOG.fatal(msg);
          throw new IOException(msg);
        }

        newValue.append(substituteValue);

        varEnd += VARIABLE_END_LENGTH;
        varStart = value.indexOf(VARIABLE_START, varEnd);
      }
      // Special case for 'hbase.cluster.distributed' property being 'true'
      if (key.startsWith("server.")) {
        boolean mode =
            conf.getBoolean(HConstants.CLUSTER_DISTRIBUTED, HConstants.DEFAULT_CLUSTER_DISTRIBUTED);
        if (mode == HConstants.CLUSTER_IS_DISTRIBUTED && value.startsWith(HConstants.LOCALHOST)) {
          String msg = "The server in zoo.cfg cannot be set to localhost " +
              "in a fully-distributed setup because it won't be reachable. " +
              "See \"Getting Started\" for more information.";
          LOG.fatal(msg);
          throw new IOException(msg);
        }
      }
      newValue.append(value.substring(varEnd));
      properties.setProperty(key, newValue.toString());
    }
    return properties;
  }

  /**
   * Return the ZK Quorum servers string given zk properties returned by
   * makeZKProps
   * @param properties
   * @return Quorum servers String
   */
  private static String getZKQuorumServersString(Properties properties) {
    String clientPort = null;
    List<String> servers = new ArrayList<String>();

    // The clientPort option may come after the server.X hosts, so we need to
    // grab everything and then create the final host:port comma separated list.
    boolean anyValid = false;
    for (Entry<Object,Object> property : properties.entrySet()) {
      String key = property.getKey().toString().trim();
      String value = property.getValue().toString().trim();
      if (key.equals("clientPort")) {
        clientPort = value;
      }
      else if (key.startsWith("server.")) {
        String host = value.substring(0, value.indexOf(':'));
        servers.add(host);
        anyValid = true;
      }
    }

    if (!anyValid) {
      LOG.error("no valid quorum servers found in " + HConstants.ZOOKEEPER_CONFIG_NAME);
      return null;
    }

    if (clientPort == null) {
      LOG.error("no clientPort found in " + HConstants.ZOOKEEPER_CONFIG_NAME);
      return null;
    }

    if (servers.isEmpty()) {
      LOG.fatal("No servers were found in provided ZooKeeper configuration. " +
          "HBase must have a ZooKeeper cluster configured for its " +
          "operation. Ensure that you've configured '" +
          HConstants.ZOOKEEPER_QUORUM + "' properly.");
      return null;
    }

    StringBuilder hostPortBuilder = new StringBuilder();
    for (int i = 0; i < servers.size(); ++i) {
      String host = servers.get(i);
      if (i > 0) {
        hostPortBuilder.append(',');
      }
      hostPortBuilder.append(host);
      hostPortBuilder.append(':');
      hostPortBuilder.append(clientPort);
    }

    return hostPortBuilder.toString();
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
    return buildZKQuorumServerString(serverHosts, defaultClientPort);
  }

  /**
   * Return the ZK Quorum servers string given the specified configuration.
   * @param conf
   * @return Quorum servers
   */
  public static String getZKQuorumServersString(Configuration conf) {
    // First try zoo.cfg; if not applicable, then try config XML.
    Properties zkProperties = makeZKPropsFromZooCfg(conf);

    if (zkProperties != null) {
      return getZKQuorumServersString(zkProperties);
    }

    return getZKQuorumServersStringFromHbaseConfig(conf);
  }

  /**
   * Build the ZK quorum server string with "server:clientport" list, separated by ','
   *
   * @param serverHosts a list of servers for ZK quorum
   * @param clientPort the default client port
   * @return the string for a list of "server:port" separated by ","
   */
  public static String buildZKQuorumServerString(String[] serverHosts, String clientPort) {
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
   * Verifies that the given key matches the expected format for a ZooKeeper cluster key.
   * The Quorum for the ZK cluster can have one the following formats (see examples below):
   *
   * <ol>
   *   <li>s1,s2,s3 (no client port in the list, the client port could be obtained from
   *       clientPort)</li>
   *   <li>s1:p1,s2:p2,s3:p3 (with client port, which could be same or different for each server,
   *       in this case, the clientPort would be ignored)</li>
   *   <li>s1:p1,s2,s3:p3 (mix of (1) and (2) - if port is not specified in a server, it would use
   *       the clientPort; otherwise, it would use the specified port)</li>
   * </ol>
   *
   * @param key the cluster key to validate
   * @throws IOException if the key could not be parsed
   */
  public static void validateClusterKey(String key) throws IOException {
    transformClusterKey(key);
  }

  /**
   * Separate the given key into the three configurations it should contain:
   * hbase.zookeeper.quorum, hbase.zookeeper.client.port
   * and zookeeper.znode.parent
   * @param key
   * @return the three configuration in the described order
   * @throws IOException
   */
  public static ZKClusterKey transformClusterKey(String key) throws IOException {
    String[] parts = key.split(":");

    if (parts.length == 3) {
      return new ZKClusterKey(parts [0], Integer.parseInt(parts [1]), parts [2]);
    }

    if (parts.length > 3) {
      // The quorum could contain client port in server:clientport format, try to transform more.
      String zNodeParent = parts [parts.length - 1];
      String clientPort = parts [parts.length - 2];

      // The first part length is the total length minus the lengths of other parts and minus 2 ":"
      int endQuorumIndex = key.length() - zNodeParent.length() - clientPort.length() - 2;
      String quorumStringInput = key.substring(0, endQuorumIndex);
      String[] serverHosts = quorumStringInput.split(",");

      // The common case is that every server has its own client port specified - this means
      // that (total parts - the ZNodeParent part - the ClientPort part) is equal to
      // (the number of "," + 1) - "+ 1" because the last server has no ",".
      if ((parts.length - 2) == (serverHosts.length + 1)) {
        return new ZKClusterKey(quorumStringInput, Integer.parseInt(clientPort), zNodeParent);
      }

      // For the uncommon case that some servers has no port specified, we need to build the
      // server:clientport list using default client port for servers without specified port.
      return new ZKClusterKey(
          buildZKQuorumServerString(serverHosts, clientPort),
          Integer.parseInt(clientPort),
          zNodeParent);
    }

    throw new IOException("Cluster key passed " + key + " is invalid, the format should be:" +
        HConstants.ZOOKEEPER_QUORUM + ":" + HConstants.ZOOKEEPER_CLIENT_PORT + ":"
        + HConstants.ZOOKEEPER_ZNODE_PARENT);
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
    String ensemble = conf.get(HConstants.ZOOKEEPER_QUORUM).replaceAll(
        "[\\t\\n\\x0B\\f\\r]", "");
    StringBuilder builder = new StringBuilder(ensemble);
    builder.append(":");
    builder.append(conf.get(HConstants.ZOOKEEPER_CLIENT_PORT));
    builder.append(":");
    builder.append(conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
    if (name != null && !name.isEmpty()) {
      builder.append(",");
      builder.append(name);
    }
    return builder.toString();
  }

  /**
   * Standardize the ZK quorum string: make it a "server:clientport" list, separated by ','
   * @param quorumStringInput a string contains a list of servers for ZK quorum
   * @param clientPort the default client port
   * @return the string for a list of "server:port" separated by ","
   */
  @VisibleForTesting
  public static String standardizeZKQuorumServerString(String quorumStringInput,
      String clientPort) {
    String[] serverHosts = quorumStringInput.split(",");
    return buildZKQuorumServerString(serverHosts, clientPort);
  }

  // The Quorum for the ZK cluster can have one the following format (see examples below):
  // (1). s1,s2,s3 (no client port in the list, the client port could be obtained from clientPort)
  // (2). s1:p1,s2:p2,s3:p3 (with client port, which could be same or different for each server,
  //      in this case, the clientPort would be ignored)
  // (3). s1:p1,s2,s3:p3 (mix of (1) and (2) - if port is not specified in a server, it would use
  //      the clientPort; otherwise, it would use the specified port)
  @VisibleForTesting
  public static class ZKClusterKey {
    private String quorumString;
    private int clientPort;
    private String znodeParent;

    ZKClusterKey(String quorumString, int clientPort, String znodeParent) {
      this.quorumString = quorumString;
      this.clientPort = clientPort;
      this.znodeParent = znodeParent;
    }

    public String getQuorumString() {
      return quorumString;
    }

    public int getClientPort() {
      return clientPort;
    }

    public String getZnodeParent() {
      return znodeParent;
    }
  }
}
