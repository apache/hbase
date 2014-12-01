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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;

/**
 * Utility methods for reading, and building the ZooKeeper configuration.
 */
@InterfaceAudience.Private
public class ZKConfig {
  private static final Log LOG = LogFactory.getLog(ZKConfig.class);

  private static final String VARIABLE_START = "${";
  private static final int VARIABLE_START_LENGTH = VARIABLE_START.length();
  private static final String VARIABLE_END = "}";
  private static final int VARIABLE_END_LENGTH = VARIABLE_END.length();

  /**
   * Make a Properties object holding ZooKeeper config.
   * Parses the corresponding config options from the HBase XML configs
   * and generates the appropriate ZooKeeper properties.
   * @param conf Configuration to read from.
   * @return Properties holding mappings representing ZooKeeper config file.
   */
  public static Properties makeZKProps(Configuration conf) {
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
      ClassLoader cl = HQuorumPeer.class.getClassLoader();
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

    // Otherwise, use the configuration options from HBase's XML files.
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
    for (int i = 0; i < serverHosts.length; ++i) {
      String serverHost = serverHosts[i];
      String address = serverHost + ":" + peerPort + ":" + leaderPort;
      String key = "server." + i;
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
        boolean mode = conf.getBoolean(HConstants.CLUSTER_DISTRIBUTED, HConstants.DEFAULT_CLUSTER_DISTRIBUTED);
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
  public static String getZKQuorumServersString(Properties properties) {
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
   * Return the ZK Quorum servers string given the specified configuration.
   * @param conf
   * @return Quorum servers
   */
  public static String getZKQuorumServersString(Configuration conf) {
    return getZKQuorumServersString(makeZKProps(conf));
  }
}
