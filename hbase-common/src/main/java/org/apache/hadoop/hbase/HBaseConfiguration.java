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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds HBase configuration files to a Configuration
 */
@InterfaceAudience.Public
public class HBaseConfiguration extends Configuration {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseConfiguration.class);

  static {
    addDeprecatedKeys();
  }

  private static void checkDefaultsVersion(Configuration conf) {
    if (conf.getBoolean("hbase.defaults.for.version.skip", Boolean.FALSE)) return;
    String defaultsVersion = conf.get("hbase.defaults.for.version");
    String thisVersion = VersionInfo.getVersion();
    if (!thisVersion.equals(defaultsVersion)) {
      throw new RuntimeException(
        "hbase-default.xml file seems to be for an older version of HBase (" + defaultsVersion
          + "), this version is " + thisVersion);
    }
  }

  /**
   * The hbase.ipc.server.reservoir.initial.max and hbase.ipc.server.reservoir.initial.buffer.size
   * were introduced in HBase2.0.0, while in HBase3.0.0 the two config keys will be replaced by
   * hbase.server.allocator.max.buffer.count and hbase.server.allocator.buffer.size. Also the
   * hbase.ipc.server.reservoir.enabled will be replaced by hbase.server.allocator.pool.enabled.
   * Keep the three old config keys here for HBase2.x compatibility. <br>
   * HBASE-24667: This config hbase.regionserver.hostname.disable.master.reversedns will be replaced
   * by hbase.unsafe.regionserver.hostname.disable.master.reversedns. Keep the old config keys here
   * for backward compatibility. <br>
   * Note: Before Hadoop-3.3, we must call the addDeprecations method before creating the
   * Configuration object to work correctly. After this bug is fixed in hadoop-3.3, there will be no
   * order problem.
   * @see <a href="https://issues.apache.org/jira/browse/HADOOP-15708">HADOOP-15708</a>
   */
  private static void addDeprecatedKeys() {
    Configuration.addDeprecations(new DeprecationDelta[] {
      new DeprecationDelta("hbase.regionserver.hostname", "hbase.unsafe.regionserver.hostname"),
      new DeprecationDelta("hbase.regionserver.hostname.disable.master.reversedns",
        "hbase.unsafe.regionserver.hostname.disable.master.reversedns"),
      new DeprecationDelta("hbase.offheapcache.minblocksize", "hbase.blockcache.minblocksize"),
      new DeprecationDelta("hbase.ipc.server.reservoir.enabled",
        "hbase.server.allocator.pool.enabled"),
      new DeprecationDelta("hbase.ipc.server.reservoir.initial.max",
        "hbase.server.allocator.max.buffer.count"),
      new DeprecationDelta("hbase.ipc.server.reservoir.initial.buffer.size",
        "hbase.server.allocator.buffer.size"),
      new DeprecationDelta("hlog.bulk.output", "wal.bulk.output"),
      new DeprecationDelta("hlog.input.tables", "wal.input.tables"),
      new DeprecationDelta("hlog.input.tablesmap", "wal.input.tablesmap"),
      new DeprecationDelta("hbase.master.mob.ttl.cleaner.period",
        "hbase.master.mob.cleaner.period"),
      new DeprecationDelta("hbase.normalizer.min.region.count",
        "hbase.normalizer.merge.min.region.count") });
  }

  public static Configuration addHbaseResources(Configuration conf) {
    conf.addResource("hbase-default.xml");
    conf.addResource("hbase-site.xml");

    checkDefaultsVersion(conf);
    return conf;
  }

  /**
   * Creates a Configuration with HBase resources
   * @return a Configuration with HBase resources
   */
  public static Configuration create() {
    Configuration conf = new Configuration();
    // In case HBaseConfiguration is loaded from a different classloader than
    // Configuration, conf needs to be set with appropriate class loader to resolve
    // HBase resources.
    conf.setClassLoader(HBaseConfiguration.class.getClassLoader());
    return addHbaseResources(conf);
  }

  /**
   * Creates a Configuration with HBase resources
   * @param that Configuration to clone.
   * @return a Configuration created with the hbase-*.xml files plus the given configuration.
   */
  public static Configuration create(final Configuration that) {
    Configuration conf = create();
    merge(conf, that);
    return conf;
  }

  /**
   * Merge two configurations.
   * @param destConf the configuration that will be overwritten with items from the srcConf
   * @param srcConf  the source configuration
   **/
  public static void merge(Configuration destConf, Configuration srcConf) {
    for (Map.Entry<String, String> e : srcConf) {
      destConf.set(e.getKey(), e.getValue());
    }
  }

  /**
   * Returns a subset of the configuration properties, matching the given key prefix. The prefix is
   * stripped from the return keys, ie. when calling with a prefix of "myprefix", the entry
   * "myprefix.key1 = value1" would be returned as "key1 = value1". If an entry's key matches the
   * prefix exactly ("myprefix = value2"), it will <strong>not</strong> be included in the results,
   * since it would show up as an entry with an empty key.
   */
  public static Configuration subset(Configuration srcConf, String prefix) {
    Configuration newConf = new Configuration(false);
    for (Map.Entry<String, String> entry : srcConf) {
      if (entry.getKey().startsWith(prefix)) {
        String newKey = entry.getKey().substring(prefix.length());
        // avoid entries that would produce an empty key
        if (!newKey.isEmpty()) {
          newConf.set(newKey, entry.getValue());
        }
      }
    }
    return newConf;
  }

  /**
   * Sets all the entries in the provided {@code Map<String, String>} as properties in the given
   * {@code Configuration}. Each property will have the specified prefix prepended, so that the
   * configuration entries are keyed by {@code prefix + entry.getKey()}.
   */
  public static void setWithPrefix(Configuration conf, String prefix,
    Iterable<Map.Entry<String, String>> properties) {
    for (Map.Entry<String, String> entry : properties) {
      conf.set(prefix + entry.getKey(), entry.getValue());
    }
  }

  /** Returns whether to show HBase Configuration in servlet */
  public static boolean isShowConfInServlet() {
    boolean isShowConf = false;
    try {
      if (Class.forName("org.apache.hadoop.conf.ConfServlet") != null) {
        isShowConf = true;
      }
    } catch (LinkageError e) {
      // should we handle it more aggressively in addition to log the error?
      LOG.warn("Error thrown: ", e);
    } catch (ClassNotFoundException ce) {
      LOG.debug("ClassNotFound: ConfServlet");
      // ignore
    }
    return isShowConf;
  }

  /**
   * Get the password from the Configuration instance using the getPassword method if it exists. If
   * not, then fall back to the general get method for configuration elements.
   * @param conf    configuration instance for accessing the passwords
   * @param alias   the name of the password element
   * @param defPass the default password
   * @return String password or default password
   */
  public static String getPassword(Configuration conf, String alias, String defPass)
    throws IOException {
    String passwd;
    char[] p = conf.getPassword(alias);
    if (p != null) {
      LOG.debug("Config option {} was found through the Configuration getPassword method.", alias);
      passwd = new String(p);
    } else {
      LOG.debug("Config option {} was not found. Using provided default value", alias);
      passwd = defPass;
    }
    return passwd;
  }

  /**
   * Generates a {@link Configuration} instance by applying the ZooKeeper cluster key to the base
   * Configuration. Note that additional configuration properties may be needed for a remote
   * cluster, so it is preferable to use {@link #createClusterConf(Configuration, String, String)}.
   * @param baseConf   the base configuration to use, containing prefixed override properties
   * @param clusterKey the ZooKeeper quorum cluster key to apply, or {@code null} if none
   * @return the merged configuration with override properties and cluster key applied
   * @see #createClusterConf(Configuration, String, String)
   */
  public static Configuration createClusterConf(Configuration baseConf, String clusterKey)
    throws IOException {
    return createClusterConf(baseConf, clusterKey, null);
  }

  /**
   * Generates a {@link Configuration} instance by applying property overrides prefixed by a cluster
   * profile key to the base Configuration. Override properties are extracted by the
   * {@link #subset(Configuration, String)} method, then the merged on top of the base Configuration
   * and returned.
   * @param baseConf       the base configuration to use, containing prefixed override properties
   * @param clusterKey     the ZooKeeper quorum cluster key to apply, or {@code null} if none
   * @param overridePrefix the property key prefix to match for override properties, or {@code null}
   *                       if none
   * @return the merged configuration with override properties and cluster key applied
   */
  public static Configuration createClusterConf(Configuration baseConf, String clusterKey,
    String overridePrefix) throws IOException {
    Configuration clusterConf = HBaseConfiguration.create(baseConf);
    if (!StringUtils.isBlank(clusterKey)) {
      applyClusterKeyToConf(clusterConf, clusterKey);
    }

    if (!StringUtils.isBlank(overridePrefix)) {
      Configuration clusterSubset = HBaseConfiguration.subset(clusterConf, overridePrefix);
      HBaseConfiguration.merge(clusterConf, clusterSubset);
    }
    return clusterConf;
  }

  /**
   * Apply the settings in the given key to the given configuration, this is used to communicate
   * with distant clusters
   * @param conf configuration object to configure
   * @param key  string that contains the 3 required configuratins
   */
  private static void applyClusterKeyToConf(Configuration conf, String key) throws IOException {
    ZKConfig.ZKClusterKey zkClusterKey = ZKConfig.transformClusterKey(key);
    conf.set(HConstants.ZOOKEEPER_QUORUM, zkClusterKey.getQuorumString());
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkClusterKey.getClientPort());
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zkClusterKey.getZnodeParent());
    // Without the right registry, the above configs are useless. Also, we don't use setClass()
    // here because the ConnectionRegistry* classes are not resolvable from this module.
    // This will be broken if ZkConnectionRegistry class gets renamed or moved. Is there a better
    // way?
    LOG.info("Overriding client registry implementation to {}",
      HConstants.ZK_CONNECTION_REGISTRY_CLASS);
    conf.set(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
      HConstants.ZK_CONNECTION_REGISTRY_CLASS);
  }

  /**
   * For debugging. Dump configurations to system output as xml format. Master and RS configurations
   * can also be dumped using http services. e.g. "curl http://master:16010/dump"
   */
  public static void main(String[] args) throws Exception {
    HBaseConfiguration.create().writeXml(System.out);
  }
}
