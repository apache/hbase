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
package org.apache.hadoop.hbase.wal;

import static org.apache.hadoop.hbase.wal.AbstractFSWALProvider.META_WAL_PROVIDER_ID;
import static org.apache.hadoop.hbase.wal.AbstractFSWALProvider.WAL_FILE_NAME_DELIMITER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.wal.MetricsWAL;
// imports for classes still in regionserver.wal
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.KeyLocker;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A WAL Provider that returns a WAL per group of regions.
 *
 * This provider follows the decorator pattern and mainly holds the logic for WAL grouping.
 * WAL creation/roll/close is delegated to {@link #DELEGATE_PROVIDER}
 *
 * Region grouping is handled via {@link RegionGroupingStrategy} and can be configured via the
 * property "hbase.wal.regiongrouping.strategy". Current strategy choices are
 * <ul>
 *   <li><em>defaultStrategy</em> : Whatever strategy this version of HBase picks. currently
 *                                  "bounded".</li>
 *   <li><em>identity</em> : each region belongs to its own group.</li>
 *   <li><em>bounded</em> : bounded number of groups and region evenly assigned to each group.</li>
 * </ul>
 * Optionally, a FQCN to a custom implementation may be given.
 */
@InterfaceAudience.Private
public class RegionGroupingProvider implements WALProvider {
  private static final Logger LOG = LoggerFactory.getLogger(RegionGroupingProvider.class);

  /**
   * Map identifiers to a group number.
   */
  public static interface RegionGroupingStrategy {
    String GROUP_NAME_DELIMITER = ".";

    /**
     * Given an identifier and a namespace, pick a group.
     */
    String group(final byte[] identifier, byte[] namespace);
    void init(Configuration config, String providerId);
  }

  /**
   * Maps between configuration names for strategies and implementation classes.
   */
  static enum Strategies {
    defaultStrategy(BoundedGroupingStrategy.class),
    identity(IdentityGroupingStrategy.class),
    bounded(BoundedGroupingStrategy.class),
    namespace(NamespaceGroupingStrategy.class);

    final Class<? extends RegionGroupingStrategy> clazz;
    Strategies(Class<? extends RegionGroupingStrategy> clazz) {
      this.clazz = clazz;
    }
  }

  /**
   * instantiate a strategy from a config property.
   * requires conf to have already been set (as well as anything the provider might need to read).
   */
  RegionGroupingStrategy getStrategy(final Configuration conf, final String key,
      final String defaultValue) throws IOException {
    Class<? extends RegionGroupingStrategy> clazz;
    try {
      clazz = Strategies.valueOf(conf.get(key, defaultValue)).clazz;
    } catch (IllegalArgumentException exception) {
      // Fall back to them specifying a class name
      // Note that the passed default class shouldn't actually be used, since the above only fails
      // when there is a config value present.
      clazz = conf.getClass(key, IdentityGroupingStrategy.class, RegionGroupingStrategy.class);
    }
    LOG.info("Instantiating RegionGroupingStrategy of type " + clazz);
    try {
      final RegionGroupingStrategy result = clazz.getDeclaredConstructor().newInstance();
      result.init(conf, providerId);
      return result;
    } catch (Exception e) {
      LOG.error("couldn't set up region grouping strategy, check config key " +
          REGION_GROUPING_STRATEGY);
      LOG.debug("Exception details for failure to load region grouping strategy.", e);
      throw new IOException("couldn't set up region grouping strategy", e);
    }
  }

  public static final String REGION_GROUPING_STRATEGY = "hbase.wal.regiongrouping.strategy";
  public static final String DEFAULT_REGION_GROUPING_STRATEGY = Strategies.defaultStrategy.name();

  /** delegate provider for WAL creation/roll/close, but not support multiwal */
  public static final String DELEGATE_PROVIDER = "hbase.wal.regiongrouping.delegate.provider";
  public static final String DEFAULT_DELEGATE_PROVIDER = WALFactory.Providers.defaultProvider
      .name();

  private static final String META_WAL_GROUP_NAME = "meta";

  /** A group-provider mapping, make sure one-one rather than many-one mapping */
  private final ConcurrentMap<String, WALProvider> cached = new ConcurrentHashMap<>();

  private final KeyLocker<String> createLock = new KeyLocker<>();

  private RegionGroupingStrategy strategy;
  private WALFactory factory;
  private Configuration conf;
  private List<WALActionsListener> listeners = new ArrayList<>();
  private String providerId;
  private Class<? extends WALProvider> providerClass;

  @Override
  public void init(WALFactory factory, Configuration conf, String providerId) throws IOException {
    if (null != strategy) {
      throw new IllegalStateException("WALProvider.init should only be called once.");
    }
    this.conf = conf;
    this.factory = factory;

    if (META_WAL_PROVIDER_ID.equals(providerId)) {
      // do not change the provider id if it is for meta
      this.providerId = providerId;
    } else {
      StringBuilder sb = new StringBuilder().append(factory.factoryId);
      if (providerId != null) {
        if (providerId.startsWith(WAL_FILE_NAME_DELIMITER)) {
          sb.append(providerId);
        } else {
          sb.append(WAL_FILE_NAME_DELIMITER).append(providerId);
        }
      }
      this.providerId = sb.toString();
    }
    this.strategy = getStrategy(conf, REGION_GROUPING_STRATEGY, DEFAULT_REGION_GROUPING_STRATEGY);
    this.providerClass = factory.getProviderClass(DELEGATE_PROVIDER, DEFAULT_DELEGATE_PROVIDER);
    if (providerClass.equals(this.getClass())) {
      LOG.warn("delegate provider not support multiwal, falling back to defaultProvider.");
      providerClass = factory.getDefaultProvider().clazz;
    }
  }

  private WALProvider createProvider(String group) throws IOException {
    WALProvider provider = WALFactory.createProvider(providerClass);
    provider.init(factory, conf,
      META_WAL_PROVIDER_ID.equals(providerId) ? META_WAL_PROVIDER_ID : group);
    provider.addWALActionsListener(new MetricsWAL());
    return provider;
  }

  @Override
  public List<WAL> getWALs() {
    return cached.values().stream().flatMap(p -> p.getWALs().stream()).collect(Collectors.toList());
  }

  private WAL getWAL(String group) throws IOException {
    WALProvider provider = cached.get(group);
    if (provider == null) {
      Lock lock = createLock.acquireLock(group);
      try {
        provider = cached.get(group);
        if (provider == null) {
          provider = createProvider(group);
          listeners.forEach(provider::addWALActionsListener);
          cached.put(group, provider);
        }
      } finally {
        lock.unlock();
      }
    }
    return provider.getWAL(null);
  }

  @Override
  public WAL getWAL(RegionInfo region) throws IOException {
    String group;
    if (META_WAL_PROVIDER_ID.equals(this.providerId)) {
      group = META_WAL_GROUP_NAME;
    } else {
      byte[] id;
      byte[] namespace;
      if (region != null) {
        id = region.getEncodedNameAsBytes();
        namespace = region.getTable().getNamespace();
      } else {
        id = HConstants.EMPTY_BYTE_ARRAY;
        namespace = null;
      }
      group = strategy.group(id, namespace);
    }
    return getWAL(group);
  }

  @Override
  public void shutdown() throws IOException {
    // save the last exception and rethrow
    IOException failure = null;
    for (WALProvider provider: cached.values()) {
      try {
        provider.shutdown();
      } catch (IOException e) {
        LOG.error("Problem shutting down wal provider '" + provider + "': " + e.getMessage());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Details of problem shutting down wal provider '" + provider + "'", e);
        }
        failure = e;
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  @Override
  public void close() throws IOException {
    // save the last exception and rethrow
    IOException failure = null;
    for (WALProvider provider : cached.values()) {
      try {
        provider.close();
      } catch (IOException e) {
        LOG.error("Problem closing wal provider '" + provider + "': " + e.getMessage());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Details of problem closing wal provider '" + provider + "'", e);
        }
        failure = e;
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  static class IdentityGroupingStrategy implements RegionGroupingStrategy {
    @Override
    public void init(Configuration config, String providerId) {}
    @Override
    public String group(final byte[] identifier, final byte[] namespace) {
      return Bytes.toString(identifier);
    }
  }

  @Override
  public long getNumLogFiles() {
    long numLogFiles = 0;
    for (WALProvider provider : cached.values()) {
      numLogFiles += provider.getNumLogFiles();
    }
    return numLogFiles;
  }

  @Override
  public long getLogFileSize() {
    long logFileSize = 0;
    for (WALProvider provider : cached.values()) {
      logFileSize += provider.getLogFileSize();
    }
    return logFileSize;
  }

  @Override
  public void addWALActionsListener(WALActionsListener listener) {
    // Notice that there is an assumption that this method must be called before the getWAL above,
    // so we can make sure there is no sub WALProvider yet, so we only add the listener to our
    // listeners list without calling addWALActionListener for each WALProvider. Although it is no
    // hurt to execute an extra loop to call addWALActionListener for each WALProvider, but if the
    // extra code actually works, then we will have other big problems. So leave it as is.
    listeners.add(listener);
  }
}
