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

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

// imports for classes still in regionserver.wal
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A WAL Provider that returns a WAL per group of regions.
 *
 * Region grouping is handled via {@link RegionGroupingStrategy} and can be configured via the
 * property "hbase.wal.regiongrouping.strategy". Current strategy choices are
 * <ul>
 *   <li><em>defaultStrategy</em> : Whatever strategy this version of HBase picks. currently
 *                                  "identity".</li>
 *   <li><em>identity</em> : each region belongs to its own group.</li>
 * </ul>
 * Optionally, a FQCN to a custom implementation may be given.
 *
 * WAL creation is delegated to another WALProvider, configured via the property
 * "hbase.wal.regiongrouping.delegate". The property takes the same options as "hbase.wal.provider"
 * (ref {@link WALFactory}) and defaults to the defaultProvider.
 */
@InterfaceAudience.Private
class RegionGroupingProvider implements WALProvider {
  private static final Log LOG = LogFactory.getLog(RegionGroupingProvider.class);

  /**
   * Map identifiers to a group number.
   */
  public static interface RegionGroupingStrategy {
    String GROUP_NAME_DELIMITER = ".";
    /**
     * Given an identifier, pick a group.
     * the byte[] returned for a given group must always use the same instance, since we
     * will be using it as a hash key.
     */
    String group(final byte[] identifier);
    void init(Configuration config, String providerId);
  }

  /**
   * Maps between configuration names for strategies and implementation classes.
   */
  static enum Strategies {
    defaultStrategy(BoundedGroupingStrategy.class),
    identity(IdentityGroupingStrategy.class),
    bounded(BoundedGroupingStrategy.class);

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
      final RegionGroupingStrategy result = clazz.newInstance();
      result.init(conf, providerId);
      return result;
    } catch (InstantiationException exception) {
      LOG.error("couldn't set up region grouping strategy, check config key " +
          REGION_GROUPING_STRATEGY);
      LOG.debug("Exception details for failure to load region grouping strategy.", exception);
      throw new IOException("couldn't set up region grouping strategy", exception);
    } catch (IllegalAccessException exception) {
      LOG.error("couldn't set up region grouping strategy, check config key " +
          REGION_GROUPING_STRATEGY);
      LOG.debug("Exception details for failure to load region grouping strategy.", exception);
      throw new IOException("couldn't set up region grouping strategy", exception);
    }
  }

  public static final String REGION_GROUPING_STRATEGY = "hbase.wal.regiongrouping.strategy";
  public static final String DEFAULT_REGION_GROUPING_STRATEGY = Strategies.defaultStrategy.name();

  static final String DELEGATE_PROVIDER = "hbase.wal.regiongrouping.delegate";
  static final String DEFAULT_DELEGATE_PROVIDER = WALFactory.Providers.defaultProvider.name();

  /** A group-provider mapping, recommended to make sure one-one rather than many-one mapping */
  protected final ConcurrentMap<String, WALProvider> cached =
      new ConcurrentHashMap<String, WALProvider>();
  /** Stores delegation providers (no duplicated) used by this RegionGroupingProvider */
  private final Set<WALProvider> providers = Collections
      .synchronizedSet(new HashSet<WALProvider>());


  protected RegionGroupingStrategy strategy = null;
  private WALFactory factory = null;
  private List<WALActionsListener> listeners = null;
  private String providerId = null;

  @Override
  public void init(final WALFactory factory, final Configuration conf,
      final List<WALActionsListener> listeners, final String providerId) throws IOException {
    if (null != strategy) {
      throw new IllegalStateException("WALProvider.init should only be called once.");
    }
    this.factory = factory;
    this.listeners = null == listeners ? null : Collections.unmodifiableList(listeners);
    this.providerId = providerId;
    this.strategy = getStrategy(conf, REGION_GROUPING_STRATEGY, DEFAULT_REGION_GROUPING_STRATEGY);
  }

  /**
   * Populate the cache for this group.
   */
  WALProvider populateCache(final String group) throws IOException {
    final WALProvider temp = factory.getProvider(DELEGATE_PROVIDER, DEFAULT_DELEGATE_PROVIDER,
        listeners, providerId + "-" + UUID.randomUUID());
    final WALProvider extant = cached.putIfAbsent(group, temp);
    if (null != extant) {
      // someone else beat us to initializing, just take what they set.
      temp.close();
      return extant;
    }
    providers.add(temp);
    return temp;
  }

  @Override
  public WAL getWAL(final byte[] identifier) throws IOException {
    final String group = strategy.group(identifier);
    WALProvider provider = cached.get(group);
    if (null == provider) {
      provider = populateCache(group);
    }
    return provider.getWAL(identifier);
  }

  @Override
  public void shutdown() throws IOException {
    // save the last exception and rethrow
    IOException failure = null;
    synchronized (providers) {
      for (WALProvider provider : providers) {
        try {
          provider.shutdown();
        } catch (IOException exception) {
          LOG.error("Problem shutting down provider '" + provider + "': " + exception.getMessage());
          LOG.debug("Details of problem shutting down provider '" + provider + "'", exception);
          failure = exception;
        }
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
    synchronized (providers) {
      for (WALProvider provider : providers) {
        try {
          provider.close();
        } catch (IOException exception) {
          LOG.error("Problem closing provider '" + provider + "': " + exception.getMessage());
          LOG.debug("Details of problem shutting down provider '" + provider + "'", exception);
          failure = exception;
        }
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
    public String group(final byte[] identifier) {
      return Bytes.toString(identifier);
    }
  }

  @Override
  public long getNumLogFiles() {
    long numLogFiles = 0;
    synchronized (providers) {
      for (WALProvider provider : providers) {
        numLogFiles += provider.getNumLogFiles();
      }
    }
    return numLogFiles;
  }

  @Override
  public long getLogFileSize() {
    long logFileSize = 0;
    synchronized (providers) {
      for (WALProvider provider : providers) {
        logFileSize += provider.getLogFileSize();
      }
    }
    return logFileSize;
  }

}
