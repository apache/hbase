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

import static org.apache.hadoop.hbase.wal.DefaultWALProvider.META_WAL_PROVIDER_ID;
import static org.apache.hadoop.hbase.wal.DefaultWALProvider.WAL_FILE_NAME_DELIMITER;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.regionserver.wal.MetricsWAL;

// imports for classes still in regionserver.wal
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * A WAL Provider that returns a WAL per group of regions.
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
class RegionGroupingProvider implements WALProvider {
  private static final Log LOG = LogFactory.getLog(RegionGroupingProvider.class);

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

  private static final String META_WAL_GROUP_NAME = "meta";

  /** A group-wal mapping, recommended to make sure one-one rather than many-one mapping */
  protected final Map<String, FSHLog> cached = new HashMap<String, FSHLog>();
  /** Stores unique wals generated by this RegionGroupingProvider */
  private final Set<FSHLog> logs = Collections.synchronizedSet(new HashSet<FSHLog>());

  /**
   * we synchronize on walCacheLock to prevent wal recreation in different threads
   */
  final Object walCacheLock = new Object();

  protected RegionGroupingStrategy strategy = null;
  private List<WALActionsListener> listeners = null;
  private String providerId = null;
  private Configuration conf = null;

  @Override
  public void init(final WALFactory factory, final Configuration conf,
      final List<WALActionsListener> listeners, final String providerId) throws IOException {
    if (null != strategy) {
      throw new IllegalStateException("WALProvider.init should only be called once.");
    }
    this.listeners = null == listeners ? null : Collections.unmodifiableList(listeners);
    StringBuilder sb = new StringBuilder().append(factory.factoryId);
    if (providerId != null) {
      if (providerId.startsWith(WAL_FILE_NAME_DELIMITER)) {
        sb.append(providerId);
      } else {
        sb.append(WAL_FILE_NAME_DELIMITER).append(providerId);
      }
    }
    this.providerId = sb.toString();
    this.strategy = getStrategy(conf, REGION_GROUPING_STRATEGY, DEFAULT_REGION_GROUPING_STRATEGY);
    this.conf = conf;
  }

  /**
   * Populate the cache for this group.
   */
  FSHLog populateCache(String groupName) throws IOException {
    boolean isMeta = META_WAL_PROVIDER_ID.equals(providerId);
    String hlogPrefix;
    List<WALActionsListener> listeners;
    if (isMeta) {
      hlogPrefix = this.providerId;
      // don't watch log roll for meta
      listeners = Collections.<WALActionsListener> singletonList(new MetricsWAL());
    } else {
      hlogPrefix = groupName;
      listeners = this.listeners;
    }
    FSHLog log = new FSHLog(FileSystem.get(conf), FSUtils.getRootDir(conf),
        DefaultWALProvider.getWALDirectoryName(providerId), HConstants.HREGION_OLDLOGDIR_NAME,
        conf, listeners, true, hlogPrefix, isMeta ? META_WAL_PROVIDER_ID : null);
    cached.put(groupName, log);
    logs.add(log);
    return log;
  }

  private WAL getWAL(final String group) throws IOException {
    WAL log = cached.get(walCacheLock);
    if (null == log) {
      // only lock when need to create wal, and need to lock since
      // creating hlog on fs is time consuming
      synchronized (this.walCacheLock) {
        log = cached.get(group);// check again
        if (null == log) {
          log = populateCache(group);
        }
      }
    }
    return log;
  }

  @Override
  public WAL getWAL(final byte[] identifier, byte[] namespace) throws IOException {
    final String group;
    if (META_WAL_PROVIDER_ID.equals(this.providerId)) {
      group = META_WAL_GROUP_NAME;
    } else {
      group = strategy.group(identifier, namespace);
    }
    return getWAL(group);
  }

  @Override
  public void shutdown() throws IOException {
    // save the last exception and rethrow
    IOException failure = null;
    synchronized (logs) {
      for (FSHLog wal : logs) {
        try {
          wal.shutdown();
        } catch (IOException exception) {
          LOG.error("Problem shutting down log '" + wal + "': " + exception.getMessage());
          LOG.debug("Details of problem shutting down log '" + wal + "'", exception);
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
    synchronized (logs) {
      for (FSHLog wal : logs) {
        try {
          wal.close();
        } catch (IOException exception) {
          LOG.error("Problem closing log '" + wal + "': " + exception.getMessage());
          LOG.debug("Details of problem closing wal '" + wal + "'", exception);
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
    public String group(final byte[] identifier, final byte[] namespace) {
      return Bytes.toString(identifier);
    }
  }

  @Override
  public long getNumLogFiles() {
    long numLogFiles = 0;
    synchronized (logs) {
      for (FSHLog wal : logs) {
        numLogFiles += wal.getNumLogFiles();
      }
    }
    return numLogFiles;
  }

  @Override
  public long getLogFileSize() {
    long logFileSize = 0;
    synchronized (logs) {
      for (FSHLog wal : logs) {
        logFileSize += wal.getLogFileSize();
      }
    }
    return logFileSize;
  }

}
