/**
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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.wal.MetricsWAL;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Entry point for users of the Write Ahead Log.
 * Acts as the shim between internal use and the particular WALProvider we use to handle wal
 * requests.
 *
 * Configure which provider gets used with the configuration setting "hbase.wal.provider". Available
 * implementations:
 * <ul>
 *   <li><em>defaultProvider</em> : whatever provider is standard for the hbase version. Currently
 *                                  "asyncfs"</li>
 *   <li><em>asyncfs</em> : a provider that will run on top of an implementation of the Hadoop
 *                             FileSystem interface via an asynchronous client.</li>
 *   <li><em>filesystem</em> : a provider that will run on top of an implementation of the Hadoop
 *                             FileSystem interface via HDFS's synchronous DFSClient.</li>
 *   <li><em>multiwal</em> : a provider that will use multiple "filesystem" wal instances per region
 *                           server.</li>
 * </ul>
 *
 * Alternatively, you may provide a custom implementation of {@link WALProvider} by class name.
 */
@InterfaceAudience.Private
public class WALProviderFactory {

  private static final Logger LOG = LoggerFactory.getLogger(WALProviderFactory.class);

  /**
   * Maps between configuration names for providers and implementation classes.
   */
  static enum Providers {
    defaultProvider(AsyncFSWALProvider.class),
    filesystem(FSHLogProvider.class),
    multiwal(RegionGroupingProvider.class),
    asyncfs(AsyncFSWALProvider.class);

    final Class<? extends WALProvider> clazz;
    Providers(Class<? extends WALProvider> clazz) {
      this.clazz = clazz;
    }
  }

  public static final String WAL_PROVIDER = "hbase.wal.provider";
  static final String DEFAULT_WAL_PROVIDER = Providers.defaultProvider.name();

  public static final String META_WAL_PROVIDER = "hbase.wal.meta_provider";

  final String factoryId;
  private final WALProvider provider;
  // The meta updates are written to a different wal. If this
  // regionserver holds meta regions, then this ref will be non-null.
  // lazily intialized; most RegionServers don't deal with META
  private final AtomicReference<WALProvider> metaProvider = new AtomicReference<>();

  private final Configuration conf;

  @VisibleForTesting
  Providers getDefaultProvider() {
    return Providers.defaultProvider;
  }

  @VisibleForTesting
  public Class<? extends WALProvider> getProviderClass(String key, String defaultValue) {
    try {
      Providers provider = Providers.valueOf(conf.get(key, defaultValue));

      // AsyncFSWALProvider is not guaranteed to work on all Hadoop versions, when it's chosen as
      // the default and we can't use it, we want to fall back to FSHLog which we know works on
      // all versions.
      if (provider == getDefaultProvider() && provider.clazz == AsyncFSWALProvider.class
          && !AsyncFSWALProvider.load()) {
        // AsyncFSWAL has better performance in most cases, and also uses less resources, we will
        // try to use it if possible. It deeply hacks into the internal of DFSClient so will be
        // easily broken when upgrading hadoop.
        LOG.warn("Failed to load AsyncFSWALProvider, falling back to FSHLogProvider");
        return FSHLogProvider.class;
      }

      // N.b. If the user specifically requested AsyncFSWALProvider but their environment doesn't
      // support using it (e.g. AsyncFSWALProvider.load() == false), we should let this fail and
      // not fall back to FSHLogProvider.
      return provider.clazz;
    } catch (IllegalArgumentException exception) {
      // Fall back to them specifying a class name
      // Note that the passed default class shouldn't actually be used, since the above only fails
      // when there is a config value present.
      return conf.getClass(key, Providers.defaultProvider.clazz, WALProvider.class);
    }
  }

  static WALProvider createProvider(Class<? extends WALProvider> clazz) throws IOException {
    LOG.info("Instantiating WALProvider of type {}", clazz);
    try {
      return clazz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      LOG.error("couldn't set up WALProvider, the configured class is " + clazz);
      LOG.debug("Exception details for failure to load WALProvider.", e);
      throw new IOException("couldn't set up WALProvider", e);
    }
  }

  /**
   * @param conf must not be null, will keep a reference to read params in later reader/writer
   *          instances.
   * @param factoryId a unique identifier for this factory. used i.e. by filesystem implementations
   *          to make a directory
   */
  public WALProviderFactory(Configuration conf, String factoryId) throws IOException {
    // default enableSyncReplicationWALProvider is true, only disable SyncReplicationWALProvider
    // for HMaster or HRegionServer which take system table only. See HBASE-19999
    this(conf, factoryId, true);
  }

  /**
   * @param conf must not be null, will keep a reference to read params in later reader/writer
   *          instances.
   * @param factoryId a unique identifier for this factory. used i.e. by filesystem implementations
   *          to make a directory
   * @param enableSyncReplicationWALProvider whether wrap the wal provider to a
   *          {@link SyncReplicationWALProvider}
   */
  public WALProviderFactory(Configuration conf, String factoryId,
      boolean enableSyncReplicationWALProvider)
      throws IOException {
    this.conf = conf;
    this.factoryId = factoryId;
    // end required early initialization
    if (conf.getBoolean("hbase.regionserver.hlog.enabled", true)) {
      WALProvider provider =
          createProvider(getProviderClass(WAL_PROVIDER, DEFAULT_WAL_PROVIDER));
      if (enableSyncReplicationWALProvider) {
        provider = new SyncReplicationWALProvider(provider);
      }
      provider.init(this, conf, null);
      provider.addWALActionsListener(new MetricsWAL());
      this.provider = provider;
    } else {
      // special handling of existing configuration behavior.
      LOG.warn("Running with WAL disabled.");
      provider = new DisabledWALProvider();
      provider.init(this, conf, factoryId);
    }
  }

  /**
   * Shutdown all WALs and clean up any underlying storage.
   * Use only when you will not need to replay and edits that have gone to any wals from this
   * factory.
   */
  public void close() throws IOException {
    final WALProvider metaProvider = this.metaProvider.get();
    if (null != metaProvider) {
      metaProvider.close();
    }
    // close is called on a WALFactory with null provider in the case of contention handling
    // within the getInstance method.
    if (null != provider) {
      provider.close();
    }
  }

  /**
   * Tell the underlying WAL providers to shut down, but do not clean up underlying storage.
   * If you are not ending cleanly and will need to replay edits from this factory's wals,
   * use this method if you can as it will try to leave things as tidy as possible.
   */
  public void shutdown() throws IOException {
    IOException exception = null;
    final WALProvider metaProvider = this.metaProvider.get();
    if (null != metaProvider) {
      try {
        metaProvider.shutdown();
      } catch(IOException ioe) {
        exception = ioe;
      }
    }
    provider.shutdown();
    if (null != exception) {
      throw exception;
    }
  }


  @VisibleForTesting
  WALProvider getMetaProvider() throws IOException {
    for (;;) {
      WALProvider provider = this.metaProvider.get();
      if (provider != null) {
        return provider;
      }
      Class<? extends WALProvider> clz = null;
      if (conf.get(META_WAL_PROVIDER) == null) {
        try {
          clz = conf.getClass(WAL_PROVIDER, Providers.defaultProvider.clazz, WALProvider.class);
        } catch (Throwable t) {
          // the WAL provider should be an enum. Proceed
        }
      }
      if (clz == null){
        clz = getProviderClass(META_WAL_PROVIDER, conf.get(WAL_PROVIDER, DEFAULT_WAL_PROVIDER));
      }
      provider = createProvider(clz);
      provider.init(this, conf, AbstractFSWALProvider.META_WAL_PROVIDER_ID);
      provider.addWALActionsListener(new MetricsWAL());
      if (metaProvider.compareAndSet(null, provider)) {
        return provider;
      } else {
        // someone is ahead of us, close and try again.
        provider.close();
      }
    }
  }

  /**
   * @param region the region which we want to get a WAL for it. Could be null.
   */
  public WAL getWAL(RegionInfo region) throws IOException {
    // use different WAL for hbase:meta
    if (region != null && region.isMetaRegion() &&
      region.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID) {
      return getMetaProvider().getWAL(region);
    } else {
      return provider.getWAL(region);
    }
  }


  public Reader createReader(final FileSystem fs, final Path path, CancelableProgressable reporter,
      boolean allowCustom) throws IOException {
    return provider.createReader(fs, path, reporter, allowCustom);
  }

  // These static methods are currently used where it's impractical to
  // untangle the reliance on state in the filesystem. They rely on singleton
  // WALFactory that just provides Reader / Writers.
  // For now, first Configuration object wins. Practically this just impacts the reader/writer class
  private static final AtomicReference<WALProviderFactory> singleton = new AtomicReference<>();
  private static final String SINGLETON_ID = WALProviderFactory.class.getName();

  // Public only for FSHLog
  public static WALProviderFactory getInstance(Configuration configuration) throws IOException {
    WALProviderFactory factory = singleton.get();
    if (null == factory) {
      WALProviderFactory temp = new WALProviderFactory(configuration, SINGLETON_ID);
      if (singleton.compareAndSet(null, temp)) {
        factory = temp;
      } else {
        // someone else beat us to initializing
        try {
          temp.close();
        } catch (IOException exception) {
          LOG.debug("failed to close temporary singleton. ignoring.", exception);
        }
        factory = singleton.get();
      }
    }
    return factory;
  }

  public Writer createWALWriter(final FileSystem fs, final Path path, boolean overwritable)
      throws IOException {
    return provider.createWriter(conf, fs, path, overwritable);
  }

  public final WALProvider getWALProvider() {
    return this.provider;
  }

  public final WALProvider getMetaWALProvider() {
    return this.metaProvider.get();
  }

  public Reader createReader(FileSystem fs, Path path) throws IOException {
    return provider.createReader(fs, path, null, true);
  }
}
