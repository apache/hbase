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
import java.io.InterruptedIOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.wal.MetricsWAL;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader;
import org.apache.hadoop.hbase.replication.regionserver.SyncReplicationPeerProvider;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.LeaseNotRecoveredException;
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
public class WALFactory {

  private static final Logger LOG = LoggerFactory.getLogger(WALFactory.class);

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
  static final String DEFAULT_META_WAL_PROVIDER = Providers.defaultProvider.name();

  final String factoryId;
  private final WALProvider provider;
  // The meta updates are written to a different wal. If this
  // regionserver holds meta regions, then this ref will be non-null.
  // lazily intialized; most RegionServers don't deal with META
  private final AtomicReference<WALProvider> metaProvider = new AtomicReference<>();

  /**
   * Configuration-specified WAL Reader used when a custom reader is requested
   */
  private final Class<? extends AbstractFSWALProvider.Reader> logReaderClass;

  /**
   * How long to attempt opening in-recovery wals
   */
  private final int timeoutMillis;

  private final Configuration conf;

  // Used for the singleton WALFactory, see below.
  private WALFactory(Configuration conf) {
    // this code is duplicated here so we can keep our members final.
    // until we've moved reader/writer construction down into providers, this initialization must
    // happen prior to provider initialization, in case they need to instantiate a reader/writer.
    timeoutMillis = conf.getInt("hbase.hlog.open.timeout", 300000);
    /* TODO Both of these are probably specific to the fs wal provider */
    logReaderClass = conf.getClass("hbase.regionserver.hlog.reader.impl", ProtobufLogReader.class,
      AbstractFSWALProvider.Reader.class);
    this.conf = conf;
    // end required early initialization

    // this instance can't create wals, just reader/writers.
    provider = null;
    factoryId = SINGLETON_ID;
  }

  @VisibleForTesting
  public Class<? extends WALProvider> getProviderClass(String key, String defaultValue) {
    try {
      return Providers.valueOf(conf.get(key, defaultValue)).clazz;
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
      return clazz.newInstance();
    } catch (Exception e) {
      LOG.error("couldn't set up WALProvider, the configured class is " + clazz);
      LOG.debug("Exception details for failure to load WALProvider.", e);
      throw new IOException("couldn't set up WALProvider", e);
    }
  }

  /**
   * instantiate a provider from a config property. requires conf to have already been set (as well
   * as anything the provider might need to read).
   */
  private WALProvider getProvider(String key, String defaultValue, String providerId)
      throws IOException {
    WALProvider provider = createProvider(getProviderClass(key, defaultValue));
    provider.init(this, conf, providerId);
    provider.addWALActionsListener(new MetricsWAL());
    return provider;
  }

  /**
   * @param conf must not be null, will keep a reference to read params in later reader/writer
   *          instances.
   * @param factoryId a unique identifier for this factory. used i.e. by filesystem implementations
   *          to make a directory
   */
  public WALFactory(Configuration conf, String factoryId) throws IOException {
    // until we've moved reader/writer construction down into providers, this initialization must
    // happen prior to provider initialization, in case they need to instantiate a reader/writer.
    timeoutMillis = conf.getInt("hbase.hlog.open.timeout", 300000);
    /* TODO Both of these are probably specific to the fs wal provider */
    logReaderClass = conf.getClass("hbase.regionserver.hlog.reader.impl", ProtobufLogReader.class,
      AbstractFSWALProvider.Reader.class);
    this.conf = conf;
    this.factoryId = factoryId;
    // end required early initialization
    if (conf.getBoolean("hbase.regionserver.hlog.enabled", true)) {
      provider = getProvider(WAL_PROVIDER, DEFAULT_WAL_PROVIDER, null);
    } else {
      // special handling of existing configuration behavior.
      LOG.warn("Running with WAL disabled.");
      provider = new DisabledWALProvider();
      provider.init(this, conf, factoryId);
    }
  }

  /**
   * A temporary constructor for testing synchronous replication.
   * <p>
   * Remove it once we can integrate the synchronous replication logic in RS.
   */
  @VisibleForTesting
  WALFactory(Configuration conf, String factoryId, SyncReplicationPeerProvider peerProvider)
      throws IOException {
    timeoutMillis = conf.getInt("hbase.hlog.open.timeout", 300000);
    /* TODO Both of these are probably specific to the fs wal provider */
    logReaderClass = conf.getClass("hbase.regionserver.hlog.reader.impl", ProtobufLogReader.class,
      AbstractFSWALProvider.Reader.class);
    this.conf = conf;
    this.factoryId = factoryId;
    WALProvider provider = createProvider(getProviderClass(WAL_PROVIDER, DEFAULT_WAL_PROVIDER));
    this.provider = new SyncReplicationWALProvider(provider, peerProvider);
    this.provider.init(this, conf, null);
    this.provider.addWALActionsListener(new MetricsWAL());
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

  public List<WAL> getWALs() {
    return provider.getWALs();
  }

  private WALProvider getMetaProvider() throws IOException {
    for (;;) {
      WALProvider provider = this.metaProvider.get();
      if (provider != null) {
        return provider;
      }
      provider = getProvider(META_WAL_PROVIDER, DEFAULT_META_WAL_PROVIDER,
        AbstractFSWALProvider.META_WAL_PROVIDER_ID);
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

  public Reader createReader(final FileSystem fs, final Path path) throws IOException {
    return createReader(fs, path, (CancelableProgressable)null);
  }

  /**
   * Create a reader for the WAL. If you are reading from a file that's being written to and need
   * to reopen it multiple times, use {@link WAL.Reader#reset()} instead of this method
   * then just seek back to the last known good position.
   * @return A WAL reader.  Close when done with it.
   * @throws IOException
   */
  public Reader createReader(final FileSystem fs, final Path path,
      CancelableProgressable reporter) throws IOException {
    return createReader(fs, path, reporter, true);
  }

  public Reader createReader(final FileSystem fs, final Path path, CancelableProgressable reporter,
      boolean allowCustom) throws IOException {
    Class<? extends AbstractFSWALProvider.Reader> lrClass =
        allowCustom ? logReaderClass : ProtobufLogReader.class;
    try {
      // A wal file could be under recovery, so it may take several
      // tries to get it open. Instead of claiming it is corrupted, retry
      // to open it up to 5 minutes by default.
      long startWaiting = EnvironmentEdgeManager.currentTime();
      long openTimeout = timeoutMillis + startWaiting;
      int nbAttempt = 0;
      AbstractFSWALProvider.Reader reader = null;
      while (true) {
        try {
          reader = lrClass.getDeclaredConstructor().newInstance();
          reader.init(fs, path, conf, null);
          return reader;
        } catch (IOException e) {
          if (reader != null) {
            try {
              reader.close();
            } catch (IOException exception) {
              LOG.warn("Could not close FSDataInputStream" + exception.getMessage());
              LOG.debug("exception details", exception);
            }
          }

          String msg = e.getMessage();
          if (msg != null
              && (msg.contains("Cannot obtain block length")
                  || msg.contains("Could not obtain the last block") || msg
                    .matches("Blocklist for [^ ]* has changed.*"))) {
            if (++nbAttempt == 1) {
              LOG.warn("Lease should have recovered. This is not expected. Will retry", e);
            }
            if (reporter != null && !reporter.progress()) {
              throw new InterruptedIOException("Operation is cancelled");
            }
            if (nbAttempt > 2 && openTimeout < EnvironmentEdgeManager.currentTime()) {
              LOG.error("Can't open after " + nbAttempt + " attempts and "
                  + (EnvironmentEdgeManager.currentTime() - startWaiting) + "ms " + " for " + path);
            } else {
              try {
                Thread.sleep(nbAttempt < 3 ? 500 : 1000);
                continue; // retry
              } catch (InterruptedException ie) {
                InterruptedIOException iioe = new InterruptedIOException();
                iioe.initCause(ie);
                throw iioe;
              }
            }
            throw new LeaseNotRecoveredException(e);
          } else {
            throw e;
          }
        }
      }
    } catch (IOException ie) {
      throw ie;
    } catch (Exception e) {
      throw new IOException("Cannot get log reader", e);
    }
  }

  /**
   * Create a writer for the WAL.
   * Uses defaults.
   * <p>
   * Should be package-private. public only for tests and
   * {@link org.apache.hadoop.hbase.regionserver.wal.Compressor}
   * @return A WAL writer. Close when done with it.
   */
  public Writer createWALWriter(final FileSystem fs, final Path path) throws IOException {
    return FSHLogProvider.createWriter(conf, fs, path, false);
  }

  /**
   * Should be package-private, visible for recovery testing.
   * Uses defaults.
   * @return an overwritable writer for recovered edits. caller should close.
   */
  @VisibleForTesting
  public Writer createRecoveredEditsWriter(final FileSystem fs, final Path path)
      throws IOException {
    return FSHLogProvider.createWriter(conf, fs, path, true);
  }

  // These static methods are currently used where it's impractical to
  // untangle the reliance on state in the filesystem. They rely on singleton
  // WALFactory that just provides Reader / Writers.
  // For now, first Configuration object wins. Practically this just impacts the reader/writer class
  private static final AtomicReference<WALFactory> singleton = new AtomicReference<>();
  private static final String SINGLETON_ID = WALFactory.class.getName();
  
  // Public only for FSHLog
  public static WALFactory getInstance(Configuration configuration) {
    WALFactory factory = singleton.get();
    if (null == factory) {
      WALFactory temp = new WALFactory(configuration);
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

  /**
   * Create a reader for the given path, accept custom reader classes from conf.
   * If you already have a WALFactory, you should favor the instance method.
   * @return a WAL Reader, caller must close.
   */
  public static Reader createReader(final FileSystem fs, final Path path,
      final Configuration configuration) throws IOException {
    return getInstance(configuration).createReader(fs, path);
  }

  /**
   * Create a reader for the given path, accept custom reader classes from conf.
   * If you already have a WALFactory, you should favor the instance method.
   * @return a WAL Reader, caller must close.
   */
  static Reader createReader(final FileSystem fs, final Path path,
      final Configuration configuration, final CancelableProgressable reporter) throws IOException {
    return getInstance(configuration).createReader(fs, path, reporter);
  }

  /**
   * Create a reader for the given path, ignore custom reader classes from conf.
   * If you already have a WALFactory, you should favor the instance method.
   * only public pending move of {@link org.apache.hadoop.hbase.regionserver.wal.Compressor}
   * @return a WAL Reader, caller must close.
   */
  public static Reader createReaderIgnoreCustomClass(final FileSystem fs, final Path path,
      final Configuration configuration) throws IOException {
    return getInstance(configuration).createReader(fs, path, null, false);
  }

  /**
   * If you already have a WALFactory, you should favor the instance method.
   * Uses defaults.
   * @return a Writer that will overwrite files. Caller must close.
   */
  static Writer createRecoveredEditsWriter(final FileSystem fs, final Path path,
      final Configuration configuration)
      throws IOException {
    return FSHLogProvider.createWriter(configuration, fs, path, true);
  }

  /**
   * If you already have a WALFactory, you should favor the instance method.
   * Uses defaults.
   * @return a writer that won't overwrite files. Caller must close.
   */
  @VisibleForTesting
  public static Writer createWALWriter(final FileSystem fs, final Path path,
      final Configuration configuration)
      throws IOException {
    return FSHLogProvider.createWriter(configuration, fs, path, false);
  }

  public final WALProvider getWALProvider() {
    return this.provider;
  }

  public final WALProvider getMetaWALProvider() {
    return this.metaProvider.get();
  }
}
