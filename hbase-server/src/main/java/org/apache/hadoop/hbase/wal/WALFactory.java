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
package org.apache.hadoop.hbase.wal;

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.io.asyncfs.monitor.ExcludeDatanodeManager;
import org.apache.hadoop.hbase.regionserver.wal.MetricsWAL;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufWALStreamReader;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufWALTailingReader;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.LeaseNotRecoveredException;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Entry point for users of the Write Ahead Log. Acts as the shim between internal use and the
 * particular WALProvider we use to handle wal requests. Configure which provider gets used with the
 * configuration setting "hbase.wal.provider". Available implementations:
 * <ul>
 * <li><em>defaultProvider</em> : whatever provider is standard for the hbase version. Currently
 * "asyncfs"</li>
 * <li><em>asyncfs</em> : a provider that will run on top of an implementation of the Hadoop
 * FileSystem interface via an asynchronous client.</li>
 * <li><em>filesystem</em> : a provider that will run on top of an implementation of the Hadoop
 * FileSystem interface via HDFS's synchronous DFSClient.</li>
 * <li><em>multiwal</em> : a provider that will use multiple "filesystem" wal instances per region
 * server.</li>
 * </ul>
 * Alternatively, you may provide a custom implementation of {@link WALProvider} by class name.
 */
@InterfaceAudience.Private
public class WALFactory {

  /**
   * Used in tests for injecting customized stream reader implementation, for example, inject fault
   * when reading, etc.
   * <p/>
   * After removing the sequence file based WAL, we always use protobuf based WAL reader, and we
   * will also determine whether the WAL file is encrypted and we should use
   * {@link org.apache.hadoop.hbase.regionserver.wal.SecureWALCellCodec} to decode by check the
   * header of the WAL file, so we do not need to specify a specical reader to read the WAL file
   * either.
   * <p/>
   * So typically you should not use this config in production.
   */
  public static final String WAL_STREAM_READER_CLASS_IMPL =
    "hbase.regionserver.wal.stream.reader.impl";

  private static final Logger LOG = LoggerFactory.getLogger(WALFactory.class);

  /**
   * Maps between configuration names for providers and implementation classes.
   */
  enum Providers {
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

  public static final String REPLICATION_WAL_PROVIDER = "hbase.wal.replication_provider";

  public static final String WAL_ENABLED = "hbase.regionserver.hlog.enabled";

  static final String REPLICATION_WAL_PROVIDER_ID = "rep";

  final String factoryId;
  final Abortable abortable;
  private final WALProvider provider;
  // The meta updates are written to a different wal. If this
  // regionserver holds meta regions, then this ref will be non-null.
  // lazily intialized; most RegionServers don't deal with META
  private final LazyInitializedWALProvider metaProvider;
  // This is for avoid hbase:replication itself keeps trigger unnecessary updates to WAL file and
  // generate a lot useless data, see HBASE-27775 for more details.
  private final LazyInitializedWALProvider replicationProvider;

  /**
   * Configuration-specified WAL Reader used when a custom reader is requested
   */
  private final Class<? extends WALStreamReader> walStreamReaderClass;

  /**
   * How long to attempt opening in-recovery wals
   */
  private final int timeoutMillis;

  private final Configuration conf;

  private final ExcludeDatanodeManager excludeDatanodeManager;

  // Used for the singleton WALFactory, see below.
  private WALFactory(Configuration conf) {
    // this code is duplicated here so we can keep our members final.
    // until we've moved reader/writer construction down into providers, this initialization must
    // happen prior to provider initialization, in case they need to instantiate a reader/writer.
    timeoutMillis = conf.getInt("hbase.hlog.open.timeout", 300000);
    /* TODO Both of these are probably specific to the fs wal provider */
    walStreamReaderClass = conf.getClass(WAL_STREAM_READER_CLASS_IMPL,
      ProtobufWALStreamReader.class, WALStreamReader.class);
    Preconditions.checkArgument(
      AbstractFSWALProvider.Initializer.class.isAssignableFrom(walStreamReaderClass),
      "The wal stream reader class %s is not a sub class of %s", walStreamReaderClass.getName(),
      AbstractFSWALProvider.Initializer.class.getName());
    this.conf = conf;
    // end required early initialization

    // this instance can't create wals, just reader/writers.
    provider = null;
    factoryId = SINGLETON_ID;
    this.abortable = null;
    this.excludeDatanodeManager = new ExcludeDatanodeManager(conf);
    this.metaProvider = null;
    this.replicationProvider = null;
  }

  Providers getDefaultProvider() {
    return Providers.defaultProvider;
  }

  Class<? extends WALProvider> getProviderClass(String key, String defaultValue) {
    try {
      Providers provider = Providers.valueOf(conf.get(key, defaultValue));

      // AsyncFSWALProvider is not guaranteed to work on all Hadoop versions, when it's chosen as
      // the default and we can't use it, we want to fall back to FSHLog which we know works on
      // all versions.
      if (
        provider == getDefaultProvider() && provider.clazz == AsyncFSWALProvider.class
          && !AsyncFSWALProvider.load()
      ) {
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
   * Create a WALFactory.
   */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*|.*/HBaseTestingUtility.java|.*/WALPerformanceEvaluation.java")
  public WALFactory(Configuration conf, String factoryId) throws IOException {
    // default enableSyncReplicationWALProvider is true, only disable SyncReplicationWALProvider
    // for HMaster or HRegionServer which take system table only. See HBASE-19999
    this(conf, factoryId, null);
  }

  /**
   * Create a WALFactory.
   * <p/>
   * This is the constructor you should use when creating a WALFactory in normal code, to make sure
   * that the {@code factoryId} is the server name. We need this assumption in some places for
   * parsing the server name out from the wal file name.
   * @param conf       must not be null, will keep a reference to read params in later reader/writer
   *                   instances.
   * @param serverName use to generate the factoryId, which will be append at the first of the final
   *                   file name
   * @param abortable  the server associated with this WAL file
   */
  public WALFactory(Configuration conf, ServerName serverName, Abortable abortable)
    throws IOException {
    this(conf, serverName.toString(), abortable);
  }

  /**
   * @param conf      must not be null, will keep a reference to read params in later reader/writer
   *                  instances.
   * @param factoryId a unique identifier for this factory. used i.e. by filesystem implementations
   *                  to make a directory
   * @param abortable the server associated with this WAL file
   */
  private WALFactory(Configuration conf, String factoryId, Abortable abortable) throws IOException {
    // until we've moved reader/writer construction down into providers, this initialization must
    // happen prior to provider initialization, in case they need to instantiate a reader/writer.
    timeoutMillis = conf.getInt("hbase.hlog.open.timeout", 300000);
    /* TODO Both of these are probably specific to the fs wal provider */
    walStreamReaderClass = conf.getClass(WAL_STREAM_READER_CLASS_IMPL,
      ProtobufWALStreamReader.class, WALStreamReader.class);
    Preconditions.checkArgument(
      AbstractFSWALProvider.Initializer.class.isAssignableFrom(walStreamReaderClass),
      "The wal stream reader class %s is not a sub class of %s", walStreamReaderClass.getName(),
      AbstractFSWALProvider.Initializer.class.getName());
    this.conf = conf;
    this.factoryId = factoryId;
    this.excludeDatanodeManager = new ExcludeDatanodeManager(conf);
    this.abortable = abortable;
    this.metaProvider = new LazyInitializedWALProvider(this,
      AbstractFSWALProvider.META_WAL_PROVIDER_ID, META_WAL_PROVIDER, this.abortable);
    this.replicationProvider = new LazyInitializedWALProvider(this, REPLICATION_WAL_PROVIDER_ID,
      REPLICATION_WAL_PROVIDER, this.abortable);
    // end required early initialization
    if (conf.getBoolean(WAL_ENABLED, true)) {
      WALProvider provider = createProvider(getProviderClass(WAL_PROVIDER, DEFAULT_WAL_PROVIDER));
      provider.init(this, conf, null, this.abortable);
      provider.addWALActionsListener(new MetricsWAL());
      this.provider = provider;
    } else {
      // special handling of existing configuration behavior.
      LOG.warn("Running with WAL disabled.");
      provider = new DisabledWALProvider();
      provider.init(this, conf, factoryId, null);
    }
  }

  public Configuration getConf() {
    return conf;
  }

  /**
   * Shutdown all WALs and clean up any underlying storage. Use only when you will not need to
   * replay and edits that have gone to any wals from this factory.
   */
  public void close() throws IOException {
    List<IOException> ioes = new ArrayList<>();
    // these fields could be null if the WALFactory is created only for being used in the
    // getInstance method.
    if (metaProvider != null) {
      try {
        metaProvider.close();
      } catch (IOException e) {
        ioes.add(e);
      }
    }
    if (replicationProvider != null) {
      try {
        replicationProvider.close();
      } catch (IOException e) {
        ioes.add(e);
      }
    }
    if (provider != null) {
      try {
        provider.close();
      } catch (IOException e) {
        ioes.add(e);
      }
    }
    if (!ioes.isEmpty()) {
      IOException ioe = new IOException("Failed to close WALFactory");
      for (IOException e : ioes) {
        ioe.addSuppressed(e);
      }
      throw ioe;
    }
  }

  /**
   * Tell the underlying WAL providers to shut down, but do not clean up underlying storage. If you
   * are not ending cleanly and will need to replay edits from this factory's wals, use this method
   * if you can as it will try to leave things as tidy as possible.
   */
  public void shutdown() throws IOException {
    List<IOException> ioes = new ArrayList<>();
    // these fields could be null if the WALFactory is created only for being used in the
    // getInstance method.
    if (metaProvider != null) {
      try {
        metaProvider.shutdown();
      } catch (IOException e) {
        ioes.add(e);
      }
    }
    if (replicationProvider != null) {
      try {
        replicationProvider.shutdown();
      } catch (IOException e) {
        ioes.add(e);
      }
    }
    if (provider != null) {
      try {
        provider.shutdown();
      } catch (IOException e) {
        ioes.add(e);
      }
    }
    if (!ioes.isEmpty()) {
      IOException ioe = new IOException("Failed to shutdown WALFactory");
      for (IOException e : ioes) {
        ioe.addSuppressed(e);
      }
      throw ioe;
    }
  }

  public List<WAL> getWALs() {
    return provider.getWALs();
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  WALProvider getMetaProvider() throws IOException {
    return metaProvider.getProvider();
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  WALProvider getReplicationProvider() throws IOException {
    return replicationProvider.getProvider();
  }

  /**
   * @param region the region which we want to get a WAL for. Could be null.
   */
  public WAL getWAL(RegionInfo region) throws IOException {
    // Use different WAL for hbase:meta. Instantiates the meta WALProvider if not already up.
    if (region != null && RegionReplicaUtil.isDefaultReplica(region)) {
      if (region.isMetaRegion()) {
        return metaProvider.getProvider().getWAL(region);
      } else if (ReplicationStorageFactory.isReplicationQueueTable(conf, region.getTable())) {
        return replicationProvider.getProvider().getWAL(region);
      }
    }
    return provider.getWAL(region);
  }

  public WALStreamReader createStreamReader(FileSystem fs, Path path) throws IOException {
    return createStreamReader(fs, path, (CancelableProgressable) null);
  }

  /**
   * Create a one-way stream reader for the WAL.
   * @return A WAL reader. Close when done with it.
   */
  public WALStreamReader createStreamReader(FileSystem fs, Path path,
    CancelableProgressable reporter) throws IOException {
    return createStreamReader(fs, path, reporter, -1);
  }

  /**
   * Create a one-way stream reader for the WAL, and start reading from the given
   * {@code startPosition}.
   * @return A WAL reader. Close when done with it.
   */
  public WALStreamReader createStreamReader(FileSystem fs, Path path,
    CancelableProgressable reporter, long startPosition) throws IOException {
    try {
      // A wal file could be under recovery, so it may take several
      // tries to get it open. Instead of claiming it is corrupted, retry
      // to open it up to 5 minutes by default.
      long startWaiting = EnvironmentEdgeManager.currentTime();
      long openTimeout = timeoutMillis + startWaiting;
      int nbAttempt = 0;
      WALStreamReader reader = null;
      while (true) {
        try {
          reader = walStreamReaderClass.getDeclaredConstructor().newInstance();
          ((AbstractFSWALProvider.Initializer) reader).init(fs, path, conf, startPosition);
          return reader;
        } catch (Exception e) {
          // catch Exception so that we close reader for all exceptions. If we don't
          // close the reader, we leak a socket.
          if (reader != null) {
            reader.close();
          }

          // Only inspect the Exception to consider retry when it's an IOException
          if (e instanceof IOException) {
            String msg = e.getMessage();
            if (
              msg != null && (msg.contains("Cannot obtain block length")
                || msg.contains("Could not obtain the last block")
                || msg.matches("Blocklist for [^ ]* has changed.*"))
            ) {
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
            }
          }

          // Rethrow the original exception if we are not retrying due to HDFS-isms.
          throw e;
        }
      }
    } catch (IOException ie) {
      throw ie;
    } catch (Exception e) {
      throw new IOException("Cannot get log reader", e);
    }
  }

  /**
   * Create a writer for the WAL. Uses defaults.
   * <p>
   * Should be package-private. public only for tests and
   * {@link org.apache.hadoop.hbase.regionserver.wal.Compressor}
   * @return A WAL writer. Close when done with it.
   */
  public Writer createWALWriter(final FileSystem fs, final Path path) throws IOException {
    return FSHLogProvider.createWriter(conf, fs, path, false);
  }

  /**
   * Should be package-private, visible for recovery testing. Uses defaults.
   * @return an overwritable writer for recovered edits. caller should close.
   */
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
   * Create a tailing reader for the given path. Mainly used in replication.
   */
  public static WALTailingReader createTailingReader(FileSystem fs, Path path, Configuration conf,
    long startPosition) throws IOException {
    ProtobufWALTailingReader reader = new ProtobufWALTailingReader();
    reader.init(fs, path, conf, startPosition);
    return reader;
  }

  /**
   * Create a one-way stream reader for a given path.
   */
  public static WALStreamReader createStreamReader(FileSystem fs, Path path, Configuration conf)
    throws IOException {
    return createStreamReader(fs, path, conf, -1);
  }

  /**
   * Create a one-way stream reader for a given path.
   */
  public static WALStreamReader createStreamReader(FileSystem fs, Path path, Configuration conf,
    long startPosition) throws IOException {
    return getInstance(conf).createStreamReader(fs, path, (CancelableProgressable) null,
      startPosition);
  }

  /**
   * If you already have a WALFactory, you should favor the instance method. Uses defaults.
   * @return a Writer that will overwrite files. Caller must close.
   */
  static Writer createRecoveredEditsWriter(final FileSystem fs, final Path path,
    final Configuration configuration) throws IOException {
    return FSHLogProvider.createWriter(configuration, fs, path, true);
  }

  /**
   * If you already have a WALFactory, you should favor the instance method. Uses defaults.
   * @return a writer that won't overwrite files. Caller must close.
   */
  public static Writer createWALWriter(final FileSystem fs, final Path path,
    final Configuration configuration) throws IOException {
    return FSHLogProvider.createWriter(configuration, fs, path, false);
  }

  public WALProvider getWALProvider() {
    return this.provider;
  }

  /**
   * Returns all the wal providers, for example, the default one, the one for hbase:meta and the one
   * for hbase:replication.
   */
  public List<WALProvider> getAllWALProviders() {
    List<WALProvider> providers = new ArrayList<>();
    if (provider != null) {
      providers.add(provider);
    }
    WALProvider meta = metaProvider.getProviderNoCreate();
    if (meta != null) {
      providers.add(meta);
    }
    WALProvider replication = replicationProvider.getProviderNoCreate();
    if (replication != null) {
      providers.add(replication);
    }
    return providers;
  }

  public ExcludeDatanodeManager getExcludeDatanodeManager() {
    return excludeDatanodeManager;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public String getFactoryId() {
    return this.factoryId;
  }
}
