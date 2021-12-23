/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.regionserver;

import com.google.protobuf.Message;
import com.google.protobuf.Service;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RawCellBuilder;
import org.apache.hadoop.hbase.RawCellBuilderFactory;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SharedConnection;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseEnvironment;
import org.apache.hadoop.hbase.coprocessor.BulkLoadObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.CoprocessorServiceBackwardCompatiblity;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.EndpointObserver;
import org.apache.hadoop.hbase.coprocessor.HasRegionServerServices;
import org.apache.hadoop.hbase.coprocessor.MetricsCoprocessor;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.querymatcher.DeleteTracker;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.CoprocessorClassLoader;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.collections4.map.AbstractReferenceMap;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.map.ReferenceMap;

/**
 * Implements the coprocessor environment and runtime support for coprocessors
 * loaded within a {@link Region}.
 */
@InterfaceAudience.Private
public class RegionCoprocessorHost
    extends CoprocessorHost<RegionCoprocessor, RegionCoprocessorEnvironment> {

  private static final Logger LOG = LoggerFactory.getLogger(RegionCoprocessorHost.class);
  // The shared data map
  private static final ReferenceMap<String, ConcurrentMap<String, Object>> SHARED_DATA_MAP =
      new ReferenceMap<>(AbstractReferenceMap.ReferenceStrength.HARD,
          AbstractReferenceMap.ReferenceStrength.WEAK);

  // optimization: no need to call postScannerFilterRow, if no coprocessor implements it
  private final boolean hasCustomPostScannerFilterRow;

  /*
   * Whether any configured CPs override postScannerFilterRow hook
   */
  public boolean hasCustomPostScannerFilterRow() {
    return hasCustomPostScannerFilterRow;
  }

  /**
   *
   * Encapsulation of the environment of each coprocessor
   */
  private static class RegionEnvironment extends BaseEnvironment<RegionCoprocessor>
      implements RegionCoprocessorEnvironment {
    private Region region;
    ConcurrentMap<String, Object> sharedData;
    private final MetricRegistry metricRegistry;
    private final RegionServerServices services;

    /**
     * Constructor
     * @param impl the coprocessor instance
     * @param priority chaining priority
     */
    public RegionEnvironment(final RegionCoprocessor impl, final int priority,
        final int seq, final Configuration conf, final Region region,
        final RegionServerServices services, final ConcurrentMap<String, Object> sharedData) {
      super(impl, priority, seq, conf);
      this.region = region;
      this.sharedData = sharedData;
      this.services = services;
      this.metricRegistry =
          MetricsCoprocessor.createRegistryForRegionCoprocessor(impl.getClass().getName());
    }

    /** @return the region */
    @Override
    public Region getRegion() {
      return region;
    }

    @Override
    public OnlineRegions getOnlineRegions() {
      return this.services;
    }

    @Override
    public Connection getConnection() {
      // Mocks may have services as null at test time.
      return services != null ? new SharedConnection(services.getConnection()) : null;
    }

    @Override
    public Connection createConnection(Configuration conf) throws IOException {
      return services != null ? this.services.createConnection(conf) : null;
    }

    @Override
    public ServerName getServerName() {
      return services != null? services.getServerName(): null;
    }

    @Override
    public void shutdown() {
      super.shutdown();
      MetricsCoprocessor.removeRegistry(this.metricRegistry);
    }

    @Override
    public ConcurrentMap<String, Object> getSharedData() {
      return sharedData;
    }

    @Override
    public RegionInfo getRegionInfo() {
      return region.getRegionInfo();
    }

    @Override
    public MetricRegistry getMetricRegistryForRegionServer() {
      return metricRegistry;
    }

    @Override
    public RawCellBuilder getCellBuilder() {
      // We always do a DEEP_COPY only
      return RawCellBuilderFactory.create();
    }
  }

  /**
   * Special version of RegionEnvironment that exposes RegionServerServices for Core
   * Coprocessors only. Temporary hack until Core Coprocessors are integrated into Core.
   */
  private static class RegionEnvironmentForCoreCoprocessors extends
      RegionEnvironment implements HasRegionServerServices {
    private final RegionServerServices rsServices;

    public RegionEnvironmentForCoreCoprocessors(final RegionCoprocessor impl, final int priority,
      final int seq, final Configuration conf, final Region region,
      final RegionServerServices services, final ConcurrentMap<String, Object> sharedData) {
      super(impl, priority, seq, conf, region, services, sharedData);
      this.rsServices = services;
    }

    /**
     * @return An instance of RegionServerServices, an object NOT for general user-space Coprocessor
     * consumption.
     */
    @Override
    public RegionServerServices getRegionServerServices() {
      return this.rsServices;
    }
  }

  static class TableCoprocessorAttribute {
    private Path path;
    private String className;
    private int priority;
    private Configuration conf;

    public TableCoprocessorAttribute(Path path, String className, int priority,
        Configuration conf) {
      this.path = path;
      this.className = className;
      this.priority = priority;
      this.conf = conf;
    }

    public Path getPath() {
      return path;
    }

    public String getClassName() {
      return className;
    }

    public int getPriority() {
      return priority;
    }

    public Configuration getConf() {
      return conf;
    }
  }

  /** The region server services */
  RegionServerServices rsServices;
  /** The region */
  HRegion region;

  /**
   * Constructor
   * @param region the region
   * @param rsServices interface to available region server functionality
   * @param conf the configuration
   */
  @SuppressWarnings("ReturnValueIgnored") // Checking method exists as CPU optimization
  public RegionCoprocessorHost(final HRegion region,
      final RegionServerServices rsServices, final Configuration conf) {
    super(rsServices);
    this.conf = conf;
    this.rsServices = rsServices;
    this.region = region;
    this.pathPrefix = Integer.toString(this.region.getRegionInfo().hashCode());

    // load system default cp's from configuration.
    loadSystemCoprocessors(conf, REGION_COPROCESSOR_CONF_KEY);

    // load system default cp's for user tables from configuration.
    if (!region.getRegionInfo().getTable().isSystemTable()) {
      loadSystemCoprocessors(conf, USER_REGION_COPROCESSOR_CONF_KEY);
    }

    // load Coprocessor From HDFS
    loadTableCoprocessors(conf);

    // now check whether any coprocessor implements postScannerFilterRow
    boolean hasCustomPostScannerFilterRow = false;
    out: for (RegionCoprocessorEnvironment env: coprocEnvironments) {
      if (env.getInstance() instanceof RegionObserver) {
        Class<?> clazz = env.getInstance().getClass();
        for (;;) {
          if (clazz == Object.class) {
            // we dont need to look postScannerFilterRow into Object class
            break; // break the inner loop
          }
          try {
            clazz.getDeclaredMethod("postScannerFilterRow", ObserverContext.class,
              InternalScanner.class, Cell.class, boolean.class);
            // this coprocessor has a custom version of postScannerFilterRow
            hasCustomPostScannerFilterRow = true;
            break out;
          } catch (NoSuchMethodException ignore) {
          }
          // the deprecated signature still exists
          try {
            clazz.getDeclaredMethod("postScannerFilterRow", ObserverContext.class,
              InternalScanner.class, byte[].class, int.class, short.class, boolean.class);
            // this coprocessor has a custom version of postScannerFilterRow
            hasCustomPostScannerFilterRow = true;
            break out;
          } catch (NoSuchMethodException ignore) {
          }
          clazz = clazz.getSuperclass();
        }
      }
    }
    this.hasCustomPostScannerFilterRow = hasCustomPostScannerFilterRow;
  }

  static List<TableCoprocessorAttribute> getTableCoprocessorAttrsFromSchema(Configuration conf,
      TableDescriptor htd) {
    return htd.getCoprocessorDescriptors().stream().map(cp -> {
      Path path = cp.getJarPath().map(p -> new Path(p)).orElse(null);
      Configuration ourConf;
      if (!cp.getProperties().isEmpty()) {
        // do an explicit deep copy of the passed configuration
        ourConf = new Configuration(false);
        HBaseConfiguration.merge(ourConf, conf);
        cp.getProperties().forEach((k, v) -> ourConf.set(k, v));
      } else {
        ourConf = conf;
      }
      return new TableCoprocessorAttribute(path, cp.getClassName(), cp.getPriority(), ourConf);
    }).collect(Collectors.toList());
  }

  /**
   * Sanity check the table coprocessor attributes of the supplied schema. Will
   * throw an exception if there is a problem.
   * @param conf
   * @param htd
   * @throws IOException
   */
  public static void testTableCoprocessorAttrs(final Configuration conf,
      final TableDescriptor htd) throws IOException {
    String pathPrefix = UUID.randomUUID().toString();
    for (TableCoprocessorAttribute attr: getTableCoprocessorAttrsFromSchema(conf, htd)) {
      if (attr.getPriority() < 0) {
        throw new IOException("Priority for coprocessor " + attr.getClassName() +
          " cannot be less than 0");
      }
      ClassLoader old = Thread.currentThread().getContextClassLoader();
      try {
        ClassLoader cl;
        if (attr.getPath() != null) {
          cl = CoprocessorClassLoader.getClassLoader(attr.getPath(),
            CoprocessorHost.class.getClassLoader(), pathPrefix, conf);
        } else {
          cl = CoprocessorHost.class.getClassLoader();
        }
        Thread.currentThread().setContextClassLoader(cl);
        if (cl instanceof CoprocessorClassLoader) {
          String[] includedClassPrefixes = null;
          if (conf.get(HConstants.CP_HTD_ATTR_INCLUSION_KEY) != null) {
            String prefixes = attr.conf.get(HConstants.CP_HTD_ATTR_INCLUSION_KEY);
            includedClassPrefixes = prefixes.split(";");
          }
          ((CoprocessorClassLoader)cl).loadClass(attr.getClassName(), includedClassPrefixes);
        } else {
          cl.loadClass(attr.getClassName());
        }
      } catch (ClassNotFoundException e) {
        throw new IOException("Class " + attr.getClassName() + " cannot be loaded", e);
      } finally {
        Thread.currentThread().setContextClassLoader(old);
      }
    }
  }

  void loadTableCoprocessors(final Configuration conf) {
    boolean coprocessorsEnabled = conf.getBoolean(COPROCESSORS_ENABLED_CONF_KEY,
      DEFAULT_COPROCESSORS_ENABLED);
    boolean tableCoprocessorsEnabled = conf.getBoolean(USER_COPROCESSORS_ENABLED_CONF_KEY,
      DEFAULT_USER_COPROCESSORS_ENABLED);
    if (!(coprocessorsEnabled && tableCoprocessorsEnabled)) {
      return;
    }

    // scan the table attributes for coprocessor load specifications
    // initialize the coprocessors
    List<RegionCoprocessorEnvironment> configured = new ArrayList<>();
    for (TableCoprocessorAttribute attr: getTableCoprocessorAttrsFromSchema(conf,
        region.getTableDescriptor())) {
      // Load encompasses classloading and coprocessor initialization
      try {
        RegionCoprocessorEnvironment env = load(attr.getPath(), attr.getClassName(),
            attr.getPriority(), attr.getConf());
        if (env == null) {
          continue;
        }
        configured.add(env);
        LOG.info("Loaded coprocessor " + attr.getClassName() + " from HTD of " +
            region.getTableDescriptor().getTableName().getNameAsString() + " successfully.");
      } catch (Throwable t) {
        // Coprocessor failed to load, do we abort on error?
        if (conf.getBoolean(ABORT_ON_ERROR_KEY, DEFAULT_ABORT_ON_ERROR)) {
          abortServer(attr.getClassName(), t);
        } else {
          LOG.error("Failed to load coprocessor " + attr.getClassName(), t);
        }
      }
    }
    // add together to coprocessor set for COW efficiency
    coprocEnvironments.addAll(configured);
  }

  @Override
  public RegionEnvironment createEnvironment(RegionCoprocessor instance, int priority, int seq,
      Configuration conf) {
    // If coprocessor exposes any services, register them.
    for (Service service : instance.getServices()) {
      region.registerService(service);
    }
    ConcurrentMap<String, Object> classData;
    // make sure only one thread can add maps
    synchronized (SHARED_DATA_MAP) {
      // as long as at least one RegionEnvironment holds on to its classData it will
      // remain in this map
      classData =
          SHARED_DATA_MAP.computeIfAbsent(instance.getClass().getName(),
              k -> new ConcurrentHashMap<>());
    }
    // If a CoreCoprocessor, return a 'richer' environment, one laden with RegionServerServices.
    return instance.getClass().isAnnotationPresent(CoreCoprocessor.class)?
        new RegionEnvironmentForCoreCoprocessors(instance, priority, seq, conf, region,
            rsServices, classData):
        new RegionEnvironment(instance, priority, seq, conf, region, rsServices, classData);
  }

  @Override
  public RegionCoprocessor checkAndGetInstance(Class<?> implClass)
      throws InstantiationException, IllegalAccessException {
    try {
      if (RegionCoprocessor.class.isAssignableFrom(implClass)) {
        return implClass.asSubclass(RegionCoprocessor.class).getDeclaredConstructor().newInstance();
      } else if (CoprocessorService.class.isAssignableFrom(implClass)) {
        // For backward compatibility with old CoprocessorService impl which don't extend
        // RegionCoprocessor.
        CoprocessorService cs;
        cs = implClass.asSubclass(CoprocessorService.class).getDeclaredConstructor().newInstance();
        return new CoprocessorServiceBackwardCompatiblity.RegionCoprocessorService(cs);
      } else {
        LOG.error("{} is not of type RegionCoprocessor. Check the configuration of {}",
            implClass.getName(), CoprocessorHost.REGION_COPROCESSOR_CONF_KEY);
        return null;
      }
    } catch (NoSuchMethodException | InvocationTargetException e) {
      throw (InstantiationException) new InstantiationException(implClass.getName()).initCause(e);
    }
  }

  private ObserverGetter<RegionCoprocessor, RegionObserver> regionObserverGetter =
      RegionCoprocessor::getRegionObserver;

  private ObserverGetter<RegionCoprocessor, EndpointObserver> endpointObserverGetter =
      RegionCoprocessor::getEndpointObserver;

  abstract class RegionObserverOperationWithoutResult extends
      ObserverOperationWithoutResult<RegionObserver> {
    public RegionObserverOperationWithoutResult() {
      super(regionObserverGetter);
    }

    public RegionObserverOperationWithoutResult(User user) {
      super(regionObserverGetter, user);
    }

    public RegionObserverOperationWithoutResult(boolean bypassable) {
      super(regionObserverGetter, null, bypassable);
    }

    public RegionObserverOperationWithoutResult(User user, boolean bypassable) {
      super(regionObserverGetter, user, bypassable);
    }
  }

  abstract class BulkLoadObserverOperation extends
      ObserverOperationWithoutResult<BulkLoadObserver> {
    public BulkLoadObserverOperation(User user) {
      super(RegionCoprocessor::getBulkLoadObserver, user);
    }
  }


  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Observer operations
  //////////////////////////////////////////////////////////////////////////////////////////////////

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Observer operations
  //////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Invoked before a region open.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void preOpen() throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return;
    }
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preOpen(this);
      }
    });
  }


  /**
   * Invoked after a region open
   */
  public void postOpen() {
    if (coprocEnvironments.isEmpty()) {
      return;
    }
    try {
      execOperation(new RegionObserverOperationWithoutResult() {
        @Override
        public void call(RegionObserver observer) throws IOException {
          observer.postOpen(this);
        }
      });
    } catch (IOException e) {
      LOG.warn(e.toString(), e);
    }
  }

  /**
   * Invoked before a region is closed
   * @param abortRequested true if the server is aborting
   */
  public void preClose(final boolean abortRequested) throws IOException {
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preClose(this, abortRequested);
      }
    });
  }

  /**
   * Invoked after a region is closed
   * @param abortRequested true if the server is aborting
   */
  public void postClose(final boolean abortRequested) {
    try {
      execOperation(new RegionObserverOperationWithoutResult() {
        @Override
        public void call(RegionObserver observer) throws IOException {
          observer.postClose(this, abortRequested);
        }

        @Override
        public void postEnvCall() {
          shutdown(this.getEnvironment());
        }
      });
    } catch (IOException e) {
      LOG.warn(e.toString(), e);
    }
  }

  /**
   * Called prior to selecting the {@link HStoreFile}s for compaction from the list of currently
   * available candidates.
   * <p>Supports Coprocessor 'bypass' -- 'bypass' is how this method indicates that it changed
   * the passed in <code>candidates</code>.
   * @param store The store where compaction is being requested
   * @param candidates The currently available store files
   * @param tracker used to track the life cycle of a compaction
   * @param user the user
   * @throws IOException
   */
  public boolean preCompactSelection(final HStore store, final List<HStoreFile> candidates,
      final CompactionLifeCycleTracker tracker, final User user) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return false;
    }
    boolean bypassable = true;
    return execOperation(new RegionObserverOperationWithoutResult(user, bypassable) {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preCompactSelection(this, store, candidates, tracker);
      }
    });
  }

  /**
   * Called after the {@link HStoreFile}s to be compacted have been selected from the available
   * candidates.
   * @param store The store where compaction is being requested
   * @param selected The store files selected to compact
   * @param tracker used to track the life cycle of a compaction
   * @param request the compaction request
   * @param user the user
   */
  public void postCompactSelection(final HStore store, final List<HStoreFile> selected,
      final CompactionLifeCycleTracker tracker, final CompactionRequest request,
      final User user) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return;
    }
    execOperation(new RegionObserverOperationWithoutResult(user) {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postCompactSelection(this, store, selected, tracker, request);
      }
    });
  }

  /**
   * Called prior to opening store scanner for compaction.
   */
  public ScanInfo preCompactScannerOpen(HStore store, ScanType scanType,
      CompactionLifeCycleTracker tracker, CompactionRequest request, User user) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return store.getScanInfo();
    }
    CustomizedScanInfoBuilder builder = new CustomizedScanInfoBuilder(store.getScanInfo());
    execOperation(new RegionObserverOperationWithoutResult(user) {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preCompactScannerOpen(this, store, scanType, builder, tracker, request);
      }
    });
    return builder.build();
  }

  /**
   * Called prior to rewriting the store files selected for compaction
   * @param store the store being compacted
   * @param scanner the scanner used to read store data during compaction
   * @param scanType type of Scan
   * @param tracker used to track the life cycle of a compaction
   * @param request the compaction request
   * @param user the user
   * @return Scanner to use (cannot be null!)
   * @throws IOException
   */
  public InternalScanner preCompact(final HStore store, final InternalScanner scanner,
      final ScanType scanType, final CompactionLifeCycleTracker tracker,
      final CompactionRequest request, final User user) throws IOException {
    InternalScanner defaultResult = scanner;
    if (coprocEnvironments.isEmpty()) {
      return defaultResult;
    }
    return execOperationWithResult(
        new ObserverOperationWithResult<RegionObserver, InternalScanner>(regionObserverGetter,
            defaultResult, user) {
          @Override
          public InternalScanner call(RegionObserver observer) throws IOException {
            InternalScanner scanner =
                observer.preCompact(this, store, getResult(), scanType, tracker, request);
            if (scanner == null) {
              throw new CoprocessorException("Null Scanner return disallowed!");
            }
            return scanner;
          }
        });
  }

  /**
   * Called after the store compaction has completed.
   * @param store the store being compacted
   * @param resultFile the new store file written during compaction
   * @param tracker used to track the life cycle of a compaction
   * @param request the compaction request
   * @param user the user
   * @throws IOException
   */
  public void postCompact(final HStore store, final HStoreFile resultFile,
      final CompactionLifeCycleTracker tracker, final CompactionRequest request, final User user)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty()? null: new RegionObserverOperationWithoutResult(user) {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postCompact(this, store, resultFile, tracker, request);
      }
    });
  }

  /**
   * Invoked before create StoreScanner for flush.
   */
  public ScanInfo preFlushScannerOpen(HStore store, FlushLifeCycleTracker tracker)
      throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return store.getScanInfo();
    }
    CustomizedScanInfoBuilder builder = new CustomizedScanInfoBuilder(store.getScanInfo());
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preFlushScannerOpen(this, store, builder, tracker);
      }
    });
    return builder.build();
  }

  /**
   * Invoked before a memstore flush
   * @return Scanner to use (cannot be null!)
   * @throws IOException
   */
  public InternalScanner preFlush(HStore store, InternalScanner scanner,
      FlushLifeCycleTracker tracker) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return scanner;
    }
    return execOperationWithResult(
        new ObserverOperationWithResult<RegionObserver, InternalScanner>(regionObserverGetter, scanner) {
          @Override
          public InternalScanner call(RegionObserver observer) throws IOException {
            InternalScanner scanner = observer.preFlush(this, store, getResult(), tracker);
            if (scanner == null) {
              throw new CoprocessorException("Null Scanner return disallowed!");
            }
            return scanner;
          }
        });
  }

  /**
   * Invoked before a memstore flush
   * @throws IOException
   */
  public void preFlush(FlushLifeCycleTracker tracker) throws IOException {
    execOperation(coprocEnvironments.isEmpty()? null: new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preFlush(this, tracker);
      }
    });
  }

  /**
   * Invoked after a memstore flush
   * @throws IOException
   */
  public void postFlush(FlushLifeCycleTracker tracker) throws IOException {
    execOperation(coprocEnvironments.isEmpty()? null: new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postFlush(this, tracker);
      }
    });
  }

  /**
   * Invoked before in memory compaction.
   */
  public void preMemStoreCompaction(HStore store) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preMemStoreCompaction(this, store);
      }
    });
  }

  /**
   * Invoked before create StoreScanner for in memory compaction.
   */
  public ScanInfo preMemStoreCompactionCompactScannerOpen(HStore store) throws IOException {
    CustomizedScanInfoBuilder builder = new CustomizedScanInfoBuilder(store.getScanInfo());
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preMemStoreCompactionCompactScannerOpen(this, store, builder);
      }
    });
    return builder.build();
  }

  /**
   * Invoked before compacting memstore.
   */
  public InternalScanner preMemStoreCompactionCompact(HStore store, InternalScanner scanner)
      throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return scanner;
    }
    return execOperationWithResult(new ObserverOperationWithResult<RegionObserver, InternalScanner>(
        regionObserverGetter, scanner) {
      @Override
      public InternalScanner call(RegionObserver observer) throws IOException {
        return observer.preMemStoreCompactionCompact(this, store, getResult());
      }
    });
  }

  /**
   * Invoked after in memory compaction.
   */
  public void postMemStoreCompaction(HStore store) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null : new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postMemStoreCompaction(this, store);
      }
    });
  }

  /**
   * Invoked after a memstore flush
   * @throws IOException
   */
  public void postFlush(HStore store, HStoreFile storeFile, FlushLifeCycleTracker tracker)
      throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return;
    }
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postFlush(this, store, storeFile, tracker);
      }
    });
  }

  // RegionObserver support
  /**
   * Supports Coprocessor 'bypass'.
   * @param get the Get request
   * @param results What to return if return is true/'bypass'.
   * @return true if default processing should be bypassed.
   * @exception IOException Exception
   */
  public boolean preGet(final Get get, final List<Cell> results) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return false;
    }
    boolean bypassable = true;
    return execOperation(new RegionObserverOperationWithoutResult(bypassable) {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preGetOp(this, get, results);
      }
    });
  }

  /**
   * @param get the Get request
   * @param results the result set
   * @exception IOException Exception
   */
  public void postGet(final Get get, final List<Cell> results)
      throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return;
    }
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postGetOp(this, get, results);
      }
    });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param get the Get request
   * @return true or false to return to client if bypassing normal operation, or null otherwise
   * @exception IOException Exception
   */
  public Boolean preExists(final Get get) throws IOException {
    boolean bypassable = true;
    boolean defaultResult = false;
    if (coprocEnvironments.isEmpty()) {
      return null;
    }
    return execOperationWithResult(
        new ObserverOperationWithResult<RegionObserver, Boolean>(regionObserverGetter,
            defaultResult, bypassable) {
          @Override
          public Boolean call(RegionObserver observer) throws IOException {
            return observer.preExists(this, get, getResult());
          }
        });
  }

  /**
   * @param get the Get request
   * @param result the result returned by the region server
   * @return the result to return to the client
   * @exception IOException Exception
   */
  public boolean postExists(final Get get, boolean result)
      throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return result;
    }
    return execOperationWithResult(
        new ObserverOperationWithResult<RegionObserver, Boolean>(regionObserverGetter, result) {
          @Override
          public Boolean call(RegionObserver observer) throws IOException {
            return observer.postExists(this, get, getResult());
          }
        });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param put The Put object
   * @param edit The WALEdit object.
   * @return true if default processing should be bypassed
   * @exception IOException Exception
   */
  public boolean prePut(final Put put, final WALEdit edit) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return false;
    }
    boolean bypassable = true;
    return execOperation(new RegionObserverOperationWithoutResult(bypassable) {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.prePut(this, put, edit);
      }
    });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param mutation - the current mutation
   * @param kv - the current cell
   * @param byteNow - current timestamp in bytes
   * @param get - the get that could be used
   * Note that the get only does not specify the family and qualifier that should be used
   * @return true if default processing should be bypassed
   * @deprecated In hbase-2.0.0. Will be removed in hbase-3.0.0. Added explicitly for a single
   * Coprocessor for its needs only. Will be removed.
   */
  @Deprecated
  public boolean prePrepareTimeStampForDeleteVersion(final Mutation mutation,
      final Cell kv, final byte[] byteNow, final Get get) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return false;
    }
    boolean bypassable = true;
    return execOperation(new RegionObserverOperationWithoutResult(bypassable) {
      @Override
      public void call(RegionObserver observer) throws IOException {
          observer.prePrepareTimeStampForDeleteVersion(this, mutation, kv, byteNow, get);
      }
    });
  }

  /**
   * @param put The Put object
   * @param edit The WALEdit object.
   * @exception IOException Exception
   */
  public void postPut(final Put put, final WALEdit edit) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return;
    }
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postPut(this, put, edit);
      }
    });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param delete The Delete object
   * @param edit The WALEdit object.
   * @return true if default processing should be bypassed
   * @exception IOException Exception
   */
  public boolean preDelete(final Delete delete, final WALEdit edit) throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return false;
    }
    boolean bypassable = true;
    return execOperation(new RegionObserverOperationWithoutResult(bypassable) {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preDelete(this, delete, edit);
      }
    });
  }

  /**
   * @param delete The Delete object
   * @param edit The WALEdit object.
   * @exception IOException Exception
   */
  public void postDelete(final Delete delete, final WALEdit edit) throws IOException {
    execOperation(coprocEnvironments.isEmpty()? null:
      new RegionObserverOperationWithoutResult() {
        @Override
        public void call(RegionObserver observer) throws IOException {
          observer.postDelete(this, delete, edit);
        }
      });
  }

  public void preBatchMutate(
      final MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    if(this.coprocEnvironments.isEmpty()) {
      return;
    }
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preBatchMutate(this, miniBatchOp);
      }
    });
  }

  public void postBatchMutate(
      final MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return;
    }
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postBatchMutate(this, miniBatchOp);
      }
    });
  }

  public void postBatchMutateIndispensably(
      final MiniBatchOperationInProgress<Mutation> miniBatchOp, final boolean success)
      throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return;
    }
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postBatchMutateIndispensably(this, miniBatchOp, success);
      }
    });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param checkAndMutate the CheckAndMutate object
   * @return true or false to return to client if default processing should be bypassed, or null
   *   otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public CheckAndMutateResult preCheckAndMutate(CheckAndMutate checkAndMutate)
    throws IOException {
    boolean bypassable = true;
    CheckAndMutateResult defaultResult = new CheckAndMutateResult(false, null);
    if (coprocEnvironments.isEmpty()) {
      return null;
    }
    return execOperationWithResult(
      new ObserverOperationWithResult<RegionObserver, CheckAndMutateResult>(
        regionObserverGetter, defaultResult, bypassable) {
        @Override
        public CheckAndMutateResult call(RegionObserver observer) throws IOException {
          return observer.preCheckAndMutate(this, checkAndMutate, getResult());
        }
      });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param checkAndMutate the CheckAndMutate object
   * @return true or false to return to client if default processing should be bypassed, or null
   *   otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public CheckAndMutateResult preCheckAndMutateAfterRowLock(CheckAndMutate checkAndMutate)
    throws IOException {
    boolean bypassable = true;
    CheckAndMutateResult defaultResult = new CheckAndMutateResult(false, null);
    if (coprocEnvironments.isEmpty()) {
      return null;
    }
    return execOperationWithResult(
      new ObserverOperationWithResult<RegionObserver, CheckAndMutateResult>(
        regionObserverGetter, defaultResult, bypassable) {
        @Override
        public CheckAndMutateResult call(RegionObserver observer) throws IOException {
          return observer.preCheckAndMutateAfterRowLock(this, checkAndMutate, getResult());
        }
      });
  }

  /**
   * @param checkAndMutate the CheckAndMutate object
   * @param result the result returned by the checkAndMutate
   * @return true or false to return to client if default processing should be bypassed, or null
   *   otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public CheckAndMutateResult postCheckAndMutate(CheckAndMutate checkAndMutate,
    CheckAndMutateResult result) throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return result;
    }
    return execOperationWithResult(
      new ObserverOperationWithResult<RegionObserver, CheckAndMutateResult>(
        regionObserverGetter, result) {
        @Override
        public CheckAndMutateResult call(RegionObserver observer) throws IOException {
          return observer.postCheckAndMutate(this, checkAndMutate, getResult());
        }
      });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param append append object
   * @param edit The WALEdit object.
   * @return result to return to client if default operation should be bypassed, null otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result preAppend(final Append append, final WALEdit edit) throws IOException {
    boolean bypassable = true;
    Result defaultResult = null;
    if (this.coprocEnvironments.isEmpty()) {
      return defaultResult;
    }
    return execOperationWithResult(
      new ObserverOperationWithResult<RegionObserver, Result>(regionObserverGetter, defaultResult,
            bypassable) {
          @Override
          public Result call(RegionObserver observer) throws IOException {
            return observer.preAppend(this, append, edit);
          }
        });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param append append object
   * @return result to return to client if default operation should be bypassed, null otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result preAppendAfterRowLock(final Append append) throws IOException {
    boolean bypassable = true;
    Result defaultResult = null;
    if (this.coprocEnvironments.isEmpty()) {
      return defaultResult;
    }
    return execOperationWithResult(
        new ObserverOperationWithResult<RegionObserver, Result>(regionObserverGetter,
            defaultResult, bypassable) {
          @Override
          public Result call(RegionObserver observer) throws IOException {
            return observer.preAppendAfterRowLock(this, append);
          }
        });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param increment increment object
   * @param edit The WALEdit object.
   * @return result to return to client if default operation should be bypassed, null otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result preIncrement(final Increment increment, final WALEdit edit) throws IOException {
    boolean bypassable = true;
    Result defaultResult = null;
    if (coprocEnvironments.isEmpty()) {
      return defaultResult;
    }
    return execOperationWithResult(
        new ObserverOperationWithResult<RegionObserver, Result>(regionObserverGetter, defaultResult,
            bypassable) {
          @Override
          public Result call(RegionObserver observer) throws IOException {
            return observer.preIncrement(this, increment, edit);
          }
        });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param increment increment object
   * @return result to return to client if default operation should be bypassed, null otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result preIncrementAfterRowLock(final Increment increment) throws IOException {
    boolean bypassable = true;
    Result defaultResult = null;
    if (coprocEnvironments.isEmpty()) {
      return defaultResult;
    }
    return execOperationWithResult(
        new ObserverOperationWithResult<RegionObserver, Result>(regionObserverGetter, defaultResult,
            bypassable) {
          @Override
          public Result call(RegionObserver observer) throws IOException {
            return observer.preIncrementAfterRowLock(this, increment);
          }
        });
  }

  /**
   * @param append Append object
   * @param result the result returned by the append
   * @param edit The WALEdit object.
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result postAppend(final Append append, final Result result, final WALEdit edit)
    throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return result;
    }
    return execOperationWithResult(
        new ObserverOperationWithResult<RegionObserver, Result>(regionObserverGetter, result) {
          @Override
          public Result call(RegionObserver observer) throws IOException {
            return observer.postAppend(this, append, result, edit);
          }
        });
  }

  /**
   * @param increment increment object
   * @param result the result returned by postIncrement
   * @param edit The WALEdit object.
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result postIncrement(final Increment increment, Result result, final WALEdit edit)
    throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return result;
    }
    return execOperationWithResult(
        new ObserverOperationWithResult<RegionObserver, Result>(regionObserverGetter, result) {
          @Override
          public Result call(RegionObserver observer) throws IOException {
            return observer.postIncrement(this, increment, getResult(), edit);
          }
        });
  }

  /**
   * @param scan the Scan specification
   * @exception IOException Exception
   */
  public void preScannerOpen(final Scan scan) throws IOException {
    execOperation(coprocEnvironments.isEmpty()? null: new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preScannerOpen(this, scan);
      }
    });
  }

  /**
   * @param scan the Scan specification
   * @param s the scanner
   * @return the scanner instance to use
   * @exception IOException Exception
   */
  public RegionScanner postScannerOpen(final Scan scan, RegionScanner s) throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return s;
    }
    return execOperationWithResult(
        new ObserverOperationWithResult<RegionObserver, RegionScanner>(regionObserverGetter, s) {
          @Override
          public RegionScanner call(RegionObserver observer) throws IOException {
            return observer.postScannerOpen(this, scan, getResult());
          }
        });
  }

  /**
   * @param s the scanner
   * @param results the result set returned by the region server
   * @param limit the maximum number of results to return
   * @return 'has next' indication to client if bypassing default behavior, or null otherwise
   * @exception IOException Exception
   */
  public Boolean preScannerNext(final InternalScanner s,
      final List<Result> results, final int limit) throws IOException {
    boolean bypassable = true;
    boolean defaultResult = false;
    if (coprocEnvironments.isEmpty()) {
      return null;
    }
    return execOperationWithResult(
        new ObserverOperationWithResult<RegionObserver, Boolean>(regionObserverGetter,
            defaultResult, bypassable) {
          @Override
          public Boolean call(RegionObserver observer) throws IOException {
            return observer.preScannerNext(this, s, results, limit, getResult());
          }
        });
  }

  /**
   * @param s the scanner
   * @param results the result set returned by the region server
   * @param limit the maximum number of results to return
   * @param hasMore
   * @return 'has more' indication to give to client
   * @exception IOException Exception
   */
  public boolean postScannerNext(final InternalScanner s,
      final List<Result> results, final int limit, boolean hasMore)
      throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return hasMore;
    }
    return execOperationWithResult(
        new ObserverOperationWithResult<RegionObserver, Boolean>(regionObserverGetter, hasMore) {
          @Override
          public Boolean call(RegionObserver observer) throws IOException {
            return observer.postScannerNext(this, s, results, limit, getResult());
          }
        });
  }

  /**
   * This will be called by the scan flow when the current scanned row is being filtered out by the
   * filter.
   * @param s the scanner
   * @param curRowCell The cell in the current row which got filtered out
   * @return whether more rows are available for the scanner or not
   * @throws IOException
   */
  public boolean postScannerFilterRow(final InternalScanner s, final Cell curRowCell)
      throws IOException {
    // short circuit for performance
    boolean defaultResult = true;
    if (!hasCustomPostScannerFilterRow) {
      return defaultResult;
    }
    if (this.coprocEnvironments.isEmpty()) {
      return defaultResult;
    }
    return execOperationWithResult(new ObserverOperationWithResult<RegionObserver, Boolean>(
        regionObserverGetter, defaultResult) {
      @Override
      public Boolean call(RegionObserver observer) throws IOException {
        return observer.postScannerFilterRow(this, s, curRowCell, getResult());
      }
    });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @param s the scanner
   * @return true if default behavior should be bypassed, false otherwise
   * @exception IOException Exception
   */
  // Should this be bypassable?
  public boolean preScannerClose(final InternalScanner s) throws IOException {
    return execOperation(coprocEnvironments.isEmpty()? null:
        new RegionObserverOperationWithoutResult(true) {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preScannerClose(this, s);
      }
    });
  }

  /**
   * @exception IOException Exception
   */
  public void postScannerClose(final InternalScanner s) throws IOException {
    execOperation(coprocEnvironments.isEmpty()? null:
        new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postScannerClose(this, s);
      }
    });
  }

  /**
   * Called before open store scanner for user scan.
   */
  public ScanInfo preStoreScannerOpen(HStore store, Scan scan) throws IOException {
    if (coprocEnvironments.isEmpty()) return store.getScanInfo();
    CustomizedScanInfoBuilder builder = new CustomizedScanInfoBuilder(store.getScanInfo(), scan);
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preStoreScannerOpen(this, store, builder);
      }
    });
    return builder.build();
  }

  /**
   * @param info the RegionInfo for this region
   * @param edits the file of recovered edits
   */
  public void preReplayWALs(final RegionInfo info, final Path edits) throws IOException {
    execOperation(coprocEnvironments.isEmpty()? null:
        new RegionObserverOperationWithoutResult(true) {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preReplayWALs(this, info, edits);
      }
    });
  }

  /**
   * @param info the RegionInfo for this region
   * @param edits the file of recovered edits
   * @throws IOException Exception
   */
  public void postReplayWALs(final RegionInfo info, final Path edits) throws IOException {
    execOperation(coprocEnvironments.isEmpty()? null:
        new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postReplayWALs(this, info, edits);
      }
    });
  }

  /**
   * Supports Coprocessor 'bypass'.
   * @return true if default behavior should be bypassed, false otherwise
   * @deprecated Since hbase-2.0.0. No replacement. To be removed in hbase-3.0.0 and replaced
   * with something that doesn't expose IntefaceAudience.Private classes.
   */
  @Deprecated
  public boolean preWALRestore(final RegionInfo info, final WALKey logKey, final WALEdit logEdit)
      throws IOException {
    return execOperation(coprocEnvironments.isEmpty()? null:
        new RegionObserverOperationWithoutResult(true) {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preWALRestore(this, info, logKey, logEdit);
      }
    });
  }

  /**
   * @deprecated Since hbase-2.0.0. No replacement. To be removed in hbase-3.0.0 and replaced
   * with something that doesn't expose IntefaceAudience.Private classes.
   */
  @Deprecated
  public void postWALRestore(final RegionInfo info, final WALKey logKey, final WALEdit logEdit)
      throws IOException {
    execOperation(coprocEnvironments.isEmpty()? null:
        new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postWALRestore(this, info, logKey, logEdit);
      }
    });
  }

  /**
   * @param familyPaths pairs of { CF, file path } submitted for bulk load
   */
  public void preBulkLoadHFile(final List<Pair<byte[], String>> familyPaths) throws IOException {
    execOperation(coprocEnvironments.isEmpty()? null: new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preBulkLoadHFile(this, familyPaths);
      }
    });
  }

  public boolean preCommitStoreFile(final byte[] family, final List<Pair<Path, Path>> pairs)
      throws IOException {
    return execOperation(coprocEnvironments.isEmpty()? null:
        new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preCommitStoreFile(this, family, pairs);
      }
    });
  }

  public void postCommitStoreFile(final byte[] family, Path srcPath, Path dstPath) throws IOException {
    execOperation(coprocEnvironments.isEmpty()? null:
        new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postCommitStoreFile(this, family, srcPath, dstPath);
      }
    });
  }

  /**
   * @param familyPaths pairs of { CF, file path } submitted for bulk load
   * @param map Map of CF to List of file paths for the final loaded files
   * @throws IOException
   */
  public void postBulkLoadHFile(final List<Pair<byte[], String>> familyPaths,
      Map<byte[], List<Path>> map) throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return;
    }
    execOperation(coprocEnvironments.isEmpty()? null:
        new RegionObserverOperationWithoutResult() {
          @Override
          public void call(RegionObserver observer) throws IOException {
            observer.postBulkLoadHFile(this, familyPaths, map);
          }
        });
  }

  public void postStartRegionOperation(final Operation op) throws IOException {
    execOperation(coprocEnvironments.isEmpty()? null:
        new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postStartRegionOperation(this, op);
      }
    });
  }

  public void postCloseRegionOperation(final Operation op) throws IOException {
    execOperation(coprocEnvironments.isEmpty()? null:
        new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.postCloseRegionOperation(this, op);
      }
    });
  }

  /**
   * @param fs fileystem to read from
   * @param p path to the file
   * @param in {@link FSDataInputStreamWrapper}
   * @param size Full size of the file
   * @param cacheConf
   * @param r original reference file. This will be not null only when reading a split file.
   * @return a Reader instance to use instead of the base reader if overriding
   * default behavior, null otherwise
   * @throws IOException
   */
  public StoreFileReader preStoreFileReaderOpen(final FileSystem fs, final Path p,
      final FSDataInputStreamWrapper in, final long size, final CacheConfig cacheConf,
      final Reference r) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return null;
    }
    return execOperationWithResult(
        new ObserverOperationWithResult<RegionObserver, StoreFileReader>(regionObserverGetter, null) {
          @Override
          public StoreFileReader call(RegionObserver observer) throws IOException {
            return observer.preStoreFileReaderOpen(this, fs, p, in, size, cacheConf, r,
                getResult());
          }
        });
  }

  /**
   * @param fs fileystem to read from
   * @param p path to the file
   * @param in {@link FSDataInputStreamWrapper}
   * @param size Full size of the file
   * @param cacheConf
   * @param r original reference file. This will be not null only when reading a split file.
   * @param reader the base reader instance
   * @return The reader to use
   * @throws IOException
   */
  public StoreFileReader postStoreFileReaderOpen(final FileSystem fs, final Path p,
      final FSDataInputStreamWrapper in, final long size, final CacheConfig cacheConf,
      final Reference r, final StoreFileReader reader) throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return reader;
    }
    return execOperationWithResult(
        new ObserverOperationWithResult<RegionObserver, StoreFileReader>(regionObserverGetter, reader) {
          @Override
          public StoreFileReader call(RegionObserver observer) throws IOException {
            return observer.postStoreFileReaderOpen(this, fs, p, in, size, cacheConf, r,
                getResult());
          }
        });
  }

  public List<Pair<Cell, Cell>> postIncrementBeforeWAL(final Mutation mutation,
      final List<Pair<Cell, Cell>> cellPairs) throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return cellPairs;
    }
    return execOperationWithResult(
        new ObserverOperationWithResult<RegionObserver, List<Pair<Cell, Cell>>>(
            regionObserverGetter, cellPairs) {
          @Override
          public List<Pair<Cell, Cell>> call(RegionObserver observer) throws IOException {
            return observer.postIncrementBeforeWAL(this, mutation, getResult());
          }
        });
  }

  public List<Pair<Cell, Cell>> postAppendBeforeWAL(final Mutation mutation,
      final List<Pair<Cell, Cell>> cellPairs) throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return cellPairs;
    }
    return execOperationWithResult(
        new ObserverOperationWithResult<RegionObserver, List<Pair<Cell, Cell>>>(
            regionObserverGetter, cellPairs) {
          @Override
          public List<Pair<Cell, Cell>> call(RegionObserver observer) throws IOException {
            return observer.postAppendBeforeWAL(this, mutation, getResult());
          }
        });
  }

  public void preWALAppend(WALKey key, WALEdit edit) throws IOException {
    if (this.coprocEnvironments.isEmpty()){
      return;
    }
    execOperation(new RegionObserverOperationWithoutResult() {
      @Override
      public void call(RegionObserver observer) throws IOException {
        observer.preWALAppend(this, key, edit);
      }
    });
  }

  public Message preEndpointInvocation(final Service service, final String methodName,
      Message request) throws IOException {
    if (coprocEnvironments.isEmpty()) {
      return request;
    }
    return execOperationWithResult(new ObserverOperationWithResult<EndpointObserver,
        Message>(endpointObserverGetter, request) {
      @Override
      public Message call(EndpointObserver observer) throws IOException {
        return observer.preEndpointInvocation(this, service, methodName, getResult());
      }
    });
  }

  public void postEndpointInvocation(final Service service, final String methodName,
      final Message request, final Message.Builder responseBuilder) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null :
        new ObserverOperationWithoutResult<EndpointObserver>(endpointObserverGetter) {
          @Override
          public void call(EndpointObserver observer) throws IOException {
            observer.postEndpointInvocation(this, service, methodName, request, responseBuilder);
          }
        });
  }

  /**
   * @deprecated Since 2.0 with out any replacement and will be removed in 3.0
   */
  @Deprecated
  public DeleteTracker postInstantiateDeleteTracker(DeleteTracker result) throws IOException {
    if (this.coprocEnvironments.isEmpty()) {
      return result;
    }
    return execOperationWithResult(new ObserverOperationWithResult<RegionObserver, DeleteTracker>(
        regionObserverGetter, result) {
      @Override
      public DeleteTracker call(RegionObserver observer) throws IOException {
        return observer.postInstantiateDeleteTracker(this, getResult());
      }
    });
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // BulkLoadObserver hooks
  /////////////////////////////////////////////////////////////////////////////////////////////////
  public void prePrepareBulkLoad(User user) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null :
        new BulkLoadObserverOperation(user) {
          @Override protected void call(BulkLoadObserver observer) throws IOException {
            observer.prePrepareBulkLoad(this);
          }
        });
  }

  public void preCleanupBulkLoad(User user) throws IOException {
    execOperation(coprocEnvironments.isEmpty() ? null :
        new BulkLoadObserverOperation(user) {
          @Override protected void call(BulkLoadObserver observer) throws IOException {
            observer.preCleanupBulkLoad(this);
          }
        });
  }
}
