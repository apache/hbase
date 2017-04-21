/*
 *
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;

import org.apache.commons.collections.map.AbstractReferenceMap;
import org.apache.commons.collections.map.ReferenceMap;
import org.apache.commons.lang.ClassUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.EndpointObserver;
import org.apache.hadoop.hbase.coprocessor.MetricsCoprocessor;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionObserver.MutationType;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.querymatcher.DeleteTracker;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CoprocessorClassLoader;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALKey;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import com.google.protobuf.Service;

/**
 * Implements the coprocessor environment and runtime support for coprocessors
 * loaded within a {@link Region}.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public class RegionCoprocessorHost
    extends CoprocessorHost<RegionCoprocessorHost.RegionEnvironment> {

  private static final Log LOG = LogFactory.getLog(RegionCoprocessorHost.class);
  // The shared data map
  private static ReferenceMap sharedDataMap =
      new ReferenceMap(AbstractReferenceMap.HARD, AbstractReferenceMap.WEAK);

  // optimization: no need to call postScannerFilterRow, if no coprocessor implements it
  private final boolean hasCustomPostScannerFilterRow;

  /**
   * 
   * Encapsulation of the environment of each coprocessor
   */
  static class RegionEnvironment extends CoprocessorHost.Environment
      implements RegionCoprocessorEnvironment {

    private Region region;
    private RegionServerServices rsServices;
    ConcurrentMap<String, Object> sharedData;
    private final MetricRegistry metricRegistry;

    /**
     * Constructor
     * @param impl the coprocessor instance
     * @param priority chaining priority
     */
    public RegionEnvironment(final Coprocessor impl, final int priority,
        final int seq, final Configuration conf, final Region region,
        final RegionServerServices services, final ConcurrentMap<String, Object> sharedData) {
      super(impl, priority, seq, conf);
      this.region = region;
      this.rsServices = services;
      this.sharedData = sharedData;
      this.metricRegistry =
          MetricsCoprocessor.createRegistryForRegionCoprocessor(impl.getClass().getName());
    }

    /** @return the region */
    @Override
    public Region getRegion() {
      return region;
    }

    /** @return reference to the region server services */
    @Override
    public RegionServerServices getRegionServerServices() {
      return rsServices;
    }

    public void shutdown() {
      super.shutdown();
      MetricsCoprocessor.removeRegistry(this.metricRegistry);
    }

    @Override
    public ConcurrentMap<String, Object> getSharedData() {
      return sharedData;
    }

    @Override
    public HRegionInfo getRegionInfo() {
      return region.getRegionInfo();
    }

    @Override
    public MetricRegistry getMetricRegistryForRegionServer() {
      return metricRegistry;
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
  Region region;

  /**
   * Constructor
   * @param region the region
   * @param rsServices interface to available region server functionality
   * @param conf the configuration
   */
  public RegionCoprocessorHost(final Region region,
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
    out: for (RegionEnvironment env: coprocessors) {
      if (env.getInstance() instanceof RegionObserver) {
        Class<?> clazz = env.getInstance().getClass();
        for(;;) {
          if (clazz == null) {
            // we must have directly implemented RegionObserver
            hasCustomPostScannerFilterRow = true;
            break out;
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
      HTableDescriptor htd) {
    List<TableCoprocessorAttribute> result = Lists.newArrayList();
    for (Map.Entry<Bytes, Bytes> e: htd.getValues().entrySet()) {
      String key = Bytes.toString(e.getKey().get()).trim();
      if (HConstants.CP_HTD_ATTR_KEY_PATTERN.matcher(key).matches()) {
        String spec = Bytes.toString(e.getValue().get()).trim();
        // found one
        try {
          Matcher matcher = HConstants.CP_HTD_ATTR_VALUE_PATTERN.matcher(spec);
          if (matcher.matches()) {
            // jar file path can be empty if the cp class can be loaded
            // from class loader.
            Path path = matcher.group(1).trim().isEmpty() ?
                null : new Path(matcher.group(1).trim());
            String className = matcher.group(2).trim();
            if (className.isEmpty()) {
              LOG.error("Malformed table coprocessor specification: key=" +
                key + ", spec: " + spec);
              continue;
            }
            String priorityStr = matcher.group(3).trim();
            int priority = priorityStr.isEmpty() ?
                Coprocessor.PRIORITY_USER : Integer.parseInt(priorityStr);
            String cfgSpec = null;
            try {
              cfgSpec = matcher.group(4);
            } catch (IndexOutOfBoundsException ex) {
              // ignore
            }
            Configuration ourConf;
            if (cfgSpec != null && !cfgSpec.trim().equals("|")) {
              cfgSpec = cfgSpec.substring(cfgSpec.indexOf('|') + 1);
              // do an explicit deep copy of the passed configuration
              ourConf = new Configuration(false);
              HBaseConfiguration.merge(ourConf, conf);
              Matcher m = HConstants.CP_HTD_ATTR_VALUE_PARAM_PATTERN.matcher(cfgSpec);
              while (m.find()) {
                ourConf.set(m.group(1), m.group(2));
              }
            } else {
              ourConf = conf;
            }
            result.add(new TableCoprocessorAttribute(path, className, priority, ourConf));
          } else {
            LOG.error("Malformed table coprocessor specification: key=" + key +
              ", spec: " + spec);
          }
        } catch (Exception ioe) {
          LOG.error("Malformed table coprocessor specification: key=" + key +
            ", spec: " + spec);
        }
      }
    }
    return result;
  }

  /**
   * Sanity check the table coprocessor attributes of the supplied schema. Will
   * throw an exception if there is a problem.
   * @param conf
   * @param htd
   * @throws IOException
   */
  public static void testTableCoprocessorAttrs(final Configuration conf,
      final HTableDescriptor htd) throws IOException {
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
        cl.loadClass(attr.getClassName());
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
    List<RegionEnvironment> configured = new ArrayList<>();
    for (TableCoprocessorAttribute attr: getTableCoprocessorAttrsFromSchema(conf, 
        region.getTableDesc())) {
      // Load encompasses classloading and coprocessor initialization
      try {
        RegionEnvironment env = load(attr.getPath(), attr.getClassName(), attr.getPriority(),
          attr.getConf());
        configured.add(env);
        LOG.info("Loaded coprocessor " + attr.getClassName() + " from HTD of " +
            region.getTableDesc().getTableName().getNameAsString() + " successfully.");
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
    coprocessors.addAll(configured);
  }

  @Override
  public RegionEnvironment createEnvironment(Class<?> implClass,
      Coprocessor instance, int priority, int seq, Configuration conf) {
    // Check if it's an Endpoint.
    // Due to current dynamic protocol design, Endpoint
    // uses a different way to be registered and executed.
    // It uses a visitor pattern to invoke registered Endpoint
    // method.
    for (Object itf : ClassUtils.getAllInterfaces(implClass)) {
      Class<?> c = (Class<?>) itf;
      if (CoprocessorService.class.isAssignableFrom(c)) {
        region.registerService( ((CoprocessorService)instance).getService() );
      }
    }
    ConcurrentMap<String, Object> classData;
    // make sure only one thread can add maps
    synchronized (sharedDataMap) {
      // as long as at least one RegionEnvironment holds on to its classData it will
      // remain in this map
      classData = (ConcurrentMap<String, Object>)sharedDataMap.get(implClass.getName());
      if (classData == null) {
        classData = new ConcurrentHashMap<>();
        sharedDataMap.put(implClass.getName(), classData);
      }
    }
    return new RegionEnvironment(instance, priority, seq, conf, region,
        rsServices, classData);
  }

  /**
   * Invoked before a region open.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void preOpen() throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preOpen(ctx);
      }
    });
  }

  /**
   * Invoked after a region open
   */
  public void postOpen() {
    try {
      execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
        @Override
        public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
            throws IOException {
          oserver.postOpen(ctx);
        }
      });
    } catch (IOException e) {
      LOG.warn(e);
    }
  }

  /**
   * Invoked after log replay on region
   */
  public void postLogReplay() {
    try {
      execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
        @Override
        public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
            throws IOException {
          oserver.postLogReplay(ctx);
        }
      });
    } catch (IOException e) {
      LOG.warn(e);
    }
  }

  /**
   * Invoked before a region is closed
   * @param abortRequested true if the server is aborting
   */
  public void preClose(final boolean abortRequested) throws IOException {
    execOperation(false, new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preClose(ctx, abortRequested);
      }
    });
  }

  /**
   * Invoked after a region is closed
   * @param abortRequested true if the server is aborting
   */
  public void postClose(final boolean abortRequested) {
    try {
      execOperation(false, new RegionOperation() {
        @Override
        public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
            throws IOException {
          oserver.postClose(ctx, abortRequested);
        }
        public void postEnvCall(RegionEnvironment env) {
          shutdown(env);
        }
      });
    } catch (IOException e) {
      LOG.warn(e);
    }
  }

  /**
   * See
   * {@link RegionObserver#preCompactScannerOpen(ObserverContext, Store, List, ScanType, long,
   *   InternalScanner, CompactionRequest, long)}
   */
  public InternalScanner preCompactScannerOpen(final Store store,
      final List<StoreFileScanner> scanners, final ScanType scanType, final long earliestPutTs,
      final CompactionRequest request, final User user, final long readPoint) throws IOException {
    return execOperationWithResult(null,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<InternalScanner>(user) {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.preCompactScannerOpen(ctx, store, scanners, scanType,
          earliestPutTs, getResult(), request, readPoint));
      }
    });
  }

  /**
   * Called prior to selecting the {@link StoreFile}s for compaction from the list of currently
   * available candidates.
   * @param store The store where compaction is being requested
   * @param candidates The currently available store files
   * @param request custom compaction request
   * @return If {@code true}, skip the normal selection process and use the current list
   * @throws IOException
   */
  public boolean preCompactSelection(final Store store, final List<StoreFile> candidates,
      final CompactionRequest request, final User user) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new RegionOperation(user) {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preCompactSelection(ctx, store, candidates, request);
      }
    });
  }

  /**
   * Called after the {@link StoreFile}s to be compacted have been selected from the available
   * candidates.
   * @param store The store where compaction is being requested
   * @param selected The store files selected to compact
   * @param request custom compaction
   */
  public void postCompactSelection(final Store store, final ImmutableList<StoreFile> selected,
      final CompactionRequest request, final User user) {
    try {
      execOperation(coprocessors.isEmpty() ? null : new RegionOperation(user) {
        @Override
        public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
            throws IOException {
          oserver.postCompactSelection(ctx, store, selected, request);
        }
      });
    } catch (IOException e) {
      LOG.warn(e);
    }
  }

  /**
   * Called prior to rewriting the store files selected for compaction
   * @param store the store being compacted
   * @param scanner the scanner used to read store data during compaction
   * @param scanType type of Scan
   * @param request the compaction that will be executed
   * @throws IOException
   */
  public InternalScanner preCompact(final Store store, final InternalScanner scanner,
      final ScanType scanType, final CompactionRequest request, final User user)
      throws IOException {
    return execOperationWithResult(false, scanner,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<InternalScanner>(user) {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.preCompact(ctx, store, getResult(), scanType, request));
      }
    });
  }

  /**
   * Called after the store compaction has completed.
   * @param store the store being compacted
   * @param resultFile the new store file written during compaction
   * @param request the compaction that is being executed
   * @throws IOException
   */
  public void postCompact(final Store store, final StoreFile resultFile,
      final CompactionRequest request, final User user) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation(user) {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postCompact(ctx, store, resultFile, request);
      }
    });
  }

  /**
   * Invoked before a memstore flush
   * @throws IOException
   */
  public InternalScanner preFlush(final Store store, final InternalScanner scanner)
      throws IOException {
    return execOperationWithResult(false, scanner,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<InternalScanner>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.preFlush(ctx, store, getResult()));
      }
    });
  }

  /**
   * Invoked before a memstore flush
   * @throws IOException
   */
  public void preFlush() throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preFlush(ctx);
      }
    });
  }

  /**
   * See
   * {@link RegionObserver#preFlushScannerOpen(ObserverContext, Store, List, InternalScanner, long)}
   */
  public InternalScanner preFlushScannerOpen(final Store store,
      final List<KeyValueScanner> scanners, final long readPoint) throws IOException {
    return execOperationWithResult(null,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<InternalScanner>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.preFlushScannerOpen(ctx, store, scanners, getResult(), readPoint));
      }
    });
  }

  /**
   * Invoked after a memstore flush
   * @throws IOException
   */
  public void postFlush() throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postFlush(ctx);
      }
    });
  }

  /**
   * Invoked after a memstore flush
   * @throws IOException
   */
  public void postFlush(final Store store, final StoreFile storeFile) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postFlush(ctx, store, storeFile);
      }
    });
  }

  /**
   * Invoked just before a split
   * @throws IOException
   */
  @Deprecated
  public void preSplit(final User user) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation(user) {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preSplit(ctx);
      }
    });
  }

  /**
   * Invoked just before a split
   * @throws IOException
   *
   * Note: the logic moves to Master; it is unused in RS
   */
  @Deprecated
  public void preSplit(final byte[] splitRow, final User user) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation(user) {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preSplit(ctx, splitRow);
      }
    });
  }

  /**
   * Invoked just after a split
   * @param l the new left-hand daughter region
   * @param r the new right-hand daughter region
   * @throws IOException
   *
   * Note: the logic moves to Master; it is unused in RS
   */
  @Deprecated
  public void postSplit(final Region l, final Region r, final User user) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation(user) {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postSplit(ctx, l, r);
      }
    });
  }

  /**
  * Note: the logic moves to Master; it is unused in RS
  */
 @Deprecated
  public boolean preSplitBeforePONR(final byte[] splitKey,
      final List<Mutation> metaEntries, final User user) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new RegionOperation(user) {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preSplitBeforePONR(ctx, splitKey, metaEntries);
      }
    });
  }

  /**
  * Note: the logic moves to Master; it is unused in RS
  */
  @Deprecated
  public void preSplitAfterPONR(final User user) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation(user) {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preSplitAfterPONR(ctx);
      }
    });
  }

  /**
   * Invoked just before the rollback of a failed split is started
   * @throws IOException
   *
  * Note: the logic moves to Master; it is unused in RS
  */
  @Deprecated
  public void preRollBackSplit(final User user) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation(user) {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preRollBackSplit(ctx);
      }
    });
  }

  /**
   * Invoked just after the rollback of a failed split is done
   * @throws IOException
   *
  * Note: the logic moves to Master; it is unused in RS
  */
  @Deprecated
  public void postRollBackSplit(final User user) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation(user) {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postRollBackSplit(ctx);
      }
    });
  }

  /**
   * Invoked after a split is completed irrespective of a failure or success.
   * @throws IOException
   */
  public void postCompleteSplit() throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postCompleteSplit(ctx);
      }
    });
  }

  // RegionObserver support

  /**
   * @param get the Get request
   * @return true if default processing should be bypassed
   * @exception IOException Exception
   */
  public boolean preGet(final Get get, final List<Cell> results)
      throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preGetOp(ctx, get, results);
      }
    });
  }

  /**
   * @param get the Get request
   * @param results the result sett
   * @exception IOException Exception
   */
  public void postGet(final Get get, final List<Cell> results)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postGetOp(ctx, get, results);
      }
    });
  }

  /**
   * @param get the Get request
   * @return true or false to return to client if bypassing normal operation,
   * or null otherwise
   * @exception IOException Exception
   */
  public Boolean preExists(final Get get) throws IOException {
    return execOperationWithResult(true, false,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<Boolean>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.preExists(ctx, get, getResult()));
      }
    });
  }

  /**
   * @param get the Get request
   * @param exists the result returned by the region server
   * @return the result to return to the client
   * @exception IOException Exception
   */
  public boolean postExists(final Get get, boolean exists)
      throws IOException {
    return execOperationWithResult(exists,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<Boolean>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.postExists(ctx, get, getResult()));
      }
    });
  }

  /**
   * @param put The Put object
   * @param edit The WALEdit object.
   * @param durability The durability used
   * @return true if default processing should be bypassed
   * @exception IOException Exception
   */
  public boolean prePut(final Put put, final WALEdit edit, final Durability durability)
      throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.prePut(ctx, put, edit, durability);
      }
    });
  }

  /**
   * @param mutation - the current mutation
   * @param kv - the current cell
   * @param byteNow - current timestamp in bytes
   * @param get - the get that could be used
   * Note that the get only does not specify the family and qualifier that should be used
   * @return true if default processing should be bypassed
   * @exception IOException
   *              Exception
   */
  public boolean prePrepareTimeStampForDeleteVersion(final Mutation mutation,
      final Cell kv, final byte[] byteNow, final Get get) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.prePrepareTimeStampForDeleteVersion(ctx, mutation, kv, byteNow, get);
      }
    });
  }

  /**
   * @param put The Put object
   * @param edit The WALEdit object.
   * @param durability The durability used
   * @exception IOException Exception
   */
  public void postPut(final Put put, final WALEdit edit, final Durability durability)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postPut(ctx, put, edit, durability);
      }
    });
  }

  /**
   * @param delete The Delete object
   * @param edit The WALEdit object.
   * @param durability The durability used
   * @return true if default processing should be bypassed
   * @exception IOException Exception
   */
  public boolean preDelete(final Delete delete, final WALEdit edit, final Durability durability)
      throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preDelete(ctx, delete, edit, durability);
      }
    });
  }

  /**
   * @param delete The Delete object
   * @param edit The WALEdit object.
   * @param durability The durability used
   * @exception IOException Exception
   */
  public void postDelete(final Delete delete, final WALEdit edit, final Durability durability)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postDelete(ctx, delete, edit, durability);
      }
    });
  }

  /**
   * @param miniBatchOp
   * @return true if default processing should be bypassed
   * @throws IOException
   */
  public boolean preBatchMutate(
      final MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preBatchMutate(ctx, miniBatchOp);
      }
    });
  }

  /**
   * @param miniBatchOp
   * @throws IOException
   */
  public void postBatchMutate(
      final MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postBatchMutate(ctx, miniBatchOp);
      }
    });
  }

  public void postBatchMutateIndispensably(
      final MiniBatchOperationInProgress<Mutation> miniBatchOp, final boolean success)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postBatchMutateIndispensably(ctx, miniBatchOp, success);
      }
    });
  }

  /**
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param compareOp the comparison operation
   * @param comparator the comparator
   * @param put data to put if check succeeds
   * @return true or false to return to client if default processing should
   * be bypassed, or null otherwise
   * @throws IOException e
   */
  public Boolean preCheckAndPut(final byte [] row, final byte [] family,
      final byte [] qualifier, final CompareOp compareOp,
      final ByteArrayComparable comparator, final Put put)
      throws IOException {
    return execOperationWithResult(true, false,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<Boolean>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.preCheckAndPut(ctx, row, family, qualifier,
          compareOp, comparator, put, getResult()));
      }
    });
  }

  /**
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param compareOp the comparison operation
   * @param comparator the comparator
   * @param put data to put if check succeeds
   * @return true or false to return to client if default processing should
   * be bypassed, or null otherwise
   * @throws IOException e
   */
  public Boolean preCheckAndPutAfterRowLock(final byte[] row, final byte[] family,
      final byte[] qualifier, final CompareOp compareOp, final ByteArrayComparable comparator,
      final Put put) throws IOException {
    return execOperationWithResult(true, false,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<Boolean>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.preCheckAndPutAfterRowLock(ctx, row, family, qualifier,
          compareOp, comparator, put, getResult()));
      }
    });
  }

  /**
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param compareOp the comparison operation
   * @param comparator the comparator
   * @param put data to put if check succeeds
   * @throws IOException e
   */
  public boolean postCheckAndPut(final byte [] row, final byte [] family,
      final byte [] qualifier, final CompareOp compareOp,
      final ByteArrayComparable comparator, final Put put,
      boolean result) throws IOException {
    return execOperationWithResult(result,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<Boolean>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.postCheckAndPut(ctx, row, family, qualifier,
          compareOp, comparator, put, getResult()));
      }
    });
  }

  /**
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param compareOp the comparison operation
   * @param comparator the comparator
   * @param delete delete to commit if check succeeds
   * @return true or false to return to client if default processing should
   * be bypassed, or null otherwise
   * @throws IOException e
   */
  public Boolean preCheckAndDelete(final byte [] row, final byte [] family,
      final byte [] qualifier, final CompareOp compareOp,
      final ByteArrayComparable comparator, final Delete delete)
      throws IOException {
    return execOperationWithResult(true, false,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<Boolean>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.preCheckAndDelete(ctx, row, family,
            qualifier, compareOp, comparator, delete, getResult()));
      }
    });
  }

  /**
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param compareOp the comparison operation
   * @param comparator the comparator
   * @param delete delete to commit if check succeeds
   * @return true or false to return to client if default processing should
   * be bypassed, or null otherwise
   * @throws IOException e
   */
  public Boolean preCheckAndDeleteAfterRowLock(final byte[] row, final byte[] family,
      final byte[] qualifier, final CompareOp compareOp, final ByteArrayComparable comparator,
      final Delete delete) throws IOException {
    return execOperationWithResult(true, false,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<Boolean>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.preCheckAndDeleteAfterRowLock(ctx, row,
              family, qualifier, compareOp, comparator, delete, getResult()));
      }
    });
  }

  /**
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param compareOp the comparison operation
   * @param comparator the comparator
   * @param delete delete to commit if check succeeds
   * @throws IOException e
   */
  public boolean postCheckAndDelete(final byte [] row, final byte [] family,
      final byte [] qualifier, final CompareOp compareOp,
      final ByteArrayComparable comparator, final Delete delete,
      boolean result) throws IOException {
    return execOperationWithResult(result,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<Boolean>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.postCheckAndDelete(ctx, row, family,
            qualifier, compareOp, comparator, delete, getResult()));
      }
    });
  }

  /**
   * @param append append object
   * @return result to return to client if default operation should be
   * bypassed, null otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result preAppend(final Append append) throws IOException {
    return execOperationWithResult(true, null,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<Result>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.preAppend(ctx, append));
      }
    });
  }

  /**
   * @param append append object
   * @return result to return to client if default operation should be
   * bypassed, null otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result preAppendAfterRowLock(final Append append) throws IOException {
    return execOperationWithResult(true, null,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<Result>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.preAppendAfterRowLock(ctx, append));
      }
    });
  }

  /**
   * @param increment increment object
   * @return result to return to client if default operation should be
   * bypassed, null otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result preIncrement(final Increment increment) throws IOException {
    return execOperationWithResult(true, null,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<Result>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.preIncrement(ctx, increment));
      }
    });
  }

  /**
   * @param increment increment object
   * @return result to return to client if default operation should be
   * bypassed, null otherwise
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result preIncrementAfterRowLock(final Increment increment) throws IOException {
    return execOperationWithResult(true, null,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<Result>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.preIncrementAfterRowLock(ctx, increment));
      }
    });
  }

  /**
   * @param append Append object
   * @param result the result returned by the append
   * @throws IOException if an error occurred on the coprocessor
   */
  public void postAppend(final Append append, final Result result) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postAppend(ctx, append, result);
      }
    });
  }

  /**
   * @param increment increment object
   * @param result the result returned by postIncrement
   * @throws IOException if an error occurred on the coprocessor
   */
  public Result postIncrement(final Increment increment, Result result) throws IOException {
    return execOperationWithResult(result,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<Result>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.postIncrement(ctx, increment, getResult()));
      }
    });
  }

  /**
   * @param scan the Scan specification
   * @return scanner id to return to client if default operation should be
   * bypassed, false otherwise
   * @exception IOException Exception
   */
  public RegionScanner preScannerOpen(final Scan scan) throws IOException {
    return execOperationWithResult(true, null,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<RegionScanner>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.preScannerOpen(ctx, scan, getResult()));
      }
    });
  }

  /**
   * See
   * {@link RegionObserver#preStoreScannerOpen(ObserverContext,
   *    Store, Scan, NavigableSet, KeyValueScanner)}
   */
  public KeyValueScanner preStoreScannerOpen(final Store store, final Scan scan,
      final NavigableSet<byte[]> targetCols, final long readPt) throws IOException {
    return execOperationWithResult(null,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<KeyValueScanner>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.preStoreScannerOpen(ctx, store, scan, targetCols, getResult(), readPt));
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
    return execOperationWithResult(s,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<RegionScanner>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.postScannerOpen(ctx, scan, getResult()));
      }
    });
  }

  /**
   * @param s the scanner
   * @param results the result set returned by the region server
   * @param limit the maximum number of results to return
   * @return 'has next' indication to client if bypassing default behavior, or
   * null otherwise
   * @exception IOException Exception
   */
  public Boolean preScannerNext(final InternalScanner s,
      final List<Result> results, final int limit) throws IOException {
    return execOperationWithResult(true, false,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<Boolean>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.preScannerNext(ctx, s, results, limit, getResult()));
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
    return execOperationWithResult(hasMore,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<Boolean>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.postScannerNext(ctx, s, results, limit, getResult()));
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
    if (!hasCustomPostScannerFilterRow) return true;
    return execOperationWithResult(true,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<Boolean>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.postScannerFilterRow(ctx, s, curRowCell, getResult()));
      }
    });
  }

  /**
   * @param s the scanner
   * @return true if default behavior should be bypassed, false otherwise
   * @exception IOException Exception
   */
  public boolean preScannerClose(final InternalScanner s) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preScannerClose(ctx, s);
      }
    });
  }

  /**
   * @exception IOException Exception
   */
  public void postScannerClose(final InternalScanner s) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postScannerClose(ctx, s);
      }
    });
  }

  /**
   * @param info the RegionInfo for this region
   * @param edits the file of recovered edits
   * @throws IOException Exception
   */
  public void preReplayWALs(final HRegionInfo info, final Path edits) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
        throws IOException {
        oserver.preReplayWALs(ctx, info, edits);
      }
    });
  }

  /**
   * @param info the RegionInfo for this region
   * @param edits the file of recovered edits
   * @throws IOException Exception
   */
  public void postReplayWALs(final HRegionInfo info, final Path edits) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
        throws IOException {
        oserver.postReplayWALs(ctx, info, edits);
      }
    });
  }

  /**
   * @param info
   * @param logKey
   * @param logEdit
   * @return true if default behavior should be bypassed, false otherwise
   * @throws IOException
   */
  public boolean preWALRestore(final HRegionInfo info, final WALKey logKey,
      final WALEdit logEdit) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preWALRestore(ctx, info, logKey, logEdit);
      }
    });
  }

  /**
   * @param info
   * @param logKey
   * @param logEdit
   * @throws IOException
   */
  public void postWALRestore(final HRegionInfo info, final WALKey logKey, final WALEdit logEdit)
      throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postWALRestore(ctx, info, logKey, logEdit);
      }
    });
  }

  /**
   * @param familyPaths pairs of { CF, file path } submitted for bulk load
   * @return true if the default operation should be bypassed
   * @throws IOException
   */
  public boolean preBulkLoadHFile(final List<Pair<byte[], String>> familyPaths) throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preBulkLoadHFile(ctx, familyPaths);
      }
    });
  }

  public boolean preCommitStoreFile(final byte[] family, final List<Pair<Path, Path>> pairs)
      throws IOException {
    return execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.preCommitStoreFile(ctx, family, pairs);
      }
    });
  }
  public void postCommitStoreFile(final byte[] family, Path srcPath, Path dstPath) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postCommitStoreFile(ctx, family, srcPath, dstPath);
      }
    });
  }

  /**
   * @param familyPaths pairs of { CF, file path } submitted for bulk load
   * @param map Map of CF to List of file paths for the final loaded files
   * @param hasLoaded whether load was successful or not
   * @return the possibly modified value of hasLoaded
   * @throws IOException
   */
  public boolean postBulkLoadHFile(final List<Pair<byte[], String>> familyPaths,
      Map<byte[], List<Path>> map, boolean hasLoaded) throws IOException {
    return execOperationWithResult(hasLoaded,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<Boolean>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.postBulkLoadHFile(ctx, familyPaths, map, getResult()));
      }
    });
  }

  public void postStartRegionOperation(final Operation op) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postStartRegionOperation(ctx, op);
      }
    });
  }

  public void postCloseRegionOperation(final Operation op) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new RegionOperation() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postCloseRegionOperation(ctx, op);
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
    return execOperationWithResult(null,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<StoreFileReader>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.preStoreFileReaderOpen(ctx, fs, p, in, size, cacheConf, r, getResult()));
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
    return execOperationWithResult(reader,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<StoreFileReader>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.postStoreFileReaderOpen(ctx, fs, p, in, size, cacheConf, r, getResult()));
      }
    });
  }

  public Cell postMutationBeforeWAL(final MutationType opType, final Mutation mutation,
      final Cell oldCell, Cell newCell) throws IOException {
    return execOperationWithResult(newCell,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<Cell>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.postMutationBeforeWAL(ctx, opType, mutation, oldCell, getResult()));
      }
    });
  }

  public Message preEndpointInvocation(final Service service, final String methodName,
      Message request) throws IOException {
    return execOperationWithResult(request,
        coprocessors.isEmpty() ? null : new EndpointOperationWithResult<Message>() {
      @Override
      public void call(EndpointObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.preEndpointInvocation(ctx, service, methodName, getResult()));
      }
    });
  }

  public void postEndpointInvocation(final Service service, final String methodName,
      final Message request, final Message.Builder responseBuilder) throws IOException {
    execOperation(coprocessors.isEmpty() ? null : new EndpointOperation() {
      @Override
      public void call(EndpointObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        oserver.postEndpointInvocation(ctx, service, methodName, request, responseBuilder);
      }
    });
  }

  public DeleteTracker postInstantiateDeleteTracker(DeleteTracker tracker) throws IOException {
    return execOperationWithResult(tracker,
        coprocessors.isEmpty() ? null : new RegionOperationWithResult<DeleteTracker>() {
      @Override
      public void call(RegionObserver oserver, ObserverContext<RegionCoprocessorEnvironment> ctx)
          throws IOException {
        setResult(oserver.postInstantiateDeleteTracker(ctx, getResult()));
      }
    });
  }

  private static abstract class CoprocessorOperation
      extends ObserverContext<RegionCoprocessorEnvironment> {
    public CoprocessorOperation() {
      this(RpcServer.getRequestUser());
    }

    public CoprocessorOperation(User user) {
      super(user);
    }

    public abstract void call(Coprocessor observer,
        ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException;
    public abstract boolean hasCall(Coprocessor observer);
    public void postEnvCall(RegionEnvironment env) { }
  }

  private static abstract class RegionOperation extends CoprocessorOperation {
    public RegionOperation() {
    }

    public RegionOperation(User user) {
      super(user);
    }

    public abstract void call(RegionObserver observer,
        ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException;

    public boolean hasCall(Coprocessor observer) {
      return observer instanceof RegionObserver;
    }

    public void call(Coprocessor observer, ObserverContext<RegionCoprocessorEnvironment> ctx)
        throws IOException {
      call((RegionObserver)observer, ctx);
    }
  }

  private static abstract class RegionOperationWithResult<T> extends RegionOperation {
    public RegionOperationWithResult() {
    }

    public RegionOperationWithResult(User user) {
      super (user);
    }

    private T result = null;
    public void setResult(final T result) { this.result = result; }
    public T getResult() { return this.result; }
  }

  private static abstract class EndpointOperation extends CoprocessorOperation {
    public abstract void call(EndpointObserver observer,
        ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException;

    public boolean hasCall(Coprocessor observer) {
      return observer instanceof EndpointObserver;
    }

    public void call(Coprocessor observer, ObserverContext<RegionCoprocessorEnvironment> ctx)
        throws IOException {
      call((EndpointObserver)observer, ctx);
    }
  }

  private static abstract class EndpointOperationWithResult<T> extends EndpointOperation {
    private T result = null;
    public void setResult(final T result) { this.result = result; }
    public T getResult() { return this.result; }
  }

  private boolean execOperation(final CoprocessorOperation ctx)
      throws IOException {
    return execOperation(true, ctx);
  }

  private <T> T execOperationWithResult(final T defaultValue,
      final RegionOperationWithResult<T> ctx) throws IOException {
    if (ctx == null) return defaultValue;
    ctx.setResult(defaultValue);
    execOperation(true, ctx);
    return ctx.getResult();
  }

  private <T> T execOperationWithResult(final boolean ifBypass, final T defaultValue,
      final RegionOperationWithResult<T> ctx) throws IOException {
    boolean bypass = false;
    T result = defaultValue;
    if (ctx != null) {
      ctx.setResult(defaultValue);
      bypass = execOperation(true, ctx);
      result = ctx.getResult();
    }
    return bypass == ifBypass ? result : null;
  }

  private <T> T execOperationWithResult(final T defaultValue,
      final EndpointOperationWithResult<T> ctx) throws IOException {
    if (ctx == null) return defaultValue;
    ctx.setResult(defaultValue);
    execOperation(true, ctx);
    return ctx.getResult();
  }

  private boolean execOperation(final boolean earlyExit, final CoprocessorOperation ctx)
      throws IOException {
    boolean bypass = false;
    List<RegionEnvironment> envs = coprocessors.get();
    for (int i = 0; i < envs.size(); i++) {
      RegionEnvironment env = envs.get(i);
      Coprocessor observer = env.getInstance();
      if (ctx.hasCall(observer)) {
        ctx.prepare(env);
        Thread currentThread = Thread.currentThread();
        ClassLoader cl = currentThread.getContextClassLoader();
        try {
          currentThread.setContextClassLoader(env.getClassLoader());
          ctx.call(observer, ctx);
        } catch (Throwable e) {
          handleCoprocessorThrowable(env, e);
        } finally {
          currentThread.setContextClassLoader(cl);
        }
        bypass |= ctx.shouldBypass();
        if (earlyExit && ctx.shouldComplete()) {
          break;
        }
      }

      ctx.postEnvCall(env);
    }
    return bypass;
  }
}
