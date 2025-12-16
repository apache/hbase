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
package org.apache.hadoop.hbase.tool;

import static java.lang.String.format;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MetaTableName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.AsyncTableRegionLocator;
import org.apache.hadoop.hbase.client.ClusterConnectionFactory;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.hadoop.hbase.io.hfile.ReaderContext;
import org.apache.hadoop.hbase.io.hfile.ReaderContextBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.FsDelegationToken;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSVisitor;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.HashMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimaps;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The implementation for {@link BulkLoadHFiles}, and also can be executed from command line as a
 * tool.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class BulkLoadHFilesTool extends Configured implements BulkLoadHFiles, Tool {

  private static final Logger LOG = LoggerFactory.getLogger(BulkLoadHFilesTool.class);

  /**
   * Keep locality while generating HFiles for bulkload. See HBASE-12596
   */
  public static final String LOCALITY_SENSITIVE_CONF_KEY =
    "hbase.bulkload.locality.sensitive.enabled";
  private static final boolean DEFAULT_LOCALITY_SENSITIVE = true;

  public static final String NAME = "completebulkload";
  /**
   * Whether to run validation on hfiles before loading.
   */
  private static final String VALIDATE_HFILES = "hbase.loadincremental.validate.hfile";
  /**
   * HBASE-24221 Support bulkLoadHFile by family to avoid long time waiting of bulkLoadHFile because
   * of compacting at server side
   */
  public static final String BULK_LOAD_HFILES_BY_FAMILY = "hbase.mapreduce.bulkload.by.family";

  public static final String FAIL_IF_NEED_SPLIT_HFILE =
    "hbase.loadincremental.fail.if.need.split.hfile";

  // We use a '.' prefix which is ignored when walking directory trees
  // above. It is invalid family name.
  static final String TMP_DIR = ".tmp";

  private int maxFilesPerRegionPerFamily;
  private boolean assignSeqIds;
  private boolean bulkLoadByFamily;

  // Source delegation token
  private FsDelegationToken fsDelegationToken;
  private UserProvider userProvider;
  private int nrThreads;
  private final AtomicInteger numRetries = new AtomicInteger(0);
  private String bulkToken;

  private List<String> clusterIds = new ArrayList<>();
  private boolean replicate = true;
  private boolean failIfNeedSplitHFile = false;

  public BulkLoadHFilesTool(Configuration conf) {
    // make a copy, just to be sure we're not overriding someone else's config
    super(new Configuration(conf));
    initialize();
  }

  public void initialize() {
    Configuration conf = getConf();
    // disable blockcache for tool invocation, see HBASE-10500
    conf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0);
    userProvider = UserProvider.instantiate(conf);
    fsDelegationToken = new FsDelegationToken(userProvider, "renewer");
    assignSeqIds = conf.getBoolean(ASSIGN_SEQ_IDS, true);
    maxFilesPerRegionPerFamily = conf.getInt(MAX_FILES_PER_REGION_PER_FAMILY, 32);
    nrThreads =
      conf.getInt("hbase.loadincremental.threads.max", Runtime.getRuntime().availableProcessors());
    bulkLoadByFamily = conf.getBoolean(BULK_LOAD_HFILES_BY_FAMILY, false);
    failIfNeedSplitHFile = conf.getBoolean(FAIL_IF_NEED_SPLIT_HFILE, false);
  }

  // Initialize a thread pool
  private ExecutorService createExecutorService() {
    ThreadPoolExecutor pool = new ThreadPoolExecutor(nrThreads, nrThreads, 60, TimeUnit.SECONDS,
      new LinkedBlockingQueue<>(),
      new ThreadFactoryBuilder().setNameFormat("BulkLoadHFilesTool-%1$d").setDaemon(true).build());
    pool.allowCoreThreadTimeOut(true);
    return pool;
  }

  private boolean isCreateTable() {
    return "yes".equalsIgnoreCase(getConf().get(CREATE_TABLE_CONF_KEY, "yes"));
  }

  private boolean isSilence() {
    return "yes".equalsIgnoreCase(getConf().get(IGNORE_UNMATCHED_CF_CONF_KEY, ""));
  }

  private boolean isAlwaysCopyFiles() {
    return getConf().getBoolean(ALWAYS_COPY_FILES, false);
  }

  private static boolean shouldCopyHFileMetaKey(byte[] key) {
    // skip encoding to keep hfile meta consistent with data block info, see HBASE-15085
    if (Bytes.equals(key, HFileDataBlockEncoder.DATA_BLOCK_ENCODING)) {
      return false;
    }

    return !HFileInfo.isReservedFileInfoKey(key);
  }

  /**
   * Checks whether there is any invalid family name in HFiles to be bulk loaded.
   */
  private static void validateFamiliesInHFiles(TableDescriptor tableDesc,
    Deque<LoadQueueItem> queue, boolean silence) throws IOException {
    Set<String> familyNames = Arrays.stream(tableDesc.getColumnFamilies())
      .map(ColumnFamilyDescriptor::getNameAsString).collect(Collectors.toSet());
    List<String> unmatchedFamilies = queue.stream().map(item -> Bytes.toString(item.getFamily()))
      .filter(fn -> !familyNames.contains(fn)).distinct().collect(Collectors.toList());
    if (unmatchedFamilies.size() > 0) {
      String msg =
        "Unmatched family names found: unmatched family names in HFiles to be bulkloaded: "
          + unmatchedFamilies + "; valid family names of table " + tableDesc.getTableName()
          + " are: " + familyNames;
      LOG.error(msg);
      if (!silence) {
        throw new IOException(msg);
      }
    }
  }

  /**
   * Populate the Queue with given HFiles
   */
  private static void populateLoadQueue(Deque<LoadQueueItem> ret, Map<byte[], List<Path>> map) {
    map.forEach((k, v) -> v.stream().map(p -> new LoadQueueItem(k, p)).forEachOrdered(ret::add));
  }

  private interface BulkHFileVisitor<TFamily> {

    TFamily bulkFamily(byte[] familyName) throws IOException;

    void bulkHFile(TFamily family, FileStatus hfileStatus) throws IOException;
  }

  /**
   * Iterate over the bulkDir hfiles. Skip reference, HFileLink, files starting with "_". Check and
   * skip non-valid hfiles by default, or skip this validation by setting {@link #VALIDATE_HFILES}
   * to false.
   */
  private static <TFamily> void visitBulkHFiles(FileSystem fs, Path bulkDir,
    BulkHFileVisitor<TFamily> visitor, boolean validateHFile) throws IOException {
    FileStatus[] familyDirStatuses = fs.listStatus(bulkDir);
    for (FileStatus familyStat : familyDirStatuses) {
      if (!familyStat.isDirectory()) {
        LOG.warn("Skipping non-directory " + familyStat.getPath());
        continue;
      }
      Path familyDir = familyStat.getPath();
      byte[] familyName = Bytes.toBytes(familyDir.getName());
      // Skip invalid family
      try {
        ColumnFamilyDescriptorBuilder.isLegalColumnFamilyName(familyName);
      } catch (IllegalArgumentException e) {
        LOG.warn("Skipping invalid " + familyStat.getPath());
        continue;
      }
      TFamily family = visitor.bulkFamily(familyName);

      FileStatus[] hfileStatuses = fs.listStatus(familyDir);
      for (FileStatus hfileStatus : hfileStatuses) {
        if (!fs.isFile(hfileStatus.getPath())) {
          LOG.warn("Skipping non-file " + hfileStatus);
          continue;
        }

        Path hfile = hfileStatus.getPath();
        // Skip "_", reference, HFileLink
        String fileName = hfile.getName();
        if (fileName.startsWith("_")) {
          continue;
        }
        if (StoreFileInfo.isReference(fileName)) {
          LOG.warn("Skipping reference " + fileName);
          continue;
        }
        if (HFileLink.isHFileLink(fileName)) {
          LOG.warn("Skipping HFileLink " + fileName);
          continue;
        }

        // Validate HFile Format if needed
        if (validateHFile) {
          try {
            if (!HFile.isHFileFormat(fs, hfile)) {
              LOG.warn("the file " + hfile + " doesn't seems to be an hfile. skipping");
              continue;
            }
          } catch (FileNotFoundException e) {
            LOG.warn("the file " + hfile + " was removed");
            continue;
          }
        }

        visitor.bulkHFile(family, hfileStatus);
      }
    }
  }

  /**
   * Walk the given directory for all HFiles, and return a Queue containing all such files.
   */
  private static void discoverLoadQueue(Configuration conf, Deque<LoadQueueItem> ret, Path hfofDir,
    boolean validateHFile) throws IOException {
    visitBulkHFiles(hfofDir.getFileSystem(conf), hfofDir, new BulkHFileVisitor<byte[]>() {
      @Override
      public byte[] bulkFamily(final byte[] familyName) {
        return familyName;
      }

      @Override
      public void bulkHFile(final byte[] family, final FileStatus hfile) {
        long length = hfile.getLen();
        if (
          length > conf.getLong(HConstants.HREGION_MAX_FILESIZE, HConstants.DEFAULT_MAX_FILE_SIZE)
        ) {
          LOG.warn("Trying to bulk load hfile " + hfile.getPath() + " with size: " + length
            + " bytes can be problematic as it may lead to oversplitting.");
        }
        ret.add(new LoadQueueItem(family, hfile.getPath()));
      }
    }, validateHFile);
  }

  /**
   * Prepare a collection of {@code LoadQueueItem} from list of source hfiles contained in the
   * passed directory and validates whether the prepared queue has all the valid table column
   * families in it.
   * @param map       map of family to List of hfiles
   * @param tableName table to which hfiles should be loaded
   * @param queue     queue which needs to be loaded into the table
   * @param silence   true to ignore unmatched column families
   * @throws IOException If any I/O or network error occurred
   */
  public static void prepareHFileQueue(AsyncClusterConnection conn, TableName tableName,
    Map<byte[], List<Path>> map, Deque<LoadQueueItem> queue, boolean silence) throws IOException {
    populateLoadQueue(queue, map);
    validateFamiliesInHFiles(FutureUtils.get(conn.getAdmin().getDescriptor(tableName)), queue,
      silence);
  }

  /**
   * Prepare a collection of {@code LoadQueueItem} from list of source hfiles contained in the
   * passed directory and validates whether the prepared queue has all the valid table column
   * families in it.
   * @param hfilesDir     directory containing list of hfiles to be loaded into the table
   * @param queue         queue which needs to be loaded into the table
   * @param validateHFile if true hfiles will be validated for its format
   * @param silence       true to ignore unmatched column families
   * @throws IOException If any I/O or network error occurred
   */
  public static void prepareHFileQueue(Configuration conf, AsyncClusterConnection conn,
    TableName tableName, Path hfilesDir, Deque<LoadQueueItem> queue, boolean validateHFile,
    boolean silence) throws IOException {
    discoverLoadQueue(conf, queue, hfilesDir, validateHFile);
    validateFamiliesInHFiles(FutureUtils.get(conn.getAdmin().getDescriptor(tableName)), queue,
      silence);
  }

  /**
   * Used by the replication sink to load the hfiles from the source cluster. It does the following,
   * <ol>
   * <li>{@link #groupOrSplitPhase(AsyncClusterConnection, TableName, ExecutorService, Deque, List)}
   * </li>
   * <li>{@link #bulkLoadPhase(AsyncClusterConnection, TableName, Deque, Multimap, boolean, Map)}
   * </li>
   * </ol>
   * @param conn      Connection to use
   * @param tableName Table to which these hfiles should be loaded to
   * @param queue     {@code LoadQueueItem} has hfiles yet to be loaded
   */
  public void loadHFileQueue(AsyncClusterConnection conn, TableName tableName,
    Deque<LoadQueueItem> queue, boolean copyFiles) throws IOException {
    ExecutorService pool = createExecutorService();
    try {
      Multimap<ByteBuffer, LoadQueueItem> regionGroups = groupOrSplitPhase(conn, tableName, pool,
        queue, FutureUtils.get(conn.getRegionLocator(tableName).getStartEndKeys())).getFirst();
      bulkLoadPhase(conn, tableName, queue, regionGroups, copyFiles, null);
    } finally {
      pool.shutdown();
    }
  }

  /**
   * Attempts to do an atomic load of many hfiles into a region. If it fails, it returns a list of
   * hfiles that need to be retried. If it is successful it will return an empty list. NOTE: To
   * maintain row atomicity guarantees, region server side should succeed atomically and fails
   * atomically.
   * @param conn      Connection to use
   * @param tableName Table to which these hfiles should be loaded to
   * @param copyFiles whether replicate to peer cluster while bulkloading
   * @param first     the start key of region
   * @param lqis      hfiles should be loaded
   * @return empty list if success, list of items to retry on recoverable failure
   */
  @InterfaceAudience.Private
  protected CompletableFuture<Collection<LoadQueueItem>> tryAtomicRegionLoad(
    final AsyncClusterConnection conn, final TableName tableName, boolean copyFiles,
    final byte[] first, Collection<LoadQueueItem> lqis) {
    List<Pair<byte[], String>> familyPaths =
      lqis.stream().map(lqi -> Pair.newPair(lqi.getFamily(), lqi.getFilePath().toString()))
        .collect(Collectors.toList());
    CompletableFuture<Collection<LoadQueueItem>> future = new CompletableFuture<>();
    FutureUtils.addListener(conn.bulkLoad(tableName, familyPaths, first, assignSeqIds,
      fsDelegationToken.getUserToken(), bulkToken, copyFiles, clusterIds, replicate),
      (loaded, error) -> {
        if (error != null) {
          LOG.error("Encountered unrecoverable error from region server", error);
          if (
            getConf().getBoolean(RETRY_ON_IO_EXCEPTION, false)
              && numRetries.get() < getConf().getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
                HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER)
          ) {
            LOG.warn("Will attempt to retry loading failed HFiles. Retry #"
              + numRetries.incrementAndGet());
            // return lqi's to retry
            future.complete(lqis);
          } else {
            LOG.error(RETRY_ON_IO_EXCEPTION
              + " is disabled or we have reached retry limit. Unable to recover");
            future.completeExceptionally(error);
          }
        } else {
          if (loaded) {
            future.complete(Collections.emptyList());
          } else {
            LOG.warn("Attempt to bulk load region containing " + Bytes.toStringBinary(first)
              + " into table " + tableName + " with files " + lqis
              + " failed.  This is recoverable and they will be retried.");
            // return lqi's to retry
            future.complete(lqis);
          }
        }
      });
    return future;
  }

  /**
   * This takes the LQI's grouped by likely regions and attempts to bulk load them. Any failures are
   * re-queued for another pass with the groupOrSplitPhase.
   * <p/>
   * protected for testing.
   */
  @InterfaceAudience.Private
  protected void bulkLoadPhase(AsyncClusterConnection conn, TableName tableName,
    Deque<LoadQueueItem> queue, Multimap<ByteBuffer, LoadQueueItem> regionGroups, boolean copyFiles,
    Map<LoadQueueItem, ByteBuffer> item2RegionMap) throws IOException {
    // atomically bulk load the groups.
    List<Future<Collection<LoadQueueItem>>> loadingFutures = new ArrayList<>();
    for (Entry<ByteBuffer, ? extends Collection<LoadQueueItem>> entry : regionGroups.asMap()
      .entrySet()) {
      byte[] first = entry.getKey().array();
      final Collection<LoadQueueItem> lqis = entry.getValue();
      if (bulkLoadByFamily) {
        groupByFamilies(lqis).values().forEach(familyQueue -> loadingFutures
          .add(tryAtomicRegionLoad(conn, tableName, copyFiles, first, familyQueue)));
      } else {
        loadingFutures.add(tryAtomicRegionLoad(conn, tableName, copyFiles, first, lqis));
      }
      if (item2RegionMap != null) {
        for (LoadQueueItem lqi : lqis) {
          item2RegionMap.put(lqi, entry.getKey());
        }
      }
    }

    // get all the results.
    for (Future<Collection<LoadQueueItem>> future : loadingFutures) {
      try {
        Collection<LoadQueueItem> toRetry = future.get();

        if (item2RegionMap != null) {
          for (LoadQueueItem lqi : toRetry) {
            item2RegionMap.remove(lqi);
          }
        }
        // LQIs that are requeued to be regrouped.
        queue.addAll(toRetry);
      } catch (ExecutionException e1) {
        Throwable t = e1.getCause();
        if (t instanceof IOException) {
          // At this point something unrecoverable has happened.
          // TODO Implement bulk load recovery
          throw new IOException("BulkLoad encountered an unrecoverable problem", t);
        }
        LOG.error("Unexpected execution exception during bulk load", e1);
        throw new IllegalStateException(t);
      } catch (InterruptedException e1) {
        LOG.error("Unexpected interrupted exception during bulk load", e1);
        throw (InterruptedIOException) new InterruptedIOException().initCause(e1);
      }
    }
  }

  private Map<byte[], Collection<LoadQueueItem>>
    groupByFamilies(Collection<LoadQueueItem> itemsInRegion) {
    Map<byte[], Collection<LoadQueueItem>> families2Queue = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    itemsInRegion.forEach(item -> families2Queue
      .computeIfAbsent(item.getFamily(), queue -> new ArrayList<>()).add(item));
    return families2Queue;
  }

  private boolean
    checkHFilesCountPerRegionPerFamily(final Multimap<ByteBuffer, LoadQueueItem> regionGroups) {
    for (Map.Entry<ByteBuffer, Collection<LoadQueueItem>> e : regionGroups.asMap().entrySet()) {
      Map<byte[], MutableInt> filesMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (LoadQueueItem lqi : e.getValue()) {
        MutableInt count = filesMap.computeIfAbsent(lqi.getFamily(), k -> new MutableInt());
        count.increment();
        if (count.intValue() > maxFilesPerRegionPerFamily) {
          LOG.error("Trying to load more than " + maxFilesPerRegionPerFamily + " hfiles to family "
            + Bytes.toStringBinary(lqi.getFamily()) + " of region with start key "
            + Bytes.toStringBinary(e.getKey()));
          return false;
        }
      }
    }
    return true;
  }

  /**
   * @param conn         the HBase cluster connection
   * @param tableName    the table name of the table to load into
   * @param pool         the ExecutorService
   * @param queue        the queue for LoadQueueItem
   * @param startEndKeys start and end keys
   * @return A map that groups LQI by likely bulk load region targets and Set of missing hfiles.
   */
  private Pair<Multimap<ByteBuffer, LoadQueueItem>, Set<String>> groupOrSplitPhase(
    AsyncClusterConnection conn, TableName tableName, ExecutorService pool,
    Deque<LoadQueueItem> queue, List<Pair<byte[], byte[]>> startEndKeys) throws IOException {
    // <region start key, LQI> need synchronized only within this scope of this
    // phase because of the puts that happen in futures.
    Multimap<ByteBuffer, LoadQueueItem> rgs = HashMultimap.create();
    final Multimap<ByteBuffer, LoadQueueItem> regionGroups = Multimaps.synchronizedMultimap(rgs);
    Set<String> missingHFiles = new HashSet<>();
    Pair<Multimap<ByteBuffer, LoadQueueItem>, Set<String>> pair =
      new Pair<>(regionGroups, missingHFiles);

    // drain LQIs and figure out bulk load groups
    Set<Future<Pair<List<LoadQueueItem>, String>>> splittingFutures = new HashSet<>();
    while (!queue.isEmpty()) {
      final LoadQueueItem item = queue.remove();
      final Callable<Pair<List<LoadQueueItem>, String>> call =
        () -> groupOrSplit(conn, tableName, regionGroups, item, startEndKeys);
      splittingFutures.add(pool.submit(call));
    }
    // get all the results. All grouping and splitting must finish before
    // we can attempt the atomic loads.
    for (Future<Pair<List<LoadQueueItem>, String>> lqis : splittingFutures) {
      try {
        Pair<List<LoadQueueItem>, String> splits = lqis.get();
        if (splits != null) {
          if (splits.getFirst() != null) {
            queue.addAll(splits.getFirst());
          } else {
            missingHFiles.add(splits.getSecond());
          }
        }
      } catch (ExecutionException e1) {
        Throwable t = e1.getCause();
        if (t instanceof IOException) {
          LOG.error("IOException during splitting", e1);
          throw (IOException) t; // would have been thrown if not parallelized,
        }
        LOG.error("Unexpected execution exception during splitting", e1);
        throw new IllegalStateException(t);
      } catch (InterruptedException e1) {
        LOG.error("Unexpected interrupted exception during splitting", e1);
        throw (InterruptedIOException) new InterruptedIOException().initCause(e1);
      }
    }
    return pair;
  }

  // unique file name for the table
  private String getUniqueName() {
    return UUID.randomUUID().toString().replaceAll("-", "");
  }

  private List<LoadQueueItem> splitStoreFile(AsyncTableRegionLocator loc, LoadQueueItem item,
    TableDescriptor tableDesc, byte[] splitKey) throws IOException {
    Path hfilePath = item.getFilePath();
    byte[] family = item.getFamily();
    Path tmpDir = hfilePath.getParent();
    if (!tmpDir.getName().equals(TMP_DIR)) {
      tmpDir = new Path(tmpDir, TMP_DIR);
    }

    LOG.info("HFile at " + hfilePath + " no longer fits inside a single " + "region. Splitting...");

    String uniqueName = getUniqueName();
    ColumnFamilyDescriptor familyDesc = tableDesc.getColumnFamily(family);

    Path botOut = new Path(tmpDir, uniqueName + ".bottom");
    Path topOut = new Path(tmpDir, uniqueName + ".top");

    splitStoreFile(loc, getConf(), hfilePath, familyDesc, splitKey, botOut, topOut);

    FileSystem fs = tmpDir.getFileSystem(getConf());
    fs.setPermission(tmpDir, FsPermission.valueOf("-rwxrwxrwx"));
    fs.setPermission(botOut, FsPermission.valueOf("-rwxrwxrwx"));
    fs.setPermission(topOut, FsPermission.valueOf("-rwxrwxrwx"));

    // Add these back at the *front* of the queue, so there's a lower
    // chance that the region will just split again before we get there.
    List<LoadQueueItem> lqis = new ArrayList<>(2);
    lqis.add(new LoadQueueItem(family, botOut));
    lqis.add(new LoadQueueItem(family, topOut));

    // If the current item is already the result of previous splits,
    // we don't need it anymore. Clean up to save space.
    // It is not part of the original input files.
    try {
      if (tmpDir.getName().equals(TMP_DIR)) {
        fs.delete(hfilePath, false);
      }
    } catch (IOException e) {
      LOG.warn("Unable to delete temporary split file " + hfilePath);
    }
    LOG.info("Successfully split into new HFiles " + botOut + " and " + topOut);
    return lqis;
  }

  /**
   * @param startEndKeys the start/end keys of regions belong to this table, the list in ascending
   *                     order by start key
   * @param key          the key need to find which region belong to
   * @return region index
   */
  private int getRegionIndex(List<Pair<byte[], byte[]>> startEndKeys, byte[] key) {
    int idx = Collections.binarySearch(startEndKeys, Pair.newPair(key, HConstants.EMPTY_END_ROW),
      (p1, p2) -> Bytes.compareTo(p1.getFirst(), p2.getFirst()));
    if (idx < 0) {
      // not on boundary, returns -(insertion index). Calculate region it
      // would be in.
      idx = -(idx + 1) - 1;
    }
    return idx;
  }

  /**
   * we can consider there is a region hole or overlap in following conditions. 1) if idx < 0,then
   * first region info is lost. 2) if the endkey of a region is not equal to the startkey of the
   * next region. 3) if the endkey of the last region is not empty.
   */
  private void checkRegionIndexValid(int idx, List<Pair<byte[], byte[]>> startEndKeys,
    TableName tableName) throws IOException {
    if (idx < 0) {
      throw new IOException("The first region info for table " + tableName + " can't be found in "
        + MetaTableName.getInstance() + ". Please use hbck tool to fix it" + " first.");
    } else if (
      (idx == startEndKeys.size() - 1)
        && !Bytes.equals(startEndKeys.get(idx).getSecond(), HConstants.EMPTY_BYTE_ARRAY)
    ) {
      throw new IOException("The last region info for table " + tableName + " can't be found in "
        + MetaTableName.getInstance() + ". Please use hbck tool to fix it" + " first.");
    } else if (
      idx + 1 < startEndKeys.size() && !(Bytes.compareTo(startEndKeys.get(idx).getSecond(),
        startEndKeys.get(idx + 1).getFirst()) == 0)
    ) {
      throw new IOException("The endkey of one region for table " + tableName
        + " is not equal to the startkey of the next region in " + MetaTableName.getInstance() + "."
        + " Please use hbck tool to fix it first.");
    }
  }

  /**
   * Attempt to assign the given load queue item into its target region group. If the hfile boundary
   * no longer fits into a region, physically splits the hfile such that the new bottom half will
   * fit and returns the list of LQI's corresponding to the resultant hfiles.
   * <p/>
   * protected for testing
   * @throws IOException if an IO failure is encountered
   */
  @InterfaceAudience.Private
  protected Pair<List<LoadQueueItem>, String> groupOrSplit(AsyncClusterConnection conn,
    TableName tableName, Multimap<ByteBuffer, LoadQueueItem> regionGroups, LoadQueueItem item,
    List<Pair<byte[], byte[]>> startEndKeys) throws IOException {
    Path hfilePath = item.getFilePath();
    Optional<byte[]> first, last;
    try (HFile.Reader hfr = HFile.createReader(hfilePath.getFileSystem(getConf()), hfilePath,
      CacheConfig.DISABLED, true, getConf())) {
      first = hfr.getFirstRowKey();
      last = hfr.getLastRowKey();
    } catch (FileNotFoundException fnfe) {
      LOG.debug("encountered", fnfe);
      return new Pair<>(null, hfilePath.getName());
    }

    LOG.info("Trying to load hfile=" + hfilePath + " first=" + first.map(Bytes::toStringBinary)
      + " last=" + last.map(Bytes::toStringBinary));
    if (!first.isPresent() || !last.isPresent()) {
      assert !first.isPresent() && !last.isPresent();
      // TODO what if this is due to a bad HFile?
      LOG.info("hfile " + hfilePath + " has no entries, skipping");
      return null;
    }
    if (Bytes.compareTo(first.get(), last.get()) > 0) {
      throw new IllegalArgumentException("Invalid range: " + Bytes.toStringBinary(first.get())
        + " > " + Bytes.toStringBinary(last.get()));
    }
    int firstKeyRegionIdx = getRegionIndex(startEndKeys, first.get());
    checkRegionIndexValid(firstKeyRegionIdx, startEndKeys, tableName);
    boolean lastKeyInRange =
      Bytes.compareTo(last.get(), startEndKeys.get(firstKeyRegionIdx).getSecond()) < 0 || Bytes
        .equals(startEndKeys.get(firstKeyRegionIdx).getSecond(), HConstants.EMPTY_BYTE_ARRAY);
    if (!lastKeyInRange) {
      if (failIfNeedSplitHFile) {
        throw new IOException(
          "The key range of hfile=" + hfilePath + " fits into no region. " + "And because "
            + FAIL_IF_NEED_SPLIT_HFILE + " was set to true, we just skip the next steps.");
      }
      int lastKeyRegionIdx = getRegionIndex(startEndKeys, last.get());
      int splitIdx = (firstKeyRegionIdx + lastKeyRegionIdx) / 2;
      // make sure the splitPoint is valid in case region overlap occur, maybe the splitPoint bigger
      // than hfile.endkey w/o this check
      if (splitIdx != firstKeyRegionIdx) {
        checkRegionIndexValid(splitIdx, startEndKeys, tableName);
      }
      byte[] splitPoint = startEndKeys.get(splitIdx).getSecond();
      List<LoadQueueItem> lqis = splitStoreFile(conn.getRegionLocator(tableName), item,
        FutureUtils.get(conn.getAdmin().getDescriptor(tableName)), splitPoint);

      return new Pair<>(lqis, null);
    }

    // group regions.
    regionGroups.put(ByteBuffer.wrap(startEndKeys.get(firstKeyRegionIdx).getFirst()), item);
    return null;
  }

  /**
   * Split a storefile into a top and bottom half with favored nodes, maintaining the metadata,
   * recreating bloom filters, etc.
   */
  @InterfaceAudience.Private
  static void splitStoreFile(AsyncTableRegionLocator loc, Configuration conf, Path inFile,
    ColumnFamilyDescriptor familyDesc, byte[] splitKey, Path bottomOut, Path topOut)
    throws IOException {
    // Open reader with no block cache, and not in-memory
    Reference topReference = Reference.createTopReference(splitKey);
    Reference bottomReference = Reference.createBottomReference(splitKey);

    copyHFileHalf(conf, inFile, topOut, topReference, familyDesc, loc);
    copyHFileHalf(conf, inFile, bottomOut, bottomReference, familyDesc, loc);
  }

  private static StoreFileWriter initStoreFileWriter(Configuration conf, Cell cell,
    HFileContext hFileContext, CacheConfig cacheConf, BloomType bloomFilterType, FileSystem fs,
    Path outFile, AsyncTableRegionLocator loc) throws IOException {
    if (conf.getBoolean(LOCALITY_SENSITIVE_CONF_KEY, DEFAULT_LOCALITY_SENSITIVE)) {
      byte[] rowKey = CellUtil.cloneRow(cell);
      HRegionLocation hRegionLocation = FutureUtils.get(loc.getRegionLocation(rowKey));
      InetSocketAddress[] favoredNodes = null;
      if (null == hRegionLocation) {
        LOG.warn("Failed get region location for  rowkey {} , Using writer without favoured nodes.",
          Bytes.toString(rowKey));
        return new StoreFileWriter.Builder(conf, cacheConf, fs).withFilePath(outFile)
          .withBloomType(bloomFilterType).withFileContext(hFileContext).build();
      } else {
        LOG.debug("First rowkey: [{}]", Bytes.toString(rowKey));
        InetSocketAddress initialIsa =
          new InetSocketAddress(hRegionLocation.getHostname(), hRegionLocation.getPort());
        if (initialIsa.isUnresolved()) {
          LOG.warn("Failed get location for region {} , Using writer without favoured nodes.",
            hRegionLocation);
          return new StoreFileWriter.Builder(conf, cacheConf, fs).withFilePath(outFile)
            .withBloomType(bloomFilterType).withFileContext(hFileContext).build();
        } else {
          LOG.debug("Use favored nodes writer: {}", initialIsa.getHostString());
          favoredNodes = new InetSocketAddress[] { initialIsa };
          return new StoreFileWriter.Builder(conf, cacheConf, fs).withFilePath(outFile)
            .withBloomType(bloomFilterType).withFileContext(hFileContext)
            .withFavoredNodes(favoredNodes).build();
        }
      }
    } else {
      return new StoreFileWriter.Builder(conf, cacheConf, fs).withFilePath(outFile)
        .withBloomType(bloomFilterType).withFileContext(hFileContext).build();
    }
  }

  /**
   * Copy half of an HFile into a new HFile with favored nodes.
   */
  private static void copyHFileHalf(Configuration conf, Path inFile, Path outFile,
    Reference reference, ColumnFamilyDescriptor familyDescriptor, AsyncTableRegionLocator loc)
    throws IOException {
    FileSystem fs = inFile.getFileSystem(conf);
    CacheConfig cacheConf = CacheConfig.DISABLED;
    StoreFileReader halfReader = null;
    StoreFileWriter halfWriter = null;
    try {
      ReaderContext context = new ReaderContextBuilder().withFileSystemAndPath(fs, inFile).build();
      StoreFileInfo storeFileInfo =
        new StoreFileInfo(conf, fs, fs.getFileStatus(inFile), reference);
      storeFileInfo.initHFileInfo(context);
      halfReader = storeFileInfo.createReader(context, cacheConf);
      storeFileInfo.getHFileInfo().initMetaAndIndex(halfReader.getHFileReader());
      Map<byte[], byte[]> fileInfo = halfReader.loadFileInfo();

      int blocksize = familyDescriptor.getBlocksize();
      Algorithm compression = familyDescriptor.getCompressionType();
      BloomType bloomFilterType = familyDescriptor.getBloomFilterType();
      HFileContext hFileContext = new HFileContextBuilder().withCompression(compression)
        .withChecksumType(StoreUtils.getChecksumType(conf))
        .withBytesPerCheckSum(StoreUtils.getBytesPerChecksum(conf)).withBlockSize(blocksize)
        .withDataBlockEncoding(familyDescriptor.getDataBlockEncoding()).withIncludesTags(true)
        .withCreateTime(EnvironmentEdgeManager.currentTime()).build();

      try (StoreFileScanner scanner =
        halfReader.getStoreFileScanner(false, false, false, Long.MAX_VALUE, 0, false)) {
        scanner.seek(KeyValue.LOWESTKEY);
        for (;;) {
          ExtendedCell cell = scanner.next();
          if (cell == null) {
            break;
          }
          if (halfWriter == null) {
            // init halfwriter
            halfWriter = initStoreFileWriter(conf, cell, hFileContext, cacheConf, bloomFilterType,
              fs, outFile, loc);
          }
          halfWriter.append(cell);
        }
      }
      for (Map.Entry<byte[], byte[]> entry : fileInfo.entrySet()) {
        if (shouldCopyHFileMetaKey(entry.getKey())) {
          halfWriter.appendFileInfo(entry.getKey(), entry.getValue());
        }
      }
    } finally {
      if (halfReader != null) {
        try {
          halfReader.close(cacheConf.shouldEvictOnClose());
        } catch (IOException e) {
          LOG.warn("failed to close hfile reader for " + inFile, e);
        }
      }
      if (halfWriter != null) {
        halfWriter.close();
      }
    }
  }

  /**
   * Infers region boundaries for a new table.
   * <p/>
   * Parameter: <br/>
   * bdryMap is a map between keys to an integer belonging to {+1, -1}
   * <ul>
   * <li>If a key is a start key of a file, then it maps to +1</li>
   * <li>If a key is an end key of a file, then it maps to -1</li>
   * </ul>
   * <p>
   * Algo:<br/>
   * <ol>
   * <li>Poll on the keys in order:
   * <ol type="a">
   * <li>Keep adding the mapped values to these keys (runningSum)</li>
   * <li>Each time runningSum reaches 0, add the start Key from when the runningSum had started to a
   * boundary list.</li>
   * </ol>
   * </li>
   * <li>Return the boundary list.</li>
   * </ol>
   */
  public static byte[][] inferBoundaries(SortedMap<byte[], Integer> bdryMap) {
    List<byte[]> keysArray = new ArrayList<>();
    int runningValue = 0;
    byte[] currStartKey = null;
    boolean firstBoundary = true;

    for (Map.Entry<byte[], Integer> item : bdryMap.entrySet()) {
      if (runningValue == 0) {
        currStartKey = item.getKey();
      }
      runningValue += item.getValue();
      if (runningValue == 0) {
        if (!firstBoundary) {
          keysArray.add(currStartKey);
        }
        firstBoundary = false;
      }
    }

    return keysArray.toArray(new byte[0][]);
  }

  /**
   * If the table is created for the first time, then "completebulkload" reads the files twice. More
   * modifications necessary if we want to avoid doing it.
   */
  private void createTable(TableName tableName, Path hfofDir, AsyncAdmin admin) throws IOException {
    final FileSystem fs = hfofDir.getFileSystem(getConf());

    // Add column families
    // Build a set of keys
    List<ColumnFamilyDescriptorBuilder> familyBuilders = new ArrayList<>();
    SortedMap<byte[], Integer> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    visitBulkHFiles(fs, hfofDir, new BulkHFileVisitor<ColumnFamilyDescriptorBuilder>() {
      @Override
      public ColumnFamilyDescriptorBuilder bulkFamily(byte[] familyName) {
        ColumnFamilyDescriptorBuilder builder =
          ColumnFamilyDescriptorBuilder.newBuilder(familyName);
        familyBuilders.add(builder);
        return builder;
      }

      @Override
      public void bulkHFile(ColumnFamilyDescriptorBuilder builder, FileStatus hfileStatus)
        throws IOException {
        Path hfile = hfileStatus.getPath();
        try (HFile.Reader reader =
          HFile.createReader(fs, hfile, CacheConfig.DISABLED, true, getConf())) {
          if (builder.getCompressionType() != reader.getFileContext().getCompression()) {
            builder.setCompressionType(reader.getFileContext().getCompression());
            LOG.info("Setting compression " + reader.getFileContext().getCompression().name()
              + " for family " + builder.getNameAsString());
          }
          byte[] first = reader.getFirstRowKey().get();
          byte[] last = reader.getLastRowKey().get();

          LOG.info("Trying to figure out region boundaries hfile=" + hfile + " first="
            + Bytes.toStringBinary(first) + " last=" + Bytes.toStringBinary(last));

          // To eventually infer start key-end key boundaries
          Integer value = map.getOrDefault(first, 0);
          map.put(first, value + 1);

          value = map.containsKey(last) ? map.get(last) : 0;
          map.put(last, value - 1);
        }
      }
    }, true);

    byte[][] keys = inferBoundaries(map);
    TableDescriptorBuilder tdBuilder = TableDescriptorBuilder.newBuilder(tableName);
    familyBuilders.stream().map(ColumnFamilyDescriptorBuilder::build)
      .forEachOrdered(tdBuilder::setColumnFamily);
    FutureUtils.get(admin.createTable(tdBuilder.build(), keys));

    LOG.info("Table " + tableName + " is available!!");
  }

  private Map<LoadQueueItem, ByteBuffer> performBulkLoad(AsyncClusterConnection conn,
    TableName tableName, Deque<LoadQueueItem> queue, ExecutorService pool, boolean copyFile)
    throws IOException {
    int count = 0;

    Path path = queue.peek().getFilePath();
    FileSystem fs = path.getFileSystem(getConf()).resolvePath(path).getFileSystem(getConf());
    fsDelegationToken.acquireDelegationToken(fs);
    bulkToken = FutureUtils.get(conn.prepareBulkLoad(tableName));
    Pair<Multimap<ByteBuffer, LoadQueueItem>, Set<String>> pair = null;

    Map<LoadQueueItem, ByteBuffer> item2RegionMap = new HashMap<>();
    // Assumes that region splits can happen while this occurs.
    while (!queue.isEmpty()) {
      // need to reload split keys each iteration.
      final List<Pair<byte[], byte[]>> startEndKeys =
        FutureUtils.get(conn.getRegionLocator(tableName).getStartEndKeys());
      if (count != 0) {
        LOG.info("Split occurred while grouping HFiles, retry attempt " + count + " with "
          + queue.size() + " files remaining to group or split");
      }

      int maxRetries = getConf().getInt(HConstants.BULKLOAD_MAX_RETRIES_NUMBER, 10);
      maxRetries = Math.max(maxRetries, startEndKeys.size() + 1);
      if (maxRetries != 0 && count >= maxRetries) {
        throw new IOException(
          "Retry attempted " + count + " times without completing, bailing out");
      }
      count++;

      // Using ByteBuffer for byte[] equality semantics
      pair = groupOrSplitPhase(conn, tableName, pool, queue, startEndKeys);
      Multimap<ByteBuffer, LoadQueueItem> regionGroups = pair.getFirst();

      if (!checkHFilesCountPerRegionPerFamily(regionGroups)) {
        // Error is logged inside checkHFilesCountPerRegionPerFamily.
        throw new IOException("Trying to load more than " + maxFilesPerRegionPerFamily
          + " hfiles to one family of one region");
      }

      bulkLoadPhase(conn, tableName, queue, regionGroups, copyFile, item2RegionMap);

      // NOTE: The next iteration's split / group could happen in parallel to
      // atomic bulkloads assuming that there are splits and no merges, and
      // that we can atomically pull out the groups we want to retry.
    }

    return item2RegionMap;
  }

  private void cleanup(AsyncClusterConnection conn, TableName tableName, Deque<LoadQueueItem> queue,
    ExecutorService pool) throws IOException {
    fsDelegationToken.releaseDelegationToken();
    if (bulkToken != null) {
      conn.cleanupBulkLoad(tableName, bulkToken);
    }
    if (pool != null) {
      pool.shutdown();
    }
    if (!queue.isEmpty()) {
      StringBuilder err = new StringBuilder();
      err.append("-------------------------------------------------\n");
      err.append("Bulk load aborted with some files not yet loaded:\n");
      err.append("-------------------------------------------------\n");
      for (LoadQueueItem q : queue) {
        err.append("  ").append(q.getFilePath()).append('\n');
      }
      LOG.error(err.toString());
    }
  }

  /**
   * Perform a bulk load of the given map of families to hfiles into the given pre-existing table.
   * This method is not threadsafe.
   * @param map       map of family to List of hfiles
   * @param tableName table to load the hfiles
   * @param silence   true to ignore unmatched column families
   * @param copyFile  always copy hfiles if true
   */
  private Map<LoadQueueItem, ByteBuffer> doBulkLoad(AsyncClusterConnection conn,
    TableName tableName, Map<byte[], List<Path>> map, boolean silence, boolean copyFile)
    throws IOException {
    tableExists(conn, tableName);
    // LQI queue does not need to be threadsafe -- all operations on this queue
    // happen in this thread
    Deque<LoadQueueItem> queue = new ArrayDeque<>();
    ExecutorService pool = null;
    try {
      prepareHFileQueue(conn, tableName, map, queue, silence);
      if (queue.isEmpty()) {
        LOG.warn("Bulk load operation did not get any files to load");
        return Collections.emptyMap();
      }
      pool = createExecutorService();
      return performBulkLoad(conn, tableName, queue, pool, copyFile);
    } finally {
      cleanup(conn, tableName, queue, pool);
    }
  }

  /**
   * Perform a bulk load of the given directory into the given pre-existing table. This method is
   * not threadsafe.
   * @param tableName table to load the hfiles
   * @param hfofDir   the directory that was provided as the output path of a job using
   *                  HFileOutputFormat
   * @param silence   true to ignore unmatched column families
   * @param copyFile  always copy hfiles if true
   */
  private Map<LoadQueueItem, ByteBuffer> doBulkLoad(AsyncClusterConnection conn,
    TableName tableName, Path hfofDir, boolean silence, boolean copyFile) throws IOException {
    tableExists(conn, tableName);

    /*
     * Checking hfile format is a time-consuming operation, we should have an option to skip this
     * step when bulkloading millions of HFiles. See HBASE-13985.
     */
    boolean validateHFile = getConf().getBoolean(VALIDATE_HFILES, true);
    if (!validateHFile) {
      LOG.warn("You are skipping HFiles validation, it might cause some data loss if files "
        + "are not correct. If you fail to read data from your table after using this "
        + "option, consider removing the files and bulkload again without this option. "
        + "See HBASE-13985");
    }
    // LQI queue does not need to be threadsafe -- all operations on this queue
    // happen in this thread
    Deque<LoadQueueItem> queue = new ArrayDeque<>();
    ExecutorService pool = null;
    try {
      prepareHFileQueue(getConf(), conn, tableName, hfofDir, queue, validateHFile, silence);

      if (queue.isEmpty()) {
        LOG.warn(
          "Bulk load operation did not find any files to load in directory {}. "
            + "Does it contain files in subdirectories that correspond to column family names?",
          (hfofDir != null ? hfofDir.toUri().toString() : ""));
        return Collections.emptyMap();
      }
      pool = createExecutorService();
      return performBulkLoad(conn, tableName, queue, pool, copyFile);
    } finally {
      cleanup(conn, tableName, queue, pool);
    }
  }

  @Override
  public Map<LoadQueueItem, ByteBuffer> bulkLoad(TableName tableName,
    Map<byte[], List<Path>> family2Files) throws IOException {
    try (AsyncClusterConnection conn = ClusterConnectionFactory
      .createAsyncClusterConnection(getConf(), null, userProvider.getCurrent())) {
      return doBulkLoad(conn, tableName, family2Files, isSilence(), isAlwaysCopyFiles());
    }
  }

  @Override
  public Map<LoadQueueItem, ByteBuffer> bulkLoad(TableName tableName, Path dir) throws IOException {
    try (AsyncClusterConnection conn = ClusterConnectionFactory
      .createAsyncClusterConnection(getConf(), null, userProvider.getCurrent())) {
      AsyncAdmin admin = conn.getAdmin();
      if (!FutureUtils.get(admin.tableExists(tableName))) {
        if (isCreateTable()) {
          createTable(tableName, dir, admin);
        } else {
          throwAndLogTableNotFoundException(tableName);
        }
      }
      return doBulkLoad(conn, tableName, dir, isSilence(), isAlwaysCopyFiles());
    }
  }

  /**
   * @throws TableNotFoundException if table does not exist.
   */
  private void tableExists(AsyncClusterConnection conn, TableName tableName) throws IOException {
    if (!FutureUtils.get(conn.getAdmin().tableExists(tableName))) {
      throwAndLogTableNotFoundException(tableName);
    }
  }

  private void throwAndLogTableNotFoundException(TableName tn) throws TableNotFoundException {
    String errorMsg = format("Table '%s' does not exist.", tn);
    LOG.error(errorMsg);
    throw new TableNotFoundException(errorMsg);
  }

  public void setBulkToken(String bulkToken) {
    this.bulkToken = bulkToken;
  }

  public void setClusterIds(List<String> clusterIds) {
    this.clusterIds = clusterIds;
  }

  private void usage() {
    System.err.println("Usage: " + "bin/hbase completebulkload [OPTIONS] "
      + "</PATH/TO/HFILEOUTPUTFORMAT-OUTPUT> <TABLENAME>\n"
      + "Loads directory of hfiles -- a region dir or product of HFileOutputFormat -- "
      + "into an hbase table.\n" + "OPTIONS (for other -D options, see source code):\n" + " -D"
      + CREATE_TABLE_CONF_KEY + "=no whether to create table; when 'no', target "
      + "table must exist.\n" + " -D" + IGNORE_UNMATCHED_CF_CONF_KEY
      + "=yes to ignore unmatched column families.\n"
      + " -loadTable for when directory of files to load has a depth of 3; target table must "
      + "exist;\n" + " must be last of the options on command line.\n"
      + "See http://hbase.apache.org/book.html#arch.bulk.load.complete.strays for "
      + "documentation.\n");
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2 && args.length != 3) {
      usage();
      return -1;
    }
    // Re-initialize to apply -D options from the command line parameters
    initialize();
    Path dirPath = new Path(args[0]);
    TableName tableName = TableName.valueOf(args[1]);
    if (args.length == 2) {
      return !bulkLoad(tableName, dirPath).isEmpty() ? 0 : -1;
    } else {
      Map<byte[], List<Path>> family2Files = Maps.newHashMap();
      FileSystem fs = FileSystem.get(getConf());
      for (FileStatus regionDir : fs.listStatus(dirPath)) {
        FSVisitor.visitRegionStoreFiles(fs, regionDir.getPath(), (region, family, hfileName) -> {
          Path path = new Path(regionDir.getPath(), new Path(family, hfileName));
          byte[] familyName = Bytes.toBytes(family);
          if (family2Files.containsKey(familyName)) {
            family2Files.get(familyName).add(path);
          } else {
            family2Files.put(familyName, Lists.newArrayList(path));
          }
        });
      }
      return !bulkLoad(tableName, family2Files).isEmpty() ? 0 : -1;
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    int ret = ToolRunner.run(conf, new BulkLoadHFilesTool(conf), args);
    System.exit(ret);
  }

  @Override
  public void disableReplication() {
    this.replicate = false;
  }

  @Override
  public boolean isReplicationDisabled() {
    return !this.replicate;
  }
}
