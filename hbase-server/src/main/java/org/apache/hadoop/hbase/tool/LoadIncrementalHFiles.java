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
package org.apache.hadoop.hbase.tool;

import static java.lang.String.format;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClientServiceCallable;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.client.SecureBulkLoadClient;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.HalfStoreFileReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.ReaderContext;
import org.apache.hadoop.hbase.io.hfile.ReaderContextBuilder;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.FsDelegationToken;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSVisitor;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.collect.HashMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimaps;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Tool to load the output of HFileOutputFormat into an existing table.
 * <p/>
 * Notice that, by default, this class should be kept till 4.0.0, but as this is a bad practice that
 * we expose an implementation class instead of an interface, we want to fix it ASAP. That's why we
 * will remove this class completely in 3.0.0. Please change your code to use
 * {@link BulkLoadHFiles}.
 * @deprecated since 2.2.0, will be removed in 3.0.0. Use {@link BulkLoadHFiles} instead. Please
 *             rewrite your code if you rely on methods other than the {@link #run(Map, TableName)}
 *             and {@link #run(String, TableName)}, as all the methods other than them will be
 *             removed with no replacement.
 */
@Deprecated
@InterfaceAudience.Public
public class LoadIncrementalHFiles extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(LoadIncrementalHFiles.class);

  /**
   * @deprecated since 2.2.0, will be removed in 3.0.0, with no replacement. End user should not
   *             depend on this value.
   */
  @Deprecated
  public static final String NAME = BulkLoadHFilesTool.NAME;
  static final String RETRY_ON_IO_EXCEPTION = BulkLoadHFiles.RETRY_ON_IO_EXCEPTION;
  public static final String MAX_FILES_PER_REGION_PER_FAMILY =
    BulkLoadHFiles.MAX_FILES_PER_REGION_PER_FAMILY;
  private static final String ASSIGN_SEQ_IDS = BulkLoadHFiles.ASSIGN_SEQ_IDS;
  public final static String CREATE_TABLE_CONF_KEY = BulkLoadHFiles.CREATE_TABLE_CONF_KEY;
  public final static String IGNORE_UNMATCHED_CF_CONF_KEY =
    BulkLoadHFiles.IGNORE_UNMATCHED_CF_CONF_KEY;
  public final static String ALWAYS_COPY_FILES = BulkLoadHFiles.ALWAYS_COPY_FILES;

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
  private AtomicInteger numRetries;
  private RpcControllerFactory rpcControllerFactory;

  private String bulkToken;

  private List<String> clusterIds = new ArrayList<>();

  private boolean replicate = true;

  /**
   * Represents an HFile waiting to be loaded. An queue is used in this class in order to support
   * the case where a region has split during the process of the load. When this happens, the HFile
   * is split into two physical parts across the new region boundary, and each part is added back
   * into the queue. The import process finishes when the queue is empty.
   * @deprecated since 2.2.0 and will be removed in 3.0.0. Use {@link BulkLoadHFiles} instead.
   * @see BulkLoadHFiles
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-21782">HBASE-21782</a>
   */
  @InterfaceAudience.Public
  @Deprecated
  public static class LoadQueueItem extends BulkLoadHFiles.LoadQueueItem {

    public LoadQueueItem(byte[] family, Path hfilePath) {
      super(family, hfilePath);
    }
  }

  public LoadIncrementalHFiles(Configuration conf) {
    // make a copy, just to be sure we're not overriding someone else's config
    super(HBaseConfiguration.create(conf));
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
    bulkLoadByFamily = conf.getBoolean(BulkLoadHFiles.BULK_LOAD_HFILES_BY_FAMILY, false);
    nrThreads = conf.getInt("hbase.loadincremental.threads.max",
      Runtime.getRuntime().availableProcessors());
    numRetries = new AtomicInteger(0);
    rpcControllerFactory = new RpcControllerFactory(conf);
  }

  private void usage() {
    System.err.println("Usage: " + "bin/hbase completebulkload [OPTIONS] "
        + "</PATH/TO/HFILEOUTPUTFORMAT-OUTPUT> <TABLENAME>\n"
        + "Loads directory of hfiles -- a region dir or product of HFileOutputFormat -- "
        + "into an hbase table.\n"
        + "OPTIONS (for other -D options, see source code):\n"
        + " -D" + CREATE_TABLE_CONF_KEY + "=no whether to create table; when 'no', target "
        + "table must exist.\n"
        + " -D" + IGNORE_UNMATCHED_CF_CONF_KEY + "=yes to ignore unmatched column families.\n"
        + " -loadTable for when directory of files to load has a depth of 3; target table must "
        + "exist;\n"
        + " must be last of the options on command line.\n"
        + "See http://hbase.apache.org/book.html#arch.bulk.load.complete.strays for "
        + "documentation.\n");
  }

  /**
   * Prepare a collection of {@link LoadQueueItem} from list of source hfiles contained in the
   * passed directory and validates whether the prepared queue has all the valid table column
   * families in it.
   * @param hfilesDir directory containing list of hfiles to be loaded into the table
   * @param table table to which hfiles should be loaded
   * @param queue queue which needs to be loaded into the table
   * @param validateHFile if true hfiles will be validated for its format
   * @throws IOException If any I/O or network error occurred
   */
  public void prepareHFileQueue(Path hfilesDir, Table table, Deque<LoadQueueItem> queue,
      boolean validateHFile) throws IOException {
    prepareHFileQueue(hfilesDir, table, queue, validateHFile, false);
  }

  /**
   * Prepare a collection of {@link LoadQueueItem} from list of source hfiles contained in the
   * passed directory and validates whether the prepared queue has all the valid table column
   * families in it.
   * @param hfilesDir directory containing list of hfiles to be loaded into the table
   * @param table table to which hfiles should be loaded
   * @param queue queue which needs to be loaded into the table
   * @param validateHFile if true hfiles will be validated for its format
   * @param silence true to ignore unmatched column families
   * @throws IOException If any I/O or network error occurred
   */
  public void prepareHFileQueue(Path hfilesDir, Table table, Deque<LoadQueueItem> queue,
      boolean validateHFile, boolean silence) throws IOException {
    discoverLoadQueue(queue, hfilesDir, validateHFile);
    validateFamiliesInHFiles(table, queue, silence);
  }

  /**
   * Prepare a collection of {@link LoadQueueItem} from list of source hfiles contained in the
   * passed directory and validates whether the prepared queue has all the valid table column
   * families in it.
   * @param map map of family to List of hfiles
   * @param table table to which hfiles should be loaded
   * @param queue queue which needs to be loaded into the table
   * @param silence true to ignore unmatched column families
   * @throws IOException If any I/O or network error occurred
   */
  public void prepareHFileQueue(Map<byte[], List<Path>> map, Table table,
      Deque<LoadQueueItem> queue, boolean silence) throws IOException {
    populateLoadQueue(queue, map);
    validateFamiliesInHFiles(table, queue, silence);
  }

  /**
   * Perform a bulk load of the given directory into the given pre-existing table. This method is
   * not threadsafe.
   * @param hfofDir the directory that was provided as the output path of a job using
   *          HFileOutputFormat
   * @param admin the Admin
   * @param table the table to load into
   * @param regionLocator region locator
   * @throws TableNotFoundException if table does not yet exist
   */
  public Map<LoadQueueItem, ByteBuffer> doBulkLoad(Path hfofDir, final Admin admin, Table table,
      RegionLocator regionLocator) throws TableNotFoundException, IOException {
    return doBulkLoad(hfofDir, admin, table, regionLocator, false, false);
  }

  /**
   * Perform a bulk load of the given directory into the given pre-existing table. This method is
   * not threadsafe.
   * @param map map of family to List of hfiles
   * @param admin the Admin
   * @param table the table to load into
   * @param regionLocator region locator
   * @param silence true to ignore unmatched column families
   * @param copyFile always copy hfiles if true
   * @throws TableNotFoundException if table does not yet exist
   */
  public Map<LoadQueueItem, ByteBuffer> doBulkLoad(Map<byte[], List<Path>> map, final Admin admin,
      Table table, RegionLocator regionLocator, boolean silence, boolean copyFile)
      throws TableNotFoundException, IOException {
    if (!admin.isTableAvailable(regionLocator.getName())) {
      throw new TableNotFoundException("Table " + table.getName() + " is not currently available.");
    }
    // LQI queue does not need to be threadsafe -- all operations on this queue
    // happen in this thread
    Deque<LoadQueueItem> queue = new ArrayDeque<>();
    ExecutorService pool = null;
    SecureBulkLoadClient secureClient = null;
    try {
      prepareHFileQueue(map, table, queue, silence);
      if (queue.isEmpty()) {
        LOG.warn("Bulk load operation did not get any files to load");
        return Collections.emptyMap();
      }
      pool = createExecutorService();
      secureClient = new SecureBulkLoadClient(table.getConfiguration(), table);
      return performBulkLoad(admin, table, regionLocator, queue, pool, secureClient, copyFile);
    } finally {
      cleanup(admin, queue, pool, secureClient);
    }
  }

  /**
   * Perform a bulk load of the given directory into the given pre-existing table. This method is
   * not threadsafe.
   * @param hfofDir the directory that was provided as the output path of a job using
   *          HFileOutputFormat
   * @param admin the Admin
   * @param table the table to load into
   * @param regionLocator region locator
   * @param silence true to ignore unmatched column families
   * @param copyFile always copy hfiles if true
   * @throws TableNotFoundException if table does not yet exist
   */
  public Map<LoadQueueItem, ByteBuffer> doBulkLoad(Path hfofDir, final Admin admin, Table table,
      RegionLocator regionLocator, boolean silence, boolean copyFile)
      throws TableNotFoundException, IOException {
    if (!admin.isTableAvailable(regionLocator.getName())) {
      throw new TableNotFoundException("Table " + table.getName() + " is not currently available.");
    }

    /*
     * Checking hfile format is a time-consuming operation, we should have an option to skip this
     * step when bulkloading millions of HFiles. See HBASE-13985.
     */
    boolean validateHFile = getConf().getBoolean("hbase.loadincremental.validate.hfile", true);
    if (!validateHFile) {
      LOG.warn("You are skipping HFiles validation, it might cause some data loss if files " +
          "are not correct. If you fail to read data from your table after using this " +
          "option, consider removing the files and bulkload again without this option. " +
          "See HBASE-13985");
    }
    // LQI queue does not need to be threadsafe -- all operations on this queue
    // happen in this thread
    Deque<LoadQueueItem> queue = new ArrayDeque<>();
    ExecutorService pool = null;
    SecureBulkLoadClient secureClient = null;
    try {
      prepareHFileQueue(hfofDir, table, queue, validateHFile, silence);

      if (queue.isEmpty()) {
        LOG.warn(
            "Bulk load operation did not find any files to load in directory {}. " +
            "Does it contain files in subdirectories that correspond to column family names?",
            (hfofDir != null ? hfofDir.toUri().toString() : ""));
        return Collections.emptyMap();
      }
      pool = createExecutorService();
      secureClient = new SecureBulkLoadClient(table.getConfiguration(), table);
      return performBulkLoad(admin, table, regionLocator, queue, pool, secureClient, copyFile);
    } finally {
      cleanup(admin, queue, pool, secureClient);
    }
  }

  /**
   * Used by the replication sink to load the hfiles from the source cluster. It does the following,
   * <ol>
   * <li>LoadIncrementalHFiles#groupOrSplitPhase(Table, ExecutorService, Deque, Pair)}</li>
   * <li>LoadIncrementalHFiles#bulkLoadPhase(Table, Connection, ExecutorService, Deque, Multimap)
   * </li>
   * </ol>
   * @param table Table to which these hfiles should be loaded to
   * @param conn Connection to use
   * @param queue {@link LoadQueueItem} has hfiles yet to be loaded
   * @param startEndKeys starting and ending row keys of the region
   */
  public void loadHFileQueue(Table table, Connection conn, Deque<LoadQueueItem> queue,
      Pair<byte[][], byte[][]> startEndKeys) throws IOException {
    loadHFileQueue(table, conn, queue, startEndKeys, false);
  }

  /**
   * Used by the replication sink to load the hfiles from the source cluster. It does the following,
   * <ol>
   * <li>LoadIncrementalHFiles#groupOrSplitPhase(Table, ExecutorService, Deque, Pair)}</li>
   * <li>LoadIncrementalHFiles#bulkLoadPhase(Table, Connection, ExecutorService, Deque, Multimap)
   * </li>
   * </ol>
   * @param table Table to which these hfiles should be loaded to
   * @param conn Connection to use
   * @param queue {@link LoadQueueItem} has hfiles yet to be loaded
   * @param startEndKeys starting and ending row keys of the region
   */
  public void loadHFileQueue(Table table, Connection conn, Deque<LoadQueueItem> queue,
      Pair<byte[][], byte[][]> startEndKeys, boolean copyFile) throws IOException {
    ExecutorService pool = null;
    try {
      pool = createExecutorService();
      Multimap<ByteBuffer, LoadQueueItem> regionGroups =
          groupOrSplitPhase(table, pool, queue, startEndKeys).getFirst();
      bulkLoadPhase(table, conn, pool, queue, regionGroups, copyFile, null);
    } finally {
      if (pool != null) {
        pool.shutdown();
      }
    }
  }

  private Map<LoadQueueItem, ByteBuffer> performBulkLoad(Admin admin, Table table,
      RegionLocator regionLocator, Deque<LoadQueueItem> queue, ExecutorService pool,
      SecureBulkLoadClient secureClient, boolean copyFile) throws IOException {
    int count = 0;

    if (isSecureBulkLoadEndpointAvailable()) {
      LOG.warn("SecureBulkLoadEndpoint is deprecated. It will be removed in future releases.");
      LOG.warn("Secure bulk load has been integrated into HBase core.");
    }

    fsDelegationToken.acquireDelegationToken(queue.peek().getFilePath().getFileSystem(getConf()));
    bulkToken = secureClient.prepareBulkLoad(admin.getConnection());
    Pair<Multimap<ByteBuffer, LoadQueueItem>, Set<String>> pair = null;

    Map<LoadQueueItem, ByteBuffer> item2RegionMap = new HashMap<>();
    // Assumes that region splits can happen while this occurs.
    while (!queue.isEmpty()) {
      // need to reload split keys each iteration.
      final Pair<byte[][], byte[][]> startEndKeys = regionLocator.getStartEndKeys();
      if (count != 0) {
        LOG.info("Split occurred while grouping HFiles, retry attempt " + count + " with " +
            queue.size() + " files remaining to group or split");
      }

      int maxRetries = getConf().getInt(HConstants.BULKLOAD_MAX_RETRIES_NUMBER, 10);
      maxRetries = Math.max(maxRetries, startEndKeys.getFirst().length + 1);
      if (maxRetries != 0 && count >= maxRetries) {
        throw new IOException(
            "Retry attempted " + count + " times without completing, bailing out");
      }
      count++;

      // Using ByteBuffer for byte[] equality semantics
      pair = groupOrSplitPhase(table, pool, queue, startEndKeys);
      Multimap<ByteBuffer, LoadQueueItem> regionGroups = pair.getFirst();

      if (!checkHFilesCountPerRegionPerFamily(regionGroups)) {
        // Error is logged inside checkHFilesCountPerRegionPerFamily.
        throw new IOException("Trying to load more than " + maxFilesPerRegionPerFamily +
            " hfiles to one family of one region");
      }

      bulkLoadPhase(table, admin.getConnection(), pool, queue, regionGroups, copyFile,
        item2RegionMap);

      // NOTE: The next iteration's split / group could happen in parallel to
      // atomic bulkloads assuming that there are splits and no merges, and
      // that we can atomically pull out the groups we want to retry.
    }

    if (!queue.isEmpty()) {
      throw new RuntimeException("Bulk load aborted with some files not yet loaded." +
          "Please check log for more details.");
    }
    return item2RegionMap;
  }

  private Map<byte[], Collection<LoadQueueItem>>
      groupByFamilies(Collection<LoadQueueItem> itemsInRegion) {
    Map<byte[], Collection<LoadQueueItem>> families2Queue = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    itemsInRegion.forEach(item -> families2Queue
        .computeIfAbsent(item.getFamily(), queue -> new ArrayList<>()).add(item));
    return families2Queue;
  }

  /**
   * This takes the LQI's grouped by likely regions and attempts to bulk load them. Any failures are
   * re-queued for another pass with the groupOrSplitPhase.
   * <p>
   * protected for testing.
   */
  @VisibleForTesting
  protected void bulkLoadPhase(Table table, Connection conn, ExecutorService pool,
      Deque<LoadQueueItem> queue, Multimap<ByteBuffer, LoadQueueItem> regionGroups,
      boolean copyFile, Map<LoadQueueItem, ByteBuffer> item2RegionMap) throws IOException {
    // atomically bulk load the groups.
    Set<Future<List<LoadQueueItem>>> loadingFutures = new HashSet<>();
    for (Entry<ByteBuffer, ? extends Collection<LoadQueueItem>> e : regionGroups.asMap()
        .entrySet()) {
      byte[] first = e.getKey().array();
      Collection<LoadQueueItem> lqis = e.getValue();
      if (item2RegionMap != null) {
        for (LoadQueueItem lqi : lqis) {
          item2RegionMap.put(lqi, e.getKey());
        }
      }
      if (bulkLoadByFamily) {
        groupByFamilies(lqis).values().forEach(familyQueue -> loadingFutures.add(pool.submit(
          () -> tryAtomicRegionLoad(conn, table.getName(), first, familyQueue, copyFile))));
      } else {
        loadingFutures.add(
          pool.submit(() -> tryAtomicRegionLoad(conn, table.getName(), first, lqis, copyFile)));
      }
    }

    // get all the results.
    for (Future<List<LoadQueueItem>> future : loadingFutures) {
      try {
        List<LoadQueueItem> toRetry = future.get();

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

  @VisibleForTesting
  protected ClientServiceCallable<byte[]> buildClientServiceCallable(Connection conn,
      TableName tableName, byte[] first, Collection<LoadQueueItem> lqis, boolean copyFile) {
    List<Pair<byte[], String>> famPaths =
        lqis.stream().map(lqi -> Pair.newPair(lqi.getFamily(), lqi.getFilePath().toString()))
            .collect(Collectors.toList());
    return new ClientServiceCallable<byte[]>(conn, tableName, first,
        rpcControllerFactory.newController(), HConstants.PRIORITY_UNSET) {
      @Override
      protected byte[] rpcCall() throws Exception {
        SecureBulkLoadClient secureClient = null;
        boolean success = false;
        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Going to connect to server " + getLocation() + " for row " +
                Bytes.toStringBinary(getRow()) + " with hfile group " +
                LoadIncrementalHFiles.this.toString(famPaths));
          }
          byte[] regionName = getLocation().getRegionInfo().getRegionName();
          try (Table table = conn.getTable(getTableName())) {
            secureClient = new SecureBulkLoadClient(getConf(), table);
            success = secureClient.secureBulkLoadHFiles(getStub(), famPaths, regionName,
              assignSeqIds, fsDelegationToken.getUserToken(),
                bulkToken, copyFile, clusterIds, replicate);
          }
          return success ? regionName : null;
        } finally {
          // Best effort copying of files that might not have been imported
          // from the staging directory back to original location
          // in user directory
          if (secureClient != null && !success) {
            FileSystem targetFs = FileSystem.get(getConf());
            FileSystem sourceFs = lqis.iterator().next().getFilePath().getFileSystem(getConf());
            // Check to see if the source and target filesystems are the same
            // If they are the same filesystem, we will try move the files back
            // because previously we moved them to the staging directory.
            if (FSUtils.isSameHdfs(getConf(), sourceFs, targetFs)) {
              for (Pair<byte[], String> el : famPaths) {
                Path hfileStagingPath = null;
                Path hfileOrigPath = new Path(el.getSecond());
                try {
                  hfileStagingPath = new Path(new Path(bulkToken, Bytes.toString(el.getFirst())),
                      hfileOrigPath.getName());
                  if (targetFs.rename(hfileStagingPath, hfileOrigPath)) {
                    LOG.debug("Moved back file " + hfileOrigPath + " from " + hfileStagingPath);
                  } else if (targetFs.exists(hfileStagingPath)) {
                    LOG.debug(
                      "Unable to move back file " + hfileOrigPath + " from " + hfileStagingPath);
                  }
                } catch (Exception ex) {
                  LOG.debug(
                    "Unable to move back file " + hfileOrigPath + " from " + hfileStagingPath, ex);
                }
              }
            }
          }
        }
      }
    };
  }

  private boolean checkHFilesCountPerRegionPerFamily(
      final Multimap<ByteBuffer, LoadQueueItem> regionGroups) {
    for (Map.Entry<ByteBuffer, Collection<LoadQueueItem>> e : regionGroups.asMap().entrySet()) {
      Map<byte[], MutableInt> filesMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (LoadQueueItem lqi : e.getValue()) {
        MutableInt count = filesMap.computeIfAbsent(lqi.getFamily(), k -> new MutableInt());
        count.increment();
        if (count.intValue() > maxFilesPerRegionPerFamily) {
          LOG.error("Trying to load more than " + maxFilesPerRegionPerFamily +
              " hfiles to family " + Bytes.toStringBinary(lqi.getFamily()) +
              " of region with start key " + Bytes.toStringBinary(e.getKey()));
          return false;
        }
      }
    }
    return true;
  }

  /**
   * @param table the table to load into
   * @param pool the ExecutorService
   * @param queue the queue for LoadQueueItem
   * @param startEndKeys start and end keys
   * @return A map that groups LQI by likely bulk load region targets and Set of missing hfiles.
   */
  private Pair<Multimap<ByteBuffer, LoadQueueItem>, Set<String>> groupOrSplitPhase(
      final Table table, ExecutorService pool, Deque<LoadQueueItem> queue,
      final Pair<byte[][], byte[][]> startEndKeys) throws IOException {
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
          new Callable<Pair<List<LoadQueueItem>, String>>() {
            @Override
            public Pair<List<LoadQueueItem>, String> call() throws Exception {
              Pair<List<LoadQueueItem>, String> splits =
                  groupOrSplit(regionGroups, item, table, startEndKeys);
              return splits;
            }
          };
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

  private List<LoadQueueItem> splitStoreFile(final LoadQueueItem item, final Table table,
      byte[] startKey, byte[] splitKey) throws IOException {
    Path hfilePath = item.getFilePath();
    byte[] family = item.getFamily();
    Path tmpDir = hfilePath.getParent();
    if (!tmpDir.getName().equals(TMP_DIR)) {
      tmpDir = new Path(tmpDir, TMP_DIR);
    }

    LOG.info("HFile at " + hfilePath + " no longer fits inside a single " + "region. Splitting...");

    String uniqueName = getUniqueName();
    ColumnFamilyDescriptor familyDesc = table.getDescriptor().getColumnFamily(family);

    Path botOut = new Path(tmpDir, uniqueName + ".bottom");
    Path topOut = new Path(tmpDir, uniqueName + ".top");
    splitStoreFile(getConf(), hfilePath, familyDesc, splitKey, botOut, topOut);

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
   * Attempt to assign the given load queue item into its target region group. If the hfile boundary
   * no longer fits into a region, physically splits the hfile such that the new bottom half will
   * fit and returns the list of LQI's corresponding to the resultant hfiles.
   * <p>
   * protected for testing
   * @throws IOException if an IO failure is encountered
   */
  @VisibleForTesting
  protected Pair<List<LoadQueueItem>, String> groupOrSplit(
      Multimap<ByteBuffer, LoadQueueItem> regionGroups, final LoadQueueItem item, final Table table,
      final Pair<byte[][], byte[][]> startEndKeys) throws IOException {
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

    LOG.info("Trying to load hfile=" + hfilePath + " first=" + first.map(Bytes::toStringBinary) +
        " last=" + last.map(Bytes::toStringBinary));
    if (!first.isPresent() || !last.isPresent()) {
      assert !first.isPresent() && !last.isPresent();
      // TODO what if this is due to a bad HFile?
      LOG.info("hfile " + hfilePath + " has no entries, skipping");
      return null;
    }
    if (Bytes.compareTo(first.get(), last.get()) > 0) {
      throw new IllegalArgumentException("Invalid range: " + Bytes.toStringBinary(first.get()) +
          " > " + Bytes.toStringBinary(last.get()));
    }
    int idx = Arrays.binarySearch(startEndKeys.getFirst(), first.get(), Bytes.BYTES_COMPARATOR);
    if (idx < 0) {
      // not on boundary, returns -(insertion index). Calculate region it
      // would be in.
      idx = -(idx + 1) - 1;
    }
    int indexForCallable = idx;

    /**
     * we can consider there is a region hole in following conditions. 1) if idx < 0,then first
     * region info is lost. 2) if the endkey of a region is not equal to the startkey of the next
     * region. 3) if the endkey of the last region is not empty.
     */
    if (indexForCallable < 0) {
      throw new IOException("The first region info for table " + table.getName() +
          " can't be found in hbase:meta.Please use hbck tool to fix it first.");
    } else if ((indexForCallable == startEndKeys.getFirst().length - 1) &&
        !Bytes.equals(startEndKeys.getSecond()[indexForCallable], HConstants.EMPTY_BYTE_ARRAY)) {
      throw new IOException("The last region info for table " + table.getName() +
          " can't be found in hbase:meta.Please use hbck tool to fix it first.");
    } else if (indexForCallable + 1 < startEndKeys.getFirst().length &&
        !(Bytes.compareTo(startEndKeys.getSecond()[indexForCallable],
          startEndKeys.getFirst()[indexForCallable + 1]) == 0)) {
      throw new IOException("The endkey of one region for table " + table.getName() +
          " is not equal to the startkey of the next region in hbase:meta." +
          "Please use hbck tool to fix it first.");
    }

    boolean lastKeyInRange = Bytes.compareTo(last.get(), startEndKeys.getSecond()[idx]) < 0 ||
        Bytes.equals(startEndKeys.getSecond()[idx], HConstants.EMPTY_BYTE_ARRAY);
    if (!lastKeyInRange) {
      List<LoadQueueItem> lqis = splitStoreFile(item, table,
        startEndKeys.getFirst()[indexForCallable], startEndKeys.getSecond()[indexForCallable]);
      return new Pair<>(lqis, null);
    }

    // group regions.
    regionGroups.put(ByteBuffer.wrap(startEndKeys.getFirst()[idx]), item);
    return null;
  }

  /**
   * Attempts to do an atomic load of many hfiles into a region. If it fails, it returns a list of
   * hfiles that need to be retried. If it is successful it will return an empty list.
   * <p>
   * NOTE: To maintain row atomicity guarantees, region server callable should succeed atomically
   * and fails atomically.
   * <p>
   * Protected for testing.
   * @return empty list if success, list of items to retry on recoverable failure
   * @deprecated as of release 2.3.0. Use {@link BulkLoadHFiles} instead.
   */
  @Deprecated
  @VisibleForTesting
  protected List<LoadQueueItem> tryAtomicRegionLoad(final Connection conn,
    final TableName tableName, final byte[] first, final Collection<LoadQueueItem> lqis,
    boolean copyFile) throws IOException {
    ClientServiceCallable<byte[]> serviceCallable =
      buildClientServiceCallable(conn, tableName, first, lqis, copyFile);
    return tryAtomicRegionLoad(serviceCallable, tableName, first, lqis);
  }

  /**
   * Attempts to do an atomic load of many hfiles into a region. If it fails, it returns a list of
   * hfiles that need to be retried. If it is successful it will return an empty list.
   * <p>
   * NOTE: To maintain row atomicity guarantees, region server callable should succeed atomically
   * and fails atomically.
   * <p>
   * Protected for testing.
   * @return empty list if success, list of items to retry on recoverable failure
   * @deprecated as of release 2.3.0. Use {@link BulkLoadHFiles} instead.
   */
  @Deprecated
  @VisibleForTesting
  protected List<LoadQueueItem> tryAtomicRegionLoad(ClientServiceCallable<byte[]> serviceCallable,
    final TableName tableName, final byte[] first, final Collection<LoadQueueItem> lqis)
    throws IOException {
    List<LoadQueueItem> toRetry = new ArrayList<>();
    try {
      Configuration conf = getConf();
      byte[] region = RpcRetryingCallerFactory.instantiate(conf, null).<byte[]> newCaller()
          .callWithRetries(serviceCallable, Integer.MAX_VALUE);
      if (region == null) {
        LOG.warn("Attempt to bulk load region containing " + Bytes.toStringBinary(first) +
            " into table " + tableName + " with files " + lqis +
            " failed.  This is recoverable and they will be retried.");
        toRetry.addAll(lqis); // return lqi's to retry
      }
      // success
      return toRetry;
    } catch (IOException e) {
      LOG.error("Encountered unrecoverable error from region server, additional details: " +
                      serviceCallable.getExceptionMessageAdditionalDetail(),
              e);
      LOG.warn(
              "Received a " + e.getClass().getSimpleName()
                      + " from region server: "
                      + serviceCallable.getExceptionMessageAdditionalDetail(), e);
      if (getConf().getBoolean(RETRY_ON_IO_EXCEPTION, false)
              && numRetries.get() < getConf().getInt(
              HConstants.HBASE_CLIENT_RETRIES_NUMBER,
              HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER)) {
        LOG.warn("Will attempt to retry loading failed HFiles. Retry #"
                + numRetries.incrementAndGet());
        toRetry.addAll(lqis);
        return toRetry;
      }
      LOG.error(RETRY_ON_IO_EXCEPTION + " is disabled. Unable to recover");
      throw e;
    }
  }

  /**
   * If the table is created for the first time, then "completebulkload" reads the files twice. More
   * modifications necessary if we want to avoid doing it.
   */
  private void createTable(TableName tableName, Path hfofDir, Admin admin) throws IOException {
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
            LOG.info("Setting compression " + reader.getFileContext().getCompression().name() +
                " for family " + builder.getNameAsString());
          }
          byte[] first = reader.getFirstRowKey().get();
          byte[] last = reader.getLastRowKey().get();

          LOG.info("Trying to figure out region boundaries hfile=" + hfile + " first=" +
              Bytes.toStringBinary(first) + " last=" + Bytes.toStringBinary(last));

          // To eventually infer start key-end key boundaries
          Integer value = map.containsKey(first) ? map.get(first) : 0;
          map.put(first, value + 1);

          value = map.containsKey(last) ? map.get(last) : 0;
          map.put(last, value - 1);
        }
      }
    });

    byte[][] keys = inferBoundaries(map);
    TableDescriptorBuilder tdBuilder = TableDescriptorBuilder.newBuilder(tableName);
    familyBuilders.stream().map(ColumnFamilyDescriptorBuilder::build)
        .forEachOrdered(tdBuilder::setColumnFamily);
    admin.createTable(tdBuilder.build(), keys);

    LOG.info("Table " + tableName + " is available!!");
  }

  private void cleanup(Admin admin, Deque<LoadQueueItem> queue, ExecutorService pool,
      SecureBulkLoadClient secureClient) throws IOException {
    fsDelegationToken.releaseDelegationToken();
    if (bulkToken != null && secureClient != null) {
      secureClient.cleanupBulkLoad(admin.getConnection(), bulkToken);
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

  // unique file name for the table
  private String getUniqueName() {
    return UUID.randomUUID().toString().replaceAll("-", "");
  }

  /**
   * Checks whether there is any invalid family name in HFiles to be bulk loaded.
   */
  private void validateFamiliesInHFiles(Table table, Deque<LoadQueueItem> queue, boolean silence)
      throws IOException {
    Set<String> familyNames = Arrays.asList(table.getDescriptor().getColumnFamilies()).stream()
        .map(f -> f.getNameAsString()).collect(Collectors.toSet());
    List<String> unmatchedFamilies = queue.stream().map(item -> Bytes.toString(item.getFamily()))
        .filter(fn -> !familyNames.contains(fn)).distinct().collect(Collectors.toList());
    if (unmatchedFamilies.size() > 0) {
      String msg =
          "Unmatched family names found: unmatched family names in HFiles to be bulkloaded: " +
              unmatchedFamilies + "; valid family names of table " + table.getName() + " are: " +
              familyNames;
      LOG.error(msg);
      if (!silence) {
        throw new IOException(msg);
      }
    }
  }

  /**
   * Populate the Queue with given HFiles
   */
  private void populateLoadQueue(Deque<LoadQueueItem> ret, Map<byte[], List<Path>> map) {
    map.forEach((k, v) -> v.stream().map(p -> new LoadQueueItem(k, p)).forEachOrdered(ret::add));
  }

  /**
   * Walk the given directory for all HFiles, and return a Queue containing all such files.
   */
  private void discoverLoadQueue(final Deque<LoadQueueItem> ret, final Path hfofDir,
      final boolean validateHFile) throws IOException {
    visitBulkHFiles(hfofDir.getFileSystem(getConf()), hfofDir, new BulkHFileVisitor<byte[]>() {
      @Override
      public byte[] bulkFamily(final byte[] familyName) {
        return familyName;
      }

      @Override
      public void bulkHFile(final byte[] family, final FileStatus hfile) throws IOException {
        long length = hfile.getLen();
        if (length > getConf().getLong(HConstants.HREGION_MAX_FILESIZE,
          HConstants.DEFAULT_MAX_FILE_SIZE)) {
          LOG.warn("Trying to bulk load hfile " + hfile.getPath() + " with size: " + length +
              " bytes can be problematic as it may lead to oversplitting.");
        }
        ret.add(new LoadQueueItem(family, hfile.getPath()));
      }
    }, validateHFile);
  }

  private interface BulkHFileVisitor<TFamily> {

    TFamily bulkFamily(byte[] familyName) throws IOException;

    void bulkHFile(TFamily family, FileStatus hfileStatus) throws IOException;
  }

  /**
   * Iterate over the bulkDir hfiles. Skip reference, HFileLink, files starting with "_" and
   * non-valid hfiles.
   */
  private static <TFamily> void visitBulkHFiles(final FileSystem fs, final Path bulkDir,
      final BulkHFileVisitor<TFamily> visitor) throws IOException {
    visitBulkHFiles(fs, bulkDir, visitor, true);
  }

  /**
   * Iterate over the bulkDir hfiles. Skip reference, HFileLink, files starting with "_". Check and
   * skip non-valid hfiles by default, or skip this validation by setting
   * 'hbase.loadincremental.validate.hfile' to false.
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

  // Initialize a thread pool
  private ExecutorService createExecutorService() {
    ThreadPoolExecutor pool = new ThreadPoolExecutor(nrThreads, nrThreads, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        new ThreadFactoryBuilder().setNameFormat("LoadIncrementalHFiles-%1$d").build());
    pool.allowCoreThreadTimeOut(true);
    return pool;
  }

  private final String toString(List<Pair<byte[], String>> list) {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    list.forEach(p -> {
      sb.append('{').append(Bytes.toStringBinary(p.getFirst())).append(',').append(p.getSecond())
          .append('}');
    });
    sb.append(']');
    return sb.toString();
  }

  private boolean isSecureBulkLoadEndpointAvailable() {
    String classes = getConf().get(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, "");
    return classes.contains("org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint");
  }

  /**
   * Split a storefile into a top and bottom half, maintaining the metadata, recreating bloom
   * filters, etc.
   */
  @VisibleForTesting
  static void splitStoreFile(Configuration conf, Path inFile, ColumnFamilyDescriptor familyDesc,
      byte[] splitKey, Path bottomOut, Path topOut) throws IOException {
    // Open reader with no block cache, and not in-memory
    Reference topReference = Reference.createTopReference(splitKey);
    Reference bottomReference = Reference.createBottomReference(splitKey);

    copyHFileHalf(conf, inFile, topOut, topReference, familyDesc);
    copyHFileHalf(conf, inFile, bottomOut, bottomReference, familyDesc);
  }

  /**
   * Copy half of an HFile into a new HFile.
   */
  private static void copyHFileHalf(Configuration conf, Path inFile, Path outFile,
      Reference reference, ColumnFamilyDescriptor familyDescriptor) throws IOException {
    FileSystem fs = inFile.getFileSystem(conf);
    CacheConfig cacheConf = CacheConfig.DISABLED;
    HalfStoreFileReader halfReader = null;
    StoreFileWriter halfWriter = null;
    try {
      ReaderContext context = new ReaderContextBuilder().withFileSystemAndPath(fs, inFile).build();
      HFileInfo hfile = new HFileInfo(context, conf);
      halfReader =
        new HalfStoreFileReader(context, hfile, cacheConf, reference, new AtomicInteger(0), conf);
      hfile.initMetaAndIndex(halfReader.getHFileReader());
      Map<byte[], byte[]> fileInfo = halfReader.loadFileInfo();

      int blocksize = familyDescriptor.getBlocksize();
      Algorithm compression = familyDescriptor.getCompressionType();
      BloomType bloomFilterType = familyDescriptor.getBloomFilterType();
      HFileContext hFileContext = new HFileContextBuilder().withCompression(compression)
          .withChecksumType(HStore.getChecksumType(conf))
          .withBytesPerCheckSum(HStore.getBytesPerChecksum(conf)).withBlockSize(blocksize)
          .withDataBlockEncoding(familyDescriptor.getDataBlockEncoding()).withIncludesTags(true)
          .build();
      halfWriter = new StoreFileWriter.Builder(conf, cacheConf, fs).withFilePath(outFile)
          .withBloomType(bloomFilterType).withFileContext(hFileContext).build();
      HFileScanner scanner = halfReader.getScanner(false, false, false);
      scanner.seekTo();
      do {
        halfWriter.append(scanner.getCell());
      } while (scanner.next());

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

  private static boolean shouldCopyHFileMetaKey(byte[] key) {
    // skip encoding to keep hfile meta consistent with data block info, see HBASE-15085
    if (Bytes.equals(key, HFileDataBlockEncoder.DATA_BLOCK_ENCODING)) {
      return false;
    }

    return !HFileInfo.isReservedFileInfoKey(key);
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

  protected final Map<LoadQueueItem, ByteBuffer> run(Path hfofDir, TableName tableName)
      throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(getConf());
        Admin admin = connection.getAdmin()) {
      if (!admin.tableExists(tableName)) {
        if (isCreateTable()) {
          createTable(tableName, hfofDir, admin);
        } else {
          String errorMsg = format("Table '%s' does not exist.", tableName);
          LOG.error(errorMsg);
          throw new TableNotFoundException(errorMsg);
        }
      }
      try (Table table = connection.getTable(tableName);
          RegionLocator locator = connection.getRegionLocator(tableName)) {
        return doBulkLoad(hfofDir, admin, table, locator, isSilence(),
            isAlwaysCopyFiles());
      }
    }
  }
  /**
   * Perform bulk load on the given table.
   * @param hfofDir the directory that was provided as the output path of a job using
   *          HFileOutputFormat
   * @param tableName the table to load into
   */
  public Map<LoadQueueItem, ByteBuffer> run(String hfofDir, TableName tableName)
      throws IOException {
    return run(new Path(hfofDir), tableName);
  }

  /**
   * Perform bulk load on the given table.
   * @param family2Files map of family to List of hfiles
   * @param tableName the table to load into
   */
  public Map<LoadQueueItem, ByteBuffer> run(Map<byte[], List<Path>> family2Files,
      TableName tableName) throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(getConf());
        Admin admin = connection.getAdmin()) {
      if (!admin.tableExists(tableName)) {
        String errorMsg = format("Table '%s' does not exist.", tableName);
        LOG.error(errorMsg);
        throw new TableNotFoundException(errorMsg);
      }
      try (Table table = connection.getTable(tableName);
          RegionLocator locator = connection.getRegionLocator(tableName)) {
        return doBulkLoad(family2Files, admin, table, locator, isSilence(), isAlwaysCopyFiles());
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2 && args.length != 3) {
      usage();
      return -1;
    }
    // Re-initialize to apply -D options from the command line parameters
    initialize();
    String dirPath = args[0];
    TableName tableName = TableName.valueOf(args[1]);
    if (args.length == 2) {
      return !run(dirPath, tableName).isEmpty() ? 0 : -1;
    } else {
      Map<byte[], List<Path>> family2Files = Maps.newHashMap();
      FileSystem fs = FileSystem.get(getConf());
      for (FileStatus regionDir : fs.listStatus(new Path(dirPath))) {
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
      return !run(family2Files, tableName).isEmpty() ? 0 : -1;
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    int ret = ToolRunner.run(conf, new LoadIncrementalHFiles(conf), args);
    System.exit(ret);
  }

  /**
   * Called from replication sink, where it manages bulkToken(staging directory) by itself. This is
   * used only when SecureBulkLoadEndpoint is configured in hbase.coprocessor.region.classes
   * property. This directory is used as a temporary directory where all files are initially
   * copied/moved from user given directory, set all the required file permissions and then from
   * their it is finally loaded into a table. This should be set only when, one would like to manage
   * the staging directory by itself. Otherwise this tool will handle this by itself.
   * @param stagingDir staging directory path
   */
  public void setBulkToken(String stagingDir) {
    this.bulkToken = stagingDir;
  }

  public void setClusterIds(List<String> clusterIds) {
    this.clusterIds = clusterIds;
  }

  /**
   * Disables replication for these bulkloaded files.
   */
  public void disableReplication(){
    this.replicate = false;
  }
  /**
   * Infers region boundaries for a new table.
   * <p>
   * Parameter: <br>
   * bdryMap is a map between keys to an integer belonging to {+1, -1}
   * <ul>
   * <li>If a key is a start key of a file, then it maps to +1</li>
   * <li>If a key is an end key of a file, then it maps to -1</li>
   * </ul>
   * <p>
   * Algo:<br>
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
}
