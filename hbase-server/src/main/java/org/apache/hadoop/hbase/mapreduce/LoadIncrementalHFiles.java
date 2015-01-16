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
package org.apache.hadoop.hbase.mapreduce;

import static java.lang.String.format;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RegionServerCallable;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.client.coprocessor.SecureBulkLoadClient;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.io.HalfStoreFileReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.FsDelegationToken;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSHDFSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Tool to load the output of HFileOutputFormat into an existing table.
 * @see #usage()
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class LoadIncrementalHFiles extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(LoadIncrementalHFiles.class);
  private HBaseAdmin hbAdmin;

  public static final String NAME = "completebulkload";
  public static final String MAX_FILES_PER_REGION_PER_FAMILY
    = "hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily";
  private static final String ASSIGN_SEQ_IDS = "hbase.mapreduce.bulkload.assign.sequenceNumbers";
  public final static String CREATE_TABLE_CONF_KEY = "create.table";

  private int maxFilesPerRegionPerFamily;
  private boolean assignSeqIds;

  // Source filesystem
  private FileSystem fs;
  // Source delegation token
  private FsDelegationToken fsDelegationToken;
  private String bulkToken;
  private UserProvider userProvider;

  private LoadIncrementalHFiles() {}

  public LoadIncrementalHFiles(Configuration conf) throws Exception {
    super(conf);
    initialize();
  }

  private void initialize() throws Exception {
    if (hbAdmin == null) {
      // make a copy, just to be sure we're not overriding someone else's config
      setConf(HBaseConfiguration.create(getConf()));
      Configuration conf = getConf();
      // disable blockcache for tool invocation, see HBASE-10500
      conf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0);
      this.hbAdmin = new HBaseAdmin(conf);
      this.userProvider = UserProvider.instantiate(conf);
      this.fsDelegationToken = new FsDelegationToken(userProvider, "renewer");
      assignSeqIds = conf.getBoolean(ASSIGN_SEQ_IDS, true);
      maxFilesPerRegionPerFamily = conf.getInt(MAX_FILES_PER_REGION_PER_FAMILY, 32);
    }
  }

  private void usage() {
    System.err.println("usage: " + NAME + " /path/to/hfileoutputformat-output tablename" + "\n -D"
        + CREATE_TABLE_CONF_KEY + "=no - can be used to avoid creation of table by this tool\n"
        + "  Note: if you set this to 'no', then the target table must already exist in HBase\n"
        + "\n");
  }

  /**
   * Represents an HFile waiting to be loaded. An queue is used
   * in this class in order to support the case where a region has
   * split during the process of the load. When this happens,
   * the HFile is split into two physical parts across the new
   * region boundary, and each part is added back into the queue.
   * The import process finishes when the queue is empty.
   */
  static class LoadQueueItem {
    final byte[] family;
    final Path hfilePath;

    public LoadQueueItem(byte[] family, Path hfilePath) {
      this.family = family;
      this.hfilePath = hfilePath;
    }

    public String toString() {
      return "family:"+ Bytes.toString(family) + " path:" + hfilePath.toString();
    }
  }

  /**
   * Walk the given directory for all HFiles, and return a Queue
   * containing all such files.
   */
  private void discoverLoadQueue(Deque<LoadQueueItem> ret, Path hfofDir)
  throws IOException {
    fs = hfofDir.getFileSystem(getConf());

    if (!fs.exists(hfofDir)) {
      throw new FileNotFoundException("HFileOutputFormat dir " +
          hfofDir + " not found");
    }

    FileStatus[] familyDirStatuses = fs.listStatus(hfofDir);
    if (familyDirStatuses == null) {
      throw new FileNotFoundException("No families found in " + hfofDir);
    }

    for (FileStatus stat : familyDirStatuses) {
      if (!stat.isDir()) {
        LOG.warn("Skipping non-directory " + stat.getPath());
        continue;
      }
      Path familyDir = stat.getPath();
      if (familyDir.getName().equals("_logs")) {
        // Family name of "_logs" is not supported.
        // This is due to the presence of history directory under _logs directory
        // when hadoop-1 is used
        continue;
      }
      byte[] family = familyDir.getName().getBytes();
      FileStatus[] hfileStatuses = fs.listStatus(familyDir);
      for (FileStatus hfileStatus : hfileStatuses) {
        long length = hfileStatus.getLen();
        Path hfile = hfileStatus.getPath();
        if (hfile.getName().startsWith("_")) continue;
        if(length > getConf().getLong(HConstants.HREGION_MAX_FILESIZE,
             HConstants.DEFAULT_MAX_FILE_SIZE)) {
           LOG.warn("Trying to bulk load hfile " + hfofDir.toString() + " with size: " +
               length + " bytes can be problematic as it may lead to oversplitting.");
         }
        ret.add(new LoadQueueItem(family, hfile));
      }
    }
  }

  /**
   * Perform a bulk load of the given directory into the given
   * pre-existing table.  This method is not threadsafe.
   *
   * @param hfofDir the directory that was provided as the output path
   * of a job using HFileOutputFormat
   * @param table the table to load into
   * @throws TableNotFoundException if table does not yet exist
   */
  @SuppressWarnings("deprecation")
  public void doBulkLoad(Path hfofDir, final HTable table)
    throws TableNotFoundException, IOException
  {
    final HConnection conn = table.getConnection();

    if (!conn.isTableAvailable(table.getName())) {
      throw new TableNotFoundException("Table " +
          Bytes.toStringBinary(table.getTableName()) +
          "is not currently available.");
    }

    // initialize thread pools
    int nrThreads = getConf().getInt("hbase.loadincremental.threads.max",
      Runtime.getRuntime().availableProcessors());
    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    builder.setNameFormat("LoadIncrementalHFiles-%1$d");
    ExecutorService pool = new ThreadPoolExecutor(nrThreads, nrThreads,
        60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(),
        builder.build());
    ((ThreadPoolExecutor)pool).allowCoreThreadTimeOut(true);

    // LQI queue does not need to be threadsafe -- all operations on this queue
    // happen in this thread
    Deque<LoadQueueItem> queue = new LinkedList<LoadQueueItem>();
    try {
      discoverLoadQueue(queue, hfofDir);
      // check whether there is invalid family name in HFiles to be bulkloaded
      Collection<HColumnDescriptor> families = table.getTableDescriptor().getFamilies();
      ArrayList<String> familyNames = new ArrayList<String>();
      for (HColumnDescriptor family : families) {
        familyNames.add(family.getNameAsString());
      }
      ArrayList<String> unmatchedFamilies = new ArrayList<String>();
      for (LoadQueueItem lqi : queue) {
        String familyNameInHFile = Bytes.toString(lqi.family);
        if (!familyNames.contains(familyNameInHFile)) {
          unmatchedFamilies.add(familyNameInHFile);
        }
      }
      if (unmatchedFamilies.size() > 0) {
        String msg =
            "Unmatched family names found: unmatched family names in HFiles to be bulkloaded: "
                + unmatchedFamilies + "; valid family names of table "
                + Bytes.toString(table.getTableName()) + " are: " + familyNames;
        LOG.error(msg);
        throw new IOException(msg);
      }
      int count = 0;

      if (queue.isEmpty()) {
        LOG.warn("Bulk load operation did not find any files to load in " +
            "directory " + hfofDir.toUri() + ".  Does it contain files in " +
            "subdirectories that correspond to column family names?");
        return;
      }

      //If using secure bulk load, get source delegation token, and
      //prepare staging directory and token
      // fs is the source filesystem
      fsDelegationToken.acquireDelegationToken(fs);
      if(isSecureBulkLoadEndpointAvailable()) {
        bulkToken = new SecureBulkLoadClient(table).prepareBulkLoad(table.getName());
      }

      // Assumes that region splits can happen while this occurs.
      while (!queue.isEmpty()) {
        // need to reload split keys each iteration.
        final Pair<byte[][], byte[][]> startEndKeys = table.getStartEndKeys();
        if (count != 0) {
          LOG.info("Split occured while grouping HFiles, retry attempt " +
              + count + " with " + queue.size() + " files remaining to group or split");
        }

        int maxRetries = getConf().getInt("hbase.bulkload.retries.number", 0);
        if (maxRetries != 0 && count >= maxRetries) {
          throw new IOException("Retry attempted " + count +
            " times without completing, bailing out");
        }
        count++;

        // Using ByteBuffer for byte[] equality semantics
        Multimap<ByteBuffer, LoadQueueItem> regionGroups = groupOrSplitPhase(table,
            pool, queue, startEndKeys);

        if (!checkHFilesCountPerRegionPerFamily(regionGroups)) {
          // Error is logged inside checkHFilesCountPerRegionPerFamily.
          throw new IOException("Trying to load more than " + maxFilesPerRegionPerFamily
            + " hfiles to one family of one region");
        }

        bulkLoadPhase(table, conn, pool, queue, regionGroups);

        // NOTE: The next iteration's split / group could happen in parallel to
        // atomic bulkloads assuming that there are splits and no merges, and
        // that we can atomically pull out the groups we want to retry.
      }

    } finally {
      fsDelegationToken.releaseDelegationToken();
      if(bulkToken != null) {
        new SecureBulkLoadClient(table).cleanupBulkLoad(bulkToken);
      }
      pool.shutdown();
      if (queue != null && !queue.isEmpty()) {
        StringBuilder err = new StringBuilder();
        err.append("-------------------------------------------------\n");
        err.append("Bulk load aborted with some files not yet loaded:\n");
        err.append("-------------------------------------------------\n");
        for (LoadQueueItem q : queue) {
          err.append("  ").append(q.hfilePath).append('\n');
        }
        LOG.error(err);
      }
    }

    if (queue != null && !queue.isEmpty()) {
        throw new RuntimeException("Bulk load aborted with some files not yet loaded."
          + "Please check log for more details.");
    }
  }

  /**
   * This takes the LQI's grouped by likely regions and attempts to bulk load
   * them.  Any failures are re-queued for another pass with the
   * groupOrSplitPhase.
   */
  protected void bulkLoadPhase(final HTable table, final HConnection conn,
      ExecutorService pool, Deque<LoadQueueItem> queue,
      final Multimap<ByteBuffer, LoadQueueItem> regionGroups) throws IOException {
    // atomically bulk load the groups.
    Set<Future<List<LoadQueueItem>>> loadingFutures = new HashSet<Future<List<LoadQueueItem>>>();
    for (Entry<ByteBuffer, ? extends Collection<LoadQueueItem>> e: regionGroups.asMap().entrySet()) {
      final byte[] first = e.getKey().array();
      final Collection<LoadQueueItem> lqis =  e.getValue();

      final Callable<List<LoadQueueItem>> call = new Callable<List<LoadQueueItem>>() {
        public List<LoadQueueItem> call() throws Exception {
          List<LoadQueueItem> toRetry =
              tryAtomicRegionLoad(conn, table.getName(), first, lqis);
          return toRetry;
        }
      };
      loadingFutures.add(pool.submit(call));
    }

    // get all the results.
    for (Future<List<LoadQueueItem>> future : loadingFutures) {
      try {
        List<LoadQueueItem> toRetry = future.get();

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
        throw (InterruptedIOException)new InterruptedIOException().initCause(e1);
      }
    }
  }

  private boolean checkHFilesCountPerRegionPerFamily(
      final Multimap<ByteBuffer, LoadQueueItem> regionGroups) {
    for (Entry<ByteBuffer,
        ? extends Collection<LoadQueueItem>> e: regionGroups.asMap().entrySet()) {
      final Collection<LoadQueueItem> lqis =  e.getValue();
      HashMap<byte[], MutableInt> filesMap = new HashMap<byte[], MutableInt>();
      for (LoadQueueItem lqi: lqis) {
        MutableInt count = filesMap.get(lqi.family);
        if (count == null) {
          count = new MutableInt();
          filesMap.put(lqi.family, count);
        }
        count.increment();
        if (count.intValue() > maxFilesPerRegionPerFamily) {
          LOG.error("Trying to load more than " + maxFilesPerRegionPerFamily
            + " hfiles to family " + Bytes.toStringBinary(lqi.family)
            + " of region with start key "
            + Bytes.toStringBinary(e.getKey()));
          return false;
        }
      }
    }
    return true;
  }

  /**
   * @return A Multimap<startkey, LoadQueueItem> that groups LQI by likely
   * bulk load region targets.
   */
  private Multimap<ByteBuffer, LoadQueueItem> groupOrSplitPhase(final HTable table,
      ExecutorService pool, Deque<LoadQueueItem> queue,
      final Pair<byte[][], byte[][]> startEndKeys) throws IOException {
    // <region start key, LQI> need synchronized only within this scope of this
    // phase because of the puts that happen in futures.
    Multimap<ByteBuffer, LoadQueueItem> rgs = HashMultimap.create();
    final Multimap<ByteBuffer, LoadQueueItem> regionGroups = Multimaps.synchronizedMultimap(rgs);

    // drain LQIs and figure out bulk load groups
    Set<Future<List<LoadQueueItem>>> splittingFutures = new HashSet<Future<List<LoadQueueItem>>>();
    while (!queue.isEmpty()) {
      final LoadQueueItem item = queue.remove();

      final Callable<List<LoadQueueItem>> call = new Callable<List<LoadQueueItem>>() {
        public List<LoadQueueItem> call() throws Exception {
          List<LoadQueueItem> splits = groupOrSplit(regionGroups, item, table, startEndKeys);
          return splits;
        }
      };
      splittingFutures.add(pool.submit(call));
    }
    // get all the results.  All grouping and splitting must finish before
    // we can attempt the atomic loads.
    for (Future<List<LoadQueueItem>> lqis : splittingFutures) {
      try {
        List<LoadQueueItem> splits = lqis.get();
        if (splits != null) {
          queue.addAll(splits);
        }
      } catch (ExecutionException e1) {
        Throwable t = e1.getCause();
        if (t instanceof IOException) {
          LOG.error("IOException during splitting", e1);
          throw (IOException)t; // would have been thrown if not parallelized,
        }
        LOG.error("Unexpected execution exception during splitting", e1);
        throw new IllegalStateException(t);
      } catch (InterruptedException e1) {
        LOG.error("Unexpected interrupted exception during splitting", e1);
        throw (InterruptedIOException)new InterruptedIOException().initCause(e1);
      }
    }
    return regionGroups;
  }

  // unique file name for the table
  private String getUniqueName() {
    return UUID.randomUUID().toString().replaceAll("-", "");
  }

  protected List<LoadQueueItem> splitStoreFile(final LoadQueueItem item,
      final HTable table, byte[] startKey,
      byte[] splitKey) throws IOException {
    final Path hfilePath = item.hfilePath;

    // We use a '_' prefix which is ignored when walking directory trees
    // above.
    final Path tmpDir = new Path(item.hfilePath.getParent(), "_tmp");

    LOG.info("HFile at " + hfilePath + " no longer fits inside a single " +
    "region. Splitting...");

    String uniqueName = getUniqueName();
    HColumnDescriptor familyDesc = table.getTableDescriptor().getFamily(item.family);
    Path botOut = new Path(tmpDir, uniqueName + ".bottom");
    Path topOut = new Path(tmpDir, uniqueName + ".top");
    splitStoreFile(getConf(), hfilePath, familyDesc, splitKey,
        botOut, topOut);

    FileSystem fs = tmpDir.getFileSystem(getConf());
    fs.setPermission(tmpDir, FsPermission.valueOf("-rwxrwxrwx"));
    fs.setPermission(botOut, FsPermission.valueOf("-rwxrwxrwx"));

    // Add these back at the *front* of the queue, so there's a lower
    // chance that the region will just split again before we get there.
    List<LoadQueueItem> lqis = new ArrayList<LoadQueueItem>(2);
    lqis.add(new LoadQueueItem(item.family, botOut));
    lqis.add(new LoadQueueItem(item.family, topOut));

    LOG.info("Successfully split into new HFiles " + botOut + " and " + topOut);
    return lqis;
  }

  /**
   * Attempt to assign the given load queue item into its target region group.
   * If the hfile boundary no longer fits into a region, physically splits
   * the hfile such that the new bottom half will fit and returns the list of
   * LQI's corresponding to the resultant hfiles.
   *
   * protected for testing
   * @throws IOException
   */
  protected List<LoadQueueItem> groupOrSplit(Multimap<ByteBuffer, LoadQueueItem> regionGroups,
      final LoadQueueItem item, final HTable table,
      final Pair<byte[][], byte[][]> startEndKeys)
      throws IOException {
    final Path hfilePath = item.hfilePath;
    HFile.Reader hfr = HFile.createReader(fs, hfilePath,
        new CacheConfig(getConf()), getConf());
    final byte[] first, last;
    try {
      hfr.loadFileInfo();
      first = hfr.getFirstRowKey();
      last = hfr.getLastRowKey();
    }  finally {
      hfr.close();
    }

    LOG.info("Trying to load hfile=" + hfilePath +
        " first=" + Bytes.toStringBinary(first) +
        " last="  + Bytes.toStringBinary(last));
    if (first == null || last == null) {
      assert first == null && last == null;
      // TODO what if this is due to a bad HFile?
      LOG.info("hfile " + hfilePath + " has no entries, skipping");
      return null;
    }
    if (Bytes.compareTo(first, last) > 0) {
      throw new IllegalArgumentException(
      "Invalid range: " + Bytes.toStringBinary(first) +
      " > " + Bytes.toStringBinary(last));
    }
    int idx = Arrays.binarySearch(startEndKeys.getFirst(), first,
        Bytes.BYTES_COMPARATOR);
    if (idx < 0) {
      // not on boundary, returns -(insertion index).  Calculate region it
      // would be in.
      idx = -(idx + 1) - 1;
    }
    final int indexForCallable = idx;

    /**
     * we can consider there is a region hole in following conditions. 1) if idx < 0,then first
     * region info is lost. 2) if the endkey of a region is not equal to the startkey of the next
     * region. 3) if the endkey of the last region is not empty.
     */
    if (indexForCallable < 0) {
      throw new IOException("The first region info for table "
          + Bytes.toString(table.getTableName())
          + " cann't be found in hbase:meta.Please use hbck tool to fix it first.");
    } else if ((indexForCallable == startEndKeys.getFirst().length - 1)
        && !Bytes.equals(startEndKeys.getSecond()[indexForCallable], HConstants.EMPTY_BYTE_ARRAY)) {
      throw new IOException("The last region info for table "
          + Bytes.toString(table.getTableName())
          + " cann't be found in hbase:meta.Please use hbck tool to fix it first.");
    } else if (indexForCallable + 1 < startEndKeys.getFirst().length
        && !(Bytes.compareTo(startEndKeys.getSecond()[indexForCallable],
          startEndKeys.getFirst()[indexForCallable + 1]) == 0)) {
      throw new IOException("The endkey of one region for table "
          + Bytes.toString(table.getTableName())
          + " is not equal to the startkey of the next region in hbase:meta."
          + "Please use hbck tool to fix it first.");
    }

    boolean lastKeyInRange =
      Bytes.compareTo(last, startEndKeys.getSecond()[idx]) < 0 ||
      Bytes.equals(startEndKeys.getSecond()[idx], HConstants.EMPTY_BYTE_ARRAY);
    if (!lastKeyInRange) {
      List<LoadQueueItem> lqis = splitStoreFile(item, table,
          startEndKeys.getFirst()[indexForCallable],
          startEndKeys.getSecond()[indexForCallable]);
      return lqis;
    }

    // group regions.
    regionGroups.put(ByteBuffer.wrap(startEndKeys.getFirst()[idx]), item);
    return null;
  }

  /**
   * @deprecated Use {@link #tryAtomicRegionLoad(HConnection, TableName, byte[], Collection)}
   */
  @Deprecated
  protected List<LoadQueueItem> tryAtomicRegionLoad(final HConnection conn,
      final byte [] tableName, final byte[] first, Collection<LoadQueueItem> lqis)
  throws IOException {
    return tryAtomicRegionLoad(conn, TableName.valueOf(tableName), first, lqis);
  }

  /**
   * Attempts to do an atomic load of many hfiles into a region.  If it fails,
   * it returns a list of hfiles that need to be retried.  If it is successful
   * it will return an empty list.
   *
   * NOTE: To maintain row atomicity guarantees, region server callable should
   * succeed atomically and fails atomically.
   *
   * Protected for testing.
   *
   * @return empty list if success, list of items to retry on recoverable
   * failure
   */
  protected List<LoadQueueItem> tryAtomicRegionLoad(final HConnection conn,
      final TableName tableName, final byte[] first, Collection<LoadQueueItem> lqis)
  throws IOException {
    final List<Pair<byte[], String>> famPaths =
      new ArrayList<Pair<byte[], String>>(lqis.size());
    for (LoadQueueItem lqi : lqis) {
      famPaths.add(Pair.newPair(lqi.family, lqi.hfilePath.toString()));
    }

    final RegionServerCallable<Boolean> svrCallable =
        new RegionServerCallable<Boolean>(conn, tableName, first) {
      @Override
      public Boolean call() throws Exception {
        SecureBulkLoadClient secureClient = null;
        boolean success = false;

        try {
          LOG.debug("Going to connect to server " + getLocation() + " for row "
              + Bytes.toStringBinary(getRow()) + " with hfile group " + famPaths);
          byte[] regionName = getLocation().getRegionInfo().getRegionName();
          if (!isSecureBulkLoadEndpointAvailable()) {
            success = ProtobufUtil.bulkLoadHFile(getStub(), famPaths, regionName, assignSeqIds);
          } else {
            HTable table = new HTable(conn.getConfiguration(), getTableName());
            secureClient = new SecureBulkLoadClient(table);
            success = secureClient.bulkLoadHFiles(famPaths, fsDelegationToken.getUserToken(),
              bulkToken, getLocation().getRegionInfo().getStartKey());
          }
          return success;
        } finally {
          //Best effort copying of files that might not have been imported
          //from the staging directory back to original location
          //in user directory
          if(secureClient != null && !success) {
            FileSystem targetFs = FileSystem.get(getConf());
            // Check to see if the source and target filesystems are the same
            // If they are the same filesystem, we will try move the files back
            // because previously we moved them to the staging directory.
            if (FSHDFSUtils.isSameHdfs(getConf(), fs, targetFs)) {
              for(Pair<byte[], String> el : famPaths) {
                Path hfileStagingPath = null;
                Path hfileOrigPath = new Path(el.getSecond());
                try {
                  hfileStagingPath= new Path(secureClient.getStagingPath(bulkToken, el.getFirst()),
                    hfileOrigPath.getName());
                  if(targetFs.rename(hfileStagingPath, hfileOrigPath)) {
                    LOG.debug("Moved back file " + hfileOrigPath + " from " +
                        hfileStagingPath);
                  } else if(targetFs.exists(hfileStagingPath)){
                    LOG.debug("Unable to move back file " + hfileOrigPath + " from " +
                        hfileStagingPath);
                  }
                } catch(Exception ex) {
                  LOG.debug("Unable to move back file " + hfileOrigPath + " from " +
                      hfileStagingPath, ex);
                }
              }
            }
          }
        }
      }
    };

    try {
      List<LoadQueueItem> toRetry = new ArrayList<LoadQueueItem>();
      Configuration conf = getConf();
      boolean success = RpcRetryingCallerFactory.instantiate(conf).<Boolean> newCaller()
          .callWithRetries(svrCallable);
      if (!success) {
        LOG.warn("Attempt to bulk load region containing "
            + Bytes.toStringBinary(first) + " into table "
            + tableName  + " with files " + lqis
            + " failed.  This is recoverable and they will be retried.");
        toRetry.addAll(lqis); // return lqi's to retry
      }
      // success
      return toRetry;
    } catch (IOException e) {
      LOG.error("Encountered unrecoverable error from region server, additional details: "
          + svrCallable.getExceptionMessageAdditionalDetail(), e);
      throw e;
    }
  }
  
  private boolean isSecureBulkLoadEndpointAvailable() {
    String classes = getConf().get(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, "");
    return classes.contains("org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint");
  }

  /**
   * Split a storefile into a top and bottom half, maintaining
   * the metadata, recreating bloom filters, etc.
   */
  static void splitStoreFile(
      Configuration conf, Path inFile,
      HColumnDescriptor familyDesc, byte[] splitKey,
      Path bottomOut, Path topOut) throws IOException
  {
    // Open reader with no block cache, and not in-memory
    Reference topReference = Reference.createTopReference(splitKey);
    Reference bottomReference = Reference.createBottomReference(splitKey);

    copyHFileHalf(conf, inFile, topOut, topReference, familyDesc);
    copyHFileHalf(conf, inFile, bottomOut, bottomReference, familyDesc);
  }

  /**
   * Copy half of an HFile into a new HFile.
   */
  private static void copyHFileHalf(
      Configuration conf, Path inFile, Path outFile, Reference reference,
      HColumnDescriptor familyDescriptor)
  throws IOException {
    FileSystem fs = inFile.getFileSystem(conf);
    CacheConfig cacheConf = new CacheConfig(conf);
    HalfStoreFileReader halfReader = null;
    StoreFile.Writer halfWriter = null;
    try {
      halfReader = new HalfStoreFileReader(fs, inFile, cacheConf, reference, conf);
      Map<byte[], byte[]> fileInfo = halfReader.loadFileInfo();

      int blocksize = familyDescriptor.getBlocksize();
      Algorithm compression = familyDescriptor.getCompression();
      BloomType bloomFilterType = familyDescriptor.getBloomFilterType();
      HFileContext hFileContext = new HFileContextBuilder()
                                  .withCompression(compression)
                                  .withChecksumType(HStore.getChecksumType(conf))
                                  .withBytesPerCheckSum(HStore.getBytesPerChecksum(conf))
                                  .withBlockSize(blocksize)
                                  .withDataBlockEncoding(familyDescriptor.getDataBlockEncoding())
                                  .build();
      halfWriter = new StoreFile.WriterBuilder(conf, cacheConf,
          fs)
              .withFilePath(outFile)
              .withBloomType(bloomFilterType)
              .withFileContext(hFileContext)
              .build();
      HFileScanner scanner = halfReader.getScanner(false, false, false);
      scanner.seekTo();
      do {
        KeyValue kv = scanner.getKeyValue();
        halfWriter.append(kv);
      } while (scanner.next());

      for (Map.Entry<byte[],byte[]> entry : fileInfo.entrySet()) {
        if (shouldCopyHFileMetaKey(entry.getKey())) {
          halfWriter.appendFileInfo(entry.getKey(), entry.getValue());
        }
      }
    } finally {
      if (halfWriter != null) halfWriter.close();
      if (halfReader != null) halfReader.close(cacheConf.shouldEvictOnClose());
    }
  }

  private static boolean shouldCopyHFileMetaKey(byte[] key) {
    return !HFile.isReservedFileInfoKey(key);
  }

  private boolean doesTableExist(TableName tableName) throws Exception {
    return hbAdmin.tableExists(tableName);
  }

  /*
   * Infers region boundaries for a new table.
   * Parameter:
   *   bdryMap is a map between keys to an integer belonging to {+1, -1}
   *     If a key is a start key of a file, then it maps to +1
   *     If a key is an end key of a file, then it maps to -1
   * Algo:
   * 1) Poll on the keys in order:
   *    a) Keep adding the mapped values to these keys (runningSum)
   *    b) Each time runningSum reaches 0, add the start Key from when the runningSum had started to a boundary list.
   * 2) Return the boundary list.
   */
  public static byte[][] inferBoundaries(TreeMap<byte[], Integer> bdryMap) {
    ArrayList<byte[]> keysArray = new ArrayList<byte[]>();
    int runningValue = 0;
    byte[] currStartKey = null;
    boolean firstBoundary = true;

    for (Map.Entry<byte[], Integer> item: bdryMap.entrySet()) {
      if (runningValue == 0) currStartKey = item.getKey();
      runningValue += item.getValue();
      if (runningValue == 0) {
        if (!firstBoundary) keysArray.add(currStartKey);
        firstBoundary = false;
      }
    }

    return keysArray.toArray(new byte[0][0]);
  }

  /*
   * If the table is created for the first time, then "completebulkload" reads the files twice.
   * More modifications necessary if we want to avoid doing it.
   */
  private void createTable(TableName tableName, String dirPath) throws Exception {
    Path hfofDir = new Path(dirPath);
    FileSystem fs = hfofDir.getFileSystem(getConf());

    if (!fs.exists(hfofDir)) {
      throw new FileNotFoundException("HFileOutputFormat dir " +
          hfofDir + " not found");
    }

    FileStatus[] familyDirStatuses = fs.listStatus(hfofDir);
    if (familyDirStatuses == null) {
      throw new FileNotFoundException("No families found in " + hfofDir);
    }

    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd;

    // Add column families
    // Build a set of keys
    byte[][] keys;
    TreeMap<byte[], Integer> map = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);

    for (FileStatus stat : familyDirStatuses) {
      if (!stat.isDir()) {
        LOG.warn("Skipping non-directory " + stat.getPath());
        continue;
      }
      Path familyDir = stat.getPath();
      if (familyDir.getName().equals("_logs")) {
        // Family name of "_logs" is not supported.
        // This is due to the presence of history directory under _logs directory
        // when hadoop-1 is used
        continue;
      }
      byte[] family = familyDir.getName().getBytes();

      hcd = new HColumnDescriptor(family);
      htd.addFamily(hcd);

      Path[] hfiles = FileUtil.stat2Paths(fs.listStatus(familyDir));
      for (Path hfile : hfiles) {
        if (hfile.getName().startsWith("_")) continue;
        HFile.Reader reader = HFile.createReader(fs, hfile,
            new CacheConfig(getConf()), getConf());
        final byte[] first, last;
        try {
          if (hcd.getCompressionType() != reader.getFileContext().getCompression()) {
            hcd.setCompressionType(reader.getFileContext().getCompression());
            LOG.info("Setting compression " + hcd.getCompressionType().name() +
                     " for family " + hcd.toString());
          }
          reader.loadFileInfo();
          first = reader.getFirstRowKey();
          last =  reader.getLastRowKey();

          LOG.info("Trying to figure out region boundaries hfile=" + hfile +
            " first=" + Bytes.toStringBinary(first) +
            " last="  + Bytes.toStringBinary(last));

          // To eventually infer start key-end key boundaries
          Integer value = map.containsKey(first)? map.get(first):0;
          map.put(first, value+1);

          value = map.containsKey(last)? map.get(last):0;
          map.put(last, value-1);
        }  finally {
          reader.close();
        }
      }
    }

    keys = LoadIncrementalHFiles.inferBoundaries(map);
    this.hbAdmin.createTable(htd,keys);

    LOG.info("Table "+ tableName +" is available!!");
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      usage();
      return -1;
    }

    initialize();

    String dirPath = args[0];
    TableName tableName = TableName.valueOf(args[1]);

    boolean tableExists = this.doesTableExist(tableName);
    if (!tableExists) {
      if ("yes".equalsIgnoreCase(getConf().get(CREATE_TABLE_CONF_KEY, "yes"))) {
        this.createTable(tableName, dirPath);
      } else {
        String errorMsg = format("Table '%s' does not exist.", tableName);
        LOG.error(errorMsg);
        throw new TableNotFoundException(errorMsg);
      }
    }

    Path hfofDir = new Path(dirPath);
    HTable table = new HTable(getConf(), tableName);

    doBulkLoad(hfofDir, table);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    int ret = ToolRunner.run(conf, new LoadIncrementalHFiles(), args);
    System.exit(ret);
  }

}
