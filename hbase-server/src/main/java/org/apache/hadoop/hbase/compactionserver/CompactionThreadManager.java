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
package org.apache.hadoop.hbase.compactionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncRegionServerAdmin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.mob.MobFileCache;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.regionserver.CompactThreadControl;
import org.apache.hadoop.hbase.regionserver.HMobStore;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputControllerService;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.Pair;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CompactionProtos.CompleteCompactionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CompactionProtos.CompleteCompactionRequest.Builder;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CompactionProtos.CompleteCompactionResponse;

/**
 * CompactionThreadManager reuse {@link HStore#selectCompaction}, {@link HStore#throttleCompaction},
 * {@link CompactionContext#compact}, {@link CompactThreadControl}, which are core logic of
 * compaction.
 */
@InterfaceAudience.Private
public class CompactionThreadManager implements ThroughputControllerService {
  private static Logger LOG = LoggerFactory.getLogger(CompactionThreadManager.class);
  // Configuration key for the large compaction threads.
  private final static String LARGE_COMPACTION_THREADS =
      "hbase.compaction.server.thread.compaction.large";
  private final static int LARGE_COMPACTION_THREADS_DEFAULT = 10;
  // Configuration key for the small compaction threads.
  private final static String SMALL_COMPACTION_THREADS =
      "hbase.compaction.server.thread.compaction.small";
  private final static int SMALL_COMPACTION_THREADS_DEFAULT = 50;

  private final Configuration conf;
  private final HCompactionServer server;
  private Path rootDir;
  private FSTableDescriptors tableDescriptors;
  private CompactThreadControl compactThreadControl;
  private ConcurrentHashMap<String, CompactionTask> runningCompactionTasks =
      new ConcurrentHashMap<>();
  private static CompactionFilesCache compactionFilesCache = new CompactionFilesCache();

  CompactionThreadManager(final Configuration conf, HCompactionServer server) {
    this.conf = conf;
    this.server = server;
    try {
      this.rootDir = CommonFSUtils.getRootDir(this.conf);
      this.tableDescriptors = new FSTableDescriptors(conf);
      int largeThreads =
          Math.max(1, conf.getInt(LARGE_COMPACTION_THREADS, LARGE_COMPACTION_THREADS_DEFAULT));
      int smallThreads = conf.getInt(SMALL_COMPACTION_THREADS, SMALL_COMPACTION_THREADS_DEFAULT);
      compactThreadControl = new CompactThreadControl(this, largeThreads, smallThreads,
          COMPACTION_TASK_COMPARATOR, REJECTION);
    } catch (Throwable t) {
      LOG.error("Failed construction CompactionThreadManager", t);
    }
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public ChoreService getChoreService() {
    return server.getChoreService();
  }

  @Override
  public double getCompactionPressure() {
    double max = 0;
    for (CompactionTask task : getRunningCompactionTasks().values()) {
      double normCount = task.getStore().getCompactionPressure();
      if (normCount > max) {
        max = normCount;
      }
    }
    return max;
  }

  @Override
  public double getFlushPressure() {
    return 0;
  }

  public void requestCompaction(CompactionTask compactionTask) throws IOException {
    try {
      selectFileAndExecuteTask(compactionTask);
    } catch (Throwable e) {
      LOG.error("Failed requestCompaction {}", compactionTask, e);
      server.checkFileSystem();
      // count the failed during selectFile
      server.requestFailedCount.increment();
      throw new IOException(e);
    }
  }

  /**
   * Open store, and clean stale compacted file in cache
   */
  private HStore openStore(RegionInfo regionInfo, ColumnFamilyDescriptor cfd, boolean major,
      MonitoredTask status) throws IOException {
    status.setStatus("Open store");
    HStore store = getStore(conf, server.getFileSystem(), rootDir,
      tableDescriptors.get(regionInfo.getTable()), regionInfo, cfd.getNameAsString());

    // CompactedHFilesDischarger only run on regionserver, so compactionserver does not have
    // opportunity to clean compacted file at that time, we clean compacted files here
    compactionFilesCache.cleanupCompactedFiles(regionInfo, cfd,
      store.getStorefiles().stream().map(sf -> sf.getPath().getName()).collect(Collectors.toSet()));
    if (major) {
      status.setStatus("Trigger major compaction");
      store.triggerMajorCompaction();
    }
    return store;
  }

  private void selectFileAndExecuteTask(CompactionTask compactionTask) throws IOException {
    ServerName rsServerName = compactionTask.getRsServerName();
    RegionInfo regionInfo = compactionTask.getRegionInfo();
    ColumnFamilyDescriptor cfd = compactionTask.getCfd();
    String logStr = compactionTask.toString();
    MonitoredTask status =
        TaskMonitor.get().createStatus("Compacting region: " + regionInfo.getRegionNameAsString()
            + ", family: " + cfd.getNameAsString() + " from RS: " + rsServerName);
    status.enableStatusJournal(false);
    // 1. select compaction and check compaction context is present
    LOG.info("Start select compaction {}", compactionTask);
    status.setStatus("Start select compaction");
    HStore store;
    CompactionContext compactionContext;
    Pair<Boolean, List<String>> updateSelectedFilesCacheResult;
    // the synchronized ensure file in store, selected files in cache, compacted files in cache,
    // the three has consistent state, we need this condition to guarantee correct selection
    synchronized (compactionFilesCache.getCompactedStoreFilesAsLock(regionInfo, cfd)) {
      synchronized (compactionFilesCache.getSelectedStoreFilesAsLock(regionInfo, cfd)) {
        store = openStore(regionInfo, cfd, compactionTask.isRequestMajor(), status);
        store.setFavoredNodes(compactionTask.getFavoredNodes());
        Optional<CompactionContext> compaction = selectCompaction(store, regionInfo, cfd,
          compactionTask.isRequestMajor(), compactionTask.getPriority(), status, logStr);
        if (!compaction.isPresent()) {
          store.close();
          LOG.info("Compaction context is empty: {}", compactionTask);
          status.abort("Compaction context is empty and return");
          return;
        }
        compactionContext = compaction.get();
        // 2. update compactionFilesCache
        updateSelectedFilesCacheResult =
            updateStorageAfterSelectCompaction(regionInfo, cfd, compactionContext, status, logStr);
      } // end of synchronized selected files
    } // end of synchronized compacted files
    if (!updateSelectedFilesCacheResult.getFirst()) {
      store.close();
      return;
    }
    List<String> selectedFileNames = updateSelectedFilesCacheResult.getSecond();
    compactionTask.setHStore(store);
    compactionTask.setCompactionContext(compactionContext);
    compactionTask.setSelectedFileNames(selectedFileNames);
    compactionTask.setMonitoredTask(status);
    compactionTask.setPriority(compactionContext.getRequest().getPriority());
    // 3. execute a compaction task
    ThreadPoolExecutor pool;
    pool = store.throttleCompaction(compactionContext.getRequest().getSize())
        ? compactThreadControl.getLongCompactions()
        : compactThreadControl.getShortCompactions();
    pool.submit(new CompactionTaskRunner(compactionTask));
  }

  /**
   * select compaction context
   * @return CompactionContext
   */
  Optional<CompactionContext> selectCompaction(HStore store, RegionInfo regionInfo,
      ColumnFamilyDescriptor cfd, boolean major, int priority, MonitoredTask status, String logStr)
      throws IOException {
    // get current compacting and compacted files, NOTE: these files are file names only, don't
    // include paths.
    status.setStatus("Get current compacting and compacted files from compactionFilesCache");
    Set<String> compactingFiles = compactionFilesCache.getSelectedStoreFiles(regionInfo, cfd);
    Set<String> compactedFiles = compactionFilesCache.getCompactedStoreFiles(regionInfo, cfd);
    Set<String> excludeFiles = new HashSet<>(compactingFiles);
    excludeFiles.addAll(compactedFiles);
    // Convert files names to store files
    status.setStatus("Convert current compacting and compacted files to store files");
    List<HStoreFile> excludeStoreFiles = getExcludedStoreFiles(store, excludeFiles);
    LOG.info(
      "Start select store: {}, excludeFileNames: {}, excluded: {}, compacting: {}, compacted: {}",
      logStr, excludeFiles.size(), excludeStoreFiles.size(), compactingFiles.size(),
      compactedFiles.size());
    status.setStatus("Select store files to compaction, major: " + major);
    Optional<CompactionContext> compaction = store.selectCompaction(priority,
      CompactionLifeCycleTracker.DUMMY, null, excludeStoreFiles);
    LOG.info("After select store: {}, if compaction context is present: {}", logStr,
      compaction.isPresent());
    return compaction;
  }

  /**
   * Mark files in compaction context as selected in compactionFilesCache
   * @return True if success, otherwise if files are already in selected compactionFilesCache
   */
  private Pair<Boolean, List<String>> updateStorageAfterSelectCompaction(RegionInfo regionInfo,
      ColumnFamilyDescriptor cfd, CompactionContext compactionContext, MonitoredTask status,
      String logStr) {
    LOG.info("Start update compactionFilesCache after select compaction: {}", logStr);
    // save selected files to compactionFilesCache
    List<String> selectedFilesNames = new ArrayList<>();
    for (HStoreFile selectFile : compactionContext.getRequest().getFiles()) {
      selectedFilesNames.add(selectFile.getFileInfo().getPath().getName());
    }
    if (compactionFilesCache.addSelectedFiles(regionInfo, cfd, selectedFilesNames)) {
      LOG.info("Update compactionFilesCache after select compaction success: {}", logStr);
      status.setStatus("Update compactionFilesCache after select compaction success");
      return new Pair<>(Boolean.TRUE, selectedFilesNames);
    } else {
      //should not happen
      LOG.info("selected files are already in store and return: {}", logStr);
      status.abort("Selected files are already in compactionFilesCache and return");
      return new Pair<>(Boolean.FALSE, Collections.EMPTY_LIST);
    }
  }

  /**
   * Execute compaction in the process of compaction server
   */
  private void doCompaction(CompactionTask compactionTask) throws IOException {
    RegionInfo regionInfo = compactionTask.getRegionInfo();
    ColumnFamilyDescriptor cfd = compactionTask.getCfd();
    HStore store = compactionTask.getStore();
    CompactionContext compactionContext = compactionTask.getCompactionContext();
    List<String> selectedFileNames = compactionTask.getSelectedFileNames();
    MonitoredTask status = compactionTask.getStatus();
    try {
      LOG.info("Start compact store: {}, cf: {}, compaction context: {}", store, cfd,
        compactionContext);
      List<Path> newFiles =
          compactionContext.compact(compactThreadControl.getCompactionThroughputController(), null);
      LOG.info("Finish compact store: {}, cf: {}, new files: {}", store, cfd, newFiles);
      List<String> newFileNames = new ArrayList<>();
      for (Path newFile : newFiles) {
        newFileNames.add(newFile.getName());
      }
      reportCompactionCompleted(compactionTask, newFileNames, status);
    } finally {
      status.setStatus("Remove selected files");
      LOG.info("Remove selected files: {}", compactionTask);
      compactionFilesCache.removeSelectedFiles(regionInfo, cfd, selectedFileNames);
    }
  }

  /**
   * Report compaction completed to RS
   */
  private void reportCompactionCompleted(CompactionTask task, List<String> newFiles,
      MonitoredTask status) {
    ServerName rsServerName = task.getRsServerName();
    RegionInfo regionInfo = task.getRegionInfo();
    ColumnFamilyDescriptor cfd = task.getCfd();
    List<String> selectedFileNames = task.getSelectedFileNames();
    boolean newForceMajor = task.getStore().getForceMajor();
    Builder builder =
        CompleteCompactionRequest.newBuilder().setRegionInfo(ProtobufUtil.toRegionInfo(regionInfo))
            .setFamily(ProtobufUtil.toColumnFamilySchema(cfd)).setNewForceMajor(newForceMajor);
    // use file name only, dose not include path, because the size of protobuf is too big
    for (String selectFile : selectedFileNames) {
      builder.addSelectedFiles(selectFile);
    }
    for (String newFile : newFiles) {
      builder.addNewFiles(newFile);
    }
    CompleteCompactionRequest completeCompactionRequest = builder.build();
    AsyncRegionServerAdmin rsAdmin = getRsAdmin(rsServerName);
    try {
      status
          .setStatus("Report complete compaction to RS: " + rsServerName + ", selected file size: "
              + selectedFileNames.size() + ", new file size: " + newFiles.size());
      LOG.info("Report complete compaction: {}, selectedFileSize: {}, newFileSize: {}", task,
        completeCompactionRequest.getSelectedFilesList().size(),
        completeCompactionRequest.getNewFilesList().size());
      CompleteCompactionResponse completeCompactionResponse =
          FutureUtils.get(rsAdmin.completeCompaction(completeCompactionRequest));
      if (completeCompactionResponse.getSuccess()) {
        status.markComplete("Report to RS succeeded and RS accepted");
        // move selected files to compacted files
        compactionFilesCache.addCompactedFiles(regionInfo, cfd, selectedFileNames);
        LOG.info("Compaction manager request complete compaction success. {}", task);
      } else {
        //TODO: maybe region is move, we need get latest regionserver name and retry
        status.abort("Report to RS succeeded but RS denied");
        LOG.warn("Compaction manager request complete compaction fail. {}", task);
      }
    } catch (IOException e) {
      //TODO: rpc call broken, add retry
      status.abort("Report to RS failed");
      LOG.error("Compaction manager request complete compaction error. {}", task, e);
    }
  }

  private List<HStoreFile> getExcludedStoreFiles(HStore store, Set<String> excludeFileNames) {
    Collection<HStoreFile> storefiles = store.getStorefiles();
    List<HStoreFile> storeFiles = new ArrayList<>();
    for (HStoreFile storefile : storefiles) {
      String name = storefile.getPath().getName();
      if (excludeFileNames.contains(name)) {
        storeFiles.add(storefile);
      }
    }
    return storeFiles;
  }

  private HStore getStore(final Configuration conf, final FileSystem fs, final Path rootDir,
      final TableDescriptor htd, final RegionInfo hri, final String familyName) throws IOException {
    HRegionFileSystem regionFs = new HRegionFileSystem(conf, fs,
        CommonFSUtils.getTableDir(rootDir, htd.getTableName()), hri);
    HRegion region = new HRegion(regionFs, null, conf, htd, null);
    ColumnFamilyDescriptor columnFamilyDescriptor = htd.getColumnFamily(Bytes.toBytes(familyName));
    HStore store;
    if (columnFamilyDescriptor.isMobEnabled()) {
      region.setMobFileCache(new MobFileCache(conf));
      store = new HMobStore(region, columnFamilyDescriptor, conf, false);
    } else {
      store = new HStore(region, columnFamilyDescriptor, conf, false);
    }
    OptionalLong maxSequenceId = store.getMaxSequenceId();
    LOG.info("store max sequence id: {}", maxSequenceId.orElse(0));
    region.getMVCC().advanceTo(maxSequenceId.orElse(0));
    return store;
  }

  private AsyncRegionServerAdmin getRsAdmin(final ServerName sn) {
    return server.getAsyncClusterConnection().getRegionServerAdmin(sn);
  }

  ConcurrentHashMap<String, CompactionTask> getRunningCompactionTasks() {
    return runningCompactionTasks;
  }

  void waitForStop() {
    compactThreadControl.waitForStop();
  }

  private void executeCompaction(CompactionTask compactionTask) {
    try {
      String taskName = compactionTask.getRsServerName() + "-"
          + compactionTask.getRegionInfo().getRegionNameAsString() + "-"
          + compactionTask.getCfd().getNameAsString() + "-" + System.currentTimeMillis();
      compactionTask.setTaskName(taskName);
      runningCompactionTasks.put(compactionTask.getTaskName(), compactionTask);
      doCompaction(compactionTask);
    } catch (Throwable e) {
      LOG.error("Execute compaction task error: {}", compactionTask, e);
      server.checkFileSystem();
      //count the failed during do compaction
      server.requestFailedCount.increment();
    } finally {
      runningCompactionTasks.remove(compactionTask.getTaskName());
      if (compactionTask.getStore() != null) {
        try {
          compactionTask.getStore().close();
        } catch (IOException e) {
          LOG.warn("Failed to close store: {}", compactionTask, e);
        }
      }
    }
  }

  /**
   * Cleanup class to use when rejecting a compaction request from the queue.
   */
  private static BiConsumer<Runnable, ThreadPoolExecutor> REJECTION = (runnable, pool) -> {
    {
      if (runnable instanceof CompactionTaskRunner) {
        CompactionTaskRunner runner = (CompactionTaskRunner) runnable;
        LOG.info("Compaction Rejected: " + runner);
        CompactionTask task = runner.getCompactionTask();
        if (task != null) {
          compactionFilesCache.removeSelectedFiles(task.getRegionInfo(), task.getCfd(),
            task.getSelectedFileNames());
        }
      }
    }
  };

  protected class CompactionTaskRunner implements Runnable {
    private CompactionTask compactionTask;

    CompactionTaskRunner(CompactionTask compactionTask) {
      this.compactionTask = compactionTask;
    }

    @Override
    public void run() {
      executeCompaction(compactionTask);
    }

    CompactionTask getCompactionTask() {
      return compactionTask;
    }
  }

  private static final Comparator<Runnable> COMPACTION_TASK_COMPARATOR =
    (Runnable r1, Runnable r2) -> {
      // CompactionRunner first
      if (r1 instanceof CompactionTaskRunner) {
        if (!(r2 instanceof CompactionTaskRunner)) {
          return -1;
        }
      } else {
        if (r2 instanceof CompactionTaskRunner) {
          return 1;
        } else {
          // break the tie based on hash code
          return System.identityHashCode(r1) - System.identityHashCode(r2);
        }
      }
      CompactionTask o1 = ((CompactionTaskRunner) r1).getCompactionTask();
      CompactionTask o2 = ((CompactionTaskRunner) r2).getCompactionTask();
      int cmp = Integer.compare(o1.getPriority(), o2.getPriority());
      if (cmp == 0) {
        return Long.compare(o1.getSubmitTime(), o2.getSubmitTime());
      }
      return cmp;
    };
}
