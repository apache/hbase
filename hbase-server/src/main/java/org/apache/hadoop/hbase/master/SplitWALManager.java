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
package org.apache.hadoop.hbase.master;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_SPLIT_WAL_MAX_SPLITTER;
import static org.apache.hadoop.hbase.HConstants.HBASE_SPLIT_WAL_MAX_SPLITTER;
import static org.apache.hadoop.hbase.master.MasterWalManager.META_FILTER;
import static org.apache.hadoop.hbase.master.MasterWalManager.NON_META_FILTER;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureScheduler;
import org.apache.hadoop.hbase.master.procedure.SplitWALProcedure;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create {@link SplitWALProcedure} for each WAL which need to split. Manage the workers for each
 * {@link SplitWALProcedure}.
 * Total number of workers is (number of online servers) * (HBASE_SPLIT_WAL_MAX_SPLITTER).
 * Helps assign and release workers for split tasks.
 * Provide helper method to delete split WAL file and directory.
 *
 * The user can get the SplitWALProcedures via splitWALs(crashedServer, splitMeta)
 * can get the files that need to split via getWALsToSplit(crashedServer, splitMeta)
 * can delete the splitting WAL and directory via deleteSplitWAL(wal)
 * and deleteSplitWAL(crashedServer)
 * can check if splitting WALs of a crashed server is success via isSplitWALFinished(walPath)
 * can acquire and release a worker for splitting WAL via acquireSplitWALWorker(procedure)
 * and releaseSplitWALWorker(worker, scheduler)
 *
 * This class is to replace the zk-based WAL splitting related code, {@link MasterWalManager},
 * {@link SplitLogManager}, {@link org.apache.hadoop.hbase.zookeeper.ZKSplitLog} and
 * {@link org.apache.hadoop.hbase.coordination.ZKSplitLogManagerCoordination} can be removed
 * after we switch to procedure-based WAL splitting.
 * @see SplitLogManager for the original distributed split WAL manager.
 */
@InterfaceAudience.Private
public class SplitWALManager {
  private static final Logger LOG = LoggerFactory.getLogger(SplitWALManager.class);

  private final MasterServices master;
  private final WorkerAssigner splitWorkerAssigner;
  private final Path rootDir;
  private final FileSystem fs;
  private final Configuration conf;
  private final Path walArchiveDir;

  public SplitWALManager(MasterServices master) throws IOException {
    this.master = master;
    this.conf = master.getConfiguration();
    this.splitWorkerAssigner = new WorkerAssigner(this.master,
        conf.getInt(HBASE_SPLIT_WAL_MAX_SPLITTER, DEFAULT_HBASE_SPLIT_WAL_MAX_SPLITTER),
        new ProcedureEvent<>("split-WAL-worker-assigning"));
    this.rootDir = master.getMasterFileSystem().getWALRootDir();
    this.fs = master.getMasterFileSystem().getWALFileSystem();
    this.walArchiveDir = new Path(this.rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
  }

  public List<Procedure> splitWALs(ServerName crashedServer, boolean splitMeta)
      throws IOException {
    try {
      // 1. list all splitting files
      List<FileStatus> splittingFiles = getWALsToSplit(crashedServer, splitMeta);
      // 2. create corresponding procedures
      return createSplitWALProcedures(splittingFiles, crashedServer);
    } catch (IOException e) {
      LOG.error("Failed to create procedures for splitting WALs of {}", crashedServer, e);
      throw e;
    }
  }

  public List<FileStatus> getWALsToSplit(ServerName serverName, boolean splitMeta)
      throws IOException {
    List<Path> logDirs = master.getMasterWalManager().getLogDirs(Collections.singleton(serverName));
    List<FileStatus> fileStatuses =
      SplitLogManager.getFileList(this.conf, logDirs, splitMeta ? META_FILTER : NON_META_FILTER);
    LOG.info("{} WAL count={}, meta={}", serverName, fileStatuses.size(), splitMeta);
    return fileStatuses;
  }

  private Path getWALSplitDir(ServerName serverName) {
    Path logDir =
        new Path(this.rootDir, AbstractFSWALProvider.getWALDirectoryName(serverName.toString()));
    return logDir.suffix(AbstractFSWALProvider.SPLITTING_EXT);
  }

  /**
   * Archive processed WAL
   */
  public void archive(String wal) throws IOException {
    WALSplitUtil.moveWAL(this.fs, new Path(wal), this.walArchiveDir);
  }

  public void deleteWALDir(ServerName serverName) throws IOException {
    Path splitDir = getWALSplitDir(serverName);
    try {
      if (!fs.delete(splitDir, false)) {
        LOG.warn("Failed delete {}, contains {}", splitDir, fs.listFiles(splitDir, true));
      }
    } catch (PathIsNotEmptyDirectoryException e) {
      FileStatus [] files = CommonFSUtils.listStatus(fs, splitDir);
      LOG.warn("PathIsNotEmptyDirectoryException {}",
        Arrays.stream(files).map(f -> f.getPath()).collect(Collectors.toList()));
      throw e;
    }
  }

  public boolean isSplitWALFinished(String walPath) throws IOException {
    return !fs.exists(new Path(rootDir, walPath));
  }

  List<Procedure> createSplitWALProcedures(List<FileStatus> splittingWALs,
      ServerName crashedServer) {
    return splittingWALs.stream()
        .map(wal -> new SplitWALProcedure(wal.getPath().toString(), crashedServer))
        .collect(Collectors.toList());
  }

  /**
   * Acquire a split WAL worker
   * @param procedure split WAL task
   * @return an available region server which could execute this task
   * @throws ProcedureSuspendedException if there is no available worker,
   *         it will throw this exception to WAIT the procedure.
   */
  public ServerName acquireSplitWALWorker(Procedure<?> procedure)
      throws ProcedureSuspendedException {
    Optional<ServerName> worker = splitWorkerAssigner.acquire();
    if (worker.isPresent()) {
      LOG.debug("Acquired split WAL worker={}", worker.get());
      return worker.get();
    }
    splitWorkerAssigner.suspend(procedure);
    throw new ProcedureSuspendedException();
  }

  /**
   * After the worker finished the split WAL task, it will release the worker, and wake up all the
   * suspend procedures in the ProcedureEvent
   * @param worker worker which is about to release
   * @param scheduler scheduler which is to wake up the procedure event
   */
  public void releaseSplitWALWorker(ServerName worker, MasterProcedureScheduler scheduler) {
    LOG.debug("Release split WAL worker={}", worker);
    splitWorkerAssigner.release(worker);
    splitWorkerAssigner.wake(scheduler);
  }

  /**
   * When master restart, there will be a new splitWorkerAssigner. But if there are splitting WAL
   * tasks running on the region server side, they will not be count by the new splitWorkerAssigner.
   * Thus we should add the workers of running tasks to the assigner when we load the procedures
   * from MasterProcWALs.
   * @param worker region server which is executing a split WAL task
   */
  public void addUsedSplitWALWorker(ServerName worker){
    splitWorkerAssigner.addUsedWorker(worker);
  }
}
