/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.backup.regionserver;

import java.util.HashMap;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.backup.BackupSystemTable;
import org.apache.hadoop.hbase.backup.master.LogRollMasterProcedureManager;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.procedure.ProcedureMember;
import org.apache.hadoop.hbase.procedure.Subprocedure;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;


/**
 * This backup subprocedure implementation forces a log roll on the RS.
 */
public class LogRollBackupSubprocedure extends Subprocedure {
  private static final Log LOG = LogFactory.getLog(LogRollBackupSubprocedure.class);

  private final RegionServerServices rss;
  private final LogRollBackupSubprocedurePool taskManager;
  private FSHLog hlog;

  public LogRollBackupSubprocedure(RegionServerServices rss, ProcedureMember member,
      ForeignExceptionDispatcher errorListener, long wakeFrequency, long timeout,
      LogRollBackupSubprocedurePool taskManager) {

    super(member, LogRollMasterProcedureManager.ROLLLOG_PROCEDURE_NAME, errorListener,
      wakeFrequency, timeout);
    LOG.info("Constructing a LogRollBackupSubprocedure.");
    this.rss = rss;
    this.taskManager = taskManager;
  }

  /**
   * Callable task. TODO. We don't need a thread pool to execute roll log. This can be simplified
   * with no use of subprocedurepool.
   */
  class RSRollLogTask implements Callable<Void> {
    RSRollLogTask() {
    }

    @Override
    public Void call() throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("++ DRPC started: " + rss.getServerName());
      }
      hlog = (FSHLog) rss.getWAL(null);
      long filenum = hlog.getFilenum();

      LOG.info("Trying to roll log in backup subprocedure, current log number: " + filenum);
      hlog.rollWriter(true);
      LOG.info("After roll log in backup subprocedure, current log number: " + hlog.getFilenum());
      // write the log number to hbase:backup.
      BackupSystemTable table = BackupSystemTable.getTable(rss.getConfiguration());
      // sanity check, good for testing
      HashMap<String, String> serverTimestampMap = table.readRegionServerLastLogRollResult();
      String host = rss.getServerName().getHostname();
      String sts = serverTimestampMap.get(host);
      if (sts != null && Long.parseLong(sts) > filenum) {
        LOG.warn("Won't update server's last roll log result: current=" + sts + " new=" + filenum);
        return null;
      }
      table.writeRegionServerLastLogRollResult(host, Long.toString(filenum));
      // TODO: potential leak of HBase connection
      // BackupSystemTable.close();
      return null;
    }

  }

  private void rolllog() throws ForeignException {

    monitor.rethrowException();

    taskManager.submitTask(new RSRollLogTask());
    monitor.rethrowException();

    // wait for everything to complete.
    taskManager.waitForOutstandingTasks();
    monitor.rethrowException();

  }

  @Override
  public void acquireBarrier() throws ForeignException {
    // do nothing, executing in inside barrier step.
  }

  /**
   * do a log roll.
   * @return some bytes
   */
  @Override
  public byte[] insideBarrier() throws ForeignException {
    rolllog();
    // FIXME
    return null;
  }

  /**
   * Cancel threads if they haven't finished.
   */
  @Override
  public void cleanup(Exception e) {
    taskManager.abort("Aborting log roll subprocedure tasks for backup due to error", e);
  }

  /**
   * Hooray!
   */
  public void releaseBarrier() {
    // NO OP
  }

}
