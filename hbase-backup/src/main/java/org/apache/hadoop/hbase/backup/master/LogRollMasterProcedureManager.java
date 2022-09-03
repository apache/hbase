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
package org.apache.hadoop.hbase.backup.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MetricsMaster;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure.MasterProcedureManager;
import org.apache.hadoop.hbase.procedure.Procedure;
import org.apache.hadoop.hbase.procedure.ProcedureCoordinationManager;
import org.apache.hadoop.hbase.procedure.ProcedureCoordinator;
import org.apache.hadoop.hbase.procedure.ProcedureCoordinatorRpcs;
import org.apache.hadoop.hbase.procedure.RegionServerProcedureManager;
import org.apache.hadoop.hbase.procedure.ZKProcedureCoordinationManager;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ProcedureDescription;

/**
 * Master procedure manager for coordinated cluster-wide WAL roll operation, which is run during
 * backup operation, see {@link MasterProcedureManager} and {@link RegionServerProcedureManager}
 */
@InterfaceAudience.Private
public class LogRollMasterProcedureManager extends MasterProcedureManager {
  private static final Logger LOG = LoggerFactory.getLogger(LogRollMasterProcedureManager.class);

  public static final String ROLLLOG_PROCEDURE_SIGNATURE = "rolllog-proc";
  public static final String ROLLLOG_PROCEDURE_NAME = "rolllog";
  public static final String ROLLLOG_PROCEDURE_ID = "ProcId";
  public static final String BACKUP_WAKE_MILLIS_KEY = "hbase.backup.logroll.wake.millis";
  public static final String BACKUP_TIMEOUT_MILLIS_KEY = "hbase.backup.logroll.timeout.millis";
  public static final String BACKUP_POOL_THREAD_NUMBER_KEY =
    "hbase.backup.logroll.pool.thread.number";
  public static final String LOGROLL_PROCEDURE_ENABLED =
    "hbase.backup.logroll.procedure.enabled";

  public static final int BACKUP_WAKE_MILLIS_DEFAULT = 500;
  public static final int BACKUP_TIMEOUT_MILLIS_DEFAULT = 180000;
  public static final int BACKUP_POOL_THREAD_NUMBER_DEFAULT = 8;
  public static final boolean LOGROLL_PROCEDURE_ENABLED_DEFAULT = true;

  private MasterServices master;
  private ProcedureCoordinator coordinator;
  private boolean logRollProcedureEnabled;
  private boolean done;

  @Override
  public void stop(String why) {
    LOG.info("stop: " + why);
  }

  @Override
  public boolean isStopped() {
    return false;
  }

  @Override
  public void initialize(MasterServices master, MetricsMaster metricsMaster)
    throws IOException, UnsupportedOperationException {
    this.master = master;
    this.done = false;

    // setup the default procedure coordinator
    String name = master.getServerName().toString();

    // get the configuration for the coordinator
    Configuration conf = master.getConfiguration();
    long wakeFrequency = conf.getInt(BACKUP_WAKE_MILLIS_KEY, BACKUP_WAKE_MILLIS_DEFAULT);
    long timeoutMillis = conf.getLong(BACKUP_TIMEOUT_MILLIS_KEY, BACKUP_TIMEOUT_MILLIS_DEFAULT);
    int opThreads = conf.getInt(BACKUP_POOL_THREAD_NUMBER_KEY, BACKUP_POOL_THREAD_NUMBER_DEFAULT);

    // setup the default procedure coordinator
    ThreadPoolExecutor tpool = ProcedureCoordinator.defaultPool(name, opThreads);
    ProcedureCoordinationManager coordManager = new ZKProcedureCoordinationManager(master);
    ProcedureCoordinatorRpcs comms =
      coordManager.getProcedureCoordinatorRpcs(getProcedureSignature(), name);
    this.coordinator = new ProcedureCoordinator(comms, tpool, timeoutMillis, wakeFrequency);
    this.logRollProcedureEnabled = conf.getBoolean(LOGROLL_PROCEDURE_ENABLED,
      LOGROLL_PROCEDURE_ENABLED_DEFAULT);
  }

  @Override
  public String getProcedureSignature() {
    return ROLLLOG_PROCEDURE_SIGNATURE;
  }

  @Override
  public byte[] execProcedureWithRet(ProcedureDescription desc) throws IOException {
    if (!isBackupEnabled()) {
      LOG.warn("Backup is not enabled. Check your " + BackupRestoreConstants.BACKUP_ENABLE_KEY
        + " setting");
      throw new IOException("Backup is DISABLED");
    }

    if (logRollProcedureEnabled) {
      NameStringPair pair = desc.getConfigurationList().stream()
        .filter(p -> "backupRoot".equals(p.getName())).findFirst()
        .orElseThrow(() -> new DoNotRetryIOException("backupRoot is not specified"));
      long procId = master.getMasterProcedureExecutor()
        .submitProcedure(new LogRollProcedure(pair.getValue(), master.getConfiguration()));
      return Bytes.toBytes(procId);
    } else {
      execProcedure(desc);
      return null;
    }
  }

  @Override
  public void execProcedure(ProcedureDescription desc) throws IOException {
    this.done = false;
    // start the process on the RS
    ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher(desc.getInstance());
    List<ServerName> serverNames = master.getServerManager().getOnlineServersList();
    List<String> servers = new ArrayList<>();
    for (ServerName sn : serverNames) {
      servers.add(sn.toString());
    }

    List<NameStringPair> conf = desc.getConfigurationList();
    byte[] data = new byte[0];
    if (conf.size() > 0) {
      // Get backup root path
      data = Bytes.toBytes(conf.get(0).getValue());
    }
    Procedure proc = coordinator.startProcedure(monitor, desc.getInstance(), data, servers);
    if (proc == null) {
      String msg = "Failed to submit distributed procedure for '" + desc.getInstance() + "'";
      LOG.error(msg);
      throw new IOException(msg);
    }

    try {
      // wait for the procedure to complete. A timer thread is kicked off that should cancel this
      // if it takes too long.
      proc.waitForCompleted();
      LOG.info("Done waiting - exec procedure for " + desc.getInstance());
      LOG.info("Distributed roll log procedure is successful!");
      this.done = true;
    } catch (InterruptedException e) {
      ForeignException ee =
        new ForeignException("Interrupted while waiting for roll log procdure to finish", e);
      monitor.receive(ee);
      Thread.currentThread().interrupt();
    } catch (ForeignException e) {
      ForeignException ee =
        new ForeignException("Exception while waiting for roll log procdure to finish", e);
      monitor.receive(ee);
    }
    monitor.rethrowException();
  }

  @Override
  public void checkPermissions(ProcedureDescription desc, AccessChecker accessChecker, User user)
    throws IOException {
    // TODO: what permissions checks are needed here?
  }

  private boolean isBackupEnabled() {
    return BackupManager.isBackupEnabled(master.getConfiguration());
  }

  @Override
  public boolean isProcedureDone(ProcedureDescription desc) throws IOException {
    // We have two ways of log roll, one is based on zk, the other is based on proc-v2,
    // which one to use according to the configuration. If proc-v2 is used, a procId
    // will be returned to the client, if it is based on zk, nothing will be returned.
    // So if there is a ROLLLOG_PROCEDURE_ID field in the ProcedureDescription, it means
    // we told the client we did the logroll work with the proc-v2, and we will ask the
    // ProcedureExecutor for the result and return the result to the client. it not, then
    // it means we told the client we did the logroll work with the zk-based procedure,
    // and we will just return the done result.
    Map<String, String> conf = new HashMap<>();
    desc.getConfigurationList().forEach(p -> conf.put(p.getName(), p.getValue()));
    String dummyProcIdStr = conf.get(ROLLLOG_PROCEDURE_ID);
    if (dummyProcIdStr == null) {
      return done;
    } else {
      if (!logRollProcedureEnabled) {
        throw new IOException("LogRollProcedure is DISABLED now. "
          + "Maybe Master is restarted with new settings.");
      }
      long dummyProcId = Long.parseLong(dummyProcIdStr);
      ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
      return procExec.isRunning() && procExec.isFinished(dummyProcId);
    }
  }
}
