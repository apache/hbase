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
package org.apache.hadoop.hbase.procedure.flush;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MetricsMaster;
import org.apache.hadoop.hbase.procedure.MasterProcedureManager;
import org.apache.hadoop.hbase.procedure.Procedure;
import org.apache.hadoop.hbase.procedure.ProcedureCoordinator;
import org.apache.hadoop.hbase.procedure.ProcedureCoordinatorRpcs;
import org.apache.hadoop.hbase.procedure.ZKProcedureCoordinatorRpcs;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.Lists;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class MasterFlushTableProcedureManager extends MasterProcedureManager {

  public static final String FLUSH_TABLE_PROCEDURE_SIGNATURE = "flush-table-proc";

  private static final String FLUSH_TIMEOUT_MILLIS_KEY = "hbase.flush.master.timeoutMillis";
  private static final int FLUSH_TIMEOUT_MILLIS_DEFAULT = 60000;
  private static final String FLUSH_WAKE_MILLIS_KEY = "hbase.flush.master.wakeMillis";
  private static final int FLUSH_WAKE_MILLIS_DEFAULT = 500;

  private static final String FLUSH_PROC_POOL_THREADS_KEY =
      "hbase.flush.procedure.master.threads";
  private static final int FLUSH_PROC_POOL_THREADS_DEFAULT = 1;

  private static final Log LOG = LogFactory.getLog(MasterFlushTableProcedureManager.class);

  private MasterServices master;
  private ProcedureCoordinator coordinator;
  private Map<TableName, Procedure> procMap = new HashMap<TableName, Procedure>();
  private boolean stopped;

  public MasterFlushTableProcedureManager() {};

  @Override
  public void stop(String why) {
    LOG.info("stop: " + why);
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  @Override
  public void initialize(MasterServices master, MetricsMaster metricsMaster)
      throws KeeperException, IOException, UnsupportedOperationException {
    this.master = master;

    // get the configuration for the coordinator
    Configuration conf = master.getConfiguration();
    long wakeFrequency = conf.getInt(FLUSH_WAKE_MILLIS_KEY, FLUSH_WAKE_MILLIS_DEFAULT);
    long timeoutMillis = conf.getLong(FLUSH_TIMEOUT_MILLIS_KEY, FLUSH_TIMEOUT_MILLIS_DEFAULT);
    int threads = conf.getInt(FLUSH_PROC_POOL_THREADS_KEY, FLUSH_PROC_POOL_THREADS_DEFAULT);

    // setup the procedure coordinator
    String name = master.getServerName().toString();
    ThreadPoolExecutor tpool = ProcedureCoordinator.defaultPool(name, threads);
    ProcedureCoordinatorRpcs comms = new ZKProcedureCoordinatorRpcs(
        master.getZooKeeper(), getProcedureSignature(), name);

    this.coordinator = new ProcedureCoordinator(comms, tpool, timeoutMillis, wakeFrequency);
  }

  @Override
  public String getProcedureSignature() {
    return FLUSH_TABLE_PROCEDURE_SIGNATURE;
  }

  @Override
  public void execProcedure(ProcedureDescription desc) throws IOException {

    TableName tableName = TableName.valueOf(desc.getInstance());

    // call pre coproc hook
    MasterCoprocessorHost cpHost = master.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preTableFlush(tableName);
    }

    // Get the list of region servers that host the online regions for table.
    // We use the procedure instance name to carry the table name from the client.
    // It is possible that regions may move after we get the region server list.
    // Each region server will get its own online regions for the table.
    // We may still miss regions that need to be flushed.
    List<Pair<HRegionInfo, ServerName>> regionsAndLocations;

    if (TableName.META_TABLE_NAME.equals(tableName)) {
      regionsAndLocations = new MetaTableLocator().getMetaRegionsAndLocations(
        master.getZooKeeper());
    } else {
      regionsAndLocations = MetaTableAccessor.getTableRegionsAndLocations(
        master.getConnection(), tableName, false);
    }

    Set<String> regionServers = new HashSet<String>(regionsAndLocations.size());
    for (Pair<HRegionInfo, ServerName> region : regionsAndLocations) {
      if (region != null && region.getFirst() != null && region.getSecond() != null) {
        HRegionInfo hri = region.getFirst();
        if (hri.isOffline() && (hri.isSplit() || hri.isSplitParent())) continue;
        regionServers.add(region.getSecond().toString());
      }
    }

    ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher(desc.getInstance());

    // Kick of the global procedure from the master coordinator to the region servers.
    // We rely on the existing Distributed Procedure framework to prevent any concurrent
    // procedure with the same name.
    Procedure proc = coordinator.startProcedure(monitor, desc.getInstance(),
      new byte[0], Lists.newArrayList(regionServers));
    monitor.rethrowException();
    if (proc == null) {
      String msg = "Failed to submit distributed procedure " + desc.getSignature() + " for '"
          + desc.getInstance() + "'. " + "Another flush procedure is running?";
      LOG.error(msg);
      throw new IOException(msg);
    }

    procMap.put(tableName, proc);

    try {
      // wait for the procedure to complete.  A timer thread is kicked off that should cancel this
      // if it takes too long.
      proc.waitForCompleted();
      LOG.info("Done waiting - exec procedure " + desc.getSignature() + " for '"
          + desc.getInstance() + "'");
      LOG.info("Master flush table procedure is successful!");
    } catch (InterruptedException e) {
      ForeignException ee =
          new ForeignException("Interrupted while waiting for flush table procdure to finish", e);
      monitor.receive(ee);
      Thread.currentThread().interrupt();
    } catch (ForeignException e) {
      ForeignException ee =
          new ForeignException("Exception while waiting for flush table procdure to finish", e);
      monitor.receive(ee);
    }
    monitor.rethrowException();
  }

  @Override
  public synchronized boolean isProcedureDone(ProcedureDescription desc) throws IOException {
    // Procedure instance name is the table name.
    TableName tableName = TableName.valueOf(desc.getInstance());
    Procedure proc = procMap.get(tableName);
    if (proc == null) {
      // The procedure has not even been started yet.
      // The client would request the procedure and call isProcedureDone().
      // The HBaseAdmin.execProcedure() wraps both request and isProcedureDone().
      return false;
    }
    // We reply on the existing Distributed Procedure framework to give us the status.
    return proc.isCompleted();
  }

}
