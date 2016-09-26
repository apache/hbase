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

package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoordinatedStateException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.quotas.MasterQuotaManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;

/**
 * Helper to synchronously wait on conditions.
 * This will be removed in the future (mainly when the AssignmentManager will be
 * replaced with a Procedure version) by using ProcedureYieldException,
 * and the queue will handle waiting and scheduling based on events.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class ProcedureSyncWait {
  private static final Log LOG = LogFactory.getLog(ProcedureSyncWait.class);

  private ProcedureSyncWait() {}

  @InterfaceAudience.Private
  public interface Predicate<T> {
    T evaluate() throws IOException;
  }

  public static byte[] submitAndWaitProcedure(ProcedureExecutor<MasterProcedureEnv> procExec,
      final Procedure proc) throws IOException {
    long procId = procExec.submitProcedure(proc);
    return waitForProcedureToComplete(procExec, procId);
  }

  private static byte[] waitForProcedureToComplete(ProcedureExecutor<MasterProcedureEnv> procExec,
      final long procId) throws IOException {
    while (!procExec.isFinished(procId) && procExec.isRunning()) {
      // TODO: add a config to make it tunable
      // Dev Consideration: are we waiting forever, or we can set up some timeout value?
      Threads.sleepWithoutInterrupt(250);
    }
    ProcedureInfo result = procExec.getResult(procId);
    if (result != null) {
      if (result.isFailed()) {
        // If the procedure fails, we should always have an exception captured. Throw it.
        throw RemoteProcedureException.fromProto(
          result.getForeignExceptionMessage().getForeignExchangeMessage())
            .unwrapRemoteIOException();
      }
      return result.getResult();
    } else {
      if (procExec.isRunning()) {
        throw new IOException("Procedure " + procId + "not found");
      } else {
        throw new IOException("The Master is Aborting");
      }
    }
  }

  public static <T> T waitFor(MasterProcedureEnv env, String purpose, Predicate<T> predicate)
      throws IOException {
    final Configuration conf = env.getMasterConfiguration();
    final long waitTime = conf.getLong("hbase.master.wait.on.region", 5 * 60 * 1000);
    final long waitingTimeForEvents = conf.getInt("hbase.master.event.waiting.time", 1000);
    return waitFor(env, waitTime, waitingTimeForEvents, purpose, predicate);
  }

  public static <T> T waitFor(MasterProcedureEnv env, long waitTime, long waitingTimeForEvents,
      String purpose, Predicate<T> predicate) throws IOException {
    final long done = EnvironmentEdgeManager.currentTime() + waitTime;
    do {
      T result = predicate.evaluate();
      if (result != null && !result.equals(Boolean.FALSE)) {
        return result;
      }
      try {
        Thread.sleep(waitingTimeForEvents);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while sleeping, waiting on " + purpose);
        throw (InterruptedIOException)new InterruptedIOException().initCause(e);
      }
      LOG.debug("Waiting on " + purpose);
    } while (EnvironmentEdgeManager.currentTime() < done && env.isRunning());

    throw new TimeoutIOException("Timed out while waiting on " + purpose);
  }

  protected static void waitMetaRegions(final MasterProcedureEnv env) throws IOException {
    int timeout = env.getMasterConfiguration().getInt("hbase.client.catalog.timeout", 10000);
    try {
      if (env.getMasterServices().getMetaTableLocator().waitMetaRegionLocation(
            env.getMasterServices().getZooKeeper(), timeout) == null) {
        throw new NotAllMetaRegionsOnlineException();
      }
    } catch (InterruptedException e) {
      throw (InterruptedIOException)new InterruptedIOException().initCause(e);
    }
  }

  protected static void waitRegionServers(final MasterProcedureEnv env) throws IOException {
    final ServerManager sm = env.getMasterServices().getServerManager();
    ProcedureSyncWait.waitFor(env, "server to assign region(s)",
        new ProcedureSyncWait.Predicate<Boolean>() {
      @Override
      public Boolean evaluate() throws IOException {
        List<ServerName> servers = sm.createDestinationServersList();
        return servers != null && !servers.isEmpty();
      }
    });
  }

  protected static List<HRegionInfo> getRegionsFromMeta(final MasterProcedureEnv env,
      final TableName tableName) throws IOException {
    return ProcedureSyncWait.waitFor(env, "regions of table=" + tableName + " from meta",
        new ProcedureSyncWait.Predicate<List<HRegionInfo>>() {
      @Override
      public List<HRegionInfo> evaluate() throws IOException {
        if (TableName.META_TABLE_NAME.equals(tableName)) {
          return new MetaTableLocator().getMetaRegions(env.getMasterServices().getZooKeeper());
        }
        return MetaTableAccessor.getTableRegions(env.getMasterServices().getConnection(),tableName);
      }
    });
  }

  protected static void waitRegionInTransition(final MasterProcedureEnv env,
      final List<HRegionInfo> regions) throws IOException, CoordinatedStateException {
    final AssignmentManager am = env.getMasterServices().getAssignmentManager();
    final RegionStates states = am.getRegionStates();
    for (final HRegionInfo region : regions) {
      ProcedureSyncWait.waitFor(env, "regions " + region.getRegionNameAsString() + " in transition",
          new ProcedureSyncWait.Predicate<Boolean>() {
        @Override
        public Boolean evaluate() throws IOException {
          if (states.isRegionInState(region, State.FAILED_OPEN)) {
            am.regionOffline(region);
          }
          return !states.isRegionInTransition(region);
        }
      });
    }
  }

  protected static MasterQuotaManager getMasterQuotaManager(final MasterProcedureEnv env)
      throws IOException {
    return ProcedureSyncWait.waitFor(env, "quota manager to be available",
        new ProcedureSyncWait.Predicate<MasterQuotaManager>() {
      @Override
      public MasterQuotaManager evaluate() throws IOException {
        return env.getMasterServices().getMasterQuotaManager();
      }
    });
  }
}
