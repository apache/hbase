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
package org.apache.hadoop.hbase.procedure2;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.IdLock;
import org.apache.hadoop.hbase.util.NonceKey;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal cleaner that removes the completed procedure results after a TTL.
 * <p/>
 * NOTE: This is a special case handled in timeoutLoop().
 * <p/>
 * Since the client code looks more or less like:
 *
 * <pre>
 *   procId = master.doOperation()
 *   while (master.getProcResult(procId) == ProcInProgress);
 * </pre>
 *
 * The master should not throw away the proc result as soon as the procedure is done but should wait
 * a result request from the client (see executor.removeResult(procId)) The client will call
 * something like master.isProcDone() or master.getProcResult() which will return the result/state
 * to the client, and it will mark the completed proc as ready to delete. note that the client may
 * not receive the response from the master (e.g. master failover) so, if we delay a bit the real
 * deletion of the proc result the client will be able to get the result the next try.
 */
@InterfaceAudience.Private
class CompletedProcedureCleaner<TEnvironment> extends ProcedureInMemoryChore<TEnvironment> {
  private static final Logger LOG = LoggerFactory.getLogger(CompletedProcedureCleaner.class);

  static final String CLEANER_INTERVAL_CONF_KEY = "hbase.procedure.cleaner.interval";
  private static final int DEFAULT_CLEANER_INTERVAL = 30 * 1000; // 30sec

  private static final String BATCH_SIZE_CONF_KEY = "hbase.procedure.cleaner.evict.batch.size";
  private static final int DEFAULT_BATCH_SIZE = 32;

  private final Map<Long, CompletedProcedureRetainer<TEnvironment>> completed;
  private final Map<NonceKey, Long> nonceKeysToProcIdsMap;
  private final ProcedureStore store;
  private final IdLock procExecutionLock;
  private Configuration conf;

  public CompletedProcedureCleaner(Configuration conf, ProcedureStore store,
      IdLock procExecutionLock, Map<Long, CompletedProcedureRetainer<TEnvironment>> completedMap,
      Map<NonceKey, Long> nonceKeysToProcIdsMap) {
    // set the timeout interval that triggers the periodic-procedure
    super(conf.getInt(CLEANER_INTERVAL_CONF_KEY, DEFAULT_CLEANER_INTERVAL));
    this.completed = completedMap;
    this.nonceKeysToProcIdsMap = nonceKeysToProcIdsMap;
    this.store = store;
    this.procExecutionLock = procExecutionLock;
    this.conf = conf;
  }

  @Override
  protected void periodicExecute(final TEnvironment env) {
    if (completed.isEmpty()) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No completed procedures to cleanup.");
      }
      return;
    }

    final long evictTtl =
      conf.getInt(ProcedureExecutor.EVICT_TTL_CONF_KEY, ProcedureExecutor.DEFAULT_EVICT_TTL);
    final long evictAckTtl = conf.getInt(ProcedureExecutor.EVICT_ACKED_TTL_CONF_KEY,
      ProcedureExecutor.DEFAULT_ACKED_EVICT_TTL);
    final int batchSize = conf.getInt(BATCH_SIZE_CONF_KEY, DEFAULT_BATCH_SIZE);

    final long[] batchIds = new long[batchSize];
    int batchCount = 0;

    final long now = EnvironmentEdgeManager.currentTime();
    final Iterator<Map.Entry<Long, CompletedProcedureRetainer<TEnvironment>>> it =
      completed.entrySet().iterator();
    while (it.hasNext() && store.isRunning()) {
      final Map.Entry<Long, CompletedProcedureRetainer<TEnvironment>> entry = it.next();
      final CompletedProcedureRetainer<TEnvironment> retainer = entry.getValue();
      final Procedure<?> proc = retainer.getProcedure();
      IdLock.Entry lockEntry;
      try {
        lockEntry = procExecutionLock.getLockEntry(proc.getProcId());
      } catch (IOException e) {
        // can only happen if interrupted, so not a big deal to propagate it
        throw new UncheckedIOException(e);
      }
      try {
        // TODO: Select TTL based on Procedure type
        if (retainer.isExpired(now, evictTtl, evictAckTtl)) {
          // Failed procedures aren't persisted in WAL.
          if (!(proc instanceof FailedProcedure)) {
            batchIds[batchCount++] = entry.getKey();
            if (batchCount == batchIds.length) {
              store.delete(batchIds, 0, batchCount);
              batchCount = 0;
            }
          }
          final NonceKey nonceKey = proc.getNonceKey();
          if (nonceKey != null) {
            nonceKeysToProcIdsMap.remove(nonceKey);
          }
          it.remove();
          LOG.trace("Evict completed {}", proc);
        }
      } finally {
        procExecutionLock.releaseLockEntry(lockEntry);
      }
    }
    if (batchCount > 0) {
      store.delete(batchIds, 0, batchCount);
    }
    // let the store do some cleanup works, i.e, delete the place marker for preserving the max
    // procedure id.
    store.cleanup();
  }
}