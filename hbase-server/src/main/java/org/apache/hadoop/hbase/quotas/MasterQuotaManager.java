/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.quotas;

import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionStateListener;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.namespace.NamespaceAuditor;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaResponse;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Throttle;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.ThrottleRequest;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.TimedQuota;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Master Quota Manager. It is responsible for initialize the quota table on the first-run and
 * provide the admin operations to interact with the quota table. TODO: FUTURE: The master will be
 * responsible to notify each RS of quota changes and it will do the "quota aggregation" when the
 * QuotaScope is CLUSTER.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MasterQuotaManager implements RegionStateListener {
  private static final Log LOG = LogFactory.getLog(MasterQuotaManager.class);

  private final MasterServices masterServices;
  private NamedLock<String> namespaceLocks;
  private NamedLock<TableName> tableLocks;
  private NamedLock<String> userLocks;
  private boolean initialized = false;
  private NamespaceAuditor namespaceQuotaManager;

  public MasterQuotaManager(final MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  public void start() throws IOException {
    // If the user doesn't want the quota support skip all the initializations.
    if (!QuotaUtil.isQuotaEnabled(masterServices.getConfiguration())) {
      LOG.info("Quota support disabled");
      return;
    }

    // Create the quota table if missing
    if (!MetaTableAccessor.tableExists(masterServices.getConnection(),
        QuotaUtil.QUOTA_TABLE_NAME)) {
      LOG.info("Quota table not found. Creating...");
      createQuotaTable();
    }

    LOG.info("Initializing quota support");
    namespaceLocks = new NamedLock<String>();
    tableLocks = new NamedLock<TableName>();
    userLocks = new NamedLock<String>();

    namespaceQuotaManager = new NamespaceAuditor(masterServices);
    namespaceQuotaManager.start();
    initialized = true;
  }

  public void stop() {
  }

  public boolean isQuotaInitialized() {
    return initialized && namespaceQuotaManager.isInitialized();
  }

  /*
   * ========================================================================== Admin operations to
   * manage the quota table
   */
  public SetQuotaResponse setQuota(final SetQuotaRequest req) throws IOException,
      InterruptedException {
    checkQuotaSupport();

    if (req.hasUserName()) {
      userLocks.lock(req.getUserName());
      try {
        if (req.hasTableName()) {
          setUserQuota(req.getUserName(), ProtobufUtil.toTableName(req.getTableName()), req);
        } else if (req.hasNamespace()) {
          setUserQuota(req.getUserName(), req.getNamespace(), req);
        } else {
          setUserQuota(req.getUserName(), req);
        }
      } finally {
        userLocks.unlock(req.getUserName());
      }
    } else if (req.hasTableName()) {
      TableName table = ProtobufUtil.toTableName(req.getTableName());
      tableLocks.lock(table);
      try {
        setTableQuota(table, req);
      } finally {
        tableLocks.unlock(table);
      }
    } else if (req.hasNamespace()) {
      namespaceLocks.lock(req.getNamespace());
      try {
        setNamespaceQuota(req.getNamespace(), req);
      } finally {
        namespaceLocks.unlock(req.getNamespace());
      }
    } else {
      throw new DoNotRetryIOException(new UnsupportedOperationException(
          "a user, a table or a namespace must be specified"));
    }
    return SetQuotaResponse.newBuilder().build();
  }

  public void setUserQuota(final String userName, final SetQuotaRequest req) throws IOException,
      InterruptedException {
    setQuota(req, new SetQuotaOperations() {
      @Override
      public Quotas fetch() throws IOException {
        return QuotaUtil.getUserQuota(masterServices.getConnection(), userName);
      }

      @Override
      public void update(final Quotas quotas) throws IOException {
        QuotaUtil.addUserQuota(masterServices.getConnection(), userName, quotas);
      }

      @Override
      public void delete() throws IOException {
        QuotaUtil.deleteUserQuota(masterServices.getConnection(), userName);
      }

      @Override
      public void preApply(final Quotas quotas) throws IOException {
        masterServices.getMasterCoprocessorHost().preSetUserQuota(userName, quotas);
      }

      @Override
      public void postApply(final Quotas quotas) throws IOException {
        masterServices.getMasterCoprocessorHost().postSetUserQuota(userName, quotas);
      }
    });
  }

  public void setUserQuota(final String userName, final TableName table, final SetQuotaRequest req)
      throws IOException, InterruptedException {
    setQuota(req, new SetQuotaOperations() {
      @Override
      public Quotas fetch() throws IOException {
        return QuotaUtil.getUserQuota(masterServices.getConnection(), userName, table);
      }

      @Override
      public void update(final Quotas quotas) throws IOException {
        QuotaUtil.addUserQuota(masterServices.getConnection(), userName, table, quotas);
      }

      @Override
      public void delete() throws IOException {
        QuotaUtil.deleteUserQuota(masterServices.getConnection(), userName, table);
      }

      @Override
      public void preApply(final Quotas quotas) throws IOException {
        masterServices.getMasterCoprocessorHost().preSetUserQuota(userName, table, quotas);
      }

      @Override
      public void postApply(final Quotas quotas) throws IOException {
        masterServices.getMasterCoprocessorHost().postSetUserQuota(userName, table, quotas);
      }
    });
  }

  public void
      setUserQuota(final String userName, final String namespace, final SetQuotaRequest req)
          throws IOException, InterruptedException {
    setQuota(req, new SetQuotaOperations() {
      @Override
      public Quotas fetch() throws IOException {
        return QuotaUtil.getUserQuota(masterServices.getConnection(), userName, namespace);
      }

      @Override
      public void update(final Quotas quotas) throws IOException {
        QuotaUtil.addUserQuota(masterServices.getConnection(), userName, namespace, quotas);
      }

      @Override
      public void delete() throws IOException {
        QuotaUtil.deleteUserQuota(masterServices.getConnection(), userName, namespace);
      }

      @Override
      public void preApply(final Quotas quotas) throws IOException {
        masterServices.getMasterCoprocessorHost().preSetUserQuota(userName, namespace, quotas);
      }

      @Override
      public void postApply(final Quotas quotas) throws IOException {
        masterServices.getMasterCoprocessorHost().postSetUserQuota(userName, namespace, quotas);
      }
    });
  }

  public void setTableQuota(final TableName table, final SetQuotaRequest req) throws IOException,
      InterruptedException {
    setQuota(req, new SetQuotaOperations() {
      @Override
      public Quotas fetch() throws IOException {
        return QuotaUtil.getTableQuota(masterServices.getConnection(), table);
      }

      @Override
      public void update(final Quotas quotas) throws IOException {
        QuotaUtil.addTableQuota(masterServices.getConnection(), table, quotas);
      }

      @Override
      public void delete() throws IOException {
        QuotaUtil.deleteTableQuota(masterServices.getConnection(), table);
      }

      @Override
      public void preApply(final Quotas quotas) throws IOException {
        masterServices.getMasterCoprocessorHost().preSetTableQuota(table, quotas);
      }

      @Override
      public void postApply(final Quotas quotas) throws IOException {
        masterServices.getMasterCoprocessorHost().postSetTableQuota(table, quotas);
      }
    });
  }

  public void setNamespaceQuota(final String namespace, final SetQuotaRequest req)
      throws IOException, InterruptedException {
    setQuota(req, new SetQuotaOperations() {
      @Override
      public Quotas fetch() throws IOException {
        return QuotaUtil.getNamespaceQuota(masterServices.getConnection(), namespace);
      }

      @Override
      public void update(final Quotas quotas) throws IOException {
        QuotaUtil.addNamespaceQuota(masterServices.getConnection(), namespace, quotas);
      }

      @Override
      public void delete() throws IOException {
        QuotaUtil.deleteNamespaceQuota(masterServices.getConnection(), namespace);
      }

      @Override
      public void preApply(final Quotas quotas) throws IOException {
        masterServices.getMasterCoprocessorHost().preSetNamespaceQuota(namespace, quotas);
      }

      @Override
      public void postApply(final Quotas quotas) throws IOException {
        masterServices.getMasterCoprocessorHost().postSetNamespaceQuota(namespace, quotas);
      }
    });
  }

  public void setNamespaceQuota(NamespaceDescriptor desc) throws IOException {
    if (initialized) {
      this.namespaceQuotaManager.addNamespace(desc);
    }
  }

  public void removeNamespaceQuota(String namespace) throws IOException {
    if (initialized) {
      this.namespaceQuotaManager.deleteNamespace(namespace);
    }
  }

  private void setQuota(final SetQuotaRequest req, final SetQuotaOperations quotaOps)
      throws IOException, InterruptedException {
    if (req.hasRemoveAll() && req.getRemoveAll() == true) {
      quotaOps.preApply(null);
      quotaOps.delete();
      quotaOps.postApply(null);
      return;
    }

    // Apply quota changes
    Quotas quotas = quotaOps.fetch();
    quotaOps.preApply(quotas);

    Quotas.Builder builder = (quotas != null) ? quotas.toBuilder() : Quotas.newBuilder();
    if (req.hasThrottle()) applyThrottle(builder, req.getThrottle());
    if (req.hasBypassGlobals()) applyBypassGlobals(builder, req.getBypassGlobals());

    // Submit new changes
    quotas = builder.build();
    if (QuotaUtil.isEmptyQuota(quotas)) {
      quotaOps.delete();
    } else {
      quotaOps.update(quotas);
    }
    quotaOps.postApply(quotas);
  }

  public void checkNamespaceTableAndRegionQuota(TableName tName, int regions) throws IOException {
    if (initialized) {
      namespaceQuotaManager.checkQuotaToCreateTable(tName, regions);
    }
  }
  
  public void checkAndUpdateNamespaceRegionQuota(TableName tName, int regions) throws IOException {
    if (initialized) {
      namespaceQuotaManager.checkQuotaToUpdateRegion(tName, regions);
    }
  }

  /**
   * @return cached region count, or -1 if quota manager is disabled or table status not found
  */
  public int getRegionCountOfTable(TableName tName) throws IOException {
    if (initialized) {
      return namespaceQuotaManager.getRegionCountOfTable(tName);
    }
    return -1;
  }

  @Override
  public void onRegionMerged(HRegionInfo hri) throws IOException {
    if (initialized) {
      namespaceQuotaManager.updateQuotaForRegionMerge(hri);
    }
  }

  @Override
  public void onRegionSplit(HRegionInfo hri) throws IOException {
    if (initialized) {
      namespaceQuotaManager.checkQuotaToSplitRegion(hri);
    }
  }

  /**
   * Remove table from namespace quota.
   * @param tName - The table name to update quota usage.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void removeTableFromNamespaceQuota(TableName tName) throws IOException {
    if (initialized) {
      namespaceQuotaManager.removeFromNamespaceUsage(tName);
    }
  }

  public NamespaceAuditor getNamespaceQuotaManager() {
    return this.namespaceQuotaManager;
  }

  private static interface SetQuotaOperations {
    Quotas fetch() throws IOException;

    void delete() throws IOException;

    void update(final Quotas quotas) throws IOException;

    void preApply(final Quotas quotas) throws IOException;

    void postApply(final Quotas quotas) throws IOException;
  }

  /*
   * ========================================================================== Helpers to apply
   * changes to the quotas
   */
  private void applyThrottle(final Quotas.Builder quotas, final ThrottleRequest req)
      throws IOException {
    Throttle.Builder throttle;

    if (req.hasType() && (req.hasTimedQuota() || quotas.hasThrottle())) {
      // Validate timed quota if present
      if (req.hasTimedQuota()) {
        validateTimedQuota(req.getTimedQuota());
      }

      // apply the new settings
      throttle = quotas.hasThrottle() ? quotas.getThrottle().toBuilder() : Throttle.newBuilder();

      switch (req.getType()) {
        case REQUEST_NUMBER:
          if (req.hasTimedQuota()) {
            throttle.setReqNum(req.getTimedQuota());
          } else {
            throttle.clearReqNum();
          }
          break;
        case REQUEST_SIZE:
          if (req.hasTimedQuota()) {
            throttle.setReqSize(req.getTimedQuota());
          } else {
            throttle.clearReqSize();
          }
          break;
        case WRITE_NUMBER:
          if (req.hasTimedQuota()) {
            throttle.setWriteNum(req.getTimedQuota());
          } else {
            throttle.clearWriteNum();
          }
          break;
        case WRITE_SIZE:
          if (req.hasTimedQuota()) {
            throttle.setWriteSize(req.getTimedQuota());
          } else {
            throttle.clearWriteSize();
          }
          break;
        case READ_NUMBER:
          if (req.hasTimedQuota()) {
            throttle.setReadNum(req.getTimedQuota());
          } else {
            throttle.clearReqNum();
          }
          break;
        case READ_SIZE:
          if (req.hasTimedQuota()) {
            throttle.setReadSize(req.getTimedQuota());
          } else {
            throttle.clearReadSize();
          }
          break;
        case REQUEST_CAPACITY_UNIT:
          if (req.hasTimedQuota()) {
            throttle.setReqCapacityUnit(req.getTimedQuota());
          } else {
            throttle.clearReqCapacityUnit();
          }
          break;
        case READ_CAPACITY_UNIT:
          if (req.hasTimedQuota()) {
            throttle.setReadCapacityUnit(req.getTimedQuota());
          } else {
            throttle.clearReadCapacityUnit();
          }
          break;
        case WRITE_CAPACITY_UNIT:
          if (req.hasTimedQuota()) {
            throttle.setWriteCapacityUnit(req.getTimedQuota());
          } else {
            throttle.clearWriteCapacityUnit();
          }
          break;
        default:
          throw new RuntimeException("Invalid throttle type: " + req.getType());
      }
      quotas.setThrottle(throttle.build());
    } else {
      quotas.clearThrottle();
    }
  }

  private void applyBypassGlobals(final Quotas.Builder quotas, boolean bypassGlobals) {
    if (bypassGlobals) {
      quotas.setBypassGlobals(bypassGlobals);
    } else {
      quotas.clearBypassGlobals();
    }
  }

  private void validateTimedQuota(final TimedQuota timedQuota) throws IOException {
    if (timedQuota.getSoftLimit() < 1) {
      throw new DoNotRetryIOException(new UnsupportedOperationException(
          "The throttle limit must be greater then 0, got " + timedQuota.getSoftLimit()));
    }
  }

  /*
   * ========================================================================== Helpers
   */

  private void checkQuotaSupport() throws IOException {
    if (!QuotaUtil.isQuotaEnabled(masterServices.getConfiguration())) {
      throw new DoNotRetryIOException(new UnsupportedOperationException("quota support disabled"));
    }
    if (!initialized) {
      long maxWaitTime = masterServices.getConfiguration().getLong(
        "hbase.master.wait.for.quota.manager.init", 30000); // default is 30 seconds.
      long startTime = EnvironmentEdgeManager.currentTime();
      do {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting for Quota Manager to be initialized.");
          break;
        }
      } while (!initialized && (EnvironmentEdgeManager.currentTime() - startTime) < maxWaitTime);
      if (!initialized) {
        throw new IOException("Quota manager is uninitialized, please retry later.");
      }
    }
  }

  private void createQuotaTable() throws IOException {
    masterServices.createSystemTable(QuotaUtil.QUOTA_TABLE_DESC);
  }

  private static class NamedLock<T> {
    private final HashSet<T> locks = new HashSet<T>();

    public void lock(final T name) throws InterruptedException {
      synchronized (locks) {
        while (locks.contains(name)) {
          locks.wait();
        }
        locks.add(name);
      }
    }

    public void unlock(final T name) {
      synchronized (locks) {
        locks.remove(name);
        locks.notifyAll();
      }
    }
  }

  @Override
  public void onRegionSplitReverted(HRegionInfo hri) throws IOException {
    if (initialized) {
      this.namespaceQuotaManager.removeRegionFromNamespaceUsage(hri);
    }
  }
}
