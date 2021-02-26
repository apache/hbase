/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.HasMasterServices;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;

/**
 * An observer to automatically delete quotas when a table/namespace
 * is deleted.
 */
@CoreCoprocessor
@InterfaceAudience.Private
public class MasterQuotasObserver implements MasterCoprocessor, MasterObserver {
  public static final String REMOVE_QUOTA_ON_TABLE_DELETE = "hbase.quota.remove.on.table.delete";
  public static final boolean REMOVE_QUOTA_ON_TABLE_DELETE_DEFAULT = true;

  private CoprocessorEnvironment cpEnv;
  private Configuration conf;
  private boolean quotasEnabled = false;
  private MasterServices masterServices;

  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return Optional.of(this);
  }

  @Override
  public void start(CoprocessorEnvironment ctx) throws IOException {
    this.conf = ctx.getConfiguration();
    this.quotasEnabled = QuotaUtil.isQuotaEnabled(conf);

    if (!(ctx instanceof MasterCoprocessorEnvironment)) {
      throw new CoprocessorException("Must be loaded on master.");
    }
    // if running on master
    MasterCoprocessorEnvironment mEnv = (MasterCoprocessorEnvironment) ctx;
    if (mEnv instanceof HasMasterServices) {
      this.masterServices = ((HasMasterServices) mEnv).getMasterServices();
    } else {
      throw new CoprocessorException("Must be loaded on a master having master services.");
    }
  }

  @Override
  public void postDeleteTable(
      ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws IOException {
    // Do nothing if quotas aren't enabled
    if (!quotasEnabled) {
      return;
    }
    final Connection conn = ctx.getEnvironment().getConnection();
    Quotas tableQuotas = QuotaUtil.getTableQuota(conn, tableName);
    Quotas namespaceQuotas = QuotaUtil.getNamespaceQuota(conn, tableName.getNamespaceAsString());
    if (tableQuotas != null || namespaceQuotas != null) {
      // Remove regions of table from space quota map.
      this.masterServices.getMasterQuotaManager().removeRegionSizesForTable(tableName);
      if (tableQuotas != null) {
        if (tableQuotas.hasSpace()) {
          QuotaSettings settings = QuotaSettingsFactory.removeTableSpaceLimit(tableName);
          try (Admin admin = conn.getAdmin()) {
            admin.setQuota(settings);
          }
        }
        if (tableQuotas.hasThrottle()) {
          QuotaSettings settings = QuotaSettingsFactory.unthrottleTable(tableName);
          try (Admin admin = conn.getAdmin()) {
            admin.setQuota(settings);
          }
        }
      }
    }
  }

  @Override
  public void postDeleteNamespace(
      ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace) throws IOException {
    // Do nothing if quotas aren't enabled
    if (!quotasEnabled) {
      return;
    }
    final Connection conn = ctx.getEnvironment().getConnection();
    Quotas quotas = QuotaUtil.getNamespaceQuota(conn, namespace);
    if (quotas != null) {
      if (quotas.hasSpace()) {
        QuotaSettings settings = QuotaSettingsFactory.removeNamespaceSpaceLimit(namespace);
        try (Admin admin = conn.getAdmin()) {
          admin.setQuota(settings);
        }
      }
      if (quotas.hasThrottle()) {
        QuotaSettings settings = QuotaSettingsFactory.unthrottleNamespace(namespace);
        try (Admin admin = conn.getAdmin()) {
          admin.setQuota(settings);
        }
      }
    }
  }
}