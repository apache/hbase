/*
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

package org.apache.hadoop.hbase.master.slowlog;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.slowlog.SlowLogTableAccessor;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Slowlog Master services - Table creation to be used by HMaster
 */
@InterfaceAudience.Private
public class SlowLogMasterService {

  private static final Logger LOG = LoggerFactory.getLogger(SlowLogMasterService.class);

  private final boolean slowlogTableEnabled;
  private final MasterServices masterServices;

  private static final TableDescriptorBuilder TABLE_DESCRIPTOR_BUILDER =
    TableDescriptorBuilder.newBuilder(SlowLogTableAccessor.SLOW_LOG_TABLE_NAME)
      .setRegionReplication(1)
      .setColumnFamily(
        ColumnFamilyDescriptorBuilder.newBuilder(HConstants.SLOWLOG_INFO_FAMILY)
          .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
          .setBlockCacheEnabled(false)
          .setMaxVersions(1).build());

  public SlowLogMasterService(final Configuration configuration,
      final MasterServices masterServices) {
    slowlogTableEnabled = configuration.getBoolean(HConstants.SLOW_LOG_SYS_TABLE_ENABLED_KEY,
      HConstants.DEFAULT_SLOW_LOG_SYS_TABLE_ENABLED_KEY);
    this.masterServices = masterServices;
  }

  public void init() throws IOException {
    if (!slowlogTableEnabled) {
      LOG.info("Slow/Large requests logging to system table hbase:slowlog is disabled. Quitting.");
      return;
    }
    if (!masterServices.getTableDescriptors().exists(
        SlowLogTableAccessor.SLOW_LOG_TABLE_NAME)) {
      LOG.info("slowlog table not found. Creating.");
      this.masterServices.createSystemTable(TABLE_DESCRIPTOR_BUILDER.build());
    }
  }

}
