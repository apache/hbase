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
package org.apache.hadoop.hbase.master.regionHistorian;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.regionHistorian.RegionHistorianTableAccessor;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

@InterfaceAudience.Private
public class RegionHistorianMasterService {
  private static final Logger LOG = LoggerFactory.getLogger(RegionHistorianMasterService.class);

  private final MasterServices masterServices;

  private static final TableDescriptorBuilder TABLE_DESCRIPTOR_BUILDER = TableDescriptorBuilder
    .newBuilder(RegionHistorianTableAccessor.REGION_HISTORIAN_TABLE_NAME).setRegionReplication(1)
    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(HConstants.REGIONHISTORIAN_INFO_FAMILY)
      .setScope(HConstants.REPLICATION_SCOPE_LOCAL).setBlockCacheEnabled(false).setMaxVersions(1)
      .build());

  public RegionHistorianMasterService(final MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  public void init() throws IOException {
    if (!masterServices.getTableDescriptors().exists(RegionHistorianTableAccessor.REGION_HISTORIAN_TABLE_NAME)) {
      LOG.info("regionHistorian table not found. Creating.");
      this.masterServices.createSystemTable(TABLE_DESCRIPTOR_BUILDER.build());
    }
  }
}
