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
package org.apache.hadoop.hbase.master.balancer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Master based cluster info provider.
 */
@InterfaceAudience.Private
class MasterClusterInfoProvider implements ClusterInfoProvider {

  private final MasterServices services;

  MasterClusterInfoProvider(MasterServices services) {
    this.services = services;
  }

  @Override
  public List<RegionInfo> getAssignedRegions() {
    AssignmentManager am = services.getAssignmentManager();
    return am != null ? am.getAssignedRegions() : Collections.emptyList();
  }

  @Override
  public TableDescriptor getTableDescriptor(TableName tableName) throws IOException {
    TableDescriptors tds = services.getTableDescriptors();
    return tds != null ? tds.get(tableName) : null;
  }

  @Override
  public HDFSBlocksDistribution computeHDFSBlocksDistribution(Configuration conf,
    TableDescriptor tableDescriptor, RegionInfo regionInfo) throws IOException {
    return HRegion.computeHDFSBlocksDistribution(conf, tableDescriptor, regionInfo);
  }
}
