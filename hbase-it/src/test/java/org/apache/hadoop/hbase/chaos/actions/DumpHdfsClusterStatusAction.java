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
package org.apache.hadoop.hbase.chaos.actions;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HAUtilClient;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DumpHdfsClusterStatusAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(DumpHdfsClusterStatusAction.class);
  private static final String PREFIX = "\n  ";

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void perform() throws Exception {
    StringBuilder sb = new StringBuilder();
    try (final DistributedFileSystem dfs = HdfsActionUtils.createDfs(getConf())) {
      final Configuration dfsConf = dfs.getConf();
      final URI dfsUri = dfs.getUri();
      final boolean isHaAndLogicalUri = HAUtilClient.isLogicalUri(dfsConf, dfsUri);
      sb.append("Cluster status").append('\n');
      if (isHaAndLogicalUri) {
        final String nsId = dfsUri.getHost();
        final List<ClientProtocol> namenodes =
          HAUtil.getProxiesForAllNameNodesInNameservice(dfsConf, nsId);
        final boolean atLeastOneActive = HAUtil.isAtLeastOneActive(namenodes);
        final InetSocketAddress activeAddress = HAUtil.getAddressOfActive(dfs);
        sb.append("Active NameNode=").append(activeAddress).append(", isAtLeastOneActive=")
          .append(atLeastOneActive).append('\n');
      }
      DatanodeInfo[] dns = dfs.getClient().datanodeReport(HdfsConstants.DatanodeReportType.LIVE);
      sb.append("Number of live DataNodes: ").append(dns.length);
      for (DatanodeInfo dni : dns) {
        sb.append(PREFIX).append("name=").append(dni.getName()).append(", used%=")
          .append(dni.getDfsUsedPercent()).append(", capacity=")
          .append(FileUtils.byteCountToDisplaySize(dni.getCapacity()));
      }
      sb.append('\n');
      dns = dfs.getClient().datanodeReport(HdfsConstants.DatanodeReportType.DEAD);
      sb.append("Number of dead DataNodes: ").append(dns.length);
      for (DatanodeInfo dni : dns) {
        sb.append(PREFIX).append(dni.getName()).append("/").append(dni.getNetworkLocation());
      }
    }
    // TODO: add more on NN, JNs, and ZK.
    // TODO: Print how long process has been up.
    getLogger().info(sb.toString());
  }
}
