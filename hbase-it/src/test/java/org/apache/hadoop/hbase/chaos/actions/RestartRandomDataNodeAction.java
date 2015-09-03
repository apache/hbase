/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.chaos.actions;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Action that restarts a random datanode.
 */
public class RestartRandomDataNodeAction extends RestartActionBaseAction {
  public RestartRandomDataNodeAction(long sleepTime) {
    super(sleepTime);
  }

  @Override
  public void perform() throws Exception {
    LOG.info("Performing action: Restart random data node");
    ServerName server = PolicyBasedChaosMonkey.selectRandomItem(getDataNodes());
    restartDataNode(server, sleepTime);
  }

  public ServerName[] getDataNodes() throws IOException {
    DistributedFileSystem fs = (DistributedFileSystem) FSUtils.getRootDir(getConf())
        .getFileSystem(getConf());
    DFSClient dfsClient = fs.getClient();
    List<ServerName> hosts = new LinkedList<ServerName>();
    for (DatanodeInfo dataNode: dfsClient.datanodeReport(HdfsConstants.DatanodeReportType.LIVE)) {
      hosts.add(ServerName.valueOf(dataNode.getHostName(), -1, -1));
    }
    return hosts.toArray(new ServerName[hosts.size()]);
  }
}
