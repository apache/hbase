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

import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestartRandomJournalNodeAction extends RestartActionBaseAction {
  private static final Logger LOG = LoggerFactory.getLogger(RestartRandomJournalNodeAction.class);

  public RestartRandomJournalNodeAction(long sleepTime) {
    super(sleepTime);
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  public void perform() throws Exception {
    getLogger().info("Performing action: Restart random JournalNode");

    final String qjournal;
    try (final DistributedFileSystem dfs = HdfsActionUtils.createDfs(getConf())) {
      final Configuration conf = dfs.getConf();
      final String nameServiceID = DFSUtil.getNamenodeNameServiceId(conf);
      if (!HAUtil.isHAEnabled(conf, nameServiceID)) {
        getLogger().info("HA for HDFS is not enabled; skipping");
        return;
      }

      qjournal = conf.get("dfs.namenode.shared.edits.dir");
      if (StringUtils.isEmpty(qjournal)) {
        getLogger().info("Empty qjournals!");
        return;
      }
    }

    final ServerName journalNode =
      PolicyBasedChaosMonkey.selectRandomItem(getJournalNodes(qjournal));
    restartJournalNode(journalNode, sleepTime);
  }

  private static ServerName[] getJournalNodes(final String qjournal) {
    // WARNING: HDFS internals. qjournal looks like this:
    // qjournal://journalnode-0.example.com:8485;...;journalnode-N.example.com:8485/hk8se
    // When done, we have an array of journalnodes+ports: e.g.journalnode-0.example.com:8485
    final String[] journalNodes =
      qjournal.toLowerCase().replaceAll("qjournal:\\/\\/", "").replaceAll("\\/.*$", "").split(";");
    return Arrays.stream(journalNodes).map(Address::fromString)
      .map(addr -> ServerName.valueOf(addr.getHostName(), addr.getPort()))
      .toArray(ServerName[]::new);
  }
}
