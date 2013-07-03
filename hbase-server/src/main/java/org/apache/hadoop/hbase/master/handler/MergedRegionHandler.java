/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.master.handler;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;

/**
 * Handles MERGED regions event on Master, master receive the merge report from
 * the regionserver, then offline the merging regions and online the merged
 * region.Here region_a sorts before region_b.
 */
@InterfaceAudience.Private
public class MergedRegionHandler extends EventHandler implements
    TotesHRegionInfo {
  private static final Log LOG = LogFactory.getLog(MergedRegionHandler.class);
  private final AssignmentManager assignmentManager;
  private final HRegionInfo merged;
  private final HRegionInfo region_a;
  private final HRegionInfo region_b;
  private final ServerName sn;

  public MergedRegionHandler(Server server,
      AssignmentManager assignmentManager, ServerName sn,
      final List<HRegionInfo> mergeRegions) {
    super(server, EventType.RS_ZK_REGION_MERGED);
    assert mergeRegions.size() == 3;
    this.assignmentManager = assignmentManager;
    this.merged = mergeRegions.get(0);
    this.region_a = mergeRegions.get(1);
    this.region_b = mergeRegions.get(2);
    this.sn = sn;
  }

  @Override
  public HRegionInfo getHRegionInfo() {
    return this.merged;
  }

  @Override
  public String toString() {
    String name = "UnknownServerName";
    if (server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    String mergedRegion = "UnknownRegion";
    if (merged != null) {
      mergedRegion = merged.getRegionNameAsString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid() + "-"
        + mergedRegion;
  }

  @Override
  public void process() {
    String encodedRegionName = this.merged.getEncodedName();
    LOG.debug("Handling MERGE event for " + encodedRegionName
        + "; deleting node");

    this.assignmentManager.handleRegionsMergeReport(this.sn, this.merged,
        this.region_a, this.region_b);
    // Remove region from ZK
    try {

      boolean successful = false;
      while (!successful) {
        // It's possible that the RS tickles in between the reading of the
        // znode and the deleting, so it's safe to retry.
        successful = ZKAssign.deleteNode(this.server.getZooKeeper(),
            encodedRegionName, EventType.RS_ZK_REGION_MERGED);
      }
    } catch (KeeperException e) {
      if (e instanceof NoNodeException) {
        String znodePath = ZKUtil.joinZNode(
            this.server.getZooKeeper().splitLogZNode, encodedRegionName);
        LOG.debug("The znode " + znodePath
            + " does not exist.  May be deleted already.");
      } else {
        server.abort("Error deleting MERGED node in ZK for transition ZK node ("
            + merged.getEncodedName() + ")", e);
      }
    }
    LOG.info("Handled MERGED event; merged="
        + this.merged.getRegionNameAsString() + " region_a="
        + this.region_a.getRegionNameAsString() + "region_b="
        + this.region_b.getRegionNameAsString());
  }
}
