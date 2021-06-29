/**
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
package org.apache.hadoop.hbase.master.handler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;

/**
 * Handles CLOSED region event on Master.
 * <p>
 * If table is being disabled, deletes ZK unassigned node and removes from
 * regions in transition.
 * <p>
 * Otherwise, assigns the region to another server.
 */
@InterfaceAudience.Private
public class ClosedRegionHandler extends EventHandler implements TotesHRegionInfo {
  private static final Log LOG = LogFactory.getLog(ClosedRegionHandler.class);
  private final AssignmentManager assignmentManager;
  private final HRegionInfo regionInfo;
  private final ClosedPriority priority;

  private enum ClosedPriority {
    META (1),
    USER (2);

    private final int value;
    ClosedPriority(int value) {
      this.value = value;
    }
    public int getValue() {
      return value;
    }
  };

  public ClosedRegionHandler(Server server, AssignmentManager assignmentManager,
      HRegionInfo regionInfo) {
    super(server, EventType.RS_ZK_REGION_CLOSED);
    this.assignmentManager = assignmentManager;
    this.regionInfo = regionInfo;
    if(regionInfo.isMetaRegion()) {
      priority = ClosedPriority.META;
    } else {
      priority = ClosedPriority.USER;
    }
  }

  @Override
  public int getPriority() {
    return priority.getValue();
  }

  @Override
  public HRegionInfo getHRegionInfo() {
    return this.regionInfo;
  }

  @Override
  public String toString() {
    String name = "UnknownServerName";
    if(server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid();
  }

  @Override
  public void process() {
    LOG.debug("Handling CLOSED event for " + regionInfo.getEncodedName());
    // Check if this table is being disabled or not
    if (this.assignmentManager.getTableStateManager().isTableState(this.regionInfo.getTable(),
        ZooKeeperProtos.Table.State.DISABLED, ZooKeeperProtos.Table.State.DISABLING) ||
        assignmentManager.getReplicasToClose().contains(regionInfo)) {
      assignmentManager.offlineDisabledRegion(regionInfo);
      return;
    }
    // ZK Node is in CLOSED state, assign it.
    assignmentManager.getRegionStates().setRegionStateTOCLOSED(regionInfo, null);
    // This below has to do w/ online enable/disable of a table
    assignmentManager.removeClosedRegion(regionInfo);
    assignmentManager.invokeAssign(regionInfo, false);
  }
}
