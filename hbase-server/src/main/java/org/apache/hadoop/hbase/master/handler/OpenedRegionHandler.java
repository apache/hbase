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
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.coordination.OpenRegionCoordination;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.AssignmentManager;

/**
 * Handles OPENED region event on Master.
 */
@InterfaceAudience.Private
public class OpenedRegionHandler extends EventHandler implements TotesHRegionInfo {
  private static final Log LOG = LogFactory.getLog(OpenedRegionHandler.class);
  private final AssignmentManager assignmentManager;
  private final HRegionInfo regionInfo;
  private final OpenedPriority priority;

  private OpenRegionCoordination coordination;
  private OpenRegionCoordination.OpenRegionDetails ord;

  private enum OpenedPriority {
    META (1),
    SYSTEM (2),
    USER (3);

    private final int value;
    OpenedPriority(int value) {
      this.value = value;
    }
    public int getValue() {
      return value;
    }
  };

  public OpenedRegionHandler(Server server,
      AssignmentManager assignmentManager, HRegionInfo regionInfo,
      OpenRegionCoordination coordination,
      OpenRegionCoordination.OpenRegionDetails ord) {
    super(server, EventType.RS_ZK_REGION_OPENED);
    this.assignmentManager = assignmentManager;
    this.regionInfo = regionInfo;
    this.coordination = coordination;
    this.ord = ord;
    if(regionInfo.isMetaRegion()) {
      priority = OpenedPriority.META;
    } else if(regionInfo.getTable()
        .getNamespaceAsString().equals(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR)) {
      priority = OpenedPriority.SYSTEM;
    } else {
      priority = OpenedPriority.USER;
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
    if (!coordination.commitOpenOnMasterSide(assignmentManager,regionInfo, ord)) {
        assignmentManager.unassign(regionInfo);
    }
  }
}
