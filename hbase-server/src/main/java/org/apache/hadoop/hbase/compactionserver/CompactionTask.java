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
package org.apache.hadoop.hbase.compactionserver;

import java.util.List;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

/**
 * This class hold {@link CompactionContext} relate to CompactionRequest
 */
@InterfaceAudience.Private
public final class CompactionTask {
  private ServerName rsServerName;
  private RegionInfo regionInfo;
  private ColumnFamilyDescriptor cfd;
  private CompactionContext compactionContext;
  private HStore store;
  private boolean requestMajor;
  private int priority;
  private List<String> selectedFileNames;
  private MonitoredTask status;
  private String taskName;
  private List<HBaseProtos.ServerName> favoredNodes;
  private long submitTime;

  private CompactionTask() {
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private CompactionTask compactionTask = new CompactionTask();

    Builder setRsServerName(ServerName rsServerName) {
      compactionTask.rsServerName = rsServerName;
      return this;
    }

    Builder setRegionInfo(RegionInfo regionInfo) {
      compactionTask.regionInfo = regionInfo;
      return this;
    }

    Builder setColumnFamilyDescriptor(ColumnFamilyDescriptor cfd) {
      compactionTask.cfd = cfd;
      return this;
    }

    Builder setRequestMajor(boolean requestMajor) {
      compactionTask.requestMajor = requestMajor;
      return this;
    }

    Builder setPriority(int priority) {
      compactionTask.priority = priority;
      return this;
    }

    Builder setFavoredNodes(List<HBaseProtos.ServerName> favoredNodes) {
      compactionTask.favoredNodes = favoredNodes;
      return this;
    }

    Builder setSubmitTime(long submitTime) {
      compactionTask.submitTime = submitTime;
      return this;
    }

    CompactionTask build() {
      return compactionTask;
    }
  }

  void setSelectedFileNames(List<String> selectedFileNames) {
    this.selectedFileNames = selectedFileNames;
  }

  void setMonitoredTask(MonitoredTask status) {
    this.status = status;
  }

  void setCompactionContext(CompactionContext compactionContext) {
    this.compactionContext = compactionContext;
  }

  void setHStore(HStore store) {
    this.store = store;
  }

  ServerName getRsServerName() {
    return rsServerName;
  }

  RegionInfo getRegionInfo() {
    return regionInfo;
  }

  ColumnFamilyDescriptor getCfd() {
    return cfd;
  }

  CompactionContext getCompactionContext() {
    return compactionContext;
  }

  public HStore getStore() {
    return store;
  }

  List<String> getSelectedFileNames() {
    return selectedFileNames;
  }

  MonitoredTask getStatus() {
    return status;
  }

  void setTaskName(String name) {
    this.taskName = name;
  }

  String getTaskName() {
    return taskName;
  }

  boolean isRequestMajor() {
    return requestMajor;
  }

  int getPriority() {
    return priority;
  }

  List<HBaseProtos.ServerName> getFavoredNodes() {
    return favoredNodes;
  }

  long getSubmitTime() {
    return submitTime;
  }

  void setPriority(int priority) {
    this.priority = priority;
  }

  @Override
  public String toString() {
    return new StringBuilder("RS: ").append(rsServerName).append(", region: ")
        .append(regionInfo.getRegionNameAsString()).append(", CF: ").append(cfd.getNameAsString())
        .append(", priority: ").append(priority).toString();
  }
}
