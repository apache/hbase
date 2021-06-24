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

@InterfaceAudience.Private
public class CompactionTask implements Comparable<CompactionTask> {
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

  @Override
  public int compareTo(CompactionTask task) {
    int cmp = getPriority() - task.getPriority();
    if (cmp == 0) {
      return Long.compare(getSubmitTime(), task.getSubmitTime());
    }
    return cmp;
  }

  public static class Builder {
    private CompactionTask compactionTask = new CompactionTask();

    public Builder setRsServerName(ServerName rsServerName) {
      compactionTask.rsServerName = rsServerName;
      return this;
    }

    public Builder setRegionInfo(RegionInfo regionInfo) {
      compactionTask.regionInfo = regionInfo;
      return this;
    }

    public Builder setColumnFamilyDescriptor(ColumnFamilyDescriptor cfd) {
      compactionTask.cfd = cfd;
      return this;
    }

    public Builder setRequestMajor(boolean requestMajor) {
      compactionTask.requestMajor = requestMajor;
      return this;
    }

    public Builder setPriority(int priority) {
      compactionTask.priority = priority;
      return this;
    }


    public Builder setFavoredNodes(List<HBaseProtos.ServerName> favoredNodes) {
      compactionTask.favoredNodes = favoredNodes;
      return this;
    }

    public Builder setSubmitTime(long submitTime) {
      compactionTask.submitTime = submitTime;
      return this;
    }
    
    public CompactionTask build() {
      return compactionTask;
    }
  }

  public void setSelectedFileNames(List<String> selectedFileNames) {
    this.selectedFileNames = selectedFileNames;
  }

  public void setMonitoredTask(MonitoredTask status) {
    this.status = status;
  }

  public void setCompactionContext(CompactionContext compactionContext) {
    this.compactionContext = compactionContext;
  }

  public void setHStore(HStore store) {
    this.store = store;
  }

  public ServerName getRsServerName() {
    return rsServerName;
  }

  public RegionInfo getRegionInfo() {
    return regionInfo;
  }

  public ColumnFamilyDescriptor getCfd() {
    return cfd;
  }

  public CompactionContext getCompactionContext() {
    return compactionContext;
  }

  public HStore getStore() {
    return store;
  }

  public List<String> getSelectedFileNames() {
    return selectedFileNames;
  }

  public MonitoredTask getStatus() {
    return status;
  }

  public void setTaskName(String name) {
    this.taskName = name;
  }

  public String getTaskName() {
    return taskName;
  }

  public boolean isRequestMajor() {
    return requestMajor;
  }

  public int getPriority() {
    return priority;
  }

  public List<HBaseProtos.ServerName> getFavoredNodes() {
    return favoredNodes;
  }

  public long getSubmitTime() {
    return submitTime;
  }

  @Override public String toString() {
    return new StringBuilder("RS: ").append(rsServerName).append(", region: ")
      .append(regionInfo.getRegionNameAsString()).append(", CF: ").append(cfd.getNameAsString())
      .append(", priority: ").append(priority).toString();
  }
}
