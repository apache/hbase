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
package org.apache.hadoop.hbase.hbtop.mode;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.hbtop.Record;
import org.apache.hadoop.hbase.hbtop.RecordFilter;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.field.FieldInfo;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * Represents a display mode in the top screen.
 */
@InterfaceAudience.Private
public enum Mode {
  NAMESPACE("Namespace", "Record per Namespace", new NamespaceModeStrategy()),
  TABLE("Table", "Record per Table", new TableModeStrategy()),
  REGION("Region", "Record per Region", new RegionModeStrategy()),
  REGION_SERVER("RegionServer", "Record per RegionServer", new RegionServerModeStrategy()),
  USER("User", "Record per user", new UserModeStrategy()),
  CLIENT("Client", "Record per client", new ClientModeStrategy());

  private final String header;
  private final String description;
  private final ModeStrategy modeStrategy;

  Mode(String header, String description, ModeStrategy modeStrategy) {
    this.header  = Objects.requireNonNull(header);
    this.description = Objects.requireNonNull(description);
    this.modeStrategy = Objects.requireNonNull(modeStrategy);
  }

  public String getHeader() {
    return header;
  }

  public String getDescription() {
    return description;
  }

  public List<Record> getRecords(ClusterMetrics clusterMetrics,
      List<RecordFilter> pushDownFilters) {
    return modeStrategy.getRecords(clusterMetrics, pushDownFilters);
  }

  public List<FieldInfo> getFieldInfos() {
    return modeStrategy.getFieldInfos();
  }

  public Field getDefaultSortField() {
    return modeStrategy.getDefaultSortField();
  }

  @Nullable
  public DrillDownInfo drillDown(Record currentRecord) {
    return modeStrategy.drillDown(currentRecord);
  }
}
