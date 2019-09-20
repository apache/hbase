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
package org.apache.hadoop.hbase.hbtop.screen.top;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.hbtop.Record;
import org.apache.hadoop.hbase.hbtop.RecordFilter;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.field.FieldInfo;
import org.apache.hadoop.hbase.hbtop.field.FieldValue;
import org.apache.hadoop.hbase.hbtop.mode.DrillDownInfo;
import org.apache.hadoop.hbase.hbtop.mode.Mode;

/**
 * The data and business logic for the top screen.
 */
@InterfaceAudience.Private
public class TopScreenModel {

  private static final Log LOG = LogFactory.getLog(TopScreenModel.class);

  private final Admin admin;

  private Mode currentMode;
  private Field currentSortField;
  private List<FieldInfo> fieldInfos;
  private List<Field> fields;

  private Summary summary;
  private List<Record> records;

  private final List<RecordFilter> filters = new ArrayList<>();
  private final List<String> filterHistories = new ArrayList<>();

  private boolean ascendingSort;

  public TopScreenModel(Admin admin, Mode initialMode) {
    this.admin = Objects.requireNonNull(admin);
    switchMode(Objects.requireNonNull(initialMode), null, false);
  }

  public void switchMode(Mode nextMode, List<RecordFilter> initialFilters,
    boolean keepSortFieldAndSortOrderIfPossible) {

    currentMode = nextMode;
    fieldInfos = Collections.unmodifiableList(new ArrayList<>(currentMode.getFieldInfos()));

    fields = new ArrayList<>();
    for (FieldInfo fieldInfo : currentMode.getFieldInfos()) {
      fields.add(fieldInfo.getField());
    }
    fields = Collections.unmodifiableList(fields);

    if (keepSortFieldAndSortOrderIfPossible) {
      boolean match = false;
      for (Field field : fields) {
        if (field == currentSortField) {
          match = true;
          break;
        }
      }

      if (!match) {
        currentSortField = nextMode.getDefaultSortField();
        ascendingSort = false;
      }

    } else {
      currentSortField = nextMode.getDefaultSortField();
      ascendingSort = false;
    }

    clearFilters();
    if (initialFilters != null) {
      filters.addAll(initialFilters);
    }
  }

  public void setSortFieldAndFields(Field sortField, List<Field> fields) {
    this.currentSortField = sortField;
    this.fields = Collections.unmodifiableList(new ArrayList<>(fields));
  }

  /*
   * HBTop only calls this from a single thread, and if that ever changes, this needs
   * synchronization
   */
  public void refreshMetricsData() {
    ClusterStatus clusterStatus;
    try {
      clusterStatus = admin.getClusterStatus();
    } catch (Exception e) {
      LOG.error("Unable to get cluster status", e);
      return;
    }

    refreshSummary(clusterStatus);
    refreshRecords(clusterStatus);
  }

  private void refreshSummary(ClusterStatus clusterStatus) {
    String currentTime = DateFormatUtils.ISO_8601_EXTENDED_TIME_FORMAT
      .format(System.currentTimeMillis());
    String version = clusterStatus.getHBaseVersion();
    String clusterId = clusterStatus.getClusterId();
    int liveServers = clusterStatus.getServersSize();
    int deadServers = clusterStatus.getDeadServerNames().size();
    int regionCount = clusterStatus.getRegionsCount();
    int ritCount = clusterStatus.getRegionsInTransition().size();
    double averageLoad = clusterStatus.getAverageLoad();
    long aggregateRequestPerSecond = 0;
    for (ServerName sn: clusterStatus.getServers()) {
      ServerLoad sl = clusterStatus.getLoad(sn);
      aggregateRequestPerSecond += sl.getNumberOfRequests();
    }
    summary = new Summary(currentTime, version, clusterId, liveServers + deadServers,
      liveServers, deadServers, regionCount, ritCount, averageLoad, aggregateRequestPerSecond);
  }

  private void refreshRecords(ClusterStatus clusterStatus) {
    // Filter
    List<Record> records = new ArrayList<>();
    for (Record record : currentMode.getRecords(clusterStatus)) {
      boolean filter = false;
      for (RecordFilter recordFilter : filters) {
        if (!recordFilter.execute(record)) {
          filter = true;
          break;
        }
      }
      if (!filter) {
        records.add(record);
      }
    }

    // Sort
    Collections.sort(records, new Comparator<Record>() {
      @Override
      public int compare(Record recordLeft, Record recordRight) {
        FieldValue left = recordLeft.get(currentSortField);
        FieldValue right = recordRight.get(currentSortField);
        return (ascendingSort ? 1 : -1) * left.compareTo(right);
      }
    });

    this.records = Collections.unmodifiableList(records);
  }

  public void switchSortOrder() {
    ascendingSort = !ascendingSort;
  }

  public boolean addFilter(String filterString, boolean ignoreCase) {
    RecordFilter filter = RecordFilter.parse(filterString, fields, ignoreCase);
    if (filter == null) {
      return false;
    }

    filters.add(filter);
    filterHistories.add(filterString);
    return true;
  }

  public void clearFilters() {
    filters.clear();
  }

  public boolean drillDown(Record selectedRecord) {
    DrillDownInfo drillDownInfo = currentMode.drillDown(selectedRecord);
    if (drillDownInfo == null) {
      return false;
    }
    switchMode(drillDownInfo.getNextMode(), drillDownInfo.getInitialFilters(), true);
    return true;
  }

  public Mode getCurrentMode() {
    return currentMode;
  }

  public Field getCurrentSortField() {
    return currentSortField;
  }

  public List<FieldInfo> getFieldInfos() {
    return fieldInfos;
  }

  public List<Field> getFields() {
    return fields;
  }

  public Summary getSummary() {
    return summary;
  }

  public List<Record> getRecords() {
    return records;
  }

  public List<RecordFilter> getFilters() {
    return Collections.unmodifiableList(filters);
  }

  public List<String> getFilterHistories() {
    return Collections.unmodifiableList(filterHistories);
  }
}
