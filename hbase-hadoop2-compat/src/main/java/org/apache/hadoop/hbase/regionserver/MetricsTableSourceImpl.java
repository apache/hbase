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

package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;

@InterfaceAudience.Private
public class MetricsTableSourceImpl implements MetricsTableSource {

  private static final Log LOG = LogFactory.getLog(MetricsTableSourceImpl.class);

  private AtomicBoolean closed = new AtomicBoolean(false);

  // Non-final so that we can null out the wrapper
  // This is just paranoia. We really really don't want to
  // leak a whole table by way of keeping the
  // tableWrapper around too long.
  private MetricsTableWrapperAggregate tableWrapperAgg;
  private final MetricsTableAggregateSourceImpl agg;
  private final DynamicMetricsRegistry registry;
  private final String tableNamePrefix;
  private final TableName tableName;
  private final int hashCode;

  public MetricsTableSourceImpl(String tblName,
      MetricsTableAggregateSourceImpl aggregate, MetricsTableWrapperAggregate tblWrapperAgg) {
    LOG.debug("Creating new MetricsTableSourceImpl for table ");
    this.tableName = TableName.valueOf(tblName);
    this.agg = aggregate;
    agg.register(tblName, this);
    this.tableWrapperAgg = tblWrapperAgg;
    this.registry = agg.getMetricsRegistry();
    this.tableNamePrefix = "Namespace_" + this.tableName.getNamespaceAsString() +
        "_table_" + this.tableName.getQualifierAsString() + "_metric_";
    this.hashCode = this.tableName.hashCode();
  }

  @Override
  public void close() {
    boolean wasClosed = closed.getAndSet(true);

    // Has someone else already closed this for us?
    if (wasClosed) {
      return;
    }

    // Before removing the metrics remove this table from the aggregate table bean.
    // This should mean that it's unlikely that snapshot and close happen at the same time.
    agg.deregister(tableName.getNameAsString());

    // While it's un-likely that snapshot and close happen at the same time it's still possible.
    // So grab the lock to ensure that all calls to snapshot are done before we remove the metrics
    synchronized (this) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Removing table Metrics for table ");
      }
      tableWrapperAgg = null;
    }
  }
  @Override
  public MetricsTableAggregateSource getAggregateSource() {
    return agg;
  }

  @Override
  public int compareTo(MetricsTableSource source) {
    if (!(source instanceof MetricsTableSourceImpl)) {
      return -1;
    }

    MetricsTableSourceImpl impl = (MetricsTableSourceImpl) source;
    if (impl == null) {
      return -1;
    }

    return Long.compare(hashCode, impl.hashCode);
  }

  void snapshot(MetricsRecordBuilder mrb, boolean ignored) {

    // If there is a close that started be double extra sure
    // that we're not getting any locks and not putting data
    // into the metrics that should be removed. So early out
    // before even getting the lock.
    if (closed.get()) {
      return;
    }

    // Grab the read
    // This ensures that removes of the metrics
    // can't happen while we are putting them back in.
    synchronized (this) {

      // It's possible that a close happened between checking
      // the closed variable and getting the lock.
      if (closed.get()) {
        return;
      }

      if (this.tableWrapperAgg != null) {
        mrb.addCounter(Interns.info(tableNamePrefix + MetricsTableSource.READ_REQUEST_COUNT,
          MetricsTableSource.READ_REQUEST_COUNT_DESC),
          tableWrapperAgg.getReadRequestsCount(tableName.getNameAsString()));
        mrb.addCounter(Interns.info(tableNamePrefix + MetricsTableSource.WRITE_REQUEST_COUNT,
          MetricsTableSource.WRITE_REQUEST_COUNT_DESC),
          tableWrapperAgg.getWriteRequestsCount(tableName.getNameAsString()));
        mrb.addCounter(Interns.info(tableNamePrefix + MetricsTableSource.TOTAL_REQUEST_COUNT,
          MetricsTableSource.TOTAL_REQUEST_COUNT_DESC),
          tableWrapperAgg.getTotalRequestsCount(tableName.getNameAsString()));
        mrb.addGauge(Interns.info(tableNamePrefix + MetricsTableSource.MEMSTORE_SIZE,
          MetricsTableSource.MEMSTORE_SIZE_DESC),
          tableWrapperAgg.getMemstoresSize(tableName.getNameAsString()));
        mrb.addGauge(Interns.info(tableNamePrefix + MetricsTableSource.STORE_FILE_SIZE,
          MetricsTableSource.STORE_FILE_SIZE_DESC),
          tableWrapperAgg.getStoreFilesSize(tableName.getNameAsString()));
        mrb.addGauge(Interns.info(tableNamePrefix + MetricsTableSource.TABLE_SIZE,
          MetricsTableSource.TABLE_SIZE_DESC),
          tableWrapperAgg.getTableSize(tableName.getNameAsString()));
      }
    }
  }

  @Override
  public String getTableName() {
    return tableName.getNameAsString();
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    return (o instanceof MetricsTableSourceImpl && compareTo((MetricsTableSourceImpl) o) == 0);
  }

  public MetricsTableWrapperAggregate getTableWrapper() {
    return tableWrapperAgg;
  }

  public String getTableNamePrefix() {
    return tableNamePrefix;
  }
}
