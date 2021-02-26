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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsTable {
  private final MetricsTableAggregateSource tableSourceAgg;
  private MetricsTableWrapperAggregate wrapper;

  public MetricsTable(final MetricsTableWrapperAggregate wrapper) {
    tableSourceAgg = CompatibilitySingletonFactory.getInstance(MetricsRegionServerSourceFactory.class)
                                             .getTableAggregate();
    this.wrapper = wrapper;
  }

  public MetricsTableWrapperAggregate getTableWrapperAgg() {
    return wrapper;
  }

  public MetricsTableAggregateSource getTableSourceAgg() {
    return tableSourceAgg;
  }

  public void incrSplitRequest(String table) {
    tableSourceAgg.getOrCreateTableSource(table, wrapper).incrSplitRequest();
  }

  public void incrSplitSuccess(String table) {
    tableSourceAgg.getOrCreateTableSource(table, wrapper).incrSplitSuccess();
  }

  public void updateSplitTime(String table, long t) {
    tableSourceAgg.getOrCreateTableSource(table, wrapper).updateSplitTime(t);
  }

  public void updateFlushTime(String table, long t) {
    tableSourceAgg.getOrCreateTableSource(table, wrapper).updateFlushTime(t);
  }

  public void updateFlushMemstoreSize(String table, long bytes) {
    tableSourceAgg.getOrCreateTableSource(table, wrapper).updateFlushMemstoreSize(bytes);
  }

  public void updateFlushOutputSize(String table, long bytes) {
    tableSourceAgg.getOrCreateTableSource(table, wrapper).updateFlushOutputSize(bytes);
  }

  public void updateCompactionTime(String table, boolean isMajor, long t) {
    tableSourceAgg.getOrCreateTableSource(table, wrapper).updateCompactionTime(isMajor, t);
  }

  public void updateCompactionInputFileCount(String table, boolean isMajor, long c) {
    tableSourceAgg.getOrCreateTableSource(table, wrapper)
      .updateCompactionInputFileCount(isMajor, c);
  }

  public void updateCompactionInputSize(String table, boolean isMajor, long bytes) {
    tableSourceAgg.getOrCreateTableSource(table, wrapper)
      .updateCompactionInputSize(isMajor, bytes);
  }

  public void updateCompactionOutputFileCount(String table, boolean isMajor, long c) {
    tableSourceAgg.getOrCreateTableSource(table, wrapper)
      .updateCompactionOutputFileCount(isMajor, c);
  }

  public void updateCompactionOutputSize(String table, boolean isMajor, long bytes) {
    tableSourceAgg.getOrCreateTableSource(table, wrapper)
      .updateCompactionOutputSize(isMajor, bytes);
  }
}
