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
package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsReplicationSourceFactoryImpl implements MetricsReplicationSourceFactory {

  private static enum SourceHolder {
    INSTANCE;
    final MetricsReplicationSourceImpl source = new MetricsReplicationSourceImpl();
  }

  @Override public MetricsReplicationSinkSource getSink() {
    return new MetricsReplicationSinkSourceImpl(SourceHolder.INSTANCE.source);
  }

  @Override public MetricsReplicationSourceSource getSource(String id) {
    return new MetricsReplicationSourceSourceImpl(SourceHolder.INSTANCE.source, id);
  }

  @Override public MetricsReplicationTableSource getTableSource(String tableName) {
    return new MetricsReplicationTableSourceImpl(SourceHolder.INSTANCE.source, tableName);
  }

  @Override public MetricsReplicationGlobalSourceSource getGlobalSource() {
    return new MetricsReplicationGlobalSourceSourceImpl(SourceHolder.INSTANCE.source);
  }
}
