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

package org.apache.hadoop.hbase.regionserver;

public class MetricsTableWrapperStub implements MetricsTableWrapperAggregate {

  private String tableName;

  public MetricsTableWrapperStub(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public long getReadRequestsCount(String table) {
    return 10;
  }

  @Override
  public long getWriteRequestsCount(String table) {
    return 20;
  }

  @Override
  public long getTotalRequestsCount(String table) {
    return 30;
  }

  public String getTableName() {
    return tableName;
  }
}
