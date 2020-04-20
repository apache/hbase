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

public class MetricsStoreWrapperStub implements MetricsStoreWrapper {

  @Override
  public String getStoreName() {
    return "store1";
  }

  @Override
  public String getRegionName() {
    return "region1";
  }

  @Override
  public String getTableName() {
    return "table1";
  }

  @Override
  public String getNamespace() {
    return "ns";
  }

  @Override
  public long getNumStoreFiles() {
    return 10;
  }

  @Override
  public long getMemStoreSize() {
    return 1000;
  }

  @Override
  public long getStoreFileSize() {
    return 100;
  }

  @Override
  public long getMaxStoreFileAge() {
    return 50;
  }

  @Override
  public long getMinStoreFileAge() {
    return 20;
  }

  @Override
  public long getAvgStoreFileAge() {
    return 30;
  }

  @Override
  public long getNumReferenceFiles() {
    return 11;
  }

  @Override
  public long getStoreRefCount() {
    return 10;
  }

  @Override
  public long getReadRequestCount() {
    return 1000;
  }

  @Override
  public long getMemstoreReadRequestsCount() {
    return 500;
  }

  @Override
  public long getFileReadRequestCount() {
    return 500;
  }

}
