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

public class MetricsRegionWrapperStub implements MetricsRegionWrapper {
  int replicaid = 0;

  /**
   * Replica ID set to 0
   */
  public MetricsRegionWrapperStub() {
    this.replicaid = 0;
  }

  /**
   * Pass in replica ID
   */
  public MetricsRegionWrapperStub(int replicaid) {
    this.replicaid = replicaid;
  }

  @Override
  public String getTableName() {
    return "MetricsRegionWrapperStub";
  }

  @Override
  public String getNamespace() {
    return "TestNS";
  }

  @Override
  public String getRegionName() {
    return "DEADBEEF001";
  }

  @Override
  public long getNumStores() {
    return 101;
  }

  @Override
  public long getNumStoreFiles() {
    return 102;
  }

  @Override
  public long getMemstoreSize() {
    return 103;
  }

  @Override
  public long getStoreFileSize() {
    return 104;
  }

  @Override
  public long getReadRequestCount() {
    return 105;
  }

  @Override
  public long getWriteRequestCount() {
    return 106;
  }

  @Override
  public long getNumFilesCompacted() {
    return 0;
  }

  @Override
  public long getNumBytesCompacted() {
    return 0;
  }

  @Override
  public long getNumCompactionsCompleted() {
    return 0;
  }

  @Override
  public int getRegionHashCode() {
    return 42;
  }

  /**
   * Get the replica id of this region.
   */
  @Override
  public int getReplicaId() {
    return replicaid;
  }
}
