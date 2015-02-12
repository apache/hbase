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

package org.apache.hadoop.hbase.replication.regionserver;

public interface MetricsReplicationSourceSource {

  public static final String SOURCE_SIZE_OF_LOG_QUEUE = "source.sizeOfLogQueue";
  public static final String SOURCE_AGE_OF_LAST_SHIPPED_OP = "source.ageOfLastShippedOp";
  public static final String SOURCE_SHIPPED_BATCHES = "source.shippedBatches";

  public static final String SOURCE_SHIPPED_KBS = "source.shippedKBs";
  public static final String SOURCE_SHIPPED_OPS = "source.shippedOps";

  public static final String SOURCE_LOG_READ_IN_BYTES = "source.logReadInBytes";
  public static final String SOURCE_LOG_READ_IN_EDITS = "source.logEditsRead";

  public static final String SOURCE_LOG_EDITS_FILTERED = "source.logEditsFiltered";

  void setLastShippedAge(long age);
  void setSizeOfLogQueue(int size);
  void incrSizeOfLogQueue(int size);
  void decrSizeOfLogQueue(int size);
  void incrLogEditsFiltered(long size);
  void incrBatchesShipped(int batches);
  void incrOpsShipped(long ops);
  void incrShippedKBs(long size);
  void incrLogReadInBytes(long size);
  void incrLogReadInEdits(long size);
  void clear();
  long getLastShippedAge();
}
