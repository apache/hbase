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

package org.apache.hadoop.hbase.client;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Enum describing the durability guarantees for tables and {@link Mutation}s
 * Note that the items must be sorted in order of increasing durability
 */
@InterfaceAudience.Public
public enum Durability {
  /* Developer note: Do not rename the enum field names. They are serialized in HTableDescriptor */
  /**
   * If this is for tables durability, use HBase's global default value (SYNC_WAL).
   * Otherwise, if this is for mutation, use the table's default setting to determine durability.
   * This must remain the first option.
   */
  USE_DEFAULT,
  /**
   * Do not write the Mutation to the WAL
   */
  SKIP_WAL,
  /**
   * Write the Mutation to the WAL asynchronously
   */
  ASYNC_WAL,
  /**
   * Write the Mutation to the WAL synchronously.
   * The data is flushed to the filesystem implementation, but not necessarily to disk.
   * For HDFS this will flush the data to the designated number of DataNodes.
   * See <a href="https://issues.apache.org/jira/browse/HADOOP-6313">HADOOP-6313</a>
   */
  SYNC_WAL,
  /**
   * Write the Mutation to the WAL synchronously and force the entries to disk.
   * See <a href="https://issues.apache.org/jira/browse/HADOOP-6313">HADOOP-6313</a>
   */
  FSYNC_WAL
}
