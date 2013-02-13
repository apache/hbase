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
package org.apache.hadoop.hbase.server.snapshot.error;

import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;

/**
 * Generic running snapshot failure listener
 */
public interface SnapshotFailureListener {

  /**
   * Notification that a given snapshot failed because of an error on the local server
   * @param snapshot snapshot that failed
   * @param reason explanation of why the snapshot failed
   */
  public void snapshotFailure(String reason, SnapshotDescription snapshot);

  /**
   * Notification that a given snapshot failed because of an error on the local server
   * @param reason reason the snapshot failed
   * @param snapshot the snapshot that failed
   * @param t the exception that caused the failure
   */
  public void snapshotFailure(String reason, SnapshotDescription snapshot, Exception t);
}
