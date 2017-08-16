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
package org.apache.hadoop.hbase.snapshot;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;

/**
 * Thrown when a snapshot could not be created due to a server-side error when
 * taking the snapshot.
 */
@SuppressWarnings("serial")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class SnapshotCreationException extends HBaseSnapshotException {

  /**
   * Used internally by the RPC engine to pass the exception back to the client.
   * @param msg error message to pass back
   */
  public SnapshotCreationException(String msg) {
    super(msg);
  }

  /**
   * Failure to create the specified snapshot
   * @param msg reason why the snapshot couldn't be completed
   * @param desc description of the snapshot attempted
   */
  public SnapshotCreationException(String msg, SnapshotDescription desc) {
    super(msg, desc);
  }

  /**
   * Failure to create the specified snapshot due to an external cause
   * @param msg reason why the snapshot couldn't be completed
   * @param cause root cause of the failure
   * @param desc description of the snapshot attempted
   */
  @Deprecated
  public SnapshotCreationException(String msg, Throwable cause, SnapshotDescription desc) {
    super(msg, cause, desc);
  }
}
