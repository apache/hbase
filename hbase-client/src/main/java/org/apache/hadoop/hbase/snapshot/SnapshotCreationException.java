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

import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Thrown when a snapshot could not be created due to a server-side error when
 * taking the snapshot.
 */
@SuppressWarnings("serial")
@InterfaceAudience.Public
public class SnapshotCreationException extends HBaseSnapshotException {
  /**
   * Used internally by the RPC engine to pass the exception back to the client.
   *
   * @param message error message to pass back
   */
  public SnapshotCreationException(String message) {
    super(message);
  }

  /**
   * Failure to create the specified snapshot.
   *
   * @param message reason why the snapshot couldn't be completed
   * @param snapshotDescription description of the snapshot attempted
   */
  public SnapshotCreationException(String message, SnapshotDescription snapshotDescription) {
    super(message, snapshotDescription);
  }

  /**
   * Failure to create the specified snapshot due to an external cause.
   *
   * @param message reason why the snapshot couldn't be completed
   * @param cause the root cause of the failure
   * @param snapshotDescription description of the snapshot attempted
   */
  public SnapshotCreationException(String message, Throwable cause,
      SnapshotDescription snapshotDescription) {
    super(message, cause, snapshotDescription);
  }
}
