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
 * Thrown when a snapshot could not be restored due to a server-side error when restoring it.
 */
@SuppressWarnings("serial")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RestoreSnapshotException extends HBaseSnapshotException {
  /**
   * @param message reason why restoring the snapshot fails
   * @param snapshotDescription description of the snapshot attempted
   * @deprecated since 1.3.0, will be removed in 3.0.0
   */
  @Deprecated
  public RestoreSnapshotException(String message, SnapshotDescription snapshotDescription) {
    super(message, snapshotDescription);
  }

  /**
   * @param message reason why restoring the snapshot fails
   * @param cause the root cause of the failure
   * @param snapshotDescription description of the snapshot attempted
   * @deprecated since 1.3.0, will be removed in 3.0.0
   */
  @Deprecated
  public RestoreSnapshotException(String message, Throwable cause,
      SnapshotDescription snapshotDescription) {
    super(message, cause, snapshotDescription);
  }

  /**
   * @param message reason why restoring the snapshot fails
   */
  public RestoreSnapshotException(String message) {
    super(message);
  }
  /**
   * @param message reason why restoring the snapshot fails
   * @param e the root cause of the failure
   */
  public RestoreSnapshotException(String message, Exception e) {
    super(message, e);
  }
}
