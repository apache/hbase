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
import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * General exception base class for when a snapshot fails
 */
@SuppressWarnings("serial")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HBaseSnapshotException extends DoNotRetryIOException {

  private SnapshotDescription description;

  /**
   * Some exception happened for a snapshot and don't even know the snapshot that it was about
   * @param msg Full description of the failure
   */
  public HBaseSnapshotException(String msg) {
    super(msg);
  }

  /**
   * Exception for the given snapshot that has no previous root cause
   * @param msg reason why the snapshot failed
   * @param desc description of the snapshot that is being failed
   */
  @Deprecated
  public HBaseSnapshotException(String msg, SnapshotDescription desc) {
    super(msg);
    this.description = desc;
  }

  /**
   * Exception for the given snapshot due to another exception
   * @param msg reason why the snapshot failed
   * @param cause root cause of the failure
   * @param desc description of the snapshot that is being failed
   */
  @Deprecated
  public HBaseSnapshotException(String msg, Throwable cause, SnapshotDescription desc) {
    super(msg, cause);
    this.description = desc;
  }

  /**
   * Exception when the description of the snapshot cannot be determined, due to some root other
   * root cause
   * @param message description of what caused the failure
   * @param e root cause
   */
  public HBaseSnapshotException(String message, Exception e) {
    super(message, e);
  }

  @Deprecated
  public SnapshotDescription getSnapshotDescription() {
    return this.description;
  }
}
