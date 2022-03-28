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

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * General exception base class for when a snapshot fails.
 */
@SuppressWarnings("serial")
@InterfaceAudience.Public
public class HBaseSnapshotException extends DoNotRetryIOException {
  private SnapshotDescription description;

  /**
   * Some exception happened for a snapshot and don't even know the snapshot that it was about.
   *
   * @param message the full description of the failure
   */
  public HBaseSnapshotException(String message) {
    super(message);
  }

  /**
   * Exception for the given snapshot that has no previous root cause.
   *
   * @param message the reason why the snapshot failed
   * @param snapshotDescription the description of the snapshot that is failing
   */
  public HBaseSnapshotException(String message, SnapshotDescription snapshotDescription) {
    super(message);
    this.description = snapshotDescription;
  }

  /**
   * Exception for the given snapshot due to another exception.
   *
   * @param message the reason why the snapshot failed
   * @param cause the root cause of the failure
   * @param snapshotDescription the description of the snapshot that is being failed
   */
  public HBaseSnapshotException(String message, Throwable cause,
      SnapshotDescription snapshotDescription) {
    super(message, cause);
    this.description = snapshotDescription;
  }

  /**
   * Exception when the description of the snapshot cannot be determined, due to some root other
   * root cause.
   *
   * @param message description of what caused the failure
   * @param cause the root cause
   */
  public HBaseSnapshotException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * @return the description of the snapshot that is being failed
   */
  public SnapshotDescription getSnapshotDescription() {
    return this.description;
  }
}
