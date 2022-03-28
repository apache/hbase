/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase;

import java.io.IOException;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Thrown during flush if the possibility snapshot content was not properly
 * persisted into store files.  Response should include replay of wal content.
 */
@InterfaceAudience.Public
public class DroppedSnapshotException extends IOException {
  private static final long serialVersionUID = -5463156580831677374L;

  public DroppedSnapshotException() {
    super();
  }

  /**
   * @param message the message for this exception
   */
  public DroppedSnapshotException(String message) {
    super(message);
  }

  /**
   * DroppedSnapshotException with cause
   *
   * @param message the message for this exception
   * @param cause the cause for this exception
   */
  public DroppedSnapshotException(String message, Throwable cause) {
    super(message, cause);
  }
}
