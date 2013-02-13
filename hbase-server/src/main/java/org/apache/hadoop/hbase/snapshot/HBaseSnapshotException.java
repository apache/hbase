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

import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;

/**
 * General exception when a snapshot fails.
 */
@SuppressWarnings("serial")
public class HBaseSnapshotException extends HBaseIOException {

  private SnapshotDescription description;

  public HBaseSnapshotException(String msg) {
    super(msg);
  }

  public HBaseSnapshotException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public HBaseSnapshotException(Throwable cause) {
    super(cause);
  }

  public HBaseSnapshotException(String msg, SnapshotDescription desc) {
    super(msg);
    this.description = desc;
  }

  public HBaseSnapshotException(Throwable cause, SnapshotDescription desc) {
    super(cause);
    this.description = desc;
  }

  public HBaseSnapshotException(String msg, Throwable cause, SnapshotDescription desc) {
    super(msg, cause);
    this.description = desc;
  }

  public SnapshotDescription getSnapshotDescription() {
    return this.description;
  }
}
