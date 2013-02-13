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
import org.apache.hadoop.hbase.server.errorhandling.impl.ExceptionSnare;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.UnexpectedSnapshotException;

/**
 * {@link ExceptionSnare} for snapshot exceptions, ensuring that only the first exception is
 * retained and always returned via {@link #failOnError()}.
 * <p>
 * Ensures that any generic exceptions received via
 * {@link #receiveError(String, HBaseSnapshotException, Object...)} are in fact propagated as
 * {@link HBaseSnapshotException}.
 */
public class SnapshotExceptionSnare extends ExceptionSnare<HBaseSnapshotException> implements
    SnapshotFailureListener {

  private SnapshotDescription snapshot;

  /**
   * Create a snare that expects errors for the passed snapshot. Any untyped exceptions passed to
   * {@link #receiveError(String, HBaseSnapshotException, Object...)} are wrapped as an
   * {@link UnexpectedSnapshotException} with the passed {@link SnapshotDescription}.
   * @param snapshot
   */
  public SnapshotExceptionSnare(SnapshotDescription snapshot) {
    this.snapshot = snapshot;
  }

  @Override
  public void snapshotFailure(String reason, SnapshotDescription snapshot) {
    this.receiveError(reason, null, snapshot);
  }

  @Override
  public void snapshotFailure(String reason, SnapshotDescription snapshot, Exception t) {
    this.receiveError(reason,
      t instanceof HBaseSnapshotException ? (HBaseSnapshotException) t
          : new UnexpectedSnapshotException(reason, t, snapshot), snapshot);
  }

  @Override
  public void failOnError() throws HBaseSnapshotException {
    try {
      super.failOnError();
    } catch (Exception e) {
      if (e instanceof HBaseSnapshotException) {
        throw (HBaseSnapshotException) e;
      }
      throw new UnexpectedSnapshotException(e.getMessage(), e, snapshot);
    }
  }
}
