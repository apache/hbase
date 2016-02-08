/**
 *
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * ImmutableSegment is an abstract class that extends the API supported by a {@link Segment},
 * and is not needed for a {@link MutableSegment}. Specifically, the method
 * {@link ImmutableSegment#getKeyValueScanner()} builds a special scanner for the
 * {@link MemStoreSnapshot} object.
 * In addition, this class overrides methods that are not likely to be supported by an immutable
 * segment, e.g. {@link Segment#rollback(Cell)} and {@link Segment#getCellSet()}, which
 * can be very inefficient.
 */
@InterfaceAudience.Private
public abstract class ImmutableSegment extends Segment {

  public ImmutableSegment(Segment segment) {
    super(segment);
  }

  /**
   * Removes the given cell from this segment.
   * By default immutable store segment can not rollback
   * It may be invoked by tests in specific cases where it is known to be supported {@link
   * ImmutableSegmentAdapter}
   */
  @Override
  public long rollback(Cell cell) {
    return 0;
  }

  /**
   * Returns a set of all the cells in the segment.
   * The implementation of this method might be very inefficient for some immutable segments
   * that do not maintain a cell set. Therefore by default this method is not supported.
   * It may be invoked by tests in specific cases where it is known to be supported {@link
   * ImmutableSegmentAdapter}
   */
  @Override
  public CellSet getCellSet() {
    throw new NotImplementedException("Immutable Segment does not support this operation by " +
        "default");
  }

  /**
   * Builds a special scanner for the MemStoreSnapshot object that may be different than the
   * general segment scanner.
   * @return a special scanner for the MemStoreSnapshot object
   */
  public abstract KeyValueScanner getKeyValueScanner();

}
