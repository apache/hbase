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

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class is responsible for the tracking and enforcement of Deletes
 * during the course of a Scan operation.
 *
 * It only has to enforce Delete and DeleteColumn, since the
 * DeleteFamily is handled at a higher level.
 *
 * <p>
 * This class is utilized through three methods:
 * <ul><li>{@link #add} when encountering a Delete or DeleteColumn</li>
 * <li>{@link #isDeleted} when checking if a Put KeyValue has been deleted</li>
 * <li>{@link #update} when reaching the end of a StoreFile or row for scans</li>
 * </ul>
 * <p>
 * This class is NOT thread-safe as queries are never multi-threaded
 */
@InterfaceAudience.Private
public class ScanDeleteTracker implements DeleteTracker {

  protected boolean hasFamilyStamp = false;
  protected long familyStamp = 0L;
  protected SortedSet<Long> familyVersionStamps = new TreeSet<Long>();
  protected byte [] deleteBuffer = null;
  protected int deleteOffset = 0;
  protected int deleteLength = 0;
  protected byte deleteType = 0;
  protected long deleteTimestamp = 0L;

  /**
   * Constructor for ScanDeleteTracker
   */
  public ScanDeleteTracker() {
    super();
  }

  /**
   * Add the specified KeyValue to the list of deletes to check against for
   * this row operation.
   * <p>
   * This is called when a Delete is encountered.
   * @param cell - the delete cell
   */
  @Override
  public void add(Cell cell) {
    long timestamp = cell.getTimestamp();
    int qualifierOffset = cell.getQualifierOffset();
    int qualifierLength = cell.getQualifierLength();
    byte type = cell.getTypeByte();
    if (!hasFamilyStamp || timestamp > familyStamp) {
      if (type == KeyValue.Type.DeleteFamily.getCode()) {
        hasFamilyStamp = true;
        familyStamp = timestamp;
        return;
      } else if (type == KeyValue.Type.DeleteFamilyVersion.getCode()) {
        familyVersionStamps.add(timestamp);
        return;
      }

      if (deleteBuffer != null && type < deleteType) {
        // same column, so ignore less specific delete
        if (Bytes.equals(deleteBuffer, deleteOffset, deleteLength,
            cell.getQualifierArray(), qualifierOffset, qualifierLength)){
          return;
        }
      }
      // new column, or more general delete type
      deleteBuffer = cell.getQualifierArray();
      deleteOffset = qualifierOffset;
      deleteLength = qualifierLength;
      deleteType = type;
      deleteTimestamp = timestamp;
    }
    // missing else is never called.
  }

  /**
   * Check if the specified KeyValue buffer has been deleted by a previously
   * seen delete.
   *
   * @param cell - current cell to check if deleted by a previously seen delete
   * @return deleteResult
   */
  @Override
  public DeleteResult isDeleted(Cell cell) {
    long timestamp = cell.getTimestamp();
    int qualifierOffset = cell.getQualifierOffset();
    int qualifierLength = cell.getQualifierLength();
    if (hasFamilyStamp && timestamp <= familyStamp) {
      return DeleteResult.FAMILY_DELETED;
    }

    if (familyVersionStamps.contains(Long.valueOf(timestamp))) {
        return DeleteResult.FAMILY_VERSION_DELETED;
    }

    if (deleteBuffer != null) {
      int ret = Bytes.compareTo(deleteBuffer, deleteOffset, deleteLength,
          cell.getQualifierArray(), qualifierOffset, qualifierLength);

      if (ret == 0) {
        if (deleteType == KeyValue.Type.DeleteColumn.getCode()) {
          return DeleteResult.COLUMN_DELETED;
        }
        // Delete (aka DeleteVersion)
        // If the timestamp is the same, keep this one
        if (timestamp == deleteTimestamp) {
          return DeleteResult.VERSION_DELETED;
        }
        // use assert or not?
        assert timestamp < deleteTimestamp;

        // different timestamp, let's clear the buffer.
        deleteBuffer = null;
      } else if(ret < 0){
        // Next column case.
        deleteBuffer = null;
      } else {
        throw new IllegalStateException("isDelete failed: deleteBuffer="
            + Bytes.toStringBinary(deleteBuffer, deleteOffset, deleteLength)
            + ", qualifier="
            + Bytes.toStringBinary(cell.getQualifierArray(), qualifierOffset, qualifierLength)
            + ", timestamp=" + timestamp + ", comparison result: " + ret);
      }
    }

    return DeleteResult.NOT_DELETED;
  }

  @Override
  public boolean isEmpty() {
    return deleteBuffer == null && !hasFamilyStamp &&
           familyVersionStamps.isEmpty();
  }

  @Override
  // called between every row.
  public void reset() {
    hasFamilyStamp = false;
    familyStamp = 0L;
    familyVersionStamps.clear();
    deleteBuffer = null;
  }

  @Override
  // should not be called at all even (!)
  public void update() {
    this.reset();
  }
}
