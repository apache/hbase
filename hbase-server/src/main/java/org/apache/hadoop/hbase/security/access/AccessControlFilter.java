/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.security.access;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.SimpleMutableByteRange;

/**
 * <strong>NOTE: for internal use only by AccessController implementation</strong>
 *
 * <p>
 * TODO: There is room for further performance optimization here.
 * Calling TableAuthManager.authorize() per KeyValue imposes a fair amount of
 * overhead.  A more optimized solution might look at the qualifiers where
 * permissions are actually granted and explicitly limit the scan to those.
 * </p>
 * <p>
 * We should aim to use this _only_ when access to the requested column families
 * is not granted at the column family levels.  If table or column family
 * access succeeds, then there is no need to impose the overhead of this filter.
 * </p>
 */
@InterfaceAudience.Private
class AccessControlFilter extends FilterBase {

  public static enum Strategy {
    /** Filter only by checking the table or CF permissions */
    CHECK_TABLE_AND_CF_ONLY,
    /** Cell permissions can override table or CF permissions */
    CHECK_CELL_DEFAULT,
  };

  private TableAuthManager authManager;
  private TableName table;
  private User user;
  private boolean isSystemTable;
  private Strategy strategy;
  private Map<ByteRange, Integer> cfVsMaxVersions;
  private int familyMaxVersions;
  private int currentVersions;
  private ByteRange prevFam;
  private ByteRange prevQual;

  /**
   * For Writable
   */
  AccessControlFilter() {
  }

  AccessControlFilter(TableAuthManager mgr, User ugi, TableName tableName,
      Strategy strategy, Map<ByteRange, Integer> cfVsMaxVersions) {
    authManager = mgr;
    table = tableName;
    user = ugi;
    isSystemTable = tableName.isSystemTable();
    this.strategy = strategy;
    this.cfVsMaxVersions = cfVsMaxVersions;
    this.prevFam = new SimpleMutableByteRange();
    this.prevQual = new SimpleMutableByteRange();
  }

  @Override
  public boolean filterRowKey(Cell cell) throws IOException {
    // Impl in FilterBase might do unnecessary copy for Off heap backed Cells.
    return false;
  }

  @Override
  public ReturnCode filterKeyValue(Cell cell) {
    if (isSystemTable) {
      return ReturnCode.INCLUDE;
    }
    if (prevFam.getBytes() == null
        || (Bytes.compareTo(prevFam.getBytes(), prevFam.getOffset(), prevFam.getLength(),
            cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) != 0)) {
      prevFam.set(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
      // Similar to VisibilityLabelFilter
      familyMaxVersions = cfVsMaxVersions.get(prevFam);
      // Family is changed. Just unset curQualifier.
      prevQual.unset();
    }
    if (prevQual.getBytes() == null
        || (Bytes.compareTo(prevQual.getBytes(), prevQual.getOffset(),
            prevQual.getLength(), cell.getQualifierArray(), cell.getQualifierOffset(),
            cell.getQualifierLength()) != 0)) {
      prevQual.set(cell.getQualifierArray(), cell.getQualifierOffset(),
          cell.getQualifierLength());
      currentVersions = 0;
    }
    currentVersions++;
    if (currentVersions > familyMaxVersions) {
      return ReturnCode.SKIP;
    }
    // XXX: Compare in place, don't clone
    byte[] family = CellUtil.cloneFamily(cell);
    byte[] qualifier = CellUtil.cloneQualifier(cell);
    switch (strategy) {
      // Filter only by checking the table or CF permissions
      case CHECK_TABLE_AND_CF_ONLY: {
        if (authManager.authorize(user, table, family, qualifier, Permission.Action.READ)) {
          return ReturnCode.INCLUDE;
        }
      }
      break;
      // Cell permissions can override table or CF permissions
      case CHECK_CELL_DEFAULT: {
        if (authManager.authorize(user, table, family, qualifier, Permission.Action.READ) ||
            authManager.authorize(user, table, cell, Permission.Action.READ)) {
          return ReturnCode.INCLUDE;
        }
      }
      break;
      default:
        throw new RuntimeException("Unhandled strategy " + strategy);
    }

    return ReturnCode.SKIP;
  }

  @Override
  public void reset() throws IOException {
    this.prevFam.unset();
    this.prevQual.unset();
    this.familyMaxVersions = 0;
    this.currentVersions = 0;
  }

  /**
   * @return The filter serialized using pb
   */
  public byte [] toByteArray() {
    // no implementation, server-side use only
    throw new UnsupportedOperationException(
      "Serialization not supported.  Intended for server-side use only.");
  }

  /**
   * @param pbBytes A pb serialized {@link AccessControlFilter} instance
   * @return An instance of {@link AccessControlFilter} made from <code>bytes</code>
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   * @see {@link #toByteArray()}
   */
  public static AccessControlFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    // no implementation, server-side use only
    throw new UnsupportedOperationException(
      "Serialization not supported.  Intended for server-side use only.");
  }
}
