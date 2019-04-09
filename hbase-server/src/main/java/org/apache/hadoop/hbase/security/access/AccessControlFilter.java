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
import java.util.Objects;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.SimpleMutableByteRange;

/**
 * <strong>NOTE: for internal use only by AccessController implementation</strong>
 *
 * <p>
 * TODO: There is room for further performance optimization here.
 * Calling AuthManager.authorize() per KeyValue imposes a fair amount of
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
  }

  private AuthManager authManager;
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

  AccessControlFilter(AuthManager mgr, User ugi, TableName tableName,
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
  public ReturnCode filterCell(final Cell cell) {
    if (isSystemTable) {
      return ReturnCode.INCLUDE;
    }
    if (prevFam.getBytes() == null
        || !(PrivateCellUtil.matchingFamily(cell, prevFam.getBytes(), prevFam.getOffset(),
            prevFam.getLength()))) {
      prevFam.set(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
      // Similar to VisibilityLabelFilter
      familyMaxVersions = cfVsMaxVersions.get(prevFam);
      // Family is changed. Just unset curQualifier.
      prevQual.unset();
    }
    if (prevQual.getBytes() == null
        || !(PrivateCellUtil.matchingQualifier(cell, prevQual.getBytes(), prevQual.getOffset(),
            prevQual.getLength()))) {
      prevQual.set(cell.getQualifierArray(), cell.getQualifierOffset(),
          cell.getQualifierLength());
      currentVersions = 0;
    }
    currentVersions++;
    if (currentVersions > familyMaxVersions) {
      return ReturnCode.SKIP;
    }
    // XXX: Compare in place, don't clone
    byte[] f = CellUtil.cloneFamily(cell);
    byte[] q = CellUtil.cloneQualifier(cell);
    switch (strategy) {
      // Filter only by checking the table or CF permissions
      case CHECK_TABLE_AND_CF_ONLY: {
        if (authManager.authorizeUserTable(user, table, f, q, Permission.Action.READ)) {
          return ReturnCode.INCLUDE;
        }
      }
      break;
      // Cell permissions can override table or CF permissions
      case CHECK_CELL_DEFAULT: {
        if (authManager.authorizeUserTable(user, table, f, q, Permission.Action.READ) ||
            authManager.authorizeCell(user, table, cell, Permission.Action.READ)) {
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
  @Override
  public byte [] toByteArray() {
    // no implementation, server-side use only
    throw new UnsupportedOperationException(
      "Serialization not supported.  Intended for server-side use only.");
  }

  /**
   * @param pbBytes A pb serialized {@link AccessControlFilter} instance
   * @return An instance of {@link AccessControlFilter} made from <code>bytes</code>
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   * @see #toByteArray()
   */
  public static AccessControlFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    // no implementation, server-side use only
    throw new UnsupportedOperationException(
      "Serialization not supported.  Intended for server-side use only.");
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof AccessControlFilter)) {
      return false;
    }
    if (this == obj){
      return true;
    }
    AccessControlFilter f=(AccessControlFilter)obj;
    return this.authManager.equals(f.authManager) &&
      this.table.equals(f.table) &&
      this.user.equals(f.user) &&
      this.strategy.equals(f.strategy) &&
      this.cfVsMaxVersions.equals(f.cfVsMaxVersions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.authManager, this.table, this.strategy, this.user,
      this.cfVsMaxVersions);
  }
}
