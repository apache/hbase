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
package org.apache.hadoop.hbase.ipc;

import java.util.List;

import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Optionally carries Cells across the proxy/service interface down into ipc. On its
 * way out it optionally carries a set of result Cell data.  We stick the Cells here when we want
 * to avoid having to protobuf them.  This class is used ferrying data across the proxy/protobuf
 * service chasm.  Used by client and server ipc'ing.
 */
@InterfaceAudience.Private
public class PayloadCarryingRpcController
    extends TimeLimitedRpcController implements CellScannable {
  /**
   * Priority to set on this request.  Set it here in controller so available composing the
   * request.  This is the ordained way of setting priorities going forward.  We will be
   * undoing the old annotation-based mechanism.
   */
  // Currently only multi call makes use of this.  Eventually this should be only way to set
  // priority.
  private int priority = HConstants.NORMAL_QOS;

  /**
   * They are optionally set on construction, cleared after we make the call, and then optionally
   * set on response with the result. We use this lowest common denominator access to Cells because
   * sometimes the scanner is backed by a List of Cells and other times, it is backed by an
   * encoded block that implements CellScanner.
   */
  private CellScanner cellScanner;

  public PayloadCarryingRpcController() {
    this((CellScanner)null);
  }

  public PayloadCarryingRpcController(final CellScanner cellScanner) {
    this.cellScanner = cellScanner;
  }

  public PayloadCarryingRpcController(final List<CellScannable> cellIterables) {
    this.cellScanner = cellIterables == null? null: CellUtil.createCellScanner(cellIterables);
  }

  /**
   * @return One-shot cell scanner (you cannot back it up and restart)
   */
  public CellScanner cellScanner() {
    return cellScanner;
  }

  public void setCellScanner(final CellScanner cellScanner) {
    this.cellScanner = cellScanner;
  }

  /**
   * @param priority Priority for this request; should fall roughly in the range
   * {@link HConstants#NORMAL_QOS} to {@link HConstants#HIGH_QOS}
   */
  public void setPriority(int priority) {
    this.priority = priority;
  }

  /**
   * @param tn Set priority based off the table we are going against.
   */
  public void setPriority(final TableName tn) {
    this.priority =
        (tn != null && tn.isSystemTable())? HConstants.SYSTEMTABLE_QOS: HConstants.NORMAL_QOS;
  }

  /**
   * @return The priority of this request
   */
  public int getPriority() {
    return priority;
  }

  @Override public void reset() {
    super.reset();
    priority = 0;
    cellScanner = null;
  }
}
