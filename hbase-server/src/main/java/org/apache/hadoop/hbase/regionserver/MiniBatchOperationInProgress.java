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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

/**
 * Wraps together the mutations which are applied as a batch to the region and their operation
 * status and WALEdits.
 * @see org.apache.hadoop.hbase.coprocessor.RegionObserver#preBatchMutate(
 * ObserverContext, MiniBatchOperationInProgress)
 * @see org.apache.hadoop.hbase.coprocessor.RegionObserver#postBatchMutate(
 * ObserverContext, MiniBatchOperationInProgress)
 * @param T Pair&lt;Mutation, Integer&gt; pair of Mutations and associated rowlock ids .
 */
@InterfaceAudience.LimitedPrivate("Coprocessors")
public class MiniBatchOperationInProgress<T> {
  private final T[] operations;
  private Mutation[][] operationsFromCoprocessors;
  private final OperationStatus[] retCodeDetails;
  private final WALEdit[] walEditsFromCoprocessors;
  private final int firstIndex;
  private final int lastIndexExclusive;

  public MiniBatchOperationInProgress(T[] operations, OperationStatus[] retCodeDetails,
      WALEdit[] walEditsFromCoprocessors, int firstIndex, int lastIndexExclusive) {
    this.operations = operations;
    this.retCodeDetails = retCodeDetails;
    this.walEditsFromCoprocessors = walEditsFromCoprocessors;
    this.firstIndex = firstIndex;
    this.lastIndexExclusive = lastIndexExclusive;
  }

  /**
   * @return The number of operations(Mutations) involved in this batch.
   */
  public int size() {
    return this.lastIndexExclusive - this.firstIndex;
  }

  /**
   * @param index
   * @return The operation(Mutation) at the specified position.
   */
  public T getOperation(int index) {
    return operations[getAbsoluteIndex(index)];
  }

  /**
   * Sets the status code for the operation(Mutation) at the specified position.
   * By setting this status, {@link org.apache.hadoop.hbase.coprocessor.RegionObserver}
   * can make HRegion to skip Mutations.
   * @param index
   * @param opStatus
   */
  public void setOperationStatus(int index, OperationStatus opStatus) {
    this.retCodeDetails[getAbsoluteIndex(index)] = opStatus;
  }

  /**
   * @param index
   * @return Gets the status code for the operation(Mutation) at the specified position.
   */
  public OperationStatus getOperationStatus(int index) {
    return this.retCodeDetails[getAbsoluteIndex(index)];
  }

  /**
   * Sets the walEdit for the operation(Mutation) at the specified position.
   * @param index
   * @param walEdit
   */
  public void setWalEdit(int index, WALEdit walEdit) {
    this.walEditsFromCoprocessors[getAbsoluteIndex(index)] = walEdit;
  }

  /**
   * @param index
   * @return Gets the walEdit for the operation(Mutation) at the specified position.
   */
  public WALEdit getWalEdit(int index) {
    return this.walEditsFromCoprocessors[getAbsoluteIndex(index)];
  }

  private int getAbsoluteIndex(int index) {
    if (index < 0 || this.firstIndex + index >= this.lastIndexExclusive) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    return this.firstIndex + index;
  }

  /**
   * Add more Mutations corresponding to the Mutation at the given index to be committed atomically
   * in the same batch. These mutations are applied to the WAL and applied to the memstore as well.
   * The timestamp of the cells in the given Mutations MUST be obtained from the original mutation.
   *
   * @param index the index that corresponds to the original mutation index in the batch
   * @param newOperations the Mutations to add
   */
  public void addOperationsFromCP(int index, Mutation[] newOperations) {
    if (this.operationsFromCoprocessors == null) {
      // lazy allocation to save on object allocation in case this is not used
      this.operationsFromCoprocessors = new Mutation[operations.length][];
    }
    this.operationsFromCoprocessors[getAbsoluteIndex(index)] = newOperations;
  }

  public Mutation[] getOperationsFromCoprocessors(int index) {
    return operationsFromCoprocessors == null ? null :
        operationsFromCoprocessors[getAbsoluteIndex(index)];
  }
}
