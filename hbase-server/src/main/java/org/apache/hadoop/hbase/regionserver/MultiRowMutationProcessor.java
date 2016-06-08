/*
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MultiRowMutationProcessorRequest;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MultiRowMutationProcessorResponse;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A <code>MultiRowProcessor</code> that performs multiple puts and deletes.
 */
@InterfaceAudience.Private
class MultiRowMutationProcessor extends BaseRowProcessor<MultiRowMutationProcessorRequest,
MultiRowMutationProcessorResponse> {
  Collection<byte[]> rowsToLock;
  Collection<Mutation> mutations;
  MiniBatchOperationInProgress<Mutation> miniBatch;

  MultiRowMutationProcessor(Collection<Mutation> mutations,
                            Collection<byte[]> rowsToLock) {
    this.rowsToLock = rowsToLock;
    this.mutations = mutations;
  }

  @Override
  public Collection<byte[]> getRowsToLock() {
    return rowsToLock;
  }

  @Override
  public boolean readOnly() {
    return false;
  }

  @Override
  public MultiRowMutationProcessorResponse getResult() {
    return MultiRowMutationProcessorResponse.getDefaultInstance();
  }

  @Override
  public void process(long now,
                      HRegion region,
                      List<Mutation> mutationsToApply,
                      WALEdit walEdit) throws IOException {
    byte[] byteNow = Bytes.toBytes(now);
    // Check mutations
    for (Mutation m : this.mutations) {
      if (m instanceof Put) {
        Map<byte[], List<Cell>> familyMap = m.getFamilyCellMap();
        region.checkFamilies(familyMap.keySet());
        region.checkTimestamps(familyMap, now);
        region.updateCellTimestamps(familyMap.values(), byteNow);
      } else if (m instanceof Delete) {
        Delete d = (Delete) m;
        region.prepareDelete(d);
        region.prepareDeleteTimestamps(d, d.getFamilyCellMap(), byteNow);
      } else {
        throw new DoNotRetryIOException("Action must be Put or Delete. But was: "
            + m.getClass().getName());
      }
      mutationsToApply.add(m);
    }
    // Apply edits to a single WALEdit
    for (Mutation m : mutations) {
      for (List<Cell> cells : m.getFamilyCellMap().values()) {
        boolean writeToWAL = m.getDurability() != Durability.SKIP_WAL;
        for (Cell cell : cells) {
          if (writeToWAL) walEdit.add(cell);
        }
      }
    }
  }

  @Override
  public void preProcess(HRegion region, WALEdit walEdit) throws IOException {
    RegionCoprocessorHost coprocessorHost = region.getCoprocessorHost();
    if (coprocessorHost != null) {
      for (Mutation m : mutations) {
        if (m instanceof Put) {
          if (coprocessorHost.prePut((Put) m, walEdit, m.getDurability())) {
            // by pass everything
            return;
          }
        } else if (m instanceof Delete) {
          Delete d = (Delete) m;
          region.prepareDelete(d);
          if (coprocessorHost.preDelete(d, walEdit, d.getDurability())) {
            // by pass everything
            return;
          }
        }
      }
    }
  }

  @Override
  public void preBatchMutate(HRegion region, WALEdit walEdit) throws IOException {
    // TODO we should return back the status of this hook run to HRegion so that those Mutations
    // with OperationStatus as SUCCESS or FAILURE should not get applied to memstore.
    RegionCoprocessorHost coprocessorHost = region.getCoprocessorHost();
    OperationStatus[] opStatus = new OperationStatus[mutations.size()];
    Arrays.fill(opStatus, OperationStatus.NOT_RUN);
    WALEdit[] walEditsFromCP = new WALEdit[mutations.size()];
    if (coprocessorHost != null) {
      miniBatch = new MiniBatchOperationInProgress<Mutation>(
          mutations.toArray(new Mutation[mutations.size()]), opStatus, walEditsFromCP, 0,
          mutations.size());
      coprocessorHost.preBatchMutate(miniBatch);
    }
    // Apply edits to a single WALEdit
    for (int i = 0; i < mutations.size(); i++) {
      if (opStatus[i] == OperationStatus.NOT_RUN) {
        // Other OperationStatusCode means that Mutation is already succeeded or failed in CP hook
        // itself. No need to apply again to region
        if (walEditsFromCP[i] != null) {
          // Add the WALEdit created by CP hook
          for (Cell walCell : walEditsFromCP[i].getCells()) {
            walEdit.add(walCell);
          }
        }
      }
    }
  }

  @Override
  public void postBatchMutate(HRegion region) throws IOException {
    RegionCoprocessorHost coprocessorHost = region.getCoprocessorHost();
    if (coprocessorHost != null) {
      assert miniBatch != null;
      // Use the same miniBatch state used to call the preBatchMutate()
      coprocessorHost.postBatchMutate(miniBatch);
    }
  }

  @Override
  public void postProcess(HRegion region, WALEdit walEdit, boolean success) throws IOException {
    RegionCoprocessorHost coprocessorHost = region.getCoprocessorHost();
    if (coprocessorHost != null) {
      for (Mutation m : mutations) {
        if (m instanceof Put) {
          coprocessorHost.postPut((Put) m, walEdit, m.getDurability());
        } else if (m instanceof Delete) {
          coprocessorHost.postDelete((Delete) m, walEdit, m.getDurability());
        }
      }
      // At the end call the CP hook postBatchMutateIndispensably
      if (miniBatch != null) {
        // Directly calling this hook, with out calling pre/postBatchMutate() when Processor do a
        // read only process. Then no need to call this batch based CP hook also.
        coprocessorHost.postBatchMutateIndispensably(miniBatch, success);
      }
    }
  }

  @Override
  public MultiRowMutationProcessorRequest getRequestData() {
    return MultiRowMutationProcessorRequest.getDefaultInstance();
  }

  @Override
  public void initialize(MultiRowMutationProcessorRequest msg) {
    //nothing
  }

  @Override
  public Durability useDurability() {
    // return true when at least one mutation requested a WAL flush (default)
    Durability durability = Durability.USE_DEFAULT;
    for (Mutation m : mutations) {
      if (m.getDurability().ordinal() > durability.ordinal()) {
        durability = m.getDurability();
      }
    }
    return durability;
  }
}
