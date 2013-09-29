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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MultiRowMutationProcessorRequest;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MultiRowMutationProcessorResponse;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A <code>MultiRowProcessor</code> that performs multiple puts and deletes.
 */
class MultiRowMutationProcessor extends BaseRowProcessor<MultiRowMutationProcessorRequest,
MultiRowMutationProcessorResponse> {
  Collection<byte[]> rowsToLock;
  Collection<Mutation> mutations;

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
                      List<KeyValue> mutationKvs,
                      WALEdit walEdit) throws IOException {
    byte[] byteNow = Bytes.toBytes(now);
    // Check mutations and apply edits to a single WALEdit
    for (Mutation m : mutations) {
      if (m instanceof Put) {
        Map<byte[], List<Cell>> familyMap = m.getFamilyCellMap();
        region.checkFamilies(familyMap.keySet());
        region.checkTimestamps(familyMap, now);
        region.updateKVTimestamps(familyMap.values(), byteNow);
      } else if (m instanceof Delete) {
        Delete d = (Delete) m;
        region.prepareDelete(d);
        region.prepareDeleteTimestamps(d.getFamilyCellMap(), byteNow);
      } else {
        throw new DoNotRetryIOException(
            "Action must be Put or Delete. But was: "
            + m.getClass().getName());
      }
      for (List<Cell> cells: m.getFamilyCellMap().values()) {
        boolean writeToWAL = m.getDurability() != Durability.SKIP_WAL;
        for (Cell cell : cells) {
          KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
          mutationKvs.add(kv);
          if (writeToWAL) {
            walEdit.add(kv);
          }
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
  public void postProcess(HRegion region, WALEdit walEdit) throws IOException {
    RegionCoprocessorHost coprocessorHost = region.getCoprocessorHost();
    if (coprocessorHost != null) {
      for (Mutation m : mutations) {
        if (m instanceof Put) {
          coprocessorHost.postPut((Put) m, walEdit, m.getDurability());
        } else if (m instanceof Delete) {
          coprocessorHost.postDelete((Delete) m, walEdit, m.getDurability());
        }
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
