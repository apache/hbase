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

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A <code>MultiRowProcessor</code> that performs multiple puts and deletes.
 */
class MultiRowMutationProcessor extends BaseRowProcessor<Void> {
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
  public void process(long now,
                      HRegion region,
                      List<KeyValue> mutationKvs,
                      WALEdit walEdit) throws IOException {
    byte[] byteNow = Bytes.toBytes(now);
    // Check mutations and apply edits to a single WALEdit
    for (Mutation m : mutations) {
      if (m instanceof Put) {
        Map<byte[], List<KeyValue>> familyMap = m.getFamilyMap();
        region.checkFamilies(familyMap.keySet());
        region.checkTimestamps(familyMap, now);
        region.updateKVTimestamps(familyMap.values(), byteNow);
      } else if (m instanceof Delete) {
        Delete d = (Delete) m;
        region.prepareDelete(d);
        region.prepareDeleteTimestamps(d, byteNow);
      } else {
        throw new DoNotRetryIOException(
            "Action must be Put or Delete. But was: "
            + m.getClass().getName());
      }
      for (List<KeyValue> edits : m.getFamilyMap().values()) {
        boolean writeToWAL = m.getWriteToWAL();
        for (KeyValue kv : edits) {
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
          if (coprocessorHost.prePut((Put) m, walEdit, m.getWriteToWAL())) {
            // by pass everything
            return;
          }
        } else if (m instanceof Delete) {
          Delete d = (Delete) m;
          region.prepareDelete(d);
          if (coprocessorHost.preDelete(d, walEdit, d.getWriteToWAL())) {
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
          coprocessorHost.postPut((Put) m, walEdit, m.getWriteToWAL());
        } else if (m instanceof Delete) {
          coprocessorHost.postDelete((Delete) m, walEdit, m.getWriteToWAL());
        }
      }
    }
  }

}
