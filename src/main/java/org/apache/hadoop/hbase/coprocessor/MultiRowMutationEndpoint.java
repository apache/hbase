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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class demonstrates how to implement atomic multi row transactions using
 * {@link HRegion#mutateRowsWithLocks(java.util.Collection, java.util.Collection)}
 * and Coprocessor endpoints.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MultiRowMutationEndpoint extends BaseEndpointCoprocessor implements
    MultiRowMutationProtocol {

  @Override
  public void mutateRows(List<Mutation> mutations) throws IOException {
    // get the coprocessor environment
    RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();

    // set of rows to lock, sorted to avoid deadlocks
    SortedSet<byte[]> rowsToLock = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

    HRegionInfo regionInfo = env.getRegion().getRegionInfo();
    for (Mutation m : mutations) {
      // check whether rows are in range for this region
      if (!HRegion.rowIsInRange(regionInfo, m.getRow())) {
        String msg = "Requested row out of range '"
            + Bytes.toStringBinary(m.getRow()) + "'";
        if (rowsToLock.isEmpty()) {
          // if this is the first row, region might have moved,
          // allow client to retry
          throw new WrongRegionException(msg);
        } else {
          // rows are split between regions, do not retry
          throw new DoNotRetryIOException(msg);
        }
      }
      rowsToLock.add(m.getRow());
    }
    // call utility method on region
    env.getRegion().mutateRowsWithLocks(mutations, rowsToLock);
  }
}
