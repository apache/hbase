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
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MultiRowMutationService;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * This class demonstrates how to implement atomic multi row transactions using
 * {@link HRegion#mutateRowsWithLocks(java.util.Collection, java.util.Collection)}
 * and Coprocessor endpoints.
 *
 * Defines a protocol to perform multi row transactions.
 * See {@link MultiRowMutationEndpoint} for the implementation.
 * <br>
 * See
 * {@link HRegion#mutateRowsWithLocks(java.util.Collection, java.util.Collection)}
 * for details and limitations.
 * <br>
 * Example:
 * <code>
 * List&lt;Mutation&gt; mutations = ...;
 * Put p1 = new Put(row1);
 * Put p2 = new Put(row2);
 * ...
 * Mutate m1 = ProtobufUtil.toMutate(MutateType.PUT, p1);
 * Mutate m2 = ProtobufUtil.toMutate(MutateType.PUT, p2);
 * MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
 * mrmBuilder.addMutationRequest(m1);
 * mrmBuilder.addMutationRequest(m2);
 * CoprocessorRpcChannel channel = t.coprocessorService(ROW);
 * MultiRowMutationService.BlockingInterface service =
 *    MultiRowMutationService.newBlockingStub(channel);
 * MutateRowsRequest mrm = mrmBuilder.build();
 * service.mutateRows(null, mrm);
 * </code>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public class MultiRowMutationEndpoint extends MultiRowMutationService implements
CoprocessorService, Coprocessor {
  private RegionCoprocessorEnvironment env;
  @Override
  public void mutateRows(RpcController controller, MutateRowsRequest request,
      RpcCallback<MutateRowsResponse> done) {
    MutateRowsResponse response = MutateRowsResponse.getDefaultInstance();
    try {
      // set of rows to lock, sorted to avoid deadlocks
      SortedSet<byte[]> rowsToLock = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
      List<MutationProto> mutateRequestList = request.getMutationRequestList();
      List<Mutation> mutations = new ArrayList<Mutation>(mutateRequestList.size());
      for (MutationProto m : mutateRequestList) {
        mutations.add(ProtobufUtil.toMutation(m));
      }

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
            throw new org.apache.hadoop.hbase.DoNotRetryIOException(msg);
          }
        }
        rowsToLock.add(m.getRow());
      }
      // call utility method on region
      long nonceGroup = request.hasNonceGroup() ? request.getNonceGroup() : HConstants.NO_NONCE;
      long nonce = request.hasNonce() ? request.getNonce() : HConstants.NO_NONCE;
      env.getRegion().mutateRowsWithLocks(mutations, rowsToLock, nonceGroup, nonce);
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }


  @Override
  public Service getService() {
    return this;
  }

  /**
   * Stores a reference to the coprocessor environment provided by the
   * {@link org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost} from the region where this
   * coprocessor is loaded.  Since this is a coprocessor endpoint, it always expects to be loaded
   * on a table region, so always expects this to be an instance of
   * {@link RegionCoprocessorEnvironment}.
   * @param env the environment provided by the coprocessor host
   * @throws IOException if the provided environment is not an instance of
   * {@code RegionCoprocessorEnvironment}
   */
  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment)env;
    } else {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    // nothing to do
  }
}
