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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MultiRowMutationService;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsResponse;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * This class implements atomic multi row transactions using
 * {@link HRegion#mutateRowsWithLocks(Collection, Collection, long, long)} and Coprocessor
 * endpoints. We can also specify some conditions to perform conditional update.
 *
 * Defines a protocol to perform multi row transactions.
 * See {@link MultiRowMutationEndpoint} for the implementation.
 * <br>
 * See
 * {@link HRegion#mutateRowsWithLocks(Collection, Collection, long, long)}
 * for details and limitations.
 * <br>
 * Example:
 * <code>
 * Put p = new Put(row1);
 * Delete d = new Delete(row2);
 * Increment i = new Increment(row3);
 * Append a = new Append(row4);
 * ...
 * Mutate m1 = ProtobufUtil.toMutate(MutateType.PUT, p);
 * Mutate m2 = ProtobufUtil.toMutate(MutateType.DELETE, d);
 * Mutate m3 = ProtobufUtil.toMutate(MutateType.INCREMENT, i);
 * Mutate m4 = ProtobufUtil.toMutate(MutateType.Append, a);
 *
 * MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
 * mrmBuilder.addMutationRequest(m1);
 * mrmBuilder.addMutationRequest(m2);
 * mrmBuilder.addMutationRequest(m3);
 * mrmBuilder.addMutationRequest(m4);
 *
 * // We can also specify conditions to preform conditional update
 * mrmBuilder.addCondition(ProtobufUtil.toCondition(row, FAMILY, QUALIFIER,
 *  CompareOperator.EQUAL, value, TimeRange.allTime()));
 *
 * CoprocessorRpcChannel channel = t.coprocessorService(ROW);
 * MultiRowMutationService.BlockingInterface service =
 *   MultiRowMutationService.newBlockingStub(channel);
 * MutateRowsRequest mrm = mrmBuilder.build();
 * MutateRowsResponse response = service.mutateRows(null, mrm);
 *
 * // We can get the result of the conditional update
 * boolean processed = response.getProcessed();
 * </code>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public class MultiRowMutationEndpoint extends MultiRowMutationService implements RegionCoprocessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(HRegion.class);

  private RegionCoprocessorEnvironment env;

  @Override
  public void mutateRows(RpcController controller, MutateRowsRequest request,
      RpcCallback<MutateRowsResponse> done) {
    boolean matches = true;
    List<Region.RowLock> rowLocks = null;
    try {
      // set of rows to lock, sorted to avoid deadlocks
      SortedSet<byte[]> rowsToLock = new TreeSet<>(Bytes.BYTES_COMPARATOR);
      List<MutationProto> mutateRequestList = request.getMutationRequestList();
      List<Mutation> mutations = new ArrayList<>(mutateRequestList.size());
      for (MutationProto m : mutateRequestList) {
        mutations.add(ProtobufUtil.toMutation(m));
      }

      Region region = env.getRegion();

      RegionInfo regionInfo = region.getRegionInfo();
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

      if (request.getConditionCount() > 0) {
        // Get row locks for the mutations and the conditions
        rowLocks = new ArrayList<>();
        for (ClientProtos.Condition condition : request.getConditionList()) {
          rowsToLock.add(condition.getRow().toByteArray());
        }
        for (byte[] row : rowsToLock) {
          try {
            Region.RowLock rowLock = region.getRowLock(row, false); // write lock
            rowLocks.add(rowLock);
          } catch (IOException ioe) {
            LOGGER.warn("Failed getting lock, row={}, in region {}", Bytes.toStringBinary(row),
              this, ioe);
            throw ioe;
          }
        }

        // Check if all the conditions match
        for (ClientProtos.Condition condition : request.getConditionList()) {
          if (!matches(region, condition)) {
            matches = false;
            break;
          }
        }
      }

      if (matches) {
        // call utility method on region
        long nonceGroup = request.hasNonceGroup() ? request.getNonceGroup() : HConstants.NO_NONCE;
        long nonce = request.hasNonce() ? request.getNonce() : HConstants.NO_NONCE;
        region.mutateRowsWithLocks(mutations, rowsToLock, nonceGroup, nonce);
      }
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    } finally {
      if (rowLocks != null) {
        // Release the acquired row locks
        for (Region.RowLock rowLock : rowLocks) {
          rowLock.release();
        }
      }
    }
    done.run(MutateRowsResponse.newBuilder().setProcessed(matches).build());
  }

  private boolean matches(Region region, ClientProtos.Condition condition) throws IOException {
    byte[] row = condition.getRow().toByteArray();

    Filter filter = null;
    byte[] family = null;
    byte[] qualifier = null;
    CompareOperator op = null;
    ByteArrayComparable comparator = null;

    if (condition.hasFilter()) {
      filter = ProtobufUtil.toFilter(condition.getFilter());
    } else {
      family = condition.getFamily().toByteArray();
      qualifier = condition.getQualifier().toByteArray();
      op = CompareOperator.valueOf(condition.getCompareType().name());
      comparator = ProtobufUtil.toComparator(condition.getComparator());
    }

    TimeRange timeRange = condition.hasTimeRange() ?
      ProtobufUtil.toTimeRange(condition.getTimeRange()) : TimeRange.allTime();

    Get get = new Get(row);
    if (family != null) {
      checkFamily(region, family);
      get.addColumn(family, qualifier);
    }
    if (filter != null) {
      get.setFilter(filter);
    }
    if (timeRange != null) {
      get.setTimeRange(timeRange.getMin(), timeRange.getMax());
    }

    boolean matches = false;
    try (RegionScanner scanner = region.getScanner(new Scan(get))) {
      // NOTE: Please don't use HRegion.get() instead,
      // because it will copy cells to heap. See HBASE-26036
      List<Cell> result = new ArrayList<>();
      scanner.next(result);
      if (filter != null) {
        if (!result.isEmpty()) {
          matches = true;
        }
      } else {
        boolean valueIsNull = comparator.getValue() == null || comparator.getValue().length == 0;
        if (result.isEmpty() && valueIsNull) {
          matches = true;
        } else if (result.size() > 0 && result.get(0).getValueLength() == 0 && valueIsNull) {
          matches = true;
        } else if (result.size() == 1 && !valueIsNull) {
          Cell kv = result.get(0);
          int compareResult = PrivateCellUtil.compareValue(kv, comparator);
          matches = matches(op, compareResult);
        }
      }
    }
    return matches;
  }

  private void checkFamily(Region region, byte[] family) throws NoSuchColumnFamilyException {
    if (!region.getTableDescriptor().hasColumnFamily(family)) {
      throw new NoSuchColumnFamilyException(
        "Column family " + Bytes.toString(family) + " does not exist in region " + this
          + " in table " + region.getTableDescriptor());
    }
  }

  private boolean matches(CompareOperator op, int compareResult) {
    switch (op) {
      case LESS:
        return compareResult < 0;
      case LESS_OR_EQUAL:
        return compareResult <= 0;
      case EQUAL:
        return compareResult == 0;
      case NOT_EQUAL:
        return compareResult != 0;
      case GREATER_OR_EQUAL:
        return compareResult >= 0;
      case GREATER:
        return compareResult > 0;
      default:
        throw new RuntimeException("Unknown Compare op " + op.name());
    }
  }

  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(this);
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
