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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AggregateProtos.AggregateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AggregateProtos.AggregateResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AggregateProtos.AggregateService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

@InterfaceAudience.Private
public class MockPartialResultAggregateImplemention<T, S, P extends Message, Q extends Message,
  R extends Message> extends AggregateService implements RegionCoprocessor {
  private RegionCoprocessorEnvironment env;
  private ConcurrentMap<String, Boolean> seenRegion;

  /**
   * Mocks the behavior of getMax() in the real AggregateImplementation. Makes some fake data to
   * send back some partial results, and then finally a complete result.
   */
  @Override
  public void getMax(RpcController controller, AggregateRequest request,
    RpcCallback<AggregateResponse> done) {
    AggregateResponse.Builder result = AggregateResponse.newBuilder();

    int max;
    boolean seenRegionBefore = seenRegion.put(env.getRegionInfo().getEncodedName(), true) != null;
    if (seenRegionBefore) {
      max = 10;
    } else {
      max = 2;
      seenRegion.put(env.getRegionInfo().getEncodedName(), true);
    }
    HBaseProtos.LongMsg longMsg = HBaseProtos.LongMsg.newBuilder().setLongMsg(max).build();
    result.addFirstPart(longMsg.toByteString());
    if (!seenRegionBefore) {
      result.setNextChunkStartRow(ByteString.copyFrom(env.getRegionInfo().getStartKey()));
    }
    done.run(result.build());
  }

  /**
   * Mocks the behavior of getMin() in the real AggregateImplementation. Makes some fake data to
   * send back some partial results, and then finally a complete result.
   */
  @Override
  public void getMin(RpcController controller, AggregateRequest request,
    RpcCallback<AggregateResponse> done) {
    AggregateResponse.Builder result = AggregateResponse.newBuilder();
    int min;
    boolean seenRegionBefore = seenRegion.put(env.getRegionInfo().getEncodedName(), true) != null;
    if (seenRegionBefore) {
      min = 10;
    } else {
      min = 2;
      seenRegion.put(env.getRegionInfo().getEncodedName(), true);
    }
    HBaseProtos.LongMsg longMsg = HBaseProtos.LongMsg.newBuilder().setLongMsg(min).build();
    result.addFirstPart(longMsg.toByteString());
    if (!seenRegionBefore) {
      result.setNextChunkStartRow(ByteString.copyFrom(env.getRegionInfo().getStartKey()));
    }
    done.run(result.build());
  }

  @Override
  public void getSum(RpcController controller, AggregateRequest request,
    RpcCallback<AggregateResponse> done) {

  }

  @Override
  public void getRowNum(RpcController controller, AggregateRequest request,
    RpcCallback<AggregateResponse> done) {

  }

  @Override
  public void getAvg(RpcController controller, AggregateRequest request,
    RpcCallback<AggregateResponse> done) {

  }

  @Override
  public void getStd(RpcController controller, AggregateRequest request,
    RpcCallback<AggregateResponse> done) {

  }

  @Override
  public void getMedian(RpcController controller, AggregateRequest request,
    RpcCallback<AggregateResponse> done) {

  }

  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(this);
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    this.env = (RegionCoprocessorEnvironment) env;
    this.seenRegion = new ConcurrentHashMap<>();
  }

}
