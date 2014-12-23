package org.apache.hadoop.hbase.consensus.rpc;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public final class VoteResponse {
  final String address;
  final long term;
  final VoteResult voteResult;

  @ThriftStruct
  public enum VoteResult {
    SUCCESS,
    FAILURE,
    WRONGQUORUM
  };

  @ThriftConstructor
  public VoteResponse(
      @ThriftField(1) final String address,
      @ThriftField(2) final long term,
      @ThriftField(3) final VoteResult voteResult) {
    this.address = address;
    this.term = term;
    this.voteResult = voteResult;
  }

  @ThriftField(1)
  public String getAddress() {
    return address;
  }

  @ThriftField(2)
  public long getTerm() {
    return term;
  }

  @ThriftField(3)
  public VoteResult voteResult() {
    return voteResult;
  }

  public boolean isSuccess() {
    return voteResult.equals(VoteResult.SUCCESS);
  }

  public boolean isWrongQuorum() {
    return voteResult.equals(VoteResult.WRONGQUORUM);
  }

  @Override
  public String toString() {
    return "VoteResponse{" +
        "address=" + address +
        " term=" + term +
        ", voteResult=" + voteResult +
        '}';
  }
}
