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


import javax.annotation.concurrent.Immutable;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.hbase.consensus.protocol.EditId;

@Immutable
@ThriftStruct
public final class VoteRequest extends Request<VoteResponse> {

  private final String regionId;
  private final String address;
  private final long term;
  private final EditId prevEditID;

  private SettableFuture<VoteResponse> response;

  @ThriftConstructor
  public VoteRequest(
      @ThriftField(1)final String regionId,
      @ThriftField(2)final String address,
      @ThriftField(3)final long term,
      @ThriftField(4)final EditId prevEditID) {
    this.regionId = regionId;
    this.address = address;
    this.term = term;
    this.prevEditID = prevEditID;
  }

  public VoteRequest(final VoteRequest r) {
    this.regionId = r.regionId;
    this.address = r.address;
    this.term = r.term;
    this.prevEditID = r.prevEditID;
  }

  @ThriftField(1)
  public String getRegionId() {
    return regionId;
  }

  @ThriftField(2)
  public String getAddress() {
    return address;
  }

  @ThriftField(3)
  public long getTerm() {
    return term;
  }

  @ThriftField(4)
  public EditId getPrevEditID() {
    return prevEditID;
  }
  
  @Override
  public String toString() {
    return "VoteRequest{" +
        "region=" + regionId +
        ", address='" + address + '\'' +
        ", term=" + term +
        ", prevEditID=" + prevEditID +
        '}';
  }

  public void createVoteResponse() {
    if (response == null) {
      response = SettableFuture.create();
    }
  }

  public void setResponse(VoteResponse r) {
    if (response != null) {
      response.set(r);
    }
  }

  @Override
  public ListenableFuture<VoteResponse> getResponse() {
    return response;
  }

  public void setError(final Throwable exception) {
    if (response != null) {
      response.setException(exception);
    }
  }
}
