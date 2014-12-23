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
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.consensus.protocol.EditId;

@ThriftStruct
public final class AppendResponse {

  public enum Result
  {
    SUCCESS, HIGHER_TERM, LAGGING, MISSING_EDITS
  }

  private final String address;

  /** The identifier to associate the AppendRequest and AppendResponse. */
  private final EditId id;

  private final EditId prevEditID;

  private final Result result;

  private final int rank;

  private final boolean canTakeover;

  @ThriftConstructor
  public AppendResponse (String address,
                         final EditId id,
                         final EditId prevEditID,
                         Result result,
                         int rank,
                         boolean canTakeover) {
    this.address = address;
    this.id = id;
    this.prevEditID = prevEditID;
    this.result = result;
    this.rank = rank;
    this.canTakeover = canTakeover;
  }

  @ThriftField(1)
  public String getAddress() {
    return address;
  }

  /**
   *
   * @return the identifier to associate the AppendRequest and AppendResponse.
   */
  @ThriftField(2)
  public EditId getId() {
    return id;
  }

  @ThriftField(3)
  public EditId getPrevEditID() {
    return prevEditID;
  }

  @ThriftField(4)
  public Result getResult() {
    return result;
  }

  @ThriftField(5)
  public int getRank() {
    return rank;
  }

  @ThriftField(6)
  public boolean canTakeover() {
    return canTakeover;
  }

  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();

    s.append("AppendResponse{")
      .append("address= ").append(address)
      .append(", id=").append(id)
      .append(", prevEditID=").append(prevEditID)
      .append(", result=").append(result)
      .append(", rank=").append(rank)
      .append(", canTakeOver=").append(canTakeover)
      .append('}');

    return s.toString();
  }
}
