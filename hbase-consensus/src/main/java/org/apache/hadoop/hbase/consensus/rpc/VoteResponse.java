package org.apache.hadoop.hbase.consensus.rpc;

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
