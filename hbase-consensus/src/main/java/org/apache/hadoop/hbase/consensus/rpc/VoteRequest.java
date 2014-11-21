package org.apache.hadoop.hbase.consensus.rpc;

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
