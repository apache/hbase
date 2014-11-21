package org.apache.hadoop.hbase.consensus.quorum;

import com.google.common.util.concurrent.SettableFuture;

public class ReseedRequest {

  private SettableFuture<Boolean> result;

  private long reseedIndex;

  public ReseedRequest(long reseedIndex) {
    this.reseedIndex = reseedIndex;
    result = SettableFuture.create();
  }

  public void setResponse(Boolean response) {
    result.set(response);
  }

  public SettableFuture<Boolean> getResult() {
    return result;
  }

  public long getReseedIndex() {
    return reseedIndex;
  }
}
