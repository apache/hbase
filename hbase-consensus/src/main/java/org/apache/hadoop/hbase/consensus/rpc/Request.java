package org.apache.hadoop.hbase.consensus.rpc;

import com.google.common.util.concurrent.ListenableFuture;

public abstract class Request<T> {
  public abstract void setResponse(final T response);
  public abstract ListenableFuture<T> getResponse();
}
