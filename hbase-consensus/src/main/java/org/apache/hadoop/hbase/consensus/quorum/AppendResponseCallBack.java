package org.apache.hadoop.hbase.consensus.quorum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.hbase.consensus.rpc.AppendResponse;

public class AppendResponseCallBack implements FutureCallback<AppendResponse>{
  private static Logger LOG = LoggerFactory.getLogger(AppendResponseCallBack.class);
  private ConsensusSession session;

  public AppendResponseCallBack(ConsensusSession session) {
    this.session = session;
  }

  public void onSuccess(AppendResponse response) {

  }

  @Override
  public void onFailure(Throwable arg0) {
  }
}


