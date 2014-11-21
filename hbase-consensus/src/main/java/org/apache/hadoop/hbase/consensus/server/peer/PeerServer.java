package org.apache.hadoop.hbase.consensus.server.peer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.consensus.rpc.AppendRequest;
import org.apache.hadoop.hbase.consensus.rpc.VoteRequest;
import org.apache.hadoop.hbase.regionserver.RaftEventListener;

public interface PeerServer extends PeerServerMutableContext {
  public void sendAppendEntries(AppendRequest request);
  public void sendRequestVote(VoteRequest request);
  public int getRank();
  public void setRank(int rank);
  public String getPeerServerName();
  public Configuration getConf();
  public void initialize();
  public void stop();
  public void registerDataStoreEventListener(RaftEventListener listener);
}
